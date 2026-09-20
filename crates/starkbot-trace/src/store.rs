//! Where a trace stops being a stream and becomes something you can ask
//! questions of.
//!
//! The collector receives records once and reads them many times — `tail`
//! every few hundred milliseconds, `stats` over a whole day, a dashboard
//! refreshing four panels at a time. So every question a report asks is
//! answered by SQLite over an index, never by pulling rows into Rust and
//! filtering there. The flat columns below exist for exactly that reason:
//! `kind`, `duration_ms`, `ok`, `model`, `operation`, `surface` and the token
//! counts are lifted out of the record body at ingest, once, so `stats` and
//! `turns` are index scans instead of a `json_extract` over every row in the
//! table. The full body is still stored verbatim, because a trace that has
//! been summarised into columns is no longer a trace.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow};
use neo_trace::{Body, Record};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use rusqlite_migration::{M, Migrations};
use serde::Serialize;

/// `TRC1`. A trace database and a Neo database both live under Application
/// Support and both end in `.db`; the magic number is what stops
/// `starkbot-trace --db` pointed at the wrong file from migrating it.
const APPLICATION_ID: i64 = 0x5452_4331;
const SCHEMA_VERSION: i64 = 1;

/// How much of a free-text field survives into a one-line label. Long enough
/// for a goal or an answer's first sentence, short enough that a terminal row
/// stays a row.
const LABEL_CHARS: usize = 140;

/// Every `by_*` breakdown in [`Stats`]. A trace has a handful of operations
/// and models; anything past a dozen is noise in a summary.
const BREAKDOWN_LIMIT: usize = 12;

/// The columns every row query selects, in the order [`read_row`] expects.
const ROW_COLUMNS: &str =
    "id, seq, ts_ms, run, source, pid, turn, kind, label, duration_ms, ok, body";

fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(include_str!("../migrations/0001_records.sql"))])
}

/// One record as a report sees it: the flat columns it sorts and colours by,
/// plus the original body for anything it wants to dig into.
#[derive(Clone, Debug, Serialize)]
pub struct Row {
    pub id: i64,
    pub seq: u64,
    pub ts_ms: i64,
    pub run: String,
    pub source: String,
    pub pid: u32,
    pub turn: Option<String>,
    pub kind: String,
    pub label: String,
    pub duration_ms: Option<u64>,
    pub ok: Option<bool>,
    pub body: serde_json::Value,
}

/// Fields combine with AND; an empty filter matches everything.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub run: Option<String>,
    pub turn: Option<String>,
    pub kinds: Vec<String>,
    pub since_ms: Option<i64>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TurnSummary {
    pub turn: String,
    pub run: String,
    pub source: String,
    pub started_ms: i64,
    pub ended_ms: i64,
    pub steps: u64,
    pub inferences: u64,
    pub jev_steps: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub failed: bool,
    pub text: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunSummary {
    pub run: String,
    pub source: String,
    pub pid: u32,
    pub started_ms: i64,
    pub last_ms: i64,
    pub records: u64,
    pub dropped: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Percentiles {
    pub count: u64,
    pub p50: u64,
    pub p95: u64,
    pub max: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Stats {
    pub records: u64,
    pub runs: u64,
    pub turns: u64,
    pub steps: u64,
    pub jev_steps: u64,
    pub inferences: u64,
    pub failures: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub inference_ms: Percentiles,
    pub jev_ms: Percentiles,
    pub step_ms: Percentiles,
    pub by_kind: Vec<(String, u64)>,
    pub by_operation: Vec<(String, u64)>,
    pub by_model: Vec<(String, u64)>,
}

/// The collector's database.
///
/// One connection behind a mutex rather than a pool: the writer is a single
/// socket reader and the readers are one CLI invocation at a time, so a pool
/// would buy contention handling nobody needs and cost a second WAL reader.
pub struct Trace {
    connection: Mutex<Connection>,
}

impl Trace {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("could not create {}", parent.display()))?;
        }
        let existed = path.exists() && std::fs::metadata(path).is_ok_and(|meta| meta.len() > 0);
        let mut connection =
            Connection::open(path).with_context(|| format!("could not open {}", path.display()))?;

        // First, always: switching to WAL takes an exclusive lock, and a
        // reporting command can open the file while the collector is mid
        // write. Without a busy timeout already in force one of them loses
        // that race with SQLITE_BUSY.
        connection.pragma_update(None, "busy_timeout", 5_000_i64)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "temp_store", "MEMORY")?;

        if !existed {
            connection.pragma_update(None, "application_id", APPLICATION_ID)?;
        }
        let application_id: i64 =
            connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        if application_id != 0 && application_id != APPLICATION_ID {
            return Err(anyhow!(
                "{} is not a Starkbot trace database (application_id {application_id:#x})",
                path.display()
            ));
        }
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(anyhow!(
                "{} was written by a newer starkbot-trace (schema {version}, this build speaks {SCHEMA_VERSION})",
                path.display()
            ));
        }
        connection.pragma_update(None, "application_id", APPLICATION_ID)?;
        migrations()
            .to_latest(&mut connection)
            .map_err(|error| anyhow!("could not migrate the trace database: {error}"))?;

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn default_path() -> Result<PathBuf> {
        if let Some(path) = std::env::var_os("STARKBOT_TRACE_DB") {
            return Ok(PathBuf::from(path));
        }
        let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
        Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("com.starkbot.trace")
            .join("trace.db"))
    }

    /// A poisoned mutex means an earlier call panicked partway through a
    /// statement. rusqlite leaves the connection usable, and a collector that
    /// refused every later record because one report formatter panicked would
    /// lose the trace it exists to keep.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Store one record and return its rowid.
    ///
    /// The run counters and the record land in one transaction, so a crash
    /// between them can never leave `runs.records` describing rows that are
    /// not there.
    pub fn insert(&self, record: &Record) -> Result<i64> {
        let flat = Flat::from(&record.body);
        let body = serde_json::to_string(&record.body)?;
        let mut guard = self.lock();
        let transaction = guard.transaction()?;

        // `started_ms` takes the earliest timestamp rather than the first one
        // inserted: a reconnecting producer replays nothing, but two sockets
        // for one run can still interleave.
        transaction.execute(
            "INSERT INTO runs (run, source, pid, started_ms, last_ms, records, dropped) \
             VALUES (?, ?, ?, ?, ?, 1, ?) \
             ON CONFLICT(run) DO UPDATE SET \
               source = excluded.source, \
               pid = excluded.pid, \
               started_ms = MIN(runs.started_ms, excluded.started_ms), \
               last_ms = MAX(runs.last_ms, excluded.last_ms), \
               records = runs.records + 1, \
               dropped = MAX(runs.dropped, excluded.dropped)",
            rusqlite::params![
                &record.run,
                &record.source,
                i64::from(record.pid),
                record.ts_ms,
                record.ts_ms,
                as_i64(record.dropped),
            ],
        )?;

        transaction.execute(
            "INSERT INTO records (seq, ts_ms, run, source, pid, turn, kind, label, duration_ms, \
             ok, provider, model, operation, surface, input_tokens, output_tokens, body) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                as_i64(record.seq),
                record.ts_ms,
                &record.run,
                &record.source,
                i64::from(record.pid),
                &record.turn,
                record.body.kind(),
                &flat.label,
                flat.duration_ms.map(as_i64),
                flat.ok,
                &flat.provider,
                &flat.model,
                &flat.operation,
                &flat.surface,
                flat.input_tokens.map(as_i64),
                flat.output_tokens.map(as_i64),
                &body,
            ],
        )?;
        let id = transaction.last_insert_rowid();
        transaction.commit()?;
        Ok(id)
    }

    /// The newest `limit` matching records, oldest first, the way a terminal
    /// reads.
    pub fn rows(&self, filter: &Filter, limit: usize) -> Result<Vec<Row>> {
        let (clause, mut values) = filter.sql();
        values.push(Value::Integer(as_i64(limit as u64)));
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM (SELECT * FROM records {clause} ORDER BY id DESC LIMIT ?) \
             ORDER BY id ASC"
        );
        self.query_rows(&sql, values)
    }

    /// Everything newer than `id`. This is how `tail --follow` and the
    /// dashboard advance without re-reading what they have already drawn.
    pub fn rows_after(&self, id: i64, filter: &Filter, limit: usize) -> Result<Vec<Row>> {
        let (mut clause, mut values) = filter.sql();
        clause = if clause.is_empty() {
            "WHERE id > ?".to_string()
        } else {
            format!("{clause} AND id > ?")
        };
        values.push(Value::Integer(id));
        values.push(Value::Integer(as_i64(limit as u64)));
        let sql = format!("SELECT {ROW_COLUMNS} FROM records {clause} ORDER BY id ASC LIMIT ?");
        self.query_rows(&sql, values)
    }

    /// Every record of one turn.
    ///
    /// A uuid v7 is 36 characters and the reports print the first eight, so a
    /// bare prefix is what anyone actually types after reading `turns`. The
    /// exact match is kept as its own term so the full id still hits the
    /// index instead of degrading to a scan.
    pub fn turn(&self, turn_id: &str) -> Result<Vec<Row>> {
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM records WHERE turn = ? OR turn LIKE ? ORDER BY id ASC"
        );
        self.query_rows(
            &sql,
            vec![
                Value::Text(turn_id.to_string()),
                Value::Text(format!("{turn_id}%")),
            ],
        )
    }

    pub fn turns(&self, limit: usize) -> Result<Vec<TurnSummary>> {
        let guard = self.lock();
        let mut statement = guard.prepare(
            "SELECT turn, \
                    MIN(run), \
                    MIN(source), \
                    MIN(ts_ms), \
                    MAX(ts_ms), \
                    SUM(kind = 'turn_step'), \
                    SUM(kind = 'inference'), \
                    SUM(kind = 'jev_step'), \
                    COALESCE(SUM(input_tokens), 0), \
                    COALESCE(SUM(output_tokens), 0), \
                    MAX(kind = 'turn_failed'), \
                    COALESCE( \
                      MAX(CASE WHEN kind = 'turn_finished' \
                               THEN json_extract(body, '$.text') END), \
                      MAX(CASE WHEN kind = 'turn_failed' THEN label END), \
                      MAX(CASE WHEN kind = 'turn_started' \
                               THEN json_extract(body, '$.user_text') END), \
                      '') \
             FROM records \
             WHERE turn IS NOT NULL \
             GROUP BY turn \
             ORDER BY MAX(ts_ms) DESC \
             LIMIT ?",
        )?;
        let summaries = statement
            .query_map([as_i64(limit as u64)], |row| {
                Ok(TurnSummary {
                    turn: row.get(0)?,
                    run: row.get(1)?,
                    source: row.get(2)?,
                    started_ms: row.get(3)?,
                    ended_ms: row.get(4)?,
                    steps: as_u64(row.get(5)?),
                    inferences: as_u64(row.get(6)?),
                    jev_steps: as_u64(row.get(7)?),
                    input_tokens: as_u64(row.get(8)?),
                    output_tokens: as_u64(row.get(9)?),
                    failed: row.get::<_, i64>(10)? != 0,
                    text: row.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(summaries)
    }

    pub fn runs(&self, limit: usize) -> Result<Vec<RunSummary>> {
        let guard = self.lock();
        let mut statement = guard.prepare(
            "SELECT run, source, pid, started_ms, last_ms, records, dropped \
             FROM runs ORDER BY last_ms DESC LIMIT ?",
        )?;
        let summaries = statement
            .query_map([as_i64(limit as u64)], |row| {
                Ok(RunSummary {
                    run: row.get(0)?,
                    source: row.get(1)?,
                    pid: u32::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                    started_ms: row.get(3)?,
                    last_ms: row.get(4)?,
                    records: as_u64(row.get(5)?),
                    dropped: as_u64(row.get(6)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(summaries)
    }

    pub fn stats(&self, since_ms: Option<i64>) -> Result<Stats> {
        let guard = self.lock();
        let (clause, values) = match since_ms {
            Some(since) => ("WHERE ts_ms >= ?".to_string(), vec![Value::Integer(since)]),
            None => (String::new(), Vec::new()),
        };

        let totals = guard.query_row(
            &format!(
                "SELECT COUNT(*), \
                        COUNT(DISTINCT run), \
                        COUNT(DISTINCT turn), \
                        COALESCE(SUM(kind = 'turn_step'), 0), \
                        COALESCE(SUM(kind = 'jev_step'), 0), \
                        COALESCE(SUM(kind = 'inference'), 0), \
                        COALESCE(SUM(ok = 0), 0), \
                        COALESCE(SUM(input_tokens), 0), \
                        COALESCE(SUM(output_tokens), 0) \
                 FROM records {clause}"
            ),
            params_from_iter(values.iter()),
            |row| {
                Ok((
                    as_u64(row.get(0)?),
                    as_u64(row.get(1)?),
                    as_u64(row.get(2)?),
                    as_u64(row.get(3)?),
                    as_u64(row.get(4)?),
                    as_u64(row.get(5)?),
                    as_u64(row.get(6)?),
                    as_u64(row.get(7)?),
                    as_u64(row.get(8)?),
                ))
            },
        )?;

        Ok(Stats {
            records: totals.0,
            runs: totals.1,
            turns: totals.2,
            steps: totals.3,
            jev_steps: totals.4,
            inferences: totals.5,
            failures: totals.6,
            input_tokens: totals.7,
            output_tokens: totals.8,
            inference_ms: percentiles(&guard, "inference", since_ms)?,
            jev_ms: percentiles(&guard, "jev_step", since_ms)?,
            step_ms: percentiles(&guard, "turn_step_finished", since_ms)?,
            by_kind: breakdown(&guard, "kind", since_ms)?,
            by_operation: breakdown(&guard, "operation", since_ms)?,
            by_model: breakdown(&guard, "model", since_ms)?,
        })
    }

    fn query_rows(&self, sql: &str, values: Vec<Value>) -> Result<Vec<Row>> {
        let guard = self.lock();
        let mut statement = guard.prepare(sql)?;
        let rows = statement
            .query_map(params_from_iter(values.iter()), read_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    let body: String = row.get(11)?;
    Ok(Row {
        id: row.get(0)?,
        seq: as_u64(row.get(1)?),
        ts_ms: row.get(2)?,
        run: row.get(3)?,
        source: row.get(4)?,
        pid: u32::try_from(row.get::<_, i64>(5)?).unwrap_or_default(),
        turn: row.get(6)?,
        kind: row.get(7)?,
        label: row.get(8)?,
        duration_ms: row.get::<_, Option<i64>>(9)?.map(as_u64),
        ok: row.get::<_, Option<i64>>(10)?.map(|value| value != 0),
        // A body that will not parse means the column was written by
        // something other than `insert`; report it as null rather than
        // failing the whole query and hiding every other row.
        body: serde_json::from_str(&body).unwrap_or(serde_json::Value::Null),
    })
}

/// Nearest-rank percentile: sort by the column SQLite already has an ordering
/// for and take the row at the computed offset. Three cheap `LIMIT 1 OFFSET n`
/// queries beat pulling every duration into memory to sort it again, and they
/// keep the crate free of a statistics dependency.
fn percentiles(connection: &Connection, kind: &str, since_ms: Option<i64>) -> Result<Percentiles> {
    let mut clause = "WHERE kind = ? AND duration_ms IS NOT NULL".to_string();
    let mut values = vec![Value::Text(kind.to_string())];
    if let Some(since) = since_ms {
        clause.push_str(" AND ts_ms >= ?");
        values.push(Value::Integer(since));
    }

    let count: u64 = connection.query_row(
        &format!("SELECT COUNT(*) FROM records {clause}"),
        params_from_iter(values.iter()),
        |row| row.get::<_, i64>(0).map(as_u64),
    )?;
    if count == 0 {
        return Ok(Percentiles {
            count: 0,
            p50: 0,
            p95: 0,
            max: 0,
        });
    }

    let at = |offset: u64| -> Result<u64> {
        let mut with_offset = values.clone();
        with_offset.push(Value::Integer(as_i64(offset)));
        let value: i64 = connection.query_row(
            &format!(
                "SELECT duration_ms FROM records {clause} ORDER BY duration_ms ASC LIMIT 1 OFFSET ?"
            ),
            params_from_iter(with_offset.iter()),
            |row| row.get(0),
        )?;
        Ok(as_u64(value))
    };

    Ok(Percentiles {
        count,
        p50: at(rank(count, 50))?,
        p95: at(rank(count, 95))?,
        max: at(count - 1)?,
    })
}

/// The zero-based offset of the nearest-rank `percent` value in `count` sorted
/// samples. `ceil(percent * count / 100) - 1`, clamped into the table.
fn rank(count: u64, percent: u64) -> u64 {
    let ceiling = (count * percent).div_ceil(100).max(1);
    (ceiling - 1).min(count - 1)
}

fn breakdown(
    connection: &Connection,
    column: &str,
    since_ms: Option<i64>,
) -> Result<Vec<(String, u64)>> {
    let mut clause = format!("WHERE {column} IS NOT NULL");
    let mut values = Vec::new();
    if let Some(since) = since_ms {
        clause.push_str(" AND ts_ms >= ?");
        values.push(Value::Integer(since));
    }
    let sql = format!(
        "SELECT {column}, COUNT(*) FROM records {clause} \
         GROUP BY {column} ORDER BY COUNT(*) DESC LIMIT {BREAKDOWN_LIMIT}"
    );
    let mut statement = connection.prepare(&sql)?;
    let pairs = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((row.get::<_, String>(0)?, as_u64(row.get::<_, i64>(1)?)))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(pairs)
}

impl Filter {
    /// The `WHERE` clause and its bound values, using anonymous `?` markers so
    /// callers can keep appending their own (a limit, an id) on the end.
    fn sql(&self) -> (String, Vec<Value>) {
        let mut terms: Vec<String> = Vec::new();
        let mut values: Vec<Value> = Vec::new();
        // `run` and `turn` accept a prefix for the same reason `turn()` does:
        // the ids a report shows are truncated.
        if let Some(run) = &self.run {
            terms.push("(run = ? OR run LIKE ?)".to_string());
            values.push(Value::Text(run.clone()));
            values.push(Value::Text(format!("{run}%")));
        }
        if let Some(turn) = &self.turn {
            terms.push("(turn = ? OR turn LIKE ?)".to_string());
            values.push(Value::Text(turn.clone()));
            values.push(Value::Text(format!("{turn}%")));
        }
        if !self.kinds.is_empty() {
            let markers = vec!["?"; self.kinds.len()].join(", ");
            terms.push(format!("kind IN ({markers})"));
            values.extend(self.kinds.iter().map(|kind| Value::Text(kind.clone())));
        }
        if let Some(since) = self.since_ms {
            terms.push("ts_ms >= ?".to_string());
            values.push(Value::Integer(since));
        }
        if let Some(text) = &self.text {
            // A plain substring match with no LIKE escaping: `%` and `_` in a
            // search term act as wildcards. That is a search box over your own
            // trace, not a query language, and pretending otherwise would cost
            // an ESCAPE clause and a surprise the first time someone greps for
            // a percentage.
            terms.push("(label LIKE ? OR body LIKE ?)".to_string());
            let pattern = format!("%{text}%");
            values.push(Value::Text(pattern.clone()));
            values.push(Value::Text(pattern));
        }
        if terms.is_empty() {
            (String::new(), values)
        } else {
            (format!("WHERE {}", terms.join(" AND ")), values)
        }
    }
}

/// The columns lifted out of a record body at ingest.
struct Flat {
    label: String,
    duration_ms: Option<u64>,
    ok: Option<bool>,
    provider: Option<String>,
    model: Option<String>,
    operation: Option<String>,
    surface: Option<String>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

impl Flat {
    fn from(body: &Body) -> Self {
        let mut flat = Self {
            label: String::new(),
            duration_ms: None,
            ok: None,
            provider: None,
            model: None,
            operation: None,
            surface: None,
            input_tokens: None,
            output_tokens: None,
        };
        match body {
            Body::ProcessStarted { version, args } => {
                flat.label = if args.is_empty() {
                    format!("started {version}")
                } else {
                    format!("started {version} · {}", one_line(&args.join(" ")))
                };
            }
            Body::TurnStarted {
                user_text,
                history_len,
                max_steps,
            } => {
                flat.label = format!(
                    "{} · history {history_len} · max {max_steps}",
                    one_line(user_text)
                );
            }
            Body::TurnStep {
                index,
                thought,
                action,
                target,
                goal,
            } => {
                let head = match target {
                    Some(target) => format!("{action} {target}"),
                    None => action.clone(),
                };
                let tail = goal.as_deref().unwrap_or(thought.as_str());
                flat.label = if tail.trim().is_empty() {
                    format!("#{index} {head}")
                } else {
                    format!("#{index} {head} — {}", one_line(tail))
                };
            }
            Body::TurnStepFinished {
                index,
                action,
                observation,
                duration_ms,
            } => {
                flat.duration_ms = Some(*duration_ms);
                flat.label = format!("#{index} {action} → {}", one_line(observation));
            }
            Body::TurnFinished {
                steps,
                exhausted,
                asked,
                text,
                duration_ms,
            } => {
                flat.duration_ms = Some(*duration_ms);
                flat.ok = Some(true);
                let mut note = format!("{steps} steps");
                if *exhausted {
                    note.push_str(" · exhausted");
                }
                if *asked {
                    note.push_str(" · asked");
                }
                flat.label = format!("{note} · {}", one_line(text));
            }
            Body::TurnFailed {
                code,
                message,
                duration_ms,
            } => {
                flat.duration_ms = Some(*duration_ms);
                flat.ok = Some(false);
                flat.label = format!("{code}: {}", one_line(message));
            }
            Body::Inference {
                provider,
                model,
                json,
                prompt_chars,
                duration_ms,
                usage,
                ok,
                error,
            } => {
                let (input, output) = tokens(usage);
                flat.duration_ms = Some(*duration_ms);
                flat.ok = Some(*ok);
                flat.provider = Some(provider.clone());
                flat.model = Some(model.clone());
                flat.input_tokens = Some(input);
                flat.output_tokens = Some(output);
                flat.label = if *ok {
                    let shape = if *json { " · json" } else { "" };
                    format!("{model} · {input} in / {output} out{shape} · {duration_ms} ms")
                } else {
                    let reason = error.as_deref().unwrap_or("failed");
                    format!(
                        "{model} · {prompt_chars} chars · failed: {}",
                        one_line(reason)
                    )
                };
            }
            Body::SurfaceRun {
                surface,
                target,
                goal,
                outcome,
                steps,
                duration_ms,
                ok,
                error,
            } => {
                flat.duration_ms = Some(*duration_ms);
                flat.ok = Some(*ok);
                flat.surface = Some(surface.clone());
                let reason = match (ok, error) {
                    (false, Some(error)) => format!(" ({})", one_line(error)),
                    _ => String::new(),
                };
                flat.label = format!(
                    "{surface} {target} — {} · {outcome} in {steps} steps{reason}",
                    one_line(goal)
                );
            }
            Body::JevStep {
                surface,
                target,
                index,
                operation,
                confidence,
                label,
                typed_chars,
                candidates,
                stale,
                elapsed_ms,
                ..
            } => {
                flat.duration_ms = Some(*elapsed_ms);
                flat.operation = Some(operation.clone());
                flat.surface = Some(surface.clone());
                let mut text = match label {
                    Some(label) => format!("#{index} {operation} \"{}\"", one_line(label)),
                    None => format!("#{index} {operation} on {target}"),
                };
                text.push_str(&format!(" p={confidence:.2}"));
                if let Some(chars) = typed_chars {
                    text.push_str(&format!(" · {chars} chars"));
                }
                if *stale {
                    text.push_str(" · stale");
                }
                text.push_str(&format!(" · {candidates} candidates · {elapsed_ms} ms"));
                flat.label = text;
            }
            Body::AppEvent { event } => {
                let name = event
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("event");
                flat.label = match event.get("message").and_then(serde_json::Value::as_str) {
                    Some(message) => format!("{name} · {}", one_line(message)),
                    None => name.to_string(),
                };
            }
            Body::Log { level, message } => {
                flat.label = format!("{level}: {}", one_line(message));
            }
        }
        flat
    }
}

/// Providers disagree about what the token fields are called, and a usage
/// object that is missing one is common enough (streaming, cached reads) that
/// treating it as an error would drop otherwise good records.
fn tokens(usage: &serde_json::Value) -> (u64, u64) {
    (
        token_count(usage, &["input_tokens", "prompt_tokens"]),
        token_count(usage, &["output_tokens", "completion_tokens"]),
    )
}

fn token_count(usage: &serde_json::Value, names: &[&str]) -> u64 {
    names
        .iter()
        .find_map(|name| usage.get(*name).and_then(as_count))
        .unwrap_or(0)
}

fn as_count(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|float| float.max(0.0) as u64)),
        serde_json::Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn one_line(text: &str) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= LABEL_CHARS {
        return flat;
    }
    let mut cut: String = flat.chars().take(LABEL_CHARS - 1).collect();
    cut.push('…');
    cut
}

/// SQLite has no unsigned integers. Counts this large mean a corrupt record
/// rather than a real measurement, so saturating is the honest conversion.
fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn as_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use serde_json::json;

    fn open() -> (tempfile::TempDir, Trace) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let trace = Trace::open(&directory.path().join("trace.db")).expect("open the trace store");
        (directory, trace)
    }

    fn record(seq: u64, ts_ms: i64, turn: Option<&str>, body: Body) -> Record {
        Record {
            v: neo_trace::WIRE_VERSION,
            seq,
            ts_ms,
            run: "run-a".to_string(),
            source: "neo-cli".to_string(),
            pid: 4242,
            turn: turn.map(str::to_string),
            dropped: 0,
            body,
        }
    }

    fn jev_step(seq: u64, ts_ms: i64, operation: &str, elapsed_ms: u64) -> Record {
        record(
            seq,
            ts_ms,
            Some("turn-1"),
            Body::JevStep {
                surface: "app".to_string(),
                target: "LibreOffice".to_string(),
                index: seq as usize,
                operation: operation.to_string(),
                confidence: 0.91,
                label: Some("Save".to_string()),
                typed_chars: None,
                candidates: 37,
                stale: false,
                observe_ms: 10,
                jev_ms: 20,
                text_ms: 0,
                act_ms: 5,
                elapsed_ms,
                usage: json!({}),
            },
        )
    }

    fn a_turn(trace: &Trace) {
        trace
            .insert(&record(
                1,
                1_000,
                Some("turn-1"),
                Body::TurnStarted {
                    user_text: "put 42 in the first cell".to_string(),
                    history_len: 3,
                    max_steps: 12,
                },
            ))
            .expect("insert turn_started");
        trace
            .insert(&record(
                2,
                1_100,
                Some("turn-1"),
                Body::Inference {
                    provider: "anthropic".to_string(),
                    model: "claude-sonnet-4-5".to_string(),
                    json: true,
                    prompt_chars: 900,
                    duration_ms: 1_502,
                    usage: json!({ "input_tokens": 27, "output_tokens": 5 }),
                    ok: true,
                    error: None,
                },
            ))
            .expect("insert inference");
        trace
            .insert(&record(
                3,
                1_200,
                Some("turn-1"),
                Body::TurnStep {
                    index: 0,
                    thought: "the sheet is open".to_string(),
                    action: "app".to_string(),
                    target: Some("LibreOffice".to_string()),
                    goal: Some("put 42 in the first cell".to_string()),
                },
            ))
            .expect("insert turn_step");
        trace
            .insert(&record(
                4,
                1_300,
                Some("turn-1"),
                Body::Inference {
                    provider: "anthropic".to_string(),
                    model: "claude-sonnet-4-5".to_string(),
                    json: false,
                    prompt_chars: 120,
                    duration_ms: 300,
                    usage: json!({ "prompt_tokens": 13, "completion_tokens": 2 }),
                    ok: true,
                    error: None,
                },
            ))
            .expect("insert second inference");
        trace
            .insert(&record(
                5,
                1_400,
                Some("turn-1"),
                Body::TurnFinished {
                    steps: 1,
                    exhausted: false,
                    asked: false,
                    text: "the cell now reads 42".to_string(),
                    duration_ms: 4_000,
                },
            ))
            .expect("insert turn_finished");
    }

    #[test]
    fn a_turn_reads_back_in_the_order_it_happened() {
        let (_directory, trace) = open();
        a_turn(&trace);

        let rows = trace.turn("turn-1").expect("read the turn");
        let kinds: Vec<&str> = rows.iter().map(|row| row.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec![
                "turn_started",
                "inference",
                "turn_step",
                "inference",
                "turn_finished"
            ]
        );
        assert_eq!(
            rows[1].label,
            "claude-sonnet-4-5 · 27 in / 5 out · json · 1502 ms"
        );
        assert_eq!(rows[1].duration_ms, Some(1_502));
        assert_eq!(
            rows[2].label,
            "#0 app LibreOffice — put 42 in the first cell"
        );
        assert_eq!(rows[4].ok, Some(true));
        assert_eq!(
            rows[0].body.get("kind").and_then(|kind| kind.as_str()),
            Some("turn_started")
        );
    }

    #[test]
    fn turns_totals_the_steps_and_the_tokens_of_a_turn() {
        let (_directory, trace) = open();
        a_turn(&trace);

        let turns = trace.turns(10).expect("summarise turns");
        assert_eq!(turns.len(), 1);
        let summary = &turns[0];
        assert_eq!(summary.turn, "turn-1");
        assert_eq!(summary.run, "run-a");
        assert_eq!(summary.source, "neo-cli");
        assert_eq!(summary.started_ms, 1_000);
        assert_eq!(summary.ended_ms, 1_400);
        assert_eq!(summary.steps, 1);
        assert_eq!(summary.inferences, 2);
        // 27 + 13, with the OpenAI-style names counted the same as the
        // Anthropic ones.
        assert_eq!(summary.input_tokens, 40);
        assert_eq!(summary.output_tokens, 7);
        assert!(!summary.failed);
        assert_eq!(summary.text, "the cell now reads 42");

        let runs = trace.runs(10).expect("summarise runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].records, 5);
        assert_eq!(runs[0].pid, 4242);
    }

    #[test]
    fn a_failed_turn_is_marked_failed() {
        let (_directory, trace) = open();
        trace
            .insert(&record(
                1,
                2_000,
                Some("turn-2"),
                Body::TurnStarted {
                    user_text: "open the thing".to_string(),
                    history_len: 0,
                    max_steps: 8,
                },
            ))
            .expect("insert turn_started");
        trace
            .insert(&record(
                2,
                2_100,
                Some("turn-2"),
                Body::TurnFailed {
                    code: "no_model".to_string(),
                    message: "no inference model is configured".to_string(),
                    duration_ms: 12,
                },
            ))
            .expect("insert turn_failed");

        let turns = trace.turns(10).expect("summarise turns");
        assert!(turns[0].failed);
        assert_eq!(turns[0].text, "no_model: no inference model is configured");
        assert_eq!(trace.stats(None).expect("stats").failures, 1);
    }

    #[test]
    fn stats_percentiles_and_operations_follow_the_jev_steps() {
        let (_directory, trace) = open();
        for (index, elapsed) in [100_u64, 200, 300, 4_000].into_iter().enumerate() {
            let operation = if index == 3 { "type" } else { "click" };
            trace
                .insert(&jev_step(
                    index as u64 + 1,
                    3_000 + index as i64,
                    operation,
                    elapsed,
                ))
                .expect("insert jev_step");
        }
        trace
            .insert(&record(
                5,
                3_100,
                Some("turn-1"),
                Body::TurnStepFinished {
                    index: 0,
                    action: "app".to_string(),
                    observation: "done".to_string(),
                    duration_ms: 777,
                },
            ))
            .expect("insert turn_step_finished");

        let stats = trace.stats(None).expect("stats");
        assert_eq!(stats.jev_steps, 4);
        assert_eq!(stats.jev_ms.count, 4);
        assert_eq!(stats.jev_ms.p50, 200);
        assert_eq!(stats.jev_ms.p95, 4_000);
        assert_eq!(stats.jev_ms.max, 4_000);
        assert_eq!(stats.step_ms.count, 1);
        assert_eq!(stats.step_ms.p50, 777);
        assert_eq!(stats.inference_ms.count, 0);
        assert_eq!(
            stats.by_operation,
            vec![("click".to_string(), 3), ("type".to_string(), 1)]
        );
        assert!(stats.by_model.is_empty());

        // A window that starts after everything sees nothing.
        let later = trace.stats(Some(9_000)).expect("windowed stats");
        assert_eq!(later.records, 0);
        assert_eq!(later.jev_ms.count, 0);
    }

    #[test]
    fn kind_and_text_filters_narrow_the_rows() {
        let (_directory, trace) = open();
        a_turn(&trace);
        trace
            .insert(&jev_step(6, 1_500, "click", 240))
            .expect("insert jev_step");

        let everything = trace.rows(&Filter::default(), 100).expect("all rows");
        assert_eq!(everything.len(), 6);
        assert!(everything[0].id < everything[5].id, "oldest first");

        let only_inference = trace
            .rows(
                &Filter {
                    kinds: vec!["inference".to_string()],
                    ..Filter::default()
                },
                100,
            )
            .expect("filtered rows");
        assert_eq!(only_inference.len(), 2);
        assert!(only_inference.iter().all(|row| row.kind == "inference"));

        let by_text = trace
            .rows(
                &Filter {
                    text: Some("LibreOffice".to_string()),
                    ..Filter::default()
                },
                100,
            )
            .expect("text search");
        assert_eq!(by_text.len(), 2);

        let combined = trace
            .rows(
                &Filter {
                    kinds: vec!["jev_step".to_string()],
                    text: Some("LibreOffice".to_string()),
                    run: Some("run-a".to_string()),
                    ..Filter::default()
                },
                100,
            )
            .expect("combined filter");
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].duration_ms, Some(240));

        let missing = trace
            .rows(
                &Filter {
                    run: Some("run-b".to_string()),
                    ..Filter::default()
                },
                100,
            )
            .expect("other run");
        assert!(missing.is_empty());

        let after = trace
            .rows_after(everything[3].id, &Filter::default(), 100)
            .expect("rows after an id");
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|row| row.id > everything[3].id));
    }
}
