//! Where a stream of OTLP spans stops being a stream and becomes something
//! you can ask questions of.
//!
//! The receiver takes a span once and the reports read it many times — `tail`
//! every few hundred milliseconds, `stats` over a whole day, a dashboard
//! refreshing four panes at a time. So every question a report asks is
//! answered by SQLite over an index, never by pulling rows into Rust and
//! filtering there. That is what the flat columns are for: `service`,
//! `surface`, `run`, `pid`, `turn`, `operation`, `model`, `system` and the
//! token counts are lifted out of the resource and span attributes once, at
//! ingest, so `turns` and `stats` are index scans instead of a `json_extract`
//! over every row in the table. The attribute map and the events are still
//! stored whole, because a span that has been summarised into columns is no
//! longer a span, and the vocabulary an agent emits will grow fields this
//! build has never heard of.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};
use rusqlite_migration::{M, Migrations};
use serde::Serialize;
use serde_json::{Map, Value};

/// `TRC1`. A trace database and an agent's own database both live under
/// Application Support and both end in `.db`; the magic number is what stops
/// `starkbot-trace --db` pointed at the wrong file from migrating it.
const APPLICATION_ID: i64 = 0x5452_4331;
const SCHEMA_VERSION: i64 = 1;

/// How much of a free-text attribute survives into a one-line label. Long
/// enough for a goal or an answer's first sentence, short enough that a
/// terminal row stays a row.
const LABEL_CHARS: usize = 140;

/// Every `by_*` breakdown in [`Stats`]. A trace has a handful of operations
/// and models; anything past a dozen is noise in a summary.
const BREAKDOWN_LIMIT: usize = 12;

/// The name of the span an agent turn produces, and of the spans beneath it.
/// These are the GenAI convention's names, matched here so the reports can
/// count them without the producer having to send a private marker.
const TURN_SPAN: &str = "invoke_agent";
const TOOL_SPAN: &str = "execute_tool";
/// Inference and navigator spans carry their subject in the name, so they are
/// matched by prefix: `chat claude-sonnet-4-5`, `jev CLICK`.
const CHAT_LIKE: &str = "chat %";
const JEV_LIKE: &str = "jev %";

/// The columns every span query selects, in the order [`read_span`] expects.
const SPAN_COLUMNS: &str = "id, trace_id, span_id, parent_span_id, name, kind, start_ns, end_ns, \
     duration_ms, status_code, status_message, service, surface, run, pid, turn, operation, \
     model, system, input_tokens, output_tokens, label, attributes, events";

/// The OTLP status code for a failure. `0` is unset — what a span that
/// simply finished carries — and `1` is an explicit OK; neither is an error,
/// and only the error is worth a name here.
pub const STATUS_ERROR: i64 = 2;

fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(include_str!("../migrations/0001_spans.sql"))])
}

/// One span as a report sees it: the flat columns it sorts and colours by,
/// plus the attributes and events for anything it wants to dig into.
#[derive(Clone, Debug, Serialize)]
pub struct Span {
    pub id: i64,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: Option<i64>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub duration_ms: u64,
    pub status_code: i64,
    pub status_message: Option<String>,
    pub service: String,
    pub surface: Option<String>,
    pub run: Option<String>,
    pub pid: Option<u32>,
    pub turn: Option<String>,
    pub operation: Option<String>,
    pub model: Option<String>,
    pub system: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub label: String,
    pub attributes: Value,
    pub events: Value,
}

impl Span {
    #[must_use]
    pub fn start_ms(&self) -> i64 {
        millis(self.start_ns)
    }

    #[must_use]
    pub fn end_ms(&self) -> i64 {
        millis(self.end_ns)
    }

    /// Whether the producer said this span failed. An unset status is not a
    /// failure and not a success; the reports show it as neither.
    #[must_use]
    pub fn failed(&self) -> bool {
        self.status_code == STATUS_ERROR
    }

    /// What the process that produced this span calls itself, for the column
    /// the reports print: the surface when there is one, because `neo-tui`
    /// says more than `starkbot-neo`.
    #[must_use]
    pub fn source(&self) -> &str {
        self.surface.as_deref().unwrap_or(&self.service)
    }
}

/// One span in the tree a waterfall draws, with how deep it sits.
#[derive(Clone, Debug, Serialize)]
pub struct TreeSpan {
    pub depth: usize,
    pub span: Span,
}

/// Fields combine with AND; an empty filter matches everything.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub run: Option<String>,
    pub trace: Option<String>,
    pub turn: Option<String>,
    pub names: Vec<String>,
    pub since_ns: Option<i64>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TurnSummary {
    pub turn: String,
    pub trace_id: String,
    pub service: String,
    pub surface: Option<String>,
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

impl TurnSummary {
    #[must_use]
    pub fn source(&self) -> &str {
        self.surface.as_deref().unwrap_or(&self.service)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RunSummary {
    pub run: String,
    pub service: String,
    pub surface: Option<String>,
    pub pid: Option<u32>,
    pub started_ms: i64,
    pub last_ms: i64,
    pub spans: u64,
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
    pub spans: u64,
    pub traces: u64,
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
    pub by_name: Vec<(String, u64)>,
    pub by_operation: Vec<(String, u64)>,
    pub by_model: Vec<(String, u64)>,
}

/// The receiver's database.
///
/// One connection behind a mutex rather than a pool: the writer is a single
/// receiver and the readers are one CLI invocation at a time, so a pool would
/// buy contention handling nobody needs and cost a second WAL reader.
pub struct Store {
    connection: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("could not create {}", parent.display()))?;
        }
        let existed = path.exists() && std::fs::metadata(path).is_ok_and(|meta| meta.len() > 0);
        let mut connection =
            Connection::open(path).with_context(|| format!("could not open {}", path.display()))?;

        // First, always: switching to WAL takes an exclusive lock, and a
        // reporting command can open the file while the receiver is mid
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
    /// statement. rusqlite leaves the connection usable, and a receiver that
    /// refused every later span because one report formatter panicked would
    /// lose the trace it exists to keep.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Store every span of one OTLP document and return how many there were.
    ///
    /// A document that does not parse is rejected whole rather than half
    /// stored: the sender is about to be told 400 and will decide whether to
    /// resend, and half a turn in the database is worse than none.
    pub fn insert_document(&self, document: &Value) -> Result<usize> {
        let incoming = read_document(document)?;
        let mut guard = self.lock();
        let transaction = guard.transaction()?;
        for span in &incoming {
            // An OTLP client retries on 5xx and on a dropped connection, so
            // the same span id arriving twice is ordinary traffic rather than
            // a conflict. The later copy wins and keeps the row it already
            // had, which is what leaves `tail` cursors and the dashboard's
            // stream pointing at the same place.
            transaction.execute(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, name, kind, start_ns, \
                 end_ns, duration_ms, status_code, status_message, service, surface, run, pid, \
                 turn, operation, model, system, input_tokens, output_tokens, label, attributes, \
                 events) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(span_id) DO UPDATE SET \
                   trace_id = excluded.trace_id, \
                   parent_span_id = excluded.parent_span_id, \
                   name = excluded.name, \
                   kind = excluded.kind, \
                   start_ns = excluded.start_ns, \
                   end_ns = excluded.end_ns, \
                   duration_ms = excluded.duration_ms, \
                   status_code = excluded.status_code, \
                   status_message = excluded.status_message, \
                   service = excluded.service, \
                   surface = excluded.surface, \
                   run = excluded.run, \
                   pid = excluded.pid, \
                   turn = excluded.turn, \
                   operation = excluded.operation, \
                   model = excluded.model, \
                   system = excluded.system, \
                   input_tokens = excluded.input_tokens, \
                   output_tokens = excluded.output_tokens, \
                   label = excluded.label, \
                   attributes = excluded.attributes, \
                   events = excluded.events",
                rusqlite::params![
                    span.trace_id,
                    span.span_id,
                    span.parent_span_id,
                    span.name,
                    span.kind,
                    span.start_ns,
                    span.end_ns,
                    as_i64(span.duration_ms),
                    span.status_code,
                    span.status_message,
                    span.service,
                    span.surface,
                    span.run,
                    span.pid.map(i64::from),
                    span.turn,
                    span.operation,
                    span.model,
                    span.system,
                    span.input_tokens.map(as_i64),
                    span.output_tokens.map(as_i64),
                    span.label,
                    span.attributes.to_string(),
                    span.events.to_string(),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(incoming.len())
    }

    /// The newest `limit` matching spans, oldest first, the way a terminal
    /// reads.
    pub fn spans(&self, filter: &Filter, limit: usize) -> Result<Vec<Span>> {
        let (clause, mut values) = filter.sql();
        values.push(Sql::Integer(as_i64(limit as u64)));
        let sql = format!(
            "SELECT {SPAN_COLUMNS} FROM (SELECT * FROM spans {clause} ORDER BY id DESC LIMIT ?) \
             ORDER BY id ASC"
        );
        self.query_spans(&sql, values)
    }

    /// Everything stored after `id`. This is how `tail --follow`, the
    /// dashboard and the forwarder advance without re-reading what they have
    /// already seen.
    pub fn spans_after(&self, id: i64, filter: &Filter, limit: usize) -> Result<Vec<Span>> {
        let (mut clause, mut values) = filter.sql();
        clause = if clause.is_empty() {
            "WHERE id > ?".to_string()
        } else {
            format!("{clause} AND id > ?")
        };
        values.push(Sql::Integer(id));
        values.push(Sql::Integer(as_i64(limit as u64)));
        let sql = format!("SELECT {SPAN_COLUMNS} FROM spans {clause} ORDER BY id ASC LIMIT ?");
        self.query_spans(&sql, values)
    }

    /// One whole trace, in the order a waterfall draws it: each root followed
    /// by its children, depth first, oldest first at every level.
    ///
    /// A trace id is 32 hex characters and the reports print a prefix, so a
    /// prefix is what anyone actually types after reading `turns`. It has to
    /// name exactly one trace: matching several and drawing them together
    /// folded three separate runs into one waterfall, which reads as an agent
    /// doing things it never did.
    pub fn trace(&self, trace_id: &str) -> Result<Vec<TreeSpan>> {
        let resolved = self.resolve_trace(trace_id)?;
        let sql =
            format!("SELECT {SPAN_COLUMNS} FROM spans WHERE trace_id = ? ORDER BY start_ns ASC, id ASC");
        let spans = self.query_spans(&sql, vec![Sql::Text(resolved)])?;
        Ok(tree(spans))
    }

    /// The one trace an id or a prefix names, or an error naming the
    /// candidates. Guessing is not an option here.
    fn resolve_trace(&self, trace_id: &str) -> Result<String> {
        let guard = self.lock();
        let mut statement = guard.prepare(
            "SELECT DISTINCT trace_id FROM spans WHERE trace_id = ?1 OR trace_id LIKE ?2 LIMIT 5",
        )?;
        let matches: Vec<String> = statement
            .query_map(rusqlite::params![trace_id, format!("{trace_id}%")], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        match matches.as_slice() {
            [] => Err(anyhow!("no trace starts with `{trace_id}`")),
            [only] => Ok(only.clone()),
            several => Err(anyhow!(
                "`{trace_id}` matches {} traces ({}); give more characters",
                several.len(),
                several
                    .iter()
                    .map(|id| id[..16.min(id.len())].to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// One row per agent turn, newest first.
    ///
    /// The counts and the token totals come from the turn's descendants,
    /// walked in SQL: only the root span carries `starkbot.turn`, and a child
    /// is tied to it by `parentSpanId` alone. `UNION` rather than `UNION ALL`
    /// is what makes that walk terminate if a producer ever claims a parent
    /// cycle — a repeated pair is dropped instead of recursing forever.
    pub fn turns(&self, limit: usize) -> Result<Vec<TurnSummary>> {
        let guard = self.lock();
        let mut statement = guard.prepare(
            "WITH RECURSIVE roots AS ( \
                 SELECT span_id, trace_id, turn, service, surface, start_ns, end_ns, \
                        status_code, status_message, label, attributes \
                 FROM spans WHERE name = ?1 ORDER BY start_ns DESC LIMIT ?2 \
             ), \
             tree(root, span_id) AS ( \
                 SELECT span_id, span_id FROM roots \
                 UNION \
                 SELECT tree.root, spans.span_id FROM spans \
                   JOIN tree ON spans.parent_span_id = tree.span_id \
             ) \
             SELECT roots.trace_id, \
                    COALESCE(roots.turn, roots.trace_id), \
                    roots.service, \
                    roots.surface, \
                    roots.start_ns, \
                    roots.end_ns, \
                    COALESCE(SUM(child.name = ?3), 0), \
                    COALESCE(SUM(child.name LIKE ?4), 0), \
                    COALESCE(SUM(child.name LIKE ?5), 0), \
                    COALESCE(SUM(CASE WHEN child.name LIKE ?4 THEN child.input_tokens END), 0), \
                    COALESCE(SUM(CASE WHEN child.name LIKE ?4 THEN child.output_tokens END), 0), \
                    roots.status_code, \
                    COALESCE( \
                      json_extract(roots.attributes, '$.\"starkbot.answer\"'), \
                      roots.status_message, \
                      json_extract(roots.attributes, '$.\"starkbot.user_text\"'), \
                      roots.label) \
             FROM roots \
               JOIN tree ON tree.root = roots.span_id \
               JOIN spans AS child ON child.span_id = tree.span_id \
             GROUP BY roots.span_id \
             ORDER BY roots.start_ns DESC",
        )?;
        let summaries = statement
            .query_map(
                rusqlite::params![
                    TURN_SPAN,
                    as_i64(limit as u64),
                    TOOL_SPAN,
                    CHAT_LIKE,
                    JEV_LIKE
                ],
                |row| {
                    Ok(TurnSummary {
                        trace_id: row.get(0)?,
                        turn: row.get(1)?,
                        service: row.get(2)?,
                        surface: row.get(3)?,
                        started_ms: millis(row.get(4)?),
                        ended_ms: millis(row.get(5)?),
                        steps: as_u64(row.get(6)?),
                        inferences: as_u64(row.get(7)?),
                        jev_steps: as_u64(row.get(8)?),
                        input_tokens: as_u64(row.get(9)?),
                        output_tokens: as_u64(row.get(10)?),
                        failed: row.get::<_, i64>(11)? == STATUS_ERROR,
                        text: row.get::<_, Option<String>>(12)?.unwrap_or_default(),
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(summaries)
    }

    pub fn runs(&self, limit: usize) -> Result<Vec<RunSummary>> {
        let guard = self.lock();
        let mut statement = guard.prepare(
            "SELECT run, service, surface, pid, started_ns, last_ns, spans \
             FROM runs ORDER BY last_ns DESC LIMIT ?",
        )?;
        let summaries = statement
            .query_map([as_i64(limit as u64)], |row| {
                Ok(RunSummary {
                    run: row.get(0)?,
                    service: row.get(1)?,
                    surface: row.get(2)?,
                    pid: row
                        .get::<_, Option<i64>>(3)?
                        .and_then(|pid| u32::try_from(pid).ok()),
                    started_ms: millis(row.get(4)?),
                    last_ms: millis(row.get(5)?),
                    spans: as_u64(row.get(6)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(summaries)
    }

    pub fn stats(&self, since_ns: Option<i64>) -> Result<Stats> {
        let guard = self.lock();
        let (clause, values) = match since_ns {
            Some(since) => (
                "WHERE start_ns >= ?4".to_string(),
                vec![Sql::Integer(since)],
            ),
            None => (String::new(), Vec::new()),
        };
        let mut bound: Vec<Sql> = vec![
            Sql::Text(TURN_SPAN.to_string()),
            Sql::Text(TOOL_SPAN.to_string()),
            Sql::Text(CHAT_LIKE.to_string()),
        ];
        bound.extend(values);

        let totals = guard.query_row(
            &format!(
                "SELECT COUNT(*), \
                        COUNT(DISTINCT trace_id), \
                        COUNT(DISTINCT run), \
                        COALESCE(SUM(name = ?1), 0), \
                        COALESCE(SUM(name = ?2), 0), \
                        COALESCE(SUM(name LIKE ?3), 0), \
                        COALESCE(SUM(name LIKE '{JEV_LIKE}'), 0), \
                        COALESCE(SUM(status_code = {STATUS_ERROR}), 0), \
                        COALESCE(SUM(input_tokens), 0), \
                        COALESCE(SUM(output_tokens), 0) \
                 FROM spans {clause}"
            ),
            params_from_iter(bound.iter()),
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
                    as_u64(row.get(9)?),
                ))
            },
        )?;

        Ok(Stats {
            spans: totals.0,
            traces: totals.1,
            runs: totals.2,
            turns: totals.3,
            steps: totals.4,
            inferences: totals.5,
            jev_steps: totals.6,
            failures: totals.7,
            input_tokens: totals.8,
            output_tokens: totals.9,
            inference_ms: percentiles(&guard, "name LIKE ?", CHAT_LIKE, since_ns)?,
            jev_ms: percentiles(&guard, "name LIKE ?", JEV_LIKE, since_ns)?,
            step_ms: percentiles(&guard, "name = ?", TOOL_SPAN, since_ns)?,
            by_name: breakdown(&guard, "name", since_ns)?,
            by_operation: breakdown(&guard, "operation", since_ns)?,
            by_model: breakdown(&guard, "model", since_ns)?,
        })
    }

    fn query_spans(&self, sql: &str, values: Vec<Sql>) -> Result<Vec<Span>> {
        let guard = self.lock();
        let mut statement = guard.prepare(sql)?;
        let spans = statement
            .query_map(params_from_iter(values.iter()), read_span)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(spans)
    }
}

fn read_span(row: &rusqlite::Row<'_>) -> rusqlite::Result<Span> {
    let attributes: String = row.get(22)?;
    let events: String = row.get(23)?;
    Ok(Span {
        id: row.get(0)?,
        trace_id: row.get(1)?,
        span_id: row.get(2)?,
        parent_span_id: row.get(3)?,
        name: row.get(4)?,
        kind: row.get(5)?,
        start_ns: row.get(6)?,
        end_ns: row.get(7)?,
        duration_ms: as_u64(row.get(8)?),
        status_code: row.get(9)?,
        status_message: row.get(10)?,
        service: row.get(11)?,
        surface: row.get(12)?,
        run: row.get(13)?,
        pid: row
            .get::<_, Option<i64>>(14)?
            .and_then(|pid| u32::try_from(pid).ok()),
        turn: row.get(15)?,
        operation: row.get(16)?,
        model: row.get(17)?,
        system: row.get(18)?,
        input_tokens: row.get::<_, Option<i64>>(19)?.map(as_u64),
        output_tokens: row.get::<_, Option<i64>>(20)?.map(as_u64),
        label: row.get(21)?,
        // A column that will not parse means it was written by something
        // other than `insert_document`; report it as empty rather than
        // failing the whole query and hiding every other span.
        attributes: serde_json::from_str(&attributes).unwrap_or_else(|_| Value::Object(Map::new())),
        events: serde_json::from_str(&events).unwrap_or_else(|_| Value::Array(Vec::new())),
    })
}

/// Order a flat list of spans the way a waterfall reads.
///
/// A parent can arrive after its children — a turn's root span is exported
/// last, because it is the last thing to finish — and a `--since` window can
/// cut a trace in half. Any span whose parent is not in the list is treated as
/// a root, so nothing is silently dropped from the picture.
fn tree(spans: Vec<Span>) -> Vec<TreeSpan> {
    let mut index_of: HashMap<&str, usize> = HashMap::new();
    for (index, span) in spans.iter().enumerate() {
        index_of.insert(span.span_id.as_str(), index);
    }
    let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for (index, span) in spans.iter().enumerate() {
        match span
            .parent_span_id
            .as_deref()
            .and_then(|parent| index_of.get(parent).copied())
        {
            Some(parent) if parent != index => children.entry(parent).or_default().push(index),
            _ => roots.push(index),
        }
    }

    // The rows arrive oldest first, so pushing the stack in reverse walks
    // each level in the order the work happened.
    let mut order: Vec<(usize, usize)> = Vec::with_capacity(spans.len());
    let mut visited = vec![false; spans.len()];
    let mut stack: Vec<(usize, usize)> = roots
        .into_iter()
        .rev()
        .map(|index| (index, 0_usize))
        .collect();
    while let Some((index, depth)) = stack.pop() {
        // A producer that claims a parent cycle would otherwise walk for
        // ever; a span is drawn once and the cycle ends there.
        if visited[index] {
            continue;
        }
        visited[index] = true;
        order.push((index, depth));
        if let Some(kids) = children.get(&index) {
            for kid in kids.iter().rev() {
                stack.push((*kid, depth + 1));
            }
        }
    }
    // Anything a cycle kept out of the walk still happened, so it goes on
    // the end rather than disappearing from the waterfall.
    for (index, seen) in visited.iter().enumerate() {
        if !seen {
            order.push((index, 0));
        }
    }

    let mut slots: Vec<Option<Span>> = spans.into_iter().map(Some).collect();
    order
        .into_iter()
        .filter_map(|(index, depth)| slots[index].take().map(|span| TreeSpan { depth, span }))
        .collect()
}

/// Nearest-rank percentile: sort by the column SQLite already has an ordering
/// for and take the row at the computed offset. Three cheap `LIMIT 1 OFFSET n`
/// queries beat pulling every duration into memory to sort it again, and they
/// keep the crate free of a statistics dependency.
fn percentiles(
    connection: &Connection,
    term: &str,
    name: &str,
    since_ns: Option<i64>,
) -> Result<Percentiles> {
    let mut clause = format!("WHERE {term}");
    let mut values = vec![Sql::Text(name.to_string())];
    if let Some(since) = since_ns {
        clause.push_str(" AND start_ns >= ?");
        values.push(Sql::Integer(since));
    }

    let count: u64 = connection.query_row(
        &format!("SELECT COUNT(*) FROM spans {clause}"),
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
        with_offset.push(Sql::Integer(as_i64(offset)));
        let value: i64 = connection.query_row(
            &format!(
                "SELECT duration_ms FROM spans {clause} ORDER BY duration_ms ASC LIMIT 1 OFFSET ?"
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
    since_ns: Option<i64>,
) -> Result<Vec<(String, u64)>> {
    let mut clause = format!("WHERE {column} IS NOT NULL");
    let mut values = Vec::new();
    if let Some(since) = since_ns {
        clause.push_str(" AND start_ns >= ?");
        values.push(Sql::Integer(since));
    }
    let sql = format!(
        "SELECT {column}, COUNT(*) FROM spans {clause} \
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
    fn sql(&self) -> (String, Vec<Sql>) {
        let mut terms: Vec<String> = Vec::new();
        let mut values: Vec<Sql> = Vec::new();
        // Ids are matched by prefix for the same reason `trace()` does it:
        // the ids a report shows are truncated.
        if let Some(run) = &self.run {
            terms.push("(run = ? OR run LIKE ?)".to_string());
            values.push(Sql::Text(run.clone()));
            values.push(Sql::Text(format!("{run}%")));
        }
        if let Some(trace) = &self.trace {
            terms.push("(trace_id = ? OR trace_id LIKE ?)".to_string());
            values.push(Sql::Text(trace.clone()));
            values.push(Sql::Text(format!("{trace}%")));
        }
        if let Some(turn) = &self.turn {
            // Only the root span of a turn carries `starkbot.turn`, so a turn
            // is selected through the trace that root named. Without the
            // subquery a turn filter would match one span out of a hundred.
            terms.push(
                "trace_id IN (SELECT trace_id FROM spans WHERE turn = ? OR turn LIKE ?)"
                    .to_string(),
            );
            values.push(Sql::Text(turn.clone()));
            values.push(Sql::Text(format!("{turn}%")));
        }
        if !self.names.is_empty() {
            let markers = vec!["?"; self.names.len()].join(", ");
            terms.push(format!("name IN ({markers})"));
            values.extend(self.names.iter().map(|name| Sql::Text(name.clone())));
        }
        if let Some(since) = self.since_ns {
            terms.push("start_ns >= ?".to_string());
            values.push(Sql::Integer(since));
        }
        if let Some(text) = &self.text {
            // A plain substring match with no LIKE escaping: `%` and `_` in a
            // search term act as wildcards. That is a search box over your own
            // trace, not a query language, and pretending otherwise would cost
            // an ESCAPE clause and a surprise the first time someone greps for
            // a percentage.
            terms.push("(label LIKE ? OR name LIKE ? OR attributes LIKE ?)".to_string());
            let pattern = format!("%{text}%");
            values.push(Sql::Text(pattern.clone()));
            values.push(Sql::Text(pattern.clone()));
            values.push(Sql::Text(pattern));
        }
        if terms.is_empty() {
            (String::new(), values)
        } else {
            (format!("WHERE {}", terms.join(" AND ")), values)
        }
    }
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/// A span on its way into the table: the wire's fields, already flattened.
struct Incoming {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    kind: Option<i64>,
    start_ns: i64,
    end_ns: i64,
    duration_ms: u64,
    status_code: i64,
    status_message: Option<String>,
    service: String,
    surface: Option<String>,
    run: Option<String>,
    pid: Option<u32>,
    turn: Option<String>,
    operation: Option<String>,
    model: Option<String>,
    system: Option<String>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    label: String,
    attributes: Value,
    events: Value,
}

/// The resource attributes, decoded once per `resourceSpans` entry.
struct Resource {
    attributes: Map<String, Value>,
}

impl Resource {
    fn read(resource: Option<&Value>) -> Self {
        let attributes = resource
            .and_then(|resource| resource.get("attributes"))
            .map_or_else(Map::new, decode_attributes);
        Self { attributes }
    }

    fn text(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).and_then(Value::as_str)
    }
}

/// Everything the flat columns are lifted out of. A key that lives in the
/// resource is only read there; a producer that also puts it on the span (a
/// navigator run names its own surface) is read from either.
fn read_document(document: &Value) -> Result<Vec<Incoming>> {
    let resource_spans = document
        .get("resourceSpans")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("the document has no `resourceSpans` array"))?;
    let mut incoming = Vec::new();
    for entry in resource_spans {
        let resource = Resource::read(entry.get("resource"));
        let scopes = entry
            .get("scopeSpans")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("a resourceSpans entry has no `scopeSpans` array"))?;
        for scope in scopes {
            let spans = match scope.get("spans").and_then(Value::as_array) {
                Some(spans) => spans,
                // A scope with no spans is legal and means nothing happened
                // under it; it is not a reason to reject the document.
                None => continue,
            };
            for span in spans {
                incoming.push(read_span_value(span, &resource)?);
            }
        }
    }
    Ok(incoming)
}

fn read_span_value(span: &Value, resource: &Resource) -> Result<Incoming> {
    let trace_id = required_text(span, "traceId")?;
    let span_id = required_text(span, "spanId")?;
    let name = required_text(span, "name")?;
    let start_ns = required_nanos(span, "startTimeUnixNano")?;
    // A span the producer is still holding open has no end time yet. Ending
    // it where it started is honest — zero duration — and keeps it visible.
    let end_ns = match span.get("endTimeUnixNano") {
        Some(_) => required_nanos(span, "endTimeUnixNano")?,
        None => start_ns,
    };

    let mut attributes = decode_attributes(span.get("attributes").unwrap_or(&Value::Null));
    // A resource attribute with no column of its own would otherwise be lost,
    // and `service.version` is exactly the one a person asks about when two
    // runs behave differently.
    for (key, value) in &resource.attributes {
        if !RESOURCE_COLUMNS.contains(&key.as_str()) {
            attributes.entry(key.clone()).or_insert(value.clone());
        }
    }

    let status = span.get("status");
    let status_code = status
        .and_then(|status| status.get("code"))
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let status_message = status
        .and_then(|status| status.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .map(str::to_owned);

    let attributes = Value::Object(attributes);
    let duration_ms = as_u64(millis(end_ns.saturating_sub(start_ns)));

    Ok(Incoming {
        trace_id: trace_id.to_owned(),
        span_id: span_id.to_owned(),
        parent_span_id: span
            .get("parentSpanId")
            .and_then(Value::as_str)
            .filter(|parent| !parent.is_empty())
            .map(str::to_owned),
        name: name.to_owned(),
        kind: span.get("kind").and_then(Value::as_i64),
        start_ns,
        end_ns,
        duration_ms,
        status_code,
        status_message,
        service: resource
            .text("service.name")
            .unwrap_or("unknown")
            .to_owned(),
        surface: resource
            .text("starkbot.surface")
            .map(str::to_owned)
            .or_else(|| attr_text(&attributes, "starkbot.surface").map(str::to_owned)),
        run: resource.text("starkbot.run").map(str::to_owned),
        pid: resource
            .attributes
            .get("process.pid")
            .and_then(Value::as_i64)
            .and_then(|pid| u32::try_from(pid).ok()),
        turn: attr_text(&attributes, "starkbot.turn").map(str::to_owned),
        operation: attr_text(&attributes, "starkbot.operation").map(str::to_owned),
        model: attr_text(&attributes, "gen_ai.request.model").map(str::to_owned),
        system: attr_text(&attributes, "gen_ai.system").map(str::to_owned),
        input_tokens: attr_count(&attributes, "gen_ai.usage.input_tokens"),
        output_tokens: attr_count(&attributes, "gen_ai.usage.output_tokens"),
        label: label_for(name, &attributes),
        events: decode_events(span.get("events")),
        attributes,
    })
}

/// The resource attributes that already have a column. Everything else is
/// folded into the span's own attribute map.
const RESOURCE_COLUMNS: [&str; 4] = [
    "service.name",
    "process.pid",
    "starkbot.surface",
    "starkbot.run",
];

fn required_text<'a>(span: &'a Value, key: &str) -> Result<&'a str> {
    span.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("a span has no `{key}`"))
}

/// OTLP/JSON writes a 64-bit timestamp as a decimal string, because a JSON
/// number is a double and nanoseconds since 1970 do not survive one. A
/// producer that sent a number anyway is read too rather than rejected.
fn required_nanos(span: &Value, key: &str) -> Result<i64> {
    match span.get(key) {
        Some(Value::String(text)) => text
            .parse::<i64>()
            .map_err(|_| anyhow!("`{key}` is not a decimal number of nanoseconds: {text}")),
        Some(Value::Number(number)) => number
            .as_i64()
            .ok_or_else(|| anyhow!("`{key}` is not a whole number of nanoseconds")),
        _ => Err(anyhow!("a span has no `{key}`")),
    }
}

/// `[{"key":k,"value":{"stringValue":v}}]` into a plain object.
///
/// The typed wrapper exists on the wire so protobuf can carry a union; once
/// it is JSON in a database it is only in the way of `json_extract` and of
/// anyone reading the column with `jq`. An `intValue` arrives as a string and
/// becomes a number here, and the export turns it back into a string, so the
/// round trip keeps the type OTLP gave it.
fn decode_attributes(attributes: &Value) -> Map<String, Value> {
    let mut map = Map::new();
    for attribute in attributes.as_array().into_iter().flatten() {
        let Some(key) = attribute.get("key").and_then(Value::as_str) else {
            continue;
        };
        if let Some(value) = decode_value(attribute.get("value")) {
            map.insert(key.to_owned(), value);
        }
    }
    map
}

fn decode_value(value: Option<&Value>) -> Option<Value> {
    let value = value?.as_object()?;
    if let Some(text) = value.get("stringValue") {
        return Some(text.clone());
    }
    if let Some(number) = value.get("intValue") {
        return match number {
            Value::String(text) => text.parse::<i64>().ok().map(Value::from),
            Value::Number(number) => number.as_i64().map(Value::from),
            _ => None,
        };
    }
    if let Some(number) = value.get("doubleValue").and_then(Value::as_f64) {
        return serde_json::Number::from_f64(number).map(Value::Number);
    }
    if let Some(flag) = value.get("boolValue") {
        return Some(flag.clone());
    }
    // `arrayValue` and `kvlistValue` have no column, no report and no query
    // that reads them; they are kept whole so nothing is lost.
    value
        .get("arrayValue")
        .or_else(|| value.get("kvlistValue"))
        .cloned()
}

fn decode_events(events: Option<&Value>) -> Value {
    let decoded: Vec<Value> = events
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|event| {
            let mut object = Map::new();
            object.insert(
                "name".to_owned(),
                event.get("name").cloned().unwrap_or(Value::Null),
            );
            object.insert(
                "time_ns".to_owned(),
                Value::from(required_nanos(event, "timeUnixNano").unwrap_or_default()),
            );
            object.insert(
                "attributes".to_owned(),
                Value::Object(decode_attributes(
                    event.get("attributes").unwrap_or(&Value::Null),
                )),
            );
            Value::Object(object)
        })
        .collect();
    Value::Array(decoded)
}

fn attr<'a>(attributes: &'a Value, key: &str) -> Option<&'a Value> {
    attributes.get(key)
}

fn attr_text<'a>(attributes: &'a Value, key: &str) -> Option<&'a str> {
    attr(attributes, key).and_then(Value::as_str)
}

fn attr_count(attributes: &Value, key: &str) -> Option<u64> {
    match attr(attributes, key)? {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|float| float.max(0.0) as u64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn attr_f64(attributes: &Value, key: &str) -> Option<f64> {
    attr(attributes, key).and_then(Value::as_f64)
}

/// The one-line human summary a report prints next to a span.
///
/// The vocabulary is Starkbot's, but nothing here requires it: a span whose
/// name this build has never seen falls back to its attributes, so an
/// unrelated OpenTelemetry program pointed at this receiver still reads as
/// something rather than as a blank column.
fn label_for(name: &str, attributes: &Value) -> String {
    if name == TURN_SPAN {
        let text = attr_text(attributes, "starkbot.answer")
            .or_else(|| attr_text(attributes, "starkbot.user_text"))
            .unwrap_or_default();
        return match attr_count(attributes, "starkbot.steps") {
            Some(steps) if !text.is_empty() => format!("{steps} steps · {}", one_line(text)),
            Some(steps) => format!("{steps} steps"),
            None => one_line(text),
        };
    }
    if name == TOOL_SPAN {
        let tool = attr_text(attributes, "gen_ai.tool.name").unwrap_or("step");
        let head = match attr_text(attributes, "starkbot.target") {
            Some(target) => format!("{tool} {target}"),
            None => tool.to_owned(),
        };
        let index = attr_text(attributes, "gen_ai.tool.call.id")
            .map(str::to_owned)
            .or_else(|| attr_count(attributes, "gen_ai.tool.call.id").map(|id| id.to_string()));
        let head = match index {
            Some(index) => format!("#{index} {head}"),
            None => head,
        };
        let tail = attr_text(attributes, "starkbot.observation")
            .or_else(|| attr_text(attributes, "starkbot.goal"))
            .or_else(|| attr_text(attributes, "starkbot.thought"))
            .unwrap_or_default();
        return if tail.trim().is_empty() {
            head
        } else {
            format!("{head} — {}", one_line(tail))
        };
    }
    if let Some(model) = name.strip_prefix("chat ") {
        let input = attr_count(attributes, "gen_ai.usage.input_tokens");
        let output = attr_count(attributes, "gen_ai.usage.output_tokens");
        return match (input, output) {
            (Some(input), Some(output)) => format!("{model} · {input} in / {output} out"),
            // A usage object the provider did not send stays absent rather
            // than being printed as zero, which is a different claim.
            _ => model.to_owned(),
        };
    }
    if let Some(surface) = name.strip_prefix("navigate ") {
        let target = attr_text(attributes, "starkbot.target").unwrap_or_default();
        let goal = attr_text(attributes, "starkbot.goal").unwrap_or_default();
        let head = format!("{surface} {target}").trim_end().to_owned();
        return if goal.is_empty() {
            head
        } else {
            format!("{head} — {}", one_line(goal))
        };
    }
    if let Some(operation) = name.strip_prefix("jev ") {
        let mut text = match attr_text(attributes, "starkbot.label") {
            Some(label) => format!("{operation} \"{}\"", one_line(label)),
            None => operation.to_owned(),
        };
        if let Some(confidence) = attr_f64(attributes, "starkbot.confidence") {
            text.push_str(&format!(" p={confidence:.2}"));
        }
        if let Some(chars) = attr_count(attributes, "starkbot.typed_chars") {
            text.push_str(&format!(" · {chars} chars"));
        }
        return text;
    }
    match attributes.as_object() {
        Some(fields) if !fields.is_empty() => one_line(&Value::Object(fields.clone()).to_string()),
        _ => String::new(),
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

/// Unix nanoseconds to unix milliseconds. Every report works in milliseconds
/// because that is the resolution a person reads; the table keeps nanoseconds
/// because that is what arrived.
#[must_use]
pub fn millis(ns: i64) -> i64 {
    ns / 1_000_000
}

/// SQLite has no unsigned integers. Counts this large mean a corrupt span
/// rather than a real measurement, so saturating is the honest conversion.
fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn as_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    // These are the questions the reports ask, asked directly: what a posted
    // document turns into, what a turn adds up to, and what happens when a
    // span says something this build has never heard of. The SQL is the
    // interesting part and it is only exercised through the public calls.
    use serde_json::{Value, json};

    use super::{Filter, Store};

    const TRACE: &str = "7f3a1c9e5d2b48a6913f0e7c4b5a6d2e";
    const RUN: &str = "0199a7f0-0000-7000-8000-00000000c0de";

    fn store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let store = Store::open(&directory.path().join("trace.db")).expect("an open database");
        (directory, store)
    }

    fn document(spans: Vec<Value>) -> Value {
        json!({
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "starkbot-neo"}},
                    {"key": "service.version", "value": {"stringValue": "0.0.1"}},
                    {"key": "process.pid", "value": {"intValue": "4242"}},
                    {"key": "starkbot.surface", "value": {"stringValue": "neo-cli"}},
                    {"key": "starkbot.run", "value": {"stringValue": RUN}},
                ]},
                "scopeSpans": [{
                    "scope": {"name": "starkbot-neo", "version": "0.0.1"},
                    "spans": spans,
                }],
            }],
        })
    }

    /// Milliseconds since the epoch are easier to read in a fixture than
    /// nanoseconds, and the wire wants a decimal string either way.
    fn at(ms: i64) -> Value {
        Value::String((ms * 1_000_000).to_string())
    }

    fn span(
        id: &str,
        parent: Option<&str>,
        name: &str,
        start_ms: i64,
        end_ms: i64,
        attributes: &[(&str, Value)],
        status: Option<Value>,
    ) -> Value {
        let mut span = json!({
            "traceId": TRACE,
            "spanId": id,
            "name": name,
            "kind": 1,
            "startTimeUnixNano": at(start_ms),
            "endTimeUnixNano": at(end_ms),
            "attributes": attributes
                .iter()
                .map(|(key, value)| json!({"key": key, "value": value}))
                .collect::<Vec<_>>(),
        });
        if let Some(parent) = parent {
            span["parentSpanId"] = json!(parent);
        }
        if let Some(status) = status {
            span["status"] = status;
        }
        span
    }

    fn turn_document() -> Value {
        // Deliberately out of tree order, the way an SDK exports: a child
        // ends before its parent does, so it is sent first.
        document(vec![
            span(
                "e5f60718293a4b5c",
                Some("d4e5f60718293a4b"),
                "jev CLICK",
                1_700,
                2_650,
                &[
                    ("starkbot.operation", json!({"stringValue": "CLICK"})),
                    ("starkbot.confidence", json!({"doubleValue": 0.95})),
                    ("starkbot.label", json!({"stringValue": "Learn more"})),
                    ("starkbot.typed_chars", json!({"intValue": "0"})),
                ],
                None,
            ),
            span(
                "d4e5f60718293a4b",
                Some("c3d4e5f60718293a"),
                "navigate app",
                1_600,
                4_300,
                &[
                    ("starkbot.target", json!({"stringValue": "Numbers"})),
                    (
                        "starkbot.goal",
                        json!({"stringValue": "click the Blank template"}),
                    ),
                ],
                Some(json!({"code": 2, "message": "no element matched"})),
            ),
            span(
                "c3d4e5f60718293a",
                Some("a1b2c3d4e5f60718"),
                "execute_tool",
                1_500,
                4_400,
                &[
                    ("gen_ai.tool.name", json!({"stringValue": "app"})),
                    ("gen_ai.tool.call.id", json!({"intValue": "0"})),
                    ("starkbot.target", json!({"stringValue": "Numbers"})),
                ],
                Some(json!({"code": 1})),
            ),
            span(
                "b2c3d4e5f6071829",
                Some("a1b2c3d4e5f60718"),
                "chat claude-sonnet-4-5",
                300,
                1_451,
                &[
                    (
                        "gen_ai.request.model",
                        json!({"stringValue": "claude-sonnet-4-5"}),
                    ),
                    ("gen_ai.system", json!({"stringValue": "anthropic"})),
                    ("gen_ai.usage.input_tokens", json!({"intValue": "1151"})),
                    ("gen_ai.usage.output_tokens", json!({"intValue": "131"})),
                ],
                Some(json!({"code": 1})),
            ),
            span(
                "a1b2c3d4e5f60718",
                None,
                "invoke_agent",
                0,
                4_500,
                &[
                    (
                        "starkbot.turn",
                        json!({"stringValue": "0199a7f0-0000-7000-8000-0000000000aa"}),
                    ),
                    ("starkbot.steps", json!({"intValue": "1"})),
                    (
                        "starkbot.answer",
                        json!({"stringValue": "Numbers would not open"}),
                    ),
                ],
                Some(json!({"code": 2, "message": "the navigator gave up"})),
            ),
        ])
    }

    #[test]
    fn a_posted_document_comes_back_as_a_tree() {
        let (_dir, store) = store();
        assert_eq!(
            store
                .insert_document(&turn_document())
                .expect("the document is stored"),
            5
        );

        let tree = store.trace(TRACE).expect("the trace");
        let shape: Vec<(usize, &str)> = tree
            .iter()
            .map(|node| (node.depth, node.span.name.as_str()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (0, "invoke_agent"),
                (1, "chat claude-sonnet-4-5"),
                (1, "execute_tool"),
                (2, "navigate app"),
                (3, "jev CLICK"),
            ],
            "the waterfall follows parentSpanId, not the order spans arrived"
        );

        // An eight-character prefix is what `turns` prints, so it has to
        // resolve to the same trace.
        assert_eq!(store.trace("7f3a1c9e").expect("the trace").len(), 5);

        let chat = &tree[1].span;
        assert_eq!(chat.model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(chat.input_tokens, Some(1_151));
        assert_eq!(chat.output_tokens, Some(131));
        assert_eq!(chat.label, "claude-sonnet-4-5 · 1151 in / 131 out");
        assert_eq!(chat.service, "starkbot-neo");
        assert_eq!(chat.surface.as_deref(), Some("neo-cli"));
        assert_eq!(chat.run.as_deref(), Some(RUN));
        assert_eq!(chat.duration_ms, 1_151);
        // A resource attribute with no column of its own is not lost.
        assert_eq!(chat.attributes["service.version"], json!("0.0.1"));

        let jev = &tree[4].span;
        assert_eq!(jev.label, "CLICK \"Learn more\" p=0.95 · 0 chars");
        assert_eq!(jev.operation.as_deref(), Some("CLICK"));

        let navigate = &tree[3].span;
        assert!(navigate.failed(), "a status code of 2 is a failure");
        assert_eq!(
            navigate.status_message.as_deref(),
            Some("no element matched")
        );
        assert_eq!(navigate.label, "app Numbers — click the Blank template");
    }

    #[test]
    fn a_turn_sums_its_descendants_and_fails_from_its_own_status() {
        let (_dir, store) = store();
        store
            .insert_document(&turn_document())
            .expect("the document is stored");

        let turns = store.turns(10).expect("the turns");
        assert_eq!(turns.len(), 1);
        let turn = &turns[0];
        assert_eq!(turn.turn, "0199a7f0-0000-7000-8000-0000000000aa");
        assert_eq!(turn.trace_id, TRACE);
        // The tokens are on a grandchild of nothing and a child of the root;
        // either way they belong to the turn.
        assert_eq!(turn.input_tokens, 1_151);
        assert_eq!(turn.output_tokens, 131);
        assert_eq!(turn.steps, 1);
        assert_eq!(turn.inferences, 1);
        assert_eq!(turn.jev_steps, 1);
        assert!(turn.failed, "the root's status is what failed the turn");
        assert_eq!(turn.text, "Numbers would not open");
        assert_eq!(turn.source(), "neo-cli");
    }

    #[test]
    fn a_turn_whose_root_is_ok_is_not_failed_by_a_failing_child() {
        let (_dir, store) = store();
        store
            .insert_document(&document(vec![
                span(
                    "1111111111111111",
                    None,
                    "invoke_agent",
                    0,
                    900,
                    &[("starkbot.steps", json!({"intValue": "1"}))],
                    Some(json!({"code": 1})),
                ),
                span(
                    "2222222222222222",
                    Some("1111111111111111"),
                    "navigate browser",
                    100,
                    800,
                    &[],
                    Some(json!({"code": 2, "message": "the page never loaded"})),
                ),
            ]))
            .expect("the document is stored");

        let turns = store.turns(10).expect("the turns");
        assert_eq!(turns.len(), 1);
        assert!(
            !turns[0].failed,
            "a turn that recovered from a failed navigator run still succeeded"
        );
    }

    #[test]
    fn stats_reports_percentiles_and_breakdowns_over_span_names() {
        let (_dir, store) = store();
        store
            .insert_document(&turn_document())
            .expect("the document is stored");
        // Three more inferences, so the percentiles have something to rank.
        for (index, duration) in [100_i64, 200, 4_000].into_iter().enumerate() {
            store
                .insert_document(&document(vec![span(
                    &format!("aaaaaaaaaaaa000{index}"),
                    None,
                    "chat gpt-5",
                    10_000,
                    10_000 + duration,
                    &[
                        ("gen_ai.request.model", json!({"stringValue": "gpt-5"})),
                        ("gen_ai.usage.input_tokens", json!({"intValue": "10"})),
                    ],
                    Some(json!({"code": 1})),
                )]))
                .expect("the document is stored");
        }

        let stats = store.stats(None).expect("the stats");
        assert_eq!(stats.spans, 8);
        assert_eq!(stats.turns, 1);
        assert_eq!(stats.inferences, 4);
        assert_eq!(stats.jev_steps, 1);
        assert_eq!(stats.failures, 2, "the turn and its navigator run failed");
        assert_eq!(stats.input_tokens, 1_181);

        // Nearest rank over 100, 200, 1151 and 4000 milliseconds.
        assert_eq!(stats.inference_ms.count, 4);
        assert_eq!(stats.inference_ms.p50, 200);
        assert_eq!(stats.inference_ms.p95, 4_000);
        assert_eq!(stats.inference_ms.max, 4_000);

        let by_model: Vec<(&str, u64)> = stats
            .by_model
            .iter()
            .map(|(model, count)| (model.as_str(), *count))
            .collect();
        assert_eq!(by_model, vec![("gpt-5", 3), ("claude-sonnet-4-5", 1)]);
        assert!(
            stats
                .by_name
                .iter()
                .any(|(name, count)| name == "chat gpt-5" && *count == 3)
        );
        assert_eq!(stats.by_operation, vec![("CLICK".to_owned(), 1)]);

        // A window that starts after the turn keeps only the later spans.
        let recent = store.stats(Some(9_000 * 1_000_000)).expect("the stats");
        assert_eq!(recent.spans, 3);
        assert_eq!(recent.turns, 0);
    }

    #[test]
    fn a_span_this_build_has_never_heard_of_is_still_stored_and_listed() {
        let (_dir, store) = store();
        store
            .insert_document(&document(vec![span(
                "f00ff00ff00ff00f",
                None,
                "db.query",
                0,
                42,
                &[
                    ("db.system", json!({"stringValue": "sqlite"})),
                    ("db.rows", json!({"intValue": "9007199254740993"})),
                    ("weird.ratio", json!({"doubleValue": 0.5})),
                    ("weird.flag", json!({"boolValue": true})),
                ],
                None,
            )]))
            .expect("the document is stored");

        let spans = store.spans(&Filter::default(), 10).expect("the spans");
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "db.query");
        assert_eq!(span.duration_ms, 42);
        assert!(
            span.label.contains("sqlite"),
            "an unknown span still gets a readable label: {}",
            span.label
        );
        // A 64-bit count survives the wire's string encoding; a JSON number
        // would have rounded it to 9007199254740992.
        assert_eq!(span.attributes["db.rows"], json!(9_007_199_254_740_993_i64));
        assert_eq!(span.attributes["weird.ratio"], json!(0.5));
        assert_eq!(span.attributes["weird.flag"], json!(true));

        let named = store
            .spans(
                &Filter {
                    names: vec!["db.query".to_owned()],
                    ..Filter::default()
                },
                10,
            )
            .expect("the spans");
        assert_eq!(named.len(), 1, "an unknown name is still filterable");
    }

    #[test]
    fn a_span_delivered_twice_is_stored_once() {
        let (_dir, store) = store();
        store
            .insert_document(&turn_document())
            .expect("the document is stored");
        store
            .insert_document(&turn_document())
            .expect("the retry is stored");

        // An OTLP client retries after a timeout it lost the answer to, and
        // a trace that doubled every span on a retry would be unreadable.
        assert_eq!(store.trace(TRACE).expect("the trace").len(), 5);
        assert_eq!(store.turns(10).expect("the turns").len(), 1);
    }

    #[test]
    fn a_document_that_is_not_otlp_is_refused_whole() {
        let (_dir, store) = store();
        assert!(store.insert_document(&json!({"spans": []})).is_err());
        assert!(
            store
                .insert_document(&document(vec![json!({"name": "no ids"})]))
                .is_err()
        );
        assert!(
            store
                .spans(&Filter::default(), 10)
                .expect("the spans")
                .is_empty(),
            "a rejected document leaves nothing behind"
        );
    }
}
