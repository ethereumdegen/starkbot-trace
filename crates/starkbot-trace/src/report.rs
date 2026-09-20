//! The views a person actually reads.
//!
//! The collector's job is to keep every record, but a wall of newline JSON is
//! not an answer to "what did it actually do". These five reports turn the
//! rows back into a story: the live tail, the totals, the recent turns, one
//! turn's waterfall, and the processes that produced them.
//!
//! Each of them also speaks JSON, because the same database is the input to
//! ad-hoc analysis, and a report that cannot be piped into `jq` gets copied
//! out of the terminal by hand instead.

use std::io::Write;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::store::{Filter, Percentiles, Row, RunSummary, Trace, TurnSummary};

/// How often `--follow` asks for rows newer than the last one it printed.
/// Short enough to read as live, long enough that an idle terminal is not
/// a busy loop against SQLite.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);
/// The most rows one follow poll prints. A backlog drains over several polls
/// rather than filling the scrollback in one burst.
const FOLLOW_BATCH: usize = 500;

/// What an empty database says. The socket is the usual reason nothing is
/// here: the agent only emits when it can connect to this collector.
const NOTHING_YET: &str =
    "nothing has arrived yet — run the agent with STARKBOT_TRACE_SOCKET pointing at this collector";

// ---------------------------------------------------------------------- tail

/// The rows, newest last, optionally following new ones as they land.
pub fn tail(trace: &Trace, filter: &Filter, limit: usize, follow: bool, json: bool) -> Result<()> {
    let rows = trace.rows(filter, limit)?;

    if json {
        print_json(&serde_json::to_value(&rows)?)?;
    } else if rows.is_empty() && !follow {
        print_line(NOTHING_YET);
    } else {
        print_line(&tail_header());
        if rows.is_empty() {
            print_line("  (waiting)");
        }
        for row in &rows {
            print_line(&tail_line(row));
        }
    }

    if !follow {
        return Ok(());
    }

    // The caller has no async runtime — following is a plain sleep loop, and
    // Ctrl-C ends it the way it ends `tail -f`.
    let mut last = rows.last().map_or(0, |row| row.id);
    loop {
        flush();
        thread::sleep(FOLLOW_INTERVAL);
        let next = trace.rows_after(last, filter, FOLLOW_BATCH)?;
        if let Some(row) = next.last() {
            last = row.id;
        }
        if next.is_empty() {
            continue;
        }
        if json {
            // A stream of pretty documents rather than one truncated array:
            // the reader is a person or a `jq` in streaming mode, and neither
            // can wait for a closing bracket that only arrives at Ctrl-C.
            print_json(&serde_json::to_value(&next)?)?;
        } else {
            for row in &next {
                print_line(&tail_line(row));
            }
        }
    }
}

fn tail_header() -> String {
    format!(
        "{:<12} {} {:<11}  {:<8}  {:<18}  {:>8}  {}",
        "TIME", " ", "SOURCE", "TURN", "KIND", "MS", "DETAIL"
    )
}

fn tail_line(row: &Row) -> String {
    let turn = row
        .turn
        .as_deref()
        .map_or_else(|| " ".repeat(8), |turn| format!("{:<8}", short_id(turn)));
    let duration = row.duration_ms.map(fmt_ms).unwrap_or_default();
    format!(
        "{:<12} {} {:<11}  {turn}  {:<18}  {duration:>8}  {}",
        clock(row.ts_ms),
        outcome_glyph(row.ok),
        clip(&row.source, 11),
        clip(&row.kind, 18),
        clip(&row.label, 160)
    )
}

/// A failed row has to be visible in a scrolling tail without colour, because
/// the tail is usually being piped somewhere by the time it matters.
fn outcome_glyph(ok: Option<bool>) -> char {
    match ok {
        Some(false) => 'x',
        _ => ' ',
    }
}

// --------------------------------------------------------------------- stats

/// Totals and latencies over a window, or over everything.
pub fn stats(trace: &Trace, since_ms: Option<i64>, json: bool) -> Result<()> {
    let stats = trace.stats(since_ms)?;
    if json {
        return print_json(&serde_json::to_value(&stats)?);
    }
    if stats.records == 0 {
        print_line(NOTHING_YET);
        return Ok(());
    }

    print_line(&match since_ms {
        Some(since) => format!("since {} UTC", stamp(since)),
        None => "all time".to_owned(),
    });
    print_line(&format!(
        "records {} · runs {} · turns {} · steps {} · jev steps {} · inferences {} · failures {}",
        stats.records,
        stats.runs,
        stats.turns,
        stats.steps,
        stats.jev_steps,
        stats.inferences,
        stats.failures
    ));

    print_line("");
    print_line(&format!(
        "{:<12} {:>8} {:>9} {:>9} {:>9}",
        "", "count", "p50", "p95", "max"
    ));
    print_line(&percentile_line("inference", &stats.inference_ms));
    print_line(&percentile_line("jev step", &stats.jev_ms));
    print_line(&percentile_line("turn step", &stats.step_ms));

    breakdown("by kind", &stats.by_kind);
    breakdown("by operation", &stats.by_operation);
    breakdown("by model", &stats.by_model);

    print_line("");
    let mut tokens = format!(
        "tokens  {} in · {} out",
        stats.input_tokens, stats.output_tokens
    );
    if stats.turns > 0 {
        tokens.push_str(&format!(
            "  ({} in / {} out per turn)",
            stats.input_tokens / stats.turns,
            stats.output_tokens / stats.turns
        ));
    }
    print_line(&tokens);
    Ok(())
}

fn percentile_line(label: &str, percentiles: &Percentiles) -> String {
    if percentiles.count == 0 {
        return format!("{label:<12} {:>8} {:>9} {:>9} {:>9}", 0, "-", "-", "-");
    }
    format!(
        "{label:<12} {:>8} {:>9} {:>9} {:>9}",
        percentiles.count,
        fmt_ms(percentiles.p50),
        fmt_ms(percentiles.p95),
        fmt_ms(percentiles.max)
    )
}

fn breakdown(title: &str, counts: &[(String, u64)]) {
    print_line("");
    print_line(title);
    if counts.is_empty() {
        print_line("  none");
        return;
    }
    let width = counts
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or(0)
        .max(8);
    for (name, count) in counts {
        print_line(&format!("  {name:<width$}  {count:>8}"));
    }
}

// --------------------------------------------------------------------- turns

/// The recent turns, newest first: one line is meant to be enough to decide
/// which turn to open.
pub fn turns(trace: &Trace, limit: usize, json: bool) -> Result<()> {
    let turns = trace.turns(limit)?;
    if json {
        return print_json(&serde_json::to_value(&turns)?);
    }
    if turns.is_empty() {
        print_line("no turns yet — a turn appears here as soon as the agent starts one");
        return Ok(());
    }
    print_line(&format!(
        "{:<15}  {:<8}  {:<11}  {:>5} {:>4} {:>5}  {:>8} {:>8}  {:<6}  {}",
        "STARTED", "TURN", "SOURCE", "STEPS", "INF", "JEV", "IN", "OUT", "STATUS", "TEXT"
    ));
    for turn in &turns {
        print_line(&turn_line(turn));
    }
    Ok(())
}

fn turn_line(turn: &TurnSummary) -> String {
    format!(
        "{:<15}  {:<8}  {:<11}  {:>5} {:>4} {:>5}  {:>8} {:>8}  {:<6}  {}",
        stamp(turn.started_ms),
        short_id(&turn.turn),
        clip(&turn.source, 11),
        turn.steps,
        turn.inferences,
        turn.jev_steps,
        turn.input_tokens,
        turn.output_tokens,
        if turn.failed { "failed" } else { "ok" },
        clip(&turn.text, 60)
    )
}

// ---------------------------------------------------------------------- turn

/// One turn as a waterfall. This is the view someone opens when they ask what
/// the agent actually did, so the order, the offsets and the nesting matter
/// more than any aggregate: a surface run's jev steps have to read as the
/// children of the step that started them.
pub fn turn(trace: &Trace, turn_id: &str, json: bool) -> Result<()> {
    let rows = trace.turn(turn_id)?;
    if json {
        // The argument may be a prefix; the answer names the turn it resolved
        // to, so a script does not have to guess which one it got.
        let resolved = rows
            .first()
            .and_then(|row| row.turn.clone())
            .unwrap_or_else(|| turn_id.to_owned());
        return print_json(&json!({
            "turn": resolved,
            "summary": summary_json(&rows),
            "rows": serde_json::to_value(&rows)?,
        }));
    }
    let Some(first) = rows.first() else {
        print_line(&format!(
            "no records for turn {turn_id} — check `starkbot-trace turns` for the ids this database has"
        ));
        return Ok(());
    };

    let totals = Totals::of(&rows);
    print_line(&format!(
        "turn {}  run {}  {}  started {} UTC",
        first.turn.as_deref().unwrap_or(turn_id),
        first.run,
        first.source,
        stamp(first.ts_ms)
    ));
    print_line("");

    let base = first.ts_ms;
    for row in &rows {
        let offset = row.ts_ms.saturating_sub(base).max(0).unsigned_abs();
        let depth = indent_of(&row.kind);
        let indent = "  ".repeat(depth);
        let kind_width = KIND_COLUMN.saturating_sub(depth * 2);
        let duration = row.duration_ms.map(fmt_ms).unwrap_or_default();
        print_line(&format!(
            "{:>9}  {duration:>8} {} {indent}{:<kind_width$}  {}",
            format!("+{}", fmt_ms(offset)),
            outcome_glyph(row.ok),
            clip(&row.kind, kind_width),
            clip(&row.label, 200)
        ));
    }

    print_line("");
    print_line(&format!(
        "summary  wall {} · inference {} ({}) · jev {} ({}) · {} in / {} out · {} step(s) · {}",
        fmt_ms(totals.wall_ms),
        fmt_ms(totals.inference_ms),
        totals.inferences,
        fmt_ms(totals.jev_ms),
        totals.jev_steps,
        totals.input_tokens,
        totals.output_tokens,
        totals.steps,
        if totals.failed { "failed" } else { "ok" }
    ));
    Ok(())
}

/// Width of the kind column in the waterfall, before indentation eats into it.
const KIND_COLUMN: usize = 20;

/// How deep a kind sits in the tree. Depth is a property of the kind rather
/// than of the record, because the wire format carries no parent pointer —
/// and it does not need one: a jev step only ever happens inside a surface
/// run, which only ever happens inside a step.
fn indent_of(kind: &str) -> usize {
    match kind {
        "turn_step" | "turn_step_finished" | "inference" => 1,
        "surface_run" | "app_event" | "log" => 2,
        "jev_step" => 3,
        _ => 0,
    }
}

/// Everything the summary line needs, folded out of the rows once.
struct Totals {
    wall_ms: u64,
    inference_ms: u64,
    jev_ms: u64,
    input_tokens: u64,
    output_tokens: u64,
    steps: usize,
    inferences: usize,
    jev_steps: usize,
    failed: bool,
}

impl Totals {
    fn of(rows: &[Row]) -> Self {
        let mut totals = Self {
            wall_ms: 0,
            inference_ms: 0,
            jev_ms: 0,
            input_tokens: 0,
            output_tokens: 0,
            steps: 0,
            inferences: 0,
            jev_steps: 0,
            failed: false,
        };
        let first = rows.first().map_or(0, |row| row.ts_ms);
        let last = rows.last().map_or(0, |row| row.ts_ms);
        totals.wall_ms = last.saturating_sub(first).max(0).unsigned_abs();
        for row in rows {
            match row.kind.as_str() {
                "turn_step" => totals.steps += 1,
                "inference" => {
                    totals.inferences += 1;
                    totals.inference_ms += duration_of(row);
                    let (input, output) = tokens_of(row.body.get("usage"));
                    totals.input_tokens += input;
                    totals.output_tokens += output;
                }
                "jev_step" => {
                    totals.jev_steps += 1;
                    totals.jev_ms += duration_of(row);
                }
                // The turn's own record is authoritative for wall time: it was
                // measured around the work, not inferred from record stamps.
                "turn_finished" => totals.wall_ms = duration_of(row).max(totals.wall_ms),
                "turn_failed" => {
                    totals.failed = true;
                    totals.wall_ms = duration_of(row).max(totals.wall_ms);
                }
                _ => {}
            }
        }
        totals
    }
}

fn summary_json(rows: &[Row]) -> Value {
    let totals = Totals::of(rows);
    json!({
        "records": rows.len(),
        "wall_ms": totals.wall_ms,
        "inference_ms": totals.inference_ms,
        "jev_ms": totals.jev_ms,
        "steps": totals.steps,
        "inferences": totals.inferences,
        "jev_steps": totals.jev_steps,
        "input_tokens": totals.input_tokens,
        "output_tokens": totals.output_tokens,
        "failed": totals.failed,
    })
}

/// A record's own duration, falling back to the body: the ingest column is
/// filled for the kinds that have one obvious duration, while a jev step
/// carries its wall time as `elapsed_ms` next to its four phase timings.
fn duration_of(row: &Row) -> u64 {
    row.duration_ms
        .or_else(|| field_u64(&row.body, "duration_ms"))
        .or_else(|| field_u64(&row.body, "elapsed_ms"))
        .unwrap_or(0)
}

fn field_u64(body: &Value, key: &str) -> Option<u64> {
    body.get(key).and_then(Value::as_u64)
}

/// Usage is whatever the provider sent, kept verbatim, so both spellings are
/// in the wild: Anthropic and Codex say `input_tokens`, an OpenAI-shaped
/// completion says `prompt_tokens`.
fn tokens_of(usage: Option<&Value>) -> (u64, u64) {
    let Some(usage) = usage else {
        return (0, 0);
    };
    let read = |keys: [&str; 2]| {
        keys.iter()
            .find_map(|key| field_u64(usage, key))
            .unwrap_or(0)
    };
    (
        read(["input_tokens", "prompt_tokens"]),
        read(["output_tokens", "completion_tokens"]),
    )
}

// ---------------------------------------------------------------------- runs

/// One line per process that has ever connected. Run ids are printed in full
/// here, because this is where a `--run` filter is copied from.
pub fn runs(trace: &Trace, limit: usize, json: bool) -> Result<()> {
    let runs = trace.runs(limit)?;
    if json {
        return print_json(&serde_json::to_value(&runs)?);
    }
    if runs.is_empty() {
        print_line(NOTHING_YET);
        return Ok(());
    }
    print_line(&format!(
        "{:<36}  {:<11}  {:>7}  {:<15}  {:<15}  {:>8}",
        "RUN", "SOURCE", "PID", "FIRST", "LAST", "RECORDS"
    ));
    for run in &runs {
        print_line(&run_line(run));
    }
    Ok(())
}

fn run_line(run: &RunSummary) -> String {
    let mut line = format!(
        "{:<36}  {:<11}  {:>7}  {:<15}  {:<15}  {:>8}",
        run.run,
        clip(&run.source, 11),
        run.pid,
        stamp(run.started_ms),
        stamp(run.last_ms),
        run.records
    );
    // A dropped record is a hole in the story, and a hole nobody is told about
    // reads as "the agent never did that".
    if run.dropped > 0 {
        line.push_str(&format!(
            "  {} dropped — this run's trace is incomplete",
            run.dropped
        ));
    }
    line
}

// ------------------------------------------------------------------ plumbing

/// `HH:MM:SS.mmm` in UTC. The `time` crate can only read the local offset with
/// a feature this workspace does not enable, and a wrong local time is worse
/// than an honest UTC one when two machines' traces are compared.
pub(crate) fn clock(ts_ms: i64) -> String {
    let millis = ts_ms.rem_euclid(1_000);
    match OffsetDateTime::from_unix_timestamp(ts_ms.div_euclid(1_000)) {
        Ok(at) => format!(
            "{:02}:{:02}:{:02}.{millis:03}",
            at.hour(),
            at.minute(),
            at.second()
        ),
        Err(_) => format!("{ts_ms}"),
    }
}

/// `MM-DD HH:MM:SS` in UTC, for the reports where a row can be days old.
pub(crate) fn stamp(ts_ms: i64) -> String {
    match OffsetDateTime::from_unix_timestamp(ts_ms.div_euclid(1_000)) {
        Ok(at) => format!(
            "{:02}-{:02} {:02}:{:02}:{:02}",
            u8::from(at.month()),
            at.day(),
            at.hour(),
            at.minute(),
            at.second()
        ),
        Err(_) => format!("{ts_ms}"),
    }
}

/// Milliseconds a person can read at a glance: anything past a second is
/// reported in seconds, because "1502 ms" is arithmetic and "1.5 s" is not.
pub(crate) fn fmt_ms(ms: u64) -> String {
    if ms >= 1_000 {
        format!("{:.1} s", ms as f64 / 1_000.0)
    } else {
        format!("{ms} ms")
    }
}

pub(crate) fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

/// One line, at most `width` characters. Labels are built from model text and
/// observations, so they can carry newlines that would otherwise shear a
/// column layout in half.
pub(crate) fn clip(text: &str, width: usize) -> String {
    let flat: String = text
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    let flat = flat.trim();
    if flat.chars().count() <= width {
        return flat.to_owned();
    }
    let kept: String = flat.chars().take(width.saturating_sub(1)).collect();
    format!("{kept}…")
}

/// The workspace denies `print_stdout`; a report that cannot print is not a
/// report, so the allowance lives on these two functions and nowhere else,
/// exactly as `neo-cli` does it.
#[allow(clippy::print_stdout)]
fn print_line(line: &str) {
    println!("{line}");
}

#[allow(clippy::print_stdout)]
fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Follow mode is usually piped, and a block-buffered pipe hides the last few
/// seconds of the trace until the buffer happens to fill.
fn flush() {
    let _ = std::io::stdout().flush();
}
