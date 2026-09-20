//! The views a person actually reads.
//!
//! The receiver's job is to keep every span, but a wall of OTLP JSON is not
//! an answer to "what did it actually do". These five reports turn the rows
//! back into a story: the live tail, the totals, the recent turns, one
//! trace's waterfall, and the processes that produced them.
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

use crate::store::{Filter, Percentiles, RunSummary, Span, Store, TreeSpan, TurnSummary};

/// How often `--follow` asks for spans newer than the last one it printed.
/// Short enough to read as live, long enough that an idle terminal is not
/// a busy loop against SQLite.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);
/// The most rows one follow poll prints. A backlog drains over several polls
/// rather than filling the scrollback in one burst.
const FOLLOW_BATCH: usize = 500;

/// What an empty database says. A producer with no endpoint configured emits
/// nothing at all, which is the usual reason there is nothing here.
const NOTHING_YET: &str = "nothing has arrived yet — run the agent with \
     OTEL_EXPORTER_OTLP_ENDPOINT pointing at this receiver";

// ---------------------------------------------------------------------- tail

/// The spans, newest last, optionally following new ones as they land.
pub fn tail(store: &Store, filter: &Filter, limit: usize, follow: bool, json: bool) -> Result<()> {
    let spans = store.spans(filter, limit)?;

    if json {
        print_json(&serde_json::to_value(&spans)?)?;
    } else if spans.is_empty() && !follow {
        print_line(NOTHING_YET);
    } else {
        print_line(&tail_header());
        if spans.is_empty() {
            print_line("  (waiting)");
        }
        for span in &spans {
            print_line(&tail_line(span));
        }
    }

    if !follow {
        return Ok(());
    }

    // The caller has no async runtime — following is a plain sleep loop, and
    // Ctrl-C ends it the way it ends `tail -f`.
    let mut last = spans.last().map_or(0, |span| span.id);
    loop {
        flush();
        thread::sleep(FOLLOW_INTERVAL);
        let next = store.spans_after(last, filter, FOLLOW_BATCH)?;
        if let Some(span) = next.last() {
            last = span.id;
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
            for span in &next {
                print_line(&tail_line(span));
            }
        }
    }
}

fn tail_header() -> String {
    format!(
        "{:<12} {} {:<11}  {:<8}  {:<18}  {:>8}  {}",
        "TIME", " ", "SOURCE", "TRACE", "SPAN", "MS", "DETAIL"
    )
}

fn tail_line(span: &Span) -> String {
    format!(
        "{:<12} {} {:<11}  {:<8}  {:<18}  {:>8}  {}",
        clock(span.start_ms()),
        outcome_glyph(span.status_code),
        clip(span.source(), 11),
        short_id(&span.trace_id),
        clip(&span.name, 18),
        fmt_ms(span.duration_ms),
        clip(&span.label, 160)
    )
}

/// A failed span has to be visible in a scrolling tail without colour,
/// because the tail is usually being piped somewhere by the time it matters.
fn outcome_glyph(status_code: i64) -> char {
    if status_code == crate::store::STATUS_ERROR {
        'x'
    } else {
        ' '
    }
}

// --------------------------------------------------------------------- stats

/// Totals and latencies over a window, or over everything.
pub fn stats(store: &Store, since_ns: Option<i64>, json: bool) -> Result<()> {
    let stats = store.stats(since_ns)?;
    if json {
        return print_json(&serde_json::to_value(&stats)?);
    }
    if stats.spans == 0 {
        print_line(NOTHING_YET);
        return Ok(());
    }

    print_line(&match since_ns {
        Some(since) => format!("since {} UTC", stamp(crate::store::millis(since))),
        None => "all time".to_owned(),
    });
    print_line(&format!(
        "spans {} · traces {} · runs {} · turns {} · steps {} · jev steps {} · inferences {} · \
         failures {}",
        stats.spans,
        stats.traces,
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
    print_line(&percentile_line("tool step", &stats.step_ms));

    breakdown("by span", &stats.by_name);
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
pub fn turns(store: &Store, limit: usize, json: bool) -> Result<()> {
    let turns = store.turns(limit)?;
    if json {
        return print_json(&serde_json::to_value(&turns)?);
    }
    if turns.is_empty() {
        print_line("no turns yet — a turn appears here as soon as the agent finishes one");
        return Ok(());
    }
    print_line(&format!(
        "{:<15}  {:<8}  {:<11}  {:>5} {:>4} {:>5}  {:>8} {:>8}  {:<6}  {}",
        "STARTED", "TRACE", "SOURCE", "STEPS", "INF", "JEV", "IN", "OUT", "STATUS", "TEXT"
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
        short_id(&turn.trace_id),
        clip(turn.source(), 11),
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

/// One trace as a waterfall. This is the view someone opens when they ask
/// what the agent actually did, so the order, the offsets and the nesting
/// matter more than any aggregate: a navigator run's jev steps have to read
/// as the children of the step that started them.
pub fn turn(store: &Store, trace_id: &str, json: bool) -> Result<()> {
    let spans = store.trace(trace_id)?;
    if json {
        // The argument may be a prefix; the answer names the trace it
        // resolved to, so a script does not have to guess which one it got.
        let resolved = spans
            .first()
            .map_or_else(|| trace_id.to_owned(), |node| node.span.trace_id.clone());
        return print_json(&json!({
            "trace_id": resolved,
            "summary": summary_json(&spans),
            "spans": serde_json::to_value(&spans)?,
        }));
    }
    let Some(first) = spans.first() else {
        print_line(&format!(
            "no spans for trace {trace_id} — check `starkbot-trace turns` for the ids this \
             database has"
        ));
        return Ok(());
    };

    let totals = Totals::of(&spans);
    print_line(&format!(
        "trace {}  {}{}  started {} UTC",
        first.span.trace_id,
        first.span.source(),
        first
            .span
            .run
            .as_deref()
            .map(|run| format!("  run {}", short_id(run)))
            .unwrap_or_default(),
        stamp(first.span.start_ms())
    ));
    print_line("");

    let base = first.span.start_ms();
    for node in &spans {
        let offset = node
            .span
            .start_ms()
            .saturating_sub(base)
            .max(0)
            .unsigned_abs();
        let indent = "  ".repeat(node.depth);
        let name_width = NAME_COLUMN.saturating_sub(node.depth * 2);
        print_line(&format!(
            "{:>9}  {:>8} {} {indent}{:<name_width$}  {}",
            format!("+{}", fmt_ms(offset)),
            fmt_ms(node.span.duration_ms),
            outcome_glyph(node.span.status_code),
            clip(&node.span.name, name_width),
            clip(&detail(&node.span), 200)
        ));
        // A span event is a moment inside the span above it — an `AppEvent`
        // the core published — and a waterfall that hid them would be the
        // only place they never appear.
        for event in node.span.events.as_array().into_iter().flatten() {
            let name = event.get("name").and_then(Value::as_str).unwrap_or("event");
            let at = event
                .get("time_ns")
                .and_then(Value::as_i64)
                .map(crate::store::millis)
                .unwrap_or(base);
            print_line(&format!(
                "{:>9}  {:>8}   {indent}  · {name}",
                format!("+{}", fmt_ms(at.saturating_sub(base).max(0).unsigned_abs())),
                ""
            ));
        }
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

/// Width of the name column in the waterfall, before indentation eats into
/// it.
const NAME_COLUMN: usize = 22;

/// What the waterfall prints beside a span: its label, and the error when
/// there is one, because a failed span's message is the reason anyone opened
/// this view.
fn detail(span: &Span) -> String {
    match &span.status_message {
        Some(message) if span.failed() => {
            if span.label.is_empty() {
                message.clone()
            } else {
                format!("{} — {message}", span.label)
            }
        }
        _ => span.label.clone(),
    }
}

/// Everything the summary line needs, folded out of the spans once.
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
    fn of(spans: &[TreeSpan]) -> Self {
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
        let first = spans.first().map_or(0, |node| node.span.start_ms());
        let last = spans
            .iter()
            .map(|node| node.span.end_ms())
            .max()
            .unwrap_or(first);
        totals.wall_ms = last.saturating_sub(first).max(0).unsigned_abs();
        for node in spans {
            let span = &node.span;
            if span.failed() {
                totals.failed = true;
            }
            if span.name == "execute_tool" {
                totals.steps += 1;
            } else if span.name.starts_with("chat ") {
                totals.inferences += 1;
                totals.inference_ms += span.duration_ms;
                totals.input_tokens += span.input_tokens.unwrap_or(0);
                totals.output_tokens += span.output_tokens.unwrap_or(0);
            } else if span.name.starts_with("jev ") {
                totals.jev_steps += 1;
                totals.jev_ms += span.duration_ms;
            } else if span.name == "invoke_agent" {
                // The turn's own span is authoritative for wall time: it was
                // measured around the work, not inferred from its children.
                totals.wall_ms = span.duration_ms.max(totals.wall_ms);
            }
        }
        totals
    }
}

fn summary_json(spans: &[TreeSpan]) -> Value {
    let totals = Totals::of(spans);
    json!({
        "spans": spans.len(),
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

// ---------------------------------------------------------------------- runs

/// One line per process that has traced. Run ids are printed in full here,
/// because this is where a `--run` filter is copied from.
pub fn runs(store: &Store, limit: usize, json: bool) -> Result<()> {
    let runs = store.runs(limit)?;
    if json {
        return print_json(&serde_json::to_value(&runs)?);
    }
    if runs.is_empty() {
        print_line(NOTHING_YET);
        return Ok(());
    }
    print_line(&format!(
        "{:<36}  {:<14}  {:<11}  {:>7}  {:<15}  {:<15}  {:>8}",
        "RUN", "SERVICE", "SURFACE", "PID", "FIRST", "LAST", "SPANS"
    ));
    for run in &runs {
        print_line(&run_line(run));
    }
    Ok(())
}

fn run_line(run: &RunSummary) -> String {
    format!(
        "{:<36}  {:<14}  {:<11}  {:>7}  {:<15}  {:<15}  {:>8}",
        run.run,
        clip(&run.service, 14),
        clip(run.surface.as_deref().unwrap_or("-"), 11),
        run.pid
            .map_or_else(|| "-".to_owned(), |pid| pid.to_string()),
        stamp(run.started_ms),
        stamp(run.last_ms),
        run.spans
    )
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
/// report, so the allowance lives on these two functions and nowhere else.
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
