//! The exit the rest of the world already has a reader for.
//!
//! The socket and the database are this tool's own shape, and a trace that only
//! this tool can read is a trace nobody else will look at. Everything a report
//! prints is already a span in all but name — a turn contains steps, a step
//! contains an inference, a navigator run contains its decisions, and every
//! record carries a timestamp and a duration — so the mapping here is a
//! rename, not a redesign. Once the records are spans they can go to an
//! OpenTelemetry Collector, and from there to Jaeger, Tempo, Honeycomb or any
//! of the AI-observability backends that speak OTLP.
//!
//! The payload is built by hand with `serde_json` and posted with `reqwest`
//! rather than through the OpenTelemetry SDK. `resourceSpans[].scopeSpans[]
//! .spans[]` is a documented, stable schema, and the alternative is a protobuf
//! toolchain, a code generator and an async SDK with its own batching,
//! sampling and shutdown semantics — all of it inside a program whose whole
//! job is to be boring and to still be running in six months. OTLP over HTTP
//! with JSON encoding is the one OTLP transport a person can verify by reading
//! the bytes, which is also what makes `--dry-run` worth having.
//!
//! Attribute names follow the OpenTelemetry GenAI semantic conventions where
//! they exist (`gen_ai.operation.name`, `gen_ai.request.model`,
//! `gen_ai.usage.input_tokens`, `gen_ai.tool.name`) so a backend that already
//! understands agent traces understands these. Everything with no convention
//! keeps its own name under `starkbot.`, rather than being bent into a
//! convention it does not mean.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::store::{Filter, Row, Trace};

/// The standard endpoint variable, so `starkbot-trace` needs no configuration
/// of its own in an environment that already exports one.
const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// The standard header variable, in its `k=v,k2=v2` form.
const HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_HEADERS";

/// Spans per request. Small enough that a rejected batch loses little and a
/// retry is cheap, large enough that a busy turn is one or two round trips.
const SPANS_PER_REQUEST: usize = 200;

/// Including the first. A fifth attempt on an endpoint that has failed four
/// times is an endpoint that is down, and the export has other batches to get
/// through.
const MAX_ATTEMPTS: u32 = 4;

/// The first retry delay; doubled per attempt.
const BACKOFF_BASE: Duration = Duration::from_millis(250);

/// A `Retry-After` longer than this is treated as this long: the header comes
/// from the other end, and an export that parks for an hour because a proxy
/// said so is indistinguishable from a hang.
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How often follow mode asks the store for new records. The same interval the
/// dashboard uses: fast enough to feel live, slow enough to stay invisible in
/// the WAL.
const POLL: Duration = Duration::from_millis(250);

/// How long a turn with no terminal record waits before it is exported anyway.
/// A turn span cannot be emitted before its children exist, but an agent that
/// was killed mid-turn will never send `turn_finished`, and holding those
/// records forever would lose exactly the turn somebody is trying to explain.
const QUIET: Duration = Duration::from_secs(30);

/// Records per follow poll. A burst larger than this is caught by the next
/// poll 250 ms later.
const FOLLOW_BATCH: usize = 2_000;

/// The ceiling on a one-shot export. A trace database holds months of runs and
/// the mapping needs a whole turn in memory at once, so the export is bounded
/// and says so rather than growing until the allocator complains.
const MAX_ROWS: usize = 200_000;

/// Wake-ups the collector may have outstanding before it starts dropping them.
/// Dropping is safe by construction: the exporter polls the store on a timer
/// anyway, so a lost wake-up delays a batch by one tick and loses nothing.
const WAKE_DEPTH: usize = 1_024;

/// Where a mapped batch goes.
///
/// `--dry-run` is not a debug flag: printing the document is how a person
/// checks the mapping against their own records without standing up a
/// collector first, so it is a first-class destination rather than an early
/// return inside the sender.
pub enum Egress {
    Print,
    Post(Sink),
}

impl Egress {
    /// `endpoint` is the base URL. A missing endpoint falls back to
    /// [`ENDPOINT_ENV`], and `--header` values are applied after
    /// [`HEADERS_ENV`] so the flag wins.
    pub fn new(endpoint: Option<&str>, headers: &[String], dry_run: bool) -> Result<Self> {
        if dry_run {
            return Ok(Self::Print);
        }
        let endpoint = match endpoint {
            Some(endpoint) => endpoint.to_owned(),
            None => std::env::var(ENDPOINT_ENV).map_err(|_| {
                anyhow!("no OTLP endpoint: pass --otlp <url>, set {ENDPOINT_ENV}, or use --dry-run")
            })?,
        };
        let mut resolved = env_headers();
        for header in headers {
            resolved.push(parse_header(header)?);
        }
        Ok(Self::Post(Sink::new(&endpoint, &resolved)?))
    }

    /// Deliver every batch and return how many of them failed.
    ///
    /// A batch that will not go through is reported and skipped rather than
    /// ending the export: the records after it are usually fine, and an export
    /// that stops at the first bad turn leaves the operator with no trace and
    /// no idea which turn was bad.
    async fn deliver(&self, spans: &Spans) -> usize {
        let documents = spans.documents();
        let total = documents.len();
        let mut failed = 0;
        for (index, document) in documents.iter().enumerate() {
            let outcome = match self {
                Self::Print => print_json(document),
                Self::Post(sink) => sink.post(document).await,
            };
            if let Err(error) = outcome {
                eprintln!("trace: batch {} of {total} failed: {error}", index + 1);
                failed += 1;
            }
        }
        failed
    }
}

/// An OTLP/HTTP endpoint and the client that talks to it.
pub struct Sink {
    client: reqwest::Client,
    url: String,
}

impl Sink {
    pub fn new(endpoint: &str, headers: &[(String, String)]) -> Result<Self> {
        let mut map = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            let header = reqwest::header::HeaderName::try_from(name.as_str())
                .with_context(|| format!("`{name}` is not a valid header name"))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .with_context(|| format!("the value of `{name}` is not a valid header value"))?;
            // `insert` rather than `append`: a --header repeating a name from
            // the environment is somebody overriding it, not adding a second
            // copy of an API key.
            map.insert(header, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(map)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("could not build the OTLP client")?;
        Ok(Self {
            client,
            url: traces_url(endpoint),
        })
    }

    /// Post one document, retrying only what is worth retrying.
    ///
    /// 429 and 5xx mean "not now"; everything else in the 4xx range means the
    /// document, the URL or the credentials are wrong, and repeating the
    /// request cannot change that. Failing fast on those is what keeps a
    /// misconfigured endpoint from taking four times as long to tell you.
    async fn post(&self, document: &Value) -> Result<()> {
        let mut attempt = 1_u32;
        loop {
            let response = self
                .client
                .post(&self.url)
                .json(document)
                .send()
                .await
                .with_context(|| format!("could not reach {}", self.url))?;
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let advised = retry_after(response.headers());
            let detail = one_line(&response.text().await.unwrap_or_default());
            if status.as_u16() != 429 && !status.is_server_error() {
                bail!("{} rejected the batch: {status} {detail}", self.url);
            }
            if attempt >= MAX_ATTEMPTS {
                bail!(
                    "{} returned {status} on all {MAX_ATTEMPTS} attempts: {detail}",
                    self.url
                );
            }
            let wait = advised.unwrap_or_else(|| backoff(attempt));
            eprintln!(
                "trace: {} returned {status}, retrying in {} ms",
                self.url,
                wait.as_millis()
            );
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }
}

/// One pass over everything the filter matches.
pub async fn export(trace: &Trace, filter: &Filter, egress: &Egress) -> Result<()> {
    let rows = trace.rows(filter, MAX_ROWS)?;
    if rows.len() == MAX_ROWS {
        eprintln!("trace: stopped at {MAX_ROWS} records; narrow --since, --run or --turn");
    }
    let spans = Spans::from_rows(&rows);
    if spans.is_empty() {
        eprintln!("trace: nothing matched, so nothing was exported");
        return Ok(());
    }
    let failed = egress.deliver(&spans).await;
    eprintln!(
        "trace: {} records mapped to {} spans",
        rows.len(),
        spans.len()
    );
    if failed > 0 {
        bail!("{failed} batches could not be delivered");
    }
    Ok(())
}

/// Keep exporting as records land, until Ctrl-C.
///
/// The records already in the database go through the same buffer as the new
/// ones instead of being exported up front: a window that ends mid-turn would
/// otherwise emit that turn's root span immediately and then emit it a second
/// time when its remaining records arrived, which a backend reads as two
/// conflicting spans with one id.
pub async fn follow(trace: &Trace, filter: &Filter, egress: &Egress) -> Result<()> {
    let mut pending = Pending::default();
    let rows = trace.rows(filter, MAX_ROWS)?;
    let mut cursor = rows.last().map_or(0, |row| row.id);
    let mut batch = pending.absorb(rows);
    batch.extend(pending.drain_ready());
    let mut failed = deliver(egress, &batch).await;

    loop {
        tokio::select! {
            () = tokio::time::sleep(POLL) => {
                failed += step(trace, filter, egress, &mut pending, &mut cursor).await;
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    eprintln!("trace: could not listen for Ctrl-C: {error}");
                }
                break;
            }
        }
    }

    // An interrupted follow still owns records nobody else will export.
    failed += deliver(egress, &pending.take_all()).await;
    if failed > 0 {
        bail!("{failed} batches could not be delivered");
    }
    Ok(())
}

/// Start the live exporter the collector feeds while it is serving.
///
/// The returned sender is a doorbell, not a queue of work: the exporter reads
/// the records it exports out of the store itself, so ingest hands over a
/// zero-sized wake-up it can throw away under load and never waits for a
/// request to a backend that may be slow, far away or down.
pub fn spawn_live(trace: Arc<Trace>, egress: Egress) -> Result<mpsc::Sender<()>> {
    // Only what arrives from here on: the history is what `export` is for, and
    // re-sending it on every collector restart would duplicate every span.
    let mut cursor = trace
        .rows(&Filter::default(), 1)?
        .last()
        .map_or(0, |row| row.id);
    let (wake, mut woken) = mpsc::channel::<()>(WAKE_DEPTH);
    tokio::spawn(async move {
        let filter = Filter::default();
        let mut pending = Pending::default();
        loop {
            let ingesting = tokio::select! {
                doorbell = woken.recv() => doorbell.is_some(),
                () = tokio::time::sleep(POLL) => true,
            };
            step(&trace, &filter, &egress, &mut pending, &mut cursor).await;
            if !ingesting {
                // Every sender is gone, so the listener has stopped and no
                // record will ever arrive again. Flush what is held back.
                deliver(&egress, &pending.take_all()).await;
                break;
            }
        }
    });
    Ok(wake)
}

/// One poll: read what is new, buffer it by turn, deliver what is ready.
async fn step(
    trace: &Trace,
    filter: &Filter,
    egress: &Egress,
    pending: &mut Pending,
    cursor: &mut i64,
) -> usize {
    let rows = match trace.rows_after(*cursor, filter, FOLLOW_BATCH) {
        Ok(rows) => rows,
        // A read that fails is almost always a busy WAL. Neither a live
        // collector nor a follow session is allowed to end over one.
        Err(error) => {
            eprintln!("trace: could not read new records: {error}");
            return 1;
        }
    };
    if let Some(last) = rows.last() {
        *cursor = last.id;
    }
    let mut batch = pending.absorb(rows);
    batch.extend(pending.drain_ready());
    deliver(egress, &batch).await
}

async fn deliver(egress: &Egress, rows: &[Row]) -> usize {
    if rows.is_empty() {
        return 0;
    }
    let spans = Spans::from_rows(rows);
    if spans.is_empty() {
        return 0;
    }
    egress.deliver(&spans).await
}

/// Records held back until their turn is over.
#[derive(Default)]
struct Pending {
    turns: HashMap<String, Bucket>,
}

struct Bucket {
    rows: Vec<Row>,
    closed: bool,
    last: Instant,
}

impl Pending {
    /// Take in a poll's records and hand back the ones that need no waiting.
    fn absorb(&mut self, rows: Vec<Row>) -> Vec<Row> {
        let mut ready = Vec::new();
        for row in rows {
            match row.turn.clone() {
                Some(turn) => {
                    let bucket = self.turns.entry(turn).or_insert_with(|| Bucket {
                        rows: Vec::new(),
                        closed: false,
                        last: Instant::now(),
                    });
                    bucket.closed |= matches!(row.kind.as_str(), "turn_finished" | "turn_failed");
                    bucket.last = Instant::now();
                    bucket.rows.push(row);
                }
                // A record outside a turn is its own trace, so there is
                // nothing to wait for.
                None => ready.push(row),
            }
        }
        ready
    }

    fn drain_ready(&mut self) -> Vec<Row> {
        let done: Vec<String> = self
            .turns
            .iter()
            .filter(|(_, bucket)| bucket.closed || bucket.last.elapsed() >= QUIET)
            .map(|(turn, _)| turn.clone())
            .collect();
        let mut rows = Vec::new();
        for turn in done {
            if let Some(bucket) = self.turns.remove(&turn) {
                rows.extend(bucket.rows);
            }
        }
        rows
    }

    fn take_all(&mut self) -> Vec<Row> {
        let mut rows = Vec::new();
        for (_, bucket) in self.turns.drain() {
            rows.extend(bucket.rows);
        }
        rows.sort_by_key(|row| row.id);
        rows
    }
}

/// Spans mapped out of store rows, with the per-run facts an OTLP resource
/// needs but no single span carries.
pub struct Spans {
    spans: Vec<Span>,
    /// Run id to `service.version`, learned from `process_started`.
    versions: HashMap<String, String>,
}

impl Spans {
    #[must_use]
    pub fn from_rows(rows: &[Row]) -> Self {
        let mut versions = HashMap::new();
        for row in rows {
            if row.kind == "process_started"
                && let Some(version) = text(&row.body, "version")
            {
                versions.insert(row.run.clone(), version.to_owned());
            }
        }

        // Grouped by turn, in the order the turns first appear, so the output
        // reads chronologically for a human checking `--dry-run`.
        let mut order: Vec<&str> = Vec::new();
        let mut turns: HashMap<&str, Vec<&Row>> = HashMap::new();
        let mut loose: Vec<&Row> = Vec::new();
        for row in rows {
            match row.turn.as_deref() {
                Some(turn) => turns
                    .entry(turn)
                    .or_insert_with(|| {
                        order.push(turn);
                        Vec::new()
                    })
                    .push(row),
                None => loose.push(row),
            }
        }

        let mut spans = Vec::new();
        for turn in order {
            if let Some(rows) = turns.get(turn) {
                spans.extend(turn_spans(turn, rows));
            }
        }
        for row in loose {
            spans.push(loose_span(row));
        }
        Self { spans, versions }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// One OTLP document per request, each holding at most
    /// [`SPANS_PER_REQUEST`] spans.
    #[must_use]
    pub fn documents(&self) -> Vec<Value> {
        self.spans
            .chunks(SPANS_PER_REQUEST)
            .map(|chunk| document(chunk, &self.versions))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Mapping
// ---------------------------------------------------------------------------

const ROOT_NAME: &str = "invoke_agent";
const TOOL_NAME: &str = "execute_tool";

/// A span before it becomes JSON. Times stay in the store's milliseconds and
/// are widened once, at serialization, so the mapping never has to think about
/// nanosecond arithmetic.
struct Span {
    run: String,
    source: String,
    pid: u32,
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    start_ms: i64,
    end_ms: i64,
    attrs: Attrs,
    status: Status,
    events: Vec<Event>,
}

enum Status {
    Unset,
    Ok,
    Error(String),
}

struct Event {
    name: String,
    ts_ms: i64,
    attrs: Attrs,
}

impl Span {
    fn new(
        row: &Row,
        trace_id: &str,
        span_id: String,
        parent: Option<&str>,
        name: impl Into<String>,
    ) -> Self {
        let mut attrs = Attrs::default();
        // The one attribute every span has: with it, any span in a backend can
        // be taken back to `starkbot-trace tail` and the row it came from.
        attrs.int("starkbot.record_id", Some(row.id));
        Self {
            run: row.run.clone(),
            source: row.source.clone(),
            pid: row.pid,
            trace_id: trace_id.to_owned(),
            span_id,
            parent_span_id: parent.map(str::to_owned),
            name: name.into(),
            start_ms: row.ts_ms,
            end_ms: row.ts_ms,
            attrs,
            status: Status::Unset,
            events: Vec::new(),
        }
    }

    /// A record is stamped when its work finished and carries how long that
    /// took, so the start is the only end that has to be reconstructed. A
    /// record with no duration keeps `start == end`: a zero-length span is
    /// odd-looking, and losing the record is worse.
    fn timed(mut self, duration_ms: Option<i64>) -> Self {
        if let Some(duration) = duration_ms {
            self.start_ms = self.end_ms.saturating_sub(duration);
        }
        self
    }
}

/// A navigator run's span and the window it owns, so its decisions can be
/// hung under it.
struct Enclosing {
    surface: String,
    start_ms: i64,
    end_ms: i64,
    span_id: String,
}

/// Every span of one turn: one root, everything else beneath it.
fn turn_spans(turn: &str, rows: &[&Row]) -> Vec<Span> {
    let Some(first) = rows.first().copied() else {
        return Vec::new();
    };
    let trace_id = trace_id_for_turn(turn);
    let root_id = span_id(&format!("turn:{turn}"));

    let started = find(rows, "turn_started");
    let finished = find(rows, "turn_finished");
    let failed = find(rows, "turn_failed");
    // A `--since` window can start inside a turn, and then there is no
    // `turn_started` to anchor on. The root is built anyway: without it the
    // records that did make the window would be a handful of parentless spans.
    let anchor = started.unwrap_or(first);
    let mut root = Span::new(anchor, &trace_id, root_id.clone(), None, ROOT_NAME);
    root.start_ms = first.ts_ms;
    root.end_ms = rows.last().map_or(first.ts_ms, |row| row.ts_ms);
    root.attrs.text("gen_ai.operation.name", Some(ROOT_NAME));
    root.attrs.text("starkbot.run", Some(&anchor.run));
    root.attrs.text("starkbot.source", Some(&anchor.source));
    root.attrs.text("starkbot.turn", Some(turn));
    if let Some(row) = finished {
        root.attrs.int("starkbot.steps", count(&row.body, "steps"));
        root.attrs
            .flag("starkbot.exhausted", flag(&row.body, "exhausted"));
        root.attrs.flag("starkbot.asked", flag(&row.body, "asked"));
    }
    root.status = match (failed, finished) {
        (Some(row), _) => Status::Error(
            text(&row.body, "message")
                .unwrap_or("the turn failed")
                .to_owned(),
        ),
        (None, Some(_)) => Status::Ok,
        (None, None) => Status::Unset,
    };

    // Two passes, because a record's children can be stored before it is. A
    // `surface_run` is written when the run ends, so every `jev_step` it
    // encloses is already in the table by the time its parent shows up.
    let mut finishes: HashMap<i64, &Row> = HashMap::new();
    let mut enclosing: Vec<Enclosing> = Vec::new();
    for row in rows {
        match row.kind.as_str() {
            "turn_step_finished" => {
                if let Some(index) = count(&row.body, "index") {
                    finishes.insert(index, row);
                }
            }
            "surface_run" => enclosing.push(Enclosing {
                surface: text(&row.body, "surface").unwrap_or_default().to_owned(),
                start_ms: row
                    .ts_ms
                    .saturating_sub(count(&row.body, "duration_ms").unwrap_or(0)),
                end_ms: row.ts_ms,
                span_id: span_id(&format!("record:{}", row.id)),
            }),
            _ => {}
        }
    }

    let mut children = Vec::new();
    let mut paired: Vec<i64> = Vec::new();
    for row in rows {
        match row.kind.as_str() {
            "turn_started" | "turn_finished" | "turn_failed" => {}
            "turn_step" => {
                let index = count(&row.body, "index");
                let finish = index.and_then(|index| finishes.get(&index).copied());
                if let (Some(index), Some(_)) = (index, finish) {
                    paired.push(index);
                }
                children.push(step_span(row, finish, turn, &trace_id, Some(&root_id)));
            }
            "turn_step_finished" => {
                // An observation whose `turn_step` never made the window still
                // describes something the agent did.
                if !count(&row.body, "index").is_some_and(|index| paired.contains(&index)) {
                    children.push(orphan_finish_span(row, &trace_id, Some(&root_id)));
                }
            }
            "inference" => children.push(inference_span(row, &trace_id, Some(&root_id))),
            "surface_run" => children.push(surface_span(row, &trace_id, Some(&root_id))),
            "jev_step" => {
                let parent = enclose(&enclosing, text(&row.body, "surface"), row.ts_ms);
                children.push(jev_span(row, &trace_id, Some(parent.unwrap_or(&root_id))));
            }
            "app_event" | "log" | "process_started" => root.events.push(record_event(row)),
            // A body this build has no mapping for is still a thing that
            // happened at a time, inside this turn.
            _ => children.push(other_span(row, &trace_id, Some(&root_id))),
        }
    }

    let mut spans = Vec::with_capacity(children.len() + 1);
    spans.push(root);
    spans.extend(children);
    spans
}

/// A record with no turn: its own trace, so it neither disappears nor collides
/// with every other loose record in the run.
fn loose_span(row: &Row) -> Span {
    let trace_id = trace_id_for_record(&row.run, row.id);
    match row.kind.as_str() {
        "inference" => inference_span(row, &trace_id, None),
        "surface_run" => surface_span(row, &trace_id, None),
        "jev_step" => jev_span(row, &trace_id, None),
        _ => other_span(row, &trace_id, None),
    }
}

fn step_span(
    row: &Row,
    finish: Option<&Row>,
    turn: &str,
    trace_id: &str,
    parent: Option<&str>,
) -> Span {
    let index = count(&row.body, "index");
    let id = match index {
        Some(index) => span_id(&format!("step:{turn}:{index}")),
        None => span_id(&format!("record:{}", row.id)),
    };
    let mut span = Span::new(row, trace_id, id, parent, TOOL_NAME);
    // The step's own record is written when the model chose the action, and
    // the finish when the action returned, so the pair brackets the work. A
    // turn that ended on its last step has no finish; the step is still a
    // thing the agent decided to do.
    span.end_ms = finish.map_or(row.ts_ms, |finish| finish.ts_ms);
    span.attrs
        .text("gen_ai.tool.name", text(&row.body, "action"));
    if let Some(index) = index {
        span.attrs
            .text("gen_ai.tool.call.id", Some(&index.to_string()));
    }
    span.attrs
        .text("starkbot.target", text(&row.body, "target"));
    span.attrs.text("starkbot.goal", text(&row.body, "goal"));
    span.attrs
        .text("starkbot.thought", text(&row.body, "thought"));
    if let Some(finish) = finish {
        span.attrs
            .text("starkbot.observation", text(&finish.body, "observation"));
    }
    span
}

fn orphan_finish_span(row: &Row, trace_id: &str, parent: Option<&str>) -> Span {
    let mut span = Span::new(
        row,
        trace_id,
        span_id(&format!("record:{}", row.id)),
        parent,
        TOOL_NAME,
    )
    .timed(count(&row.body, "duration_ms"));
    span.attrs
        .text("gen_ai.tool.name", text(&row.body, "action"));
    if let Some(index) = count(&row.body, "index") {
        span.attrs
            .text("gen_ai.tool.call.id", Some(&index.to_string()));
    }
    span.attrs
        .text("starkbot.observation", text(&row.body, "observation"));
    span
}

fn inference_span(row: &Row, trace_id: &str, parent: Option<&str>) -> Span {
    let model = text(&row.body, "model").unwrap_or("unknown");
    let mut span = Span::new(
        row,
        trace_id,
        span_id(&format!("record:{}", row.id)),
        parent,
        format!("chat {model}"),
    )
    .timed(count(&row.body, "duration_ms"));
    span.attrs.text("gen_ai.operation.name", Some("chat"));
    span.attrs.text(
        "gen_ai.system",
        Some(&ai_system(text(&row.body, "provider").unwrap_or_default())),
    );
    span.attrs.text("gen_ai.request.model", Some(model));
    let usage = row.body.get("usage");
    span.attrs.int(
        "gen_ai.usage.input_tokens",
        tokens(usage, &["input_tokens", "prompt_tokens"]),
    );
    span.attrs.int(
        "gen_ai.usage.output_tokens",
        tokens(usage, &["output_tokens", "completion_tokens"]),
    );
    span.attrs
        .flag("starkbot.json_mode", flag(&row.body, "json"));
    span.attrs
        .int("starkbot.prompt_chars", count(&row.body, "prompt_chars"));
    span.status = outcome(row, "the inference failed");
    span
}

fn surface_span(row: &Row, trace_id: &str, parent: Option<&str>) -> Span {
    let surface = text(&row.body, "surface").unwrap_or("surface");
    let mut span = Span::new(
        row,
        trace_id,
        span_id(&format!("record:{}", row.id)),
        parent,
        format!("navigate {surface}"),
    )
    .timed(count(&row.body, "duration_ms"));
    span.attrs.text("starkbot.surface", Some(surface));
    span.attrs
        .text("starkbot.target", text(&row.body, "target"));
    span.attrs.text("starkbot.goal", text(&row.body, "goal"));
    span.attrs
        .text("starkbot.outcome", text(&row.body, "outcome"));
    span.attrs.int("starkbot.steps", count(&row.body, "steps"));
    span.status = outcome(row, "the navigator run failed");
    span
}

fn jev_span(row: &Row, trace_id: &str, parent: Option<&str>) -> Span {
    let operation = text(&row.body, "operation").unwrap_or("step");
    let mut span = Span::new(
        row,
        trace_id,
        span_id(&format!("record:{}", row.id)),
        parent,
        format!("jev {operation}"),
    )
    .timed(count(&row.body, "elapsed_ms"));
    span.attrs.text("starkbot.operation", Some(operation));
    span.attrs
        .double("starkbot.confidence", number(&row.body, "confidence"));
    span.attrs
        .int("starkbot.candidates", count(&row.body, "candidates"));
    span.attrs.flag("starkbot.stale", flag(&row.body, "stale"));
    span.attrs.text("starkbot.label", text(&row.body, "label"));
    span.attrs
        .int("starkbot.typed_chars", count(&row.body, "typed_chars"));
    // The split is the whole reason `jev_step` exists: it says whether the
    // eleven seconds went to looking at the page, deciding, or clicking.
    for phase in ["observe_ms", "jev_ms", "text_ms", "act_ms"] {
        span.attrs
            .int(&format!("starkbot.{phase}"), count(&row.body, phase));
    }
    span
}

/// The fallback: a span named after the record kind, carrying the body.
fn other_span(row: &Row, trace_id: &str, parent: Option<&str>) -> Span {
    let mut span = Span::new(
        row,
        trace_id,
        span_id(&format!("record:{}", row.id)),
        parent,
        row.kind.clone(),
    )
    .timed(count(&row.body, "duration_ms"));
    span.attrs.absorb(body_attrs(row));
    span
}

/// An `app_event`, a `log` or a `process_started` inside a turn. These are
/// moments, not work: a span with no duration for each would bury the steps
/// that matter, and a span event is exactly what a backend draws as a mark on
/// the turn's own bar.
fn record_event(row: &Row) -> Event {
    let name = match row.kind.as_str() {
        "app_event" => match row
            .body
            .get("event")
            .and_then(|event| event.get("type"))
            .and_then(Value::as_str)
        {
            Some(kind) => format!("app_event.{kind}"),
            None => "app_event".to_owned(),
        },
        kind => kind.to_owned(),
    };
    let mut attrs = Attrs::default();
    attrs.int("starkbot.record_id", Some(row.id));
    attrs.absorb(body_attrs(row));
    Event {
        name,
        ts_ms: row.ts_ms,
        attrs,
    }
}

/// Whatever the body holds, under `starkbot.`. An `app_event` is unwrapped one
/// level: the interesting fields are inside `event`, and `starkbot.event` as
/// one JSON string would be a blob nobody can filter on.
fn body_attrs(row: &Row) -> Attrs {
    let mut attrs = Attrs::default();
    let source = match row.body.get("event") {
        Some(event) if row.kind == "app_event" => event,
        _ => &row.body,
    };
    if let Some(fields) = source.as_object() {
        for (key, value) in fields {
            if key == "kind" {
                continue;
            }
            attrs.json(&format!("starkbot.{key}"), value);
        }
    }
    attrs
}

/// The `ok`/`error` pair every record that can fail carries.
fn outcome(row: &Row, fallback: &str) -> Status {
    match flag(&row.body, "ok") {
        Some(false) => Status::Error(text(&row.body, "error").unwrap_or(fallback).to_owned()),
        Some(true) => Status::Ok,
        None => Status::Unset,
    }
}

/// The navigator run whose window holds `ts_ms` on the same surface. Searched
/// newest first because two runs against the same surface in one turn are
/// normal and the later one is the one still open.
fn enclose<'a>(runs: &'a [Enclosing], surface: Option<&str>, ts_ms: i64) -> Option<&'a str> {
    let surface = surface?;
    runs.iter()
        .rev()
        .find(|run| run.surface == surface && run.start_ms <= ts_ms && ts_ms <= run.end_ms)
        .map(|run| run.span_id.as_str())
}

fn find<'a>(rows: &[&'a Row], kind: &str) -> Option<&'a Row> {
    rows.iter().copied().find(|row| row.kind == kind)
}

/// The GenAI convention's name for the provider. A provider string is whatever
/// Neo was configured with, and a backend that special-cases `anthropic` gets
/// nothing out of `anthropic-messages-v1`.
fn ai_system(provider: &str) -> String {
    let lowered = provider.to_ascii_lowercase();
    if lowered.contains("anthropic") || lowered.contains("claude") {
        "anthropic".to_owned()
    } else if lowered.contains("openai") || lowered.contains("codex") {
        "openai".to_owned()
    } else {
        provider.to_owned()
    }
}

/// Providers disagree about what the token fields are called, and a usage
/// object missing one is normal. A token count that is absent stays absent: a
/// zero would be read as "this call used no tokens", which is a different and
/// false claim.
fn tokens(usage: Option<&Value>, names: &[&str]) -> Option<i64> {
    let usage = usage?;
    names
        .iter()
        .find_map(|name| usage.get(*name).and_then(Value::as_i64))
}

fn text<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

fn count(body: &Value, key: &str) -> Option<i64> {
    body.get(key).and_then(Value::as_i64)
}

fn number(body: &Value, key: &str) -> Option<f64> {
    body.get(key).and_then(Value::as_f64)
}

fn flag(body: &Value, key: &str) -> Option<bool> {
    body.get(key).and_then(Value::as_bool)
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A second basis, so the two halves of a trace id are not the same number
/// twice. The golden-ratio constant, for no reason beyond it being a
/// well-mixed value that is not zero.
const FNV_ALT: u64 = 0x9e37_79b9_7f4a_7c15;

/// FNV-1a, because the ids only have to be stable and distinct.
///
/// Deriving them from the record instead of generating them means exporting
/// the same turn twice produces the same trace, so a re-run after a collector
/// outage updates a trace rather than duplicating it. That needs a hash, not a
/// secure hash: nothing here defends against an adversary choosing turn ids,
/// and a cryptographic dependency to name a few thousand spans would be paid
/// for on every build.
fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn trace_id_for_turn(turn: &str) -> String {
    trace_id(turn.as_bytes())
}

/// A record outside a turn gets a trace of its own rather than joining a
/// per-run trace: a run is a whole process, and a backend shown one trace
/// holding a day of unrelated log lines cannot be used for anything.
fn trace_id_for_record(run: &str, id: i64) -> String {
    trace_id(format!("{run}:{id}").as_bytes())
}

fn trace_id(bytes: &[u8]) -> String {
    let high = fnv1a(FNV_OFFSET, bytes);
    let low = fnv1a(FNV_OFFSET ^ FNV_ALT, bytes);
    // OTLP rejects an all-zero trace id, which is the one value that means
    // "absent" rather than "this trace".
    let low = if high == 0 && low == 0 { 1 } else { low };
    format!("{high:016x}{low:016x}")
}

fn span_id(key: &str) -> String {
    let id = fnv1a(FNV_OFFSET, key.as_bytes());
    let id = if id == 0 { 1 } else { id };
    format!("{id:016x}")
}

// ---------------------------------------------------------------------------
// OTLP/JSON
// ---------------------------------------------------------------------------

/// An attribute value, in the four shapes OTLP has a key for.
enum Attr {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

/// An attribute list under construction. Every setter takes an `Option` and
/// drops `None`: an attribute that is not there says "this record did not
/// carry one", while an attribute set to a zero or an empty string says
/// something false.
#[derive(Default)]
struct Attrs(Vec<(String, Attr)>);

impl Attrs {
    fn text(&mut self, key: &str, value: Option<&str>) {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            self.0.push((key.to_owned(), Attr::Str(value.to_owned())));
        }
    }

    fn int(&mut self, key: &str, value: Option<i64>) {
        if let Some(value) = value {
            self.0.push((key.to_owned(), Attr::Int(value)));
        }
    }

    fn double(&mut self, key: &str, value: Option<f64>) {
        if let Some(value) = value {
            self.0.push((key.to_owned(), Attr::Double(value)));
        }
    }

    fn flag(&mut self, key: &str, value: Option<bool>) {
        if let Some(value) = value {
            self.0.push((key.to_owned(), Attr::Bool(value)));
        }
    }

    /// A field out of a record body, whose type is whatever the producer put
    /// there. Anything that is not a scalar keeps its JSON: an attribute
    /// cannot hold an object, and dropping it would lose the field.
    fn json(&mut self, key: &str, value: &Value) {
        match value {
            Value::Null => {}
            Value::Bool(value) => self.0.push((key.to_owned(), Attr::Bool(*value))),
            Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    self.0.push((key.to_owned(), Attr::Int(value)));
                } else if let Some(value) = number.as_f64() {
                    self.0.push((key.to_owned(), Attr::Double(value)));
                }
            }
            Value::String(value) => self.text(key, Some(value)),
            other => self.0.push((key.to_owned(), Attr::Str(other.to_string()))),
        }
    }

    fn absorb(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    fn json_value(&self) -> Value {
        Value::Array(
            self.0
                .iter()
                .map(|(key, value)| {
                    let (tag, value) = match value {
                        Attr::Str(value) => ("stringValue", Value::String(value.clone())),
                        Attr::Int(value) => {
                            // OTLP/JSON carries a 64-bit integer as a string:
                            // a JSON number is a double, and a token count or
                            // a rowid past 2^53 would come out changed.
                            ("intValue", Value::String(value.to_string()))
                        }
                        Attr::Double(value) => (
                            "doubleValue",
                            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number),
                        ),
                        Attr::Bool(value) => ("boolValue", Value::Bool(*value)),
                    };
                    let mut object = Map::new();
                    object.insert("key".to_owned(), Value::String(key.clone()));
                    object.insert("value".to_owned(), Value::Object(one(tag, value)));
                    Value::Object(object)
                })
                .collect(),
        )
    }
}

fn one(key: &str, value: Value) -> Map<String, Value> {
    let mut object = Map::new();
    object.insert(key.to_owned(), value);
    object
}

/// Grouped by run, because a resource in OTLP is one producer: two Neo
/// processes have different pids and possibly different versions, and merging
/// them into one resource would attribute one's spans to the other.
fn document(spans: &[Span], versions: &HashMap<String, String>) -> Value {
    let mut order: Vec<&str> = Vec::new();
    let mut groups: HashMap<&str, Vec<&Span>> = HashMap::new();
    for span in spans {
        groups
            .entry(span.run.as_str())
            .or_insert_with(|| {
                order.push(span.run.as_str());
                Vec::new()
            })
            .push(span);
    }

    let resource_spans: Vec<Value> = order
        .into_iter()
        .filter_map(|run| {
            let group = groups.get(run)?;
            let first = group.first()?;
            let mut attrs = Attrs::default();
            attrs.text("service.name", Some(&first.source));
            attrs.text("service.version", versions.get(run).map(String::as_str));
            attrs.int("process.pid", Some(i64::from(first.pid)));
            attrs.text("starkbot.run", Some(run));

            let mut resource = Map::new();
            resource.insert("attributes".to_owned(), attrs.json_value());

            let mut scope = Map::new();
            scope.insert(
                "name".to_owned(),
                Value::String(env!("CARGO_PKG_NAME").to_owned()),
            );
            scope.insert(
                "version".to_owned(),
                Value::String(env!("CARGO_PKG_VERSION").to_owned()),
            );

            let mut scope_spans = Map::new();
            scope_spans.insert("scope".to_owned(), Value::Object(scope));
            scope_spans.insert(
                "spans".to_owned(),
                Value::Array(group.iter().map(|span| span_value(span)).collect()),
            );

            let mut entry = Map::new();
            entry.insert("resource".to_owned(), Value::Object(resource));
            entry.insert(
                "scopeSpans".to_owned(),
                Value::Array(vec![Value::Object(scope_spans)]),
            );
            Some(Value::Object(entry))
        })
        .collect();

    Value::Object(one("resourceSpans", Value::Array(resource_spans)))
}

/// `kind` is always `SPAN_KIND_INTERNAL`: these spans describe an agent's own
/// work, not a request it served or issued on the wire.
const SPAN_KIND_INTERNAL: i64 = 1;

fn span_value(span: &Span) -> Value {
    let mut object = Map::new();
    object.insert("traceId".to_owned(), Value::String(span.trace_id.clone()));
    object.insert("spanId".to_owned(), Value::String(span.span_id.clone()));
    if let Some(parent) = &span.parent_span_id {
        object.insert("parentSpanId".to_owned(), Value::String(parent.clone()));
    }
    object.insert("name".to_owned(), Value::String(span.name.clone()));
    object.insert("kind".to_owned(), Value::from(SPAN_KIND_INTERNAL));
    object.insert(
        "startTimeUnixNano".to_owned(),
        Value::String(nanos(span.start_ms)),
    );
    object.insert(
        "endTimeUnixNano".to_owned(),
        Value::String(nanos(span.end_ms)),
    );
    object.insert("attributes".to_owned(), span.attrs.json_value());
    if !span.events.is_empty() {
        object.insert(
            "events".to_owned(),
            Value::Array(
                span.events
                    .iter()
                    .map(|event| {
                        let mut value = Map::new();
                        value.insert("name".to_owned(), Value::String(event.name.clone()));
                        value.insert("timeUnixNano".to_owned(), Value::String(nanos(event.ts_ms)));
                        value.insert("attributes".to_owned(), event.attrs.json_value());
                        Value::Object(value)
                    })
                    .collect(),
            ),
        );
    }
    match &span.status {
        Status::Unset => {}
        Status::Ok => {
            object.insert(
                "status".to_owned(),
                Value::Object(one("code", Value::from(1))),
            );
        }
        Status::Error(message) => {
            let mut status = Map::new();
            status.insert("code".to_owned(), Value::from(2));
            status.insert("message".to_owned(), Value::String(one_line(message)));
            object.insert("status".to_owned(), Value::Object(status));
        }
    }
    Value::Object(object)
}

/// Nanoseconds since the epoch, as a string. A negative timestamp is a corrupt
/// record rather than a time before 1970, and zero is the honest reading.
fn nanos(ms: i64) -> String {
    let ms = u64::try_from(ms).unwrap_or_default();
    format!("{}", ms.saturating_mul(1_000_000))
}

// ---------------------------------------------------------------------------
// Transport details
// ---------------------------------------------------------------------------

/// The endpoint is the base URL, the way every other OTLP exporter takes it,
/// but a person who has read the spec will paste the full path. Accepting both
/// costs a comparison and saves an afternoon.
fn traces_url(endpoint: &str) -> String {
    let base = endpoint.trim().trim_end_matches('/');
    if base.ends_with("/v1/traces") {
        base.to_owned()
    } else {
        format!("{base}/v1/traces")
    }
}

/// `k: v`, the way it appears in a request.
fn parse_header(header: &str) -> Result<(String, String)> {
    let (name, value) = header
        .split_once(':')
        .ok_or_else(|| anyhow!("`{header}` is not a header: expected `name: value`"))?;
    let name = name.trim();
    if name.is_empty() {
        return Err(anyhow!("`{header}` has no header name"));
    }
    Ok((name.to_owned(), value.trim().to_owned()))
}

/// `OTEL_EXPORTER_OTLP_HEADERS`, in the `k=v,k2=v2` form the specification
/// defines. A malformed entry is skipped rather than fatal: the variable is
/// often set for a whole shell session by something else entirely, and
/// refusing to export because of it would be this tool's opinion of another
/// tool's configuration.
fn env_headers() -> Vec<(String, String)> {
    let Ok(raw) = std::env::var(HEADERS_ENV) else {
        return Vec::new();
    };
    raw.split(',')
        .filter_map(|entry| {
            let (name, value) = entry.split_once('=')?;
            let name = name.trim();
            if name.is_empty() {
                return None;
            }
            Some((name.to_owned(), value.trim().to_owned()))
        })
        .collect()
}

/// `Retry-After` in its delay-seconds form. The HTTP-date form is not read: no
/// OTLP receiver sends it, and misreading a date as a delay would be worse
/// than falling back to the backoff this code already has.
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?;
    let seconds = value.to_str().ok()?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds).min(RETRY_AFTER_CAP))
}

/// Exponential, with jitter over the lower half of the window. Two collectors
/// pointed at the same endpoint would otherwise recover in lockstep and
/// re-create the overload they are backing off from.
fn backoff(attempt: u32) -> Duration {
    let window = BACKOFF_BASE.saturating_mul(1_u32 << attempt.min(6).saturating_sub(1));
    let half = window / 2;
    half + Duration::from_millis(jitter(u64::try_from(half.as_millis()).unwrap_or(1)))
}

/// Not randomness so much as a number nobody can predict from the outside,
/// which is all a backoff needs. A dependency for this would be worse than the
/// modulo.
fn jitter(span_ms: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| u64::from(since.subsec_nanos()));
    nanos % span_ms.max(1)
}

/// An error body from a receiver can be a page of HTML. One line of it is
/// enough to tell a misconfigured path from a rejected document.
fn one_line(text: &str) -> String {
    let flattened: String = text
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    let trimmed = flattened.trim();
    if trimmed.chars().count() > 200 {
        trimmed.chars().take(200).collect::<String>() + "…"
    } else {
        trimmed.to_owned()
    }
}

/// The workspace denies `print_stdout`; `--dry-run` exists to put the document
/// on stdout for `jq`, so the allowance lives on this one function.
#[allow(clippy::print_stdout)]
fn print_json(document: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(document)?);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    // The mapping is the only thing worth pinning here: it is what a backend
    // sees, and it is the part that quietly changes meaning when a record kind
    // grows a field. The HTTP client is reqwest's problem.
    use neo_trace::{Body, Record, WIRE_VERSION};
    use serde_json::{Value, json};

    use super::{Spans, traces_url};
    use crate::store::{Filter, Trace};

    const RUN: &str = "0192f000-0000-7000-8000-000000000001";
    const TURN: &str = "0192f000-0000-7000-8000-0000000000aa";

    struct Fixture {
        _dir: tempfile::TempDir,
        trace: Trace,
        seq: u64,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("a temporary directory");
            let trace = Trace::open(&dir.path().join("trace.db")).expect("an open database");
            Self {
                _dir: dir,
                trace,
                seq: 0,
            }
        }

        fn push(&mut self, ts_ms: i64, turn: Option<&str>, body: Body) {
            self.seq += 1;
            let record = Record {
                v: WIRE_VERSION,
                seq: self.seq,
                ts_ms,
                run: RUN.to_owned(),
                source: "neo-cli".to_owned(),
                pid: 4242,
                turn: turn.map(str::to_owned),
                dropped: 0,
                body,
            };
            self.trace.insert(&record).expect("a stored record");
        }

        /// Through the store, not around it: the flat columns and the body are
        /// written by ingest, and a mapping tested against hand-made rows
        /// would not notice ingest changing either.
        fn document(&self) -> Value {
            let rows = self
                .trace
                .rows(&Filter::default(), 1_000)
                .expect("the stored rows");
            let mut documents = Spans::from_rows(&rows).documents();
            assert_eq!(documents.len(), 1, "the fixtures fit in one batch");
            documents.remove(0)
        }
    }

    fn spans(document: &Value) -> Vec<&Value> {
        document["resourceSpans"]
            .as_array()
            .expect("resourceSpans")
            .iter()
            .flat_map(|resource| {
                resource["scopeSpans"]
                    .as_array()
                    .expect("scopeSpans")
                    .iter()
                    .flat_map(|scope| scope["spans"].as_array().expect("spans").iter())
            })
            .collect()
    }

    fn named<'a>(spans: &[&'a Value], name: &str) -> &'a Value {
        spans
            .iter()
            .copied()
            .find(|span| span["name"] == json!(name))
            .unwrap_or_else(|| panic!("a span named {name}"))
    }

    fn attr(span: &Value, key: &str) -> Option<Value> {
        span["attributes"]
            .as_array()
            .expect("attributes")
            .iter()
            .find(|attribute| attribute["key"] == json!(key))
            .map(|attribute| attribute["value"].clone())
    }

    fn started() -> Body {
        Body::TurnStarted {
            user_text: "book a table".to_owned(),
            history_len: 2,
            max_steps: 8,
        }
    }

    fn inference(provider: &str, usage: Value, ok: bool, error: Option<&str>) -> Body {
        Body::Inference {
            provider: provider.to_owned(),
            model: "claude-opus-4".to_owned(),
            json: true,
            prompt_chars: 1_200,
            duration_ms: 900,
            usage,
            ok,
            error: error.map(str::to_owned),
        }
    }

    #[test]
    fn a_turn_collapses_into_one_root_span_with_its_work_beneath_it() {
        let mut fixture = Fixture::new();
        fixture.push(
            1_000,
            None,
            Body::ProcessStarted {
                version: "0.4.1".to_owned(),
                args: vec!["neo".to_owned()],
            },
        );
        fixture.push(2_000, Some(TURN), started());
        fixture.push(
            2_100,
            Some(TURN),
            inference(
                "anthropic",
                json!({"input_tokens": 31, "output_tokens": 7}),
                true,
                None,
            ),
        );
        fixture.push(
            2_200,
            Some(TURN),
            Body::TurnStep {
                index: 0,
                thought: "look it up".to_owned(),
                action: "browse".to_owned(),
                target: Some("example.com".to_owned()),
                goal: Some("find the booking form".to_owned()),
            },
        );
        fixture.push(
            2_500,
            Some(TURN),
            Body::JevStep {
                surface: "browser".to_owned(),
                target: "example.com".to_owned(),
                index: 0,
                operation: "click".to_owned(),
                confidence: 0.82,
                label: Some("Book now".to_owned()),
                typed_chars: None,
                candidates: 4,
                stale: false,
                observe_ms: 40,
                jev_ms: 60,
                text_ms: 0,
                act_ms: 30,
                elapsed_ms: 130,
                usage: json!({}),
            },
        );
        fixture.push(
            2_800,
            Some(TURN),
            Body::SurfaceRun {
                surface: "browser".to_owned(),
                target: "example.com".to_owned(),
                goal: "find the booking form".to_owned(),
                outcome: "found it".to_owned(),
                steps: 1,
                duration_ms: 600,
                ok: true,
                error: None,
            },
        );
        fixture.push(
            2_900,
            Some(TURN),
            Body::TurnStepFinished {
                index: 0,
                action: "browse".to_owned(),
                observation: "the form is open".to_owned(),
                duration_ms: 700,
            },
        );
        fixture.push(
            3_000,
            Some(TURN),
            Body::AppEvent {
                event: json!({"type": "turn_finished", "message": "done"}),
            },
        );
        fixture.push(
            3_100,
            Some(TURN),
            Body::TurnFinished {
                steps: 1,
                exhausted: false,
                asked: false,
                text: "booked".to_owned(),
                duration_ms: 1_100,
            },
        );

        let document = fixture.document();
        let spans = spans(&document);

        let roots: Vec<&&Value> = spans
            .iter()
            .filter(|span| span["name"] == json!("invoke_agent"))
            .collect();
        assert_eq!(roots.len(), 1, "one root span per turn");
        let root = named(&spans, "invoke_agent");
        let trace_id = root["traceId"].clone();
        let root_id = root["spanId"].clone();

        // The turn spans its own records, not the duration a single one
        // happened to report.
        assert_eq!(root["startTimeUnixNano"], json!("2000000000"));
        assert_eq!(root["endTimeUnixNano"], json!("3100000000"));
        assert_eq!(root["status"], json!({"code": 1}));
        assert_eq!(attr(root, "starkbot.steps"), Some(json!({"intValue": "1"})));
        assert_eq!(
            attr(root, "gen_ai.operation.name"),
            Some(json!({"stringValue": "invoke_agent"}))
        );

        let step = named(&spans, "execute_tool");
        assert_eq!(step["parentSpanId"], root_id);
        assert_eq!(step["traceId"], trace_id);
        assert_eq!(step["startTimeUnixNano"], json!("2200000000"));
        assert_eq!(step["endTimeUnixNano"], json!("2900000000"));
        assert_eq!(
            attr(step, "gen_ai.tool.name"),
            Some(json!({"stringValue": "browse"}))
        );
        assert_eq!(
            attr(step, "starkbot.observation"),
            Some(json!({"stringValue": "the form is open"}))
        );

        let chat = named(&spans, "chat claude-opus-4");
        assert_eq!(chat["parentSpanId"], root_id);
        assert_eq!(
            attr(&chat.clone(), "gen_ai.system"),
            Some(json!({"stringValue": "anthropic"}))
        );

        // A navigator decision belongs under the run that was open when it
        // happened, not under the turn.
        let navigate = named(&spans, "navigate browser");
        let jev = named(&spans, "jev click");
        assert_eq!(navigate["parentSpanId"], root_id);
        assert_eq!(jev["parentSpanId"], navigate["spanId"]);
        assert_eq!(
            attr(jev, "starkbot.observe_ms"),
            Some(json!({"intValue": "40"}))
        );
        assert_eq!(
            attr(jev, "starkbot.confidence"),
            Some(json!({"doubleValue": 0.82}))
        );

        for span in &spans {
            if span["name"] == json!("process_started") {
                continue;
            }
            assert_eq!(span["traceId"], trace_id, "one trace per turn");
        }

        // The app event is a mark on the turn, not a span of its own.
        let events = root["events"].as_array().expect("root events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["name"], json!("app_event.turn_finished"));
        assert!(
            !spans.iter().any(|span| span["name"] == json!("app_event")),
            "an in-turn app_event does not also become a span"
        );

        // The resource names the process the records came from.
        let resource = &document["resourceSpans"][0]["resource"];
        let version = resource["attributes"]
            .as_array()
            .expect("resource attributes")
            .iter()
            .find(|attribute| attribute["key"] == json!("service.version"))
            .map(|attribute| attribute["value"].clone());
        assert_eq!(version, Some(json!({"stringValue": "0.4.1"})));
    }

    #[test]
    fn a_failed_turn_and_a_failed_inference_carry_their_message() {
        let mut fixture = Fixture::new();
        fixture.push(1_000, Some(TURN), started());
        fixture.push(
            1_500,
            Some(TURN),
            inference("anthropic", json!({}), false, Some("529 overloaded")),
        );
        fixture.push(
            1_600,
            Some(TURN),
            Body::TurnFailed {
                code: "inference".to_owned(),
                message: "the model gave up".to_owned(),
                duration_ms: 600,
            },
        );

        let document = fixture.document();
        let spans = spans(&document);
        assert_eq!(
            named(&spans, "invoke_agent")["status"],
            json!({"code": 2, "message": "the model gave up"})
        );
        let chat = named(&spans, "chat claude-opus-4");
        assert_eq!(
            chat["status"],
            json!({"code": 2, "message": "529 overloaded"})
        );
        // A usage object with neither naming scheme leaves the counts off
        // rather than claiming the call was free.
        assert_eq!(attr(chat, "gen_ai.usage.input_tokens"), None);
    }

    #[test]
    fn token_attributes_read_both_naming_schemes() {
        let mut fixture = Fixture::new();
        fixture.push(1_000, Some(TURN), started());
        fixture.push(
            1_100,
            Some(TURN),
            inference(
                "anthropic",
                json!({"input_tokens": 120, "output_tokens": 34}),
                true,
                None,
            ),
        );
        fixture.push(
            1_200,
            Some(TURN),
            inference(
                "openai-codex",
                json!({"prompt_tokens": 800, "completion_tokens": 21}),
                true,
                None,
            ),
        );

        let document = fixture.document();
        let spans = spans(&document);
        let chats: Vec<&&Value> = spans
            .iter()
            .filter(|span| span["name"] == json!("chat claude-opus-4"))
            .collect();
        assert_eq!(chats.len(), 2);
        let mut inputs: Vec<Value> = chats
            .iter()
            .map(|span| attr(span, "gen_ai.usage.input_tokens").expect("input tokens"))
            .collect();
        inputs.sort_by_key(std::string::ToString::to_string);
        assert_eq!(
            inputs,
            vec![json!({"intValue": "120"}), json!({"intValue": "800"})]
        );
        let outputs: Vec<Option<Value>> = chats
            .iter()
            .map(|span| attr(span, "gen_ai.usage.output_tokens"))
            .collect();
        assert!(outputs.iter().all(Option::is_some), "both schemes read");
        // A provider string is mapped to the convention's system name.
        let systems: Vec<Option<Value>> = chats
            .iter()
            .map(|span| attr(span, "gen_ai.system"))
            .collect();
        assert!(systems.contains(&Some(json!({"stringValue": "openai"}))));
    }

    #[test]
    fn an_unpaired_turn_step_still_produces_a_span() {
        let mut fixture = Fixture::new();
        fixture.push(1_000, Some(TURN), started());
        fixture.push(
            1_400,
            Some(TURN),
            Body::TurnStep {
                index: 3,
                thought: "answer now".to_owned(),
                action: "answer".to_owned(),
                target: None,
                goal: None,
            },
        );

        let document = fixture.document();
        let spans = spans(&document);
        let step = named(&spans, "execute_tool");
        assert_eq!(step["startTimeUnixNano"], step["endTimeUnixNano"]);
        assert_eq!(
            attr(step, "gen_ai.tool.call.id"),
            Some(json!({"stringValue": "3"}))
        );
        // The turn never closed, so its status stays unset rather than
        // claiming an outcome.
        assert_eq!(named(&spans, "invoke_agent").get("status"), None);
    }

    #[test]
    fn records_with_no_turn_get_a_trace_each() {
        let mut fixture = Fixture::new();
        fixture.push(1_000, Some(TURN), started());
        fixture.push(
            1_100,
            None,
            Body::Log {
                level: "warn".to_owned(),
                message: "the socket went away".to_owned(),
            },
        );
        fixture.push(
            1_200,
            None,
            Body::Log {
                level: "info".to_owned(),
                message: "and came back".to_owned(),
            },
        );

        let document = fixture.document();
        let spans = spans(&document);
        let logs: Vec<&&Value> = spans
            .iter()
            .filter(|span| span["name"] == json!("log"))
            .collect();
        assert_eq!(logs.len(), 2, "a loose record is not dropped");
        assert_ne!(
            logs[0]["traceId"], logs[1]["traceId"],
            "two unrelated records are two traces"
        );
        for log in &logs {
            assert_eq!(log.get("parentSpanId"), None);
            assert_ne!(log["traceId"], named(&spans, "invoke_agent")["traceId"]);
        }
        assert_eq!(
            attr(logs[0], "starkbot.message"),
            Some(json!({"stringValue": "the socket went away"}))
        );
    }

    #[test]
    fn an_endpoint_that_already_names_the_path_is_not_doubled() {
        assert_eq!(
            traces_url("http://localhost:4318"),
            "http://localhost:4318/v1/traces"
        );
        assert_eq!(
            traces_url("http://localhost:4318/"),
            "http://localhost:4318/v1/traces"
        );
        assert_eq!(
            traces_url("http://localhost:4318/v1/traces"),
            "http://localhost:4318/v1/traces"
        );
    }
}
