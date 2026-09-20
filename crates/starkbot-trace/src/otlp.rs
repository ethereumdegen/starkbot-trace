//! The exit, for spans that should end up somewhere else as well.
//!
//! What arrives here is already OpenTelemetry — the receiver speaks OTLP and
//! the store keeps what it was sent — so forwarding is a rebuild, not a
//! mapping: the rows of a trace go back out as `resourceSpans[].scopeSpans[]
//! .spans[]` and land in a Collector, Jaeger, Tempo, Honeycomb or any of the
//! AI-observability backends that accept OTLP. That makes this program a
//! place a trace can sit and be read without becoming a place a trace is
//! stuck.
//!
//! The document is built by hand with `serde_json` and posted with `reqwest`
//! rather than through the OpenTelemetry SDK, for the same reason the
//! receiver parses HTTP by hand: OTLP/HTTP with JSON encoding is a
//! documented, stable schema, and the alternative is a protobuf toolchain, a
//! code generator and a second async SDK with its own batching, sampling and
//! shutdown semantics inside a program whose whole job is to be boring and to
//! still be running in six months. A Collector accepts OTLP/JSON on 4318
//! unmodified, and JSON is the one OTLP encoding a person can verify by
//! reading the bytes — which is what makes `--dry-run` worth having.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::store::{Filter, Span, Store};

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

/// How often follow mode asks the store for new spans. The same interval the
/// dashboard uses: fast enough to feel live, slow enough to stay invisible in
/// the WAL.
const POLL: Duration = Duration::from_millis(250);

/// Spans per follow poll. A burst larger than this is caught by the next poll
/// 250 ms later.
const FOLLOW_BATCH: usize = 2_000;

/// The ceiling on a one-shot export. A trace database holds months of runs,
/// so the export is bounded and says so rather than growing until the
/// allocator complains.
const MAX_SPANS: usize = 200_000;

/// Wake-ups the receiver may have outstanding before it starts dropping them.
/// Dropping is safe by construction: the forwarder polls the store on a timer
/// anyway, so a lost wake-up delays a batch by one tick and loses nothing.
const WAKE_DEPTH: usize = 1_024;

/// Where a batch goes.
///
/// `--dry-run` is not a debug flag: printing the document is how a person
/// checks what a backend will be told without standing up a backend first, so
/// it is a first-class destination rather than an early return inside the
/// sender.
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

    /// Deliver every document and return how many of them failed.
    ///
    /// A batch that will not go through is reported and skipped rather than
    /// ending the export: the spans after it are usually fine, and an export
    /// that stops at the first bad batch leaves the operator with no trace
    /// and no idea which batch was bad.
    async fn deliver(&self, documents: &[Value]) -> usize {
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
pub async fn export(store: &Store, filter: &Filter, egress: &Egress) -> Result<()> {
    let spans = store.spans(filter, MAX_SPANS)?;
    if spans.len() == MAX_SPANS {
        eprintln!("trace: stopped at {MAX_SPANS} spans; narrow --since or --trace");
    }
    if spans.is_empty() {
        eprintln!("trace: nothing matched, so nothing was exported");
        return Ok(());
    }
    let documents = documents(&spans);
    let failed = egress.deliver(&documents).await;
    eprintln!(
        "trace: {} spans in {} document(s)",
        spans.len(),
        documents.len()
    );
    if failed > 0 {
        bail!("{failed} batches could not be delivered");
    }
    Ok(())
}

/// Keep forwarding as spans land, until Ctrl-C.
///
/// A span is forwarded as soon as it is stored, with no waiting for its
/// parent: OTLP is built for exactly that, because an SDK exports a child
/// when the child ends and the root only when the whole turn does. A backend
/// assembles the trace from the ids.
pub async fn follow(store: &Store, filter: &Filter, egress: &Egress) -> Result<()> {
    let spans = store.spans(filter, MAX_SPANS)?;
    let mut cursor = spans.last().map_or(0, |span| span.id);
    let mut failed = deliver(egress, &spans).await;

    loop {
        tokio::select! {
            () = tokio::time::sleep(POLL) => {
                failed += step(store, filter, egress, &mut cursor).await;
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    eprintln!("trace: could not listen for Ctrl-C: {error}");
                }
                break;
            }
        }
    }

    if failed > 0 {
        bail!("{failed} batches could not be delivered");
    }
    Ok(())
}

/// Start the forwarder the receiver feeds while it is running.
///
/// The returned sender is a doorbell, not a queue of work: the forwarder
/// reads the spans it forwards out of the store itself, so ingest hands over
/// a zero-sized wake-up it can throw away under load and never waits for a
/// request to a backend that may be slow, far away or down.
pub fn spawn_live(store: Arc<Store>, egress: Egress) -> Result<mpsc::Sender<()>> {
    // Only what arrives from here on: the history is what `export` is for,
    // and re-sending it on every restart would repeat every span.
    let mut cursor = store
        .spans(&Filter::default(), 1)?
        .last()
        .map_or(0, |span| span.id);
    let (wake, mut woken) = mpsc::channel::<()>(WAKE_DEPTH);
    tokio::spawn(async move {
        let filter = Filter::default();
        loop {
            let ringing = tokio::select! {
                doorbell = woken.recv() => doorbell.is_some(),
                () = tokio::time::sleep(POLL) => true,
            };
            step(&store, &filter, &egress, &mut cursor).await;
            if !ringing {
                // Every sender is gone, so the listener has stopped and no
                // span will ever arrive again.
                break;
            }
        }
    });
    Ok(wake)
}

/// One poll: read what is new, send it.
async fn step(store: &Store, filter: &Filter, egress: &Egress, cursor: &mut i64) -> usize {
    let spans = match store.spans_after(*cursor, filter, FOLLOW_BATCH) {
        Ok(spans) => spans,
        // A read that fails is almost always a busy WAL. Neither a live
        // forwarder nor a follow session is allowed to end over one.
        Err(error) => {
            eprintln!("trace: could not read new spans: {error}");
            return 1;
        }
    };
    if let Some(last) = spans.last() {
        *cursor = last.id;
    }
    deliver(egress, &spans).await
}

async fn deliver(egress: &Egress, spans: &[Span]) -> usize {
    if spans.is_empty() {
        return 0;
    }
    egress.deliver(&documents(spans)).await
}

// ---------------------------------------------------------------------------
// OTLP/JSON
// ---------------------------------------------------------------------------

/// `kind` when the stored span did not carry one. These describe an agent's
/// own work rather than a request it served, so internal is the honest
/// default.
const SPAN_KIND_INTERNAL: i64 = 1;

/// One document per request, each holding at most [`SPANS_PER_REQUEST`]
/// spans.
#[must_use]
pub fn documents(spans: &[Span]) -> Vec<Value> {
    spans.chunks(SPANS_PER_REQUEST).map(document).collect()
}

/// Grouped by run, because a resource in OTLP is one producer: two agent
/// processes have different pids and possibly different versions, and merging
/// them into one resource would attribute one's spans to the other.
fn document(spans: &[Span]) -> Value {
    let mut order: Vec<Resource> = Vec::new();
    let mut groups: HashMap<Resource, Vec<&Span>> = HashMap::new();
    for span in spans {
        let key = Resource::of(span);
        groups
            .entry(key.clone())
            .or_insert_with(|| {
                order.push(key);
                Vec::new()
            })
            .push(span);
    }

    let resource_spans: Vec<Value> = order
        .into_iter()
        .filter_map(|key| {
            let group = groups.get(&key)?;
            let mut resource = Map::new();
            resource.insert("attributes".to_owned(), key.attributes(group));

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

/// What makes two spans the same producer.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Resource {
    service: String,
    surface: Option<String>,
    run: Option<String>,
    pid: Option<u32>,
}

impl Resource {
    fn of(span: &Span) -> Self {
        Self {
            service: span.service.clone(),
            surface: span.surface.clone(),
            run: span.run.clone(),
            pid: span.pid,
        }
    }

    /// The resource attribute list. The four flat columns are joined by
    /// anything the producer sent under `service.` or `telemetry.`, which are
    /// resource-scoped by convention and were folded into the span at ingest
    /// because they have no column of their own — `service.version` above
    /// all, which is the first thing asked about when two runs differ.
    fn attributes(&self, spans: &[&Span]) -> Value {
        let mut attrs = Attrs::default();
        attrs.text("service.name", Some(&self.service));
        for span in spans {
            for (key, value) in span.attributes.as_object().into_iter().flatten() {
                if resource_scoped(key) {
                    attrs.unique(key, value);
                }
            }
        }
        attrs.int("process.pid", self.pid.map(i64::from));
        attrs.text("starkbot.surface", self.surface.as_deref());
        attrs.text("starkbot.run", self.run.as_deref());
        attrs.json_value()
    }
}

fn resource_scoped(key: &str) -> bool {
    key.starts_with("service.") || key.starts_with("telemetry.")
}

fn span_value(span: &Span) -> Value {
    let mut object = Map::new();
    object.insert("traceId".to_owned(), Value::String(span.trace_id.clone()));
    object.insert("spanId".to_owned(), Value::String(span.span_id.clone()));
    if let Some(parent) = &span.parent_span_id {
        object.insert("parentSpanId".to_owned(), Value::String(parent.clone()));
    }
    object.insert("name".to_owned(), Value::String(span.name.clone()));
    object.insert(
        "kind".to_owned(),
        Value::from(span.kind.unwrap_or(SPAN_KIND_INTERNAL)),
    );
    object.insert(
        "startTimeUnixNano".to_owned(),
        Value::String(span.start_ns.max(0).to_string()),
    );
    object.insert(
        "endTimeUnixNano".to_owned(),
        Value::String(span.end_ns.max(0).to_string()),
    );

    let mut attrs = Attrs::default();
    for (key, value) in span.attributes.as_object().into_iter().flatten() {
        if !resource_scoped(key) {
            attrs.json(key, value);
        }
    }
    object.insert("attributes".to_owned(), attrs.json_value());

    let events: Vec<Value> = span
        .events
        .as_array()
        .into_iter()
        .flatten()
        .map(event_value)
        .collect();
    if !events.is_empty() {
        object.insert("events".to_owned(), Value::Array(events));
    }

    if span.status_code != 0 {
        let mut status = Map::new();
        status.insert("code".to_owned(), Value::from(span.status_code));
        if let Some(message) = &span.status_message {
            status.insert("message".to_owned(), Value::String(one_line(message)));
        }
        object.insert("status".to_owned(), Value::Object(status));
    }
    Value::Object(object)
}

fn event_value(event: &Value) -> Value {
    let mut attrs = Attrs::default();
    for (key, value) in event
        .get("attributes")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        attrs.json(key, value);
    }
    let mut object = Map::new();
    object.insert(
        "name".to_owned(),
        event.get("name").cloned().unwrap_or(Value::Null),
    );
    object.insert(
        "timeUnixNano".to_owned(),
        Value::String(
            event
                .get("time_ns")
                .and_then(Value::as_i64)
                .unwrap_or_default()
                .max(0)
                .to_string(),
        ),
    );
    object.insert("attributes".to_owned(), attrs.json_value());
    Value::Object(object)
}

/// An attribute value, in the four shapes OTLP has a key for.
enum Attr {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

/// An attribute list under construction. Every setter takes an `Option` and
/// drops `None`: an attribute that is not there says "the producer did not
/// send one", while an attribute set to a zero or an empty string says
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

    /// A stored attribute, whose type is whatever the producer sent. The
    /// store decoded `intValue` to a JSON integer and `doubleValue` to a JSON
    /// float, so the distinction OTLP cares about survives the round trip.
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
            // An array or a key-value list has no scalar form. Its JSON is
            // kept rather than dropped, because a lost attribute is a lost
            // fact and a stringified one is still readable.
            other => self.0.push((key.to_owned(), Attr::Str(other.to_string()))),
        }
    }

    /// The first value wins. Two spans of one run disagreeing about
    /// `service.version` means the run was upgraded mid-flight, which cannot
    /// happen; a duplicate key in a resource, however, is a document a strict
    /// receiver may reject.
    fn unique(&mut self, key: &str, value: &Value) {
        if self.0.iter().any(|(existing, _)| existing == key) {
            return;
        }
        self.json(key, value);
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

/// Exponential, with jitter over the lower half of the window. Two forwarders
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
