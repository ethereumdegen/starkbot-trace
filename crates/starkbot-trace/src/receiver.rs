//! The half of this program that is awake while the agent runs.
//!
//! It is an OTLP/HTTP receiver: `POST /v1/traces` with a JSON document, the
//! same request an OpenTelemetry SDK makes against a Collector on 4318. That
//! is the whole contract between this program and whatever produced the
//! spans — no shared crate, no private socket, nothing either side has to
//! release in step with the other.
//!
//! The HTTP is parsed by hand, on top of `tokio`. This listens on loopback
//! for one agent on the same machine, not on a public ingress: there is no
//! TLS to terminate, no routing, no middleware and exactly one path, so a web
//! framework would be several thousand lines of dependency to answer one
//! request shape. The parser is deliberately strict and small — a request
//! line, headers, `content-length`, optional gzip — and everything it does
//! not understand is answered with a status code that says why.
//!
//! Only the encoding is opinionated: OTLP's default is protobuf and this
//! accepts JSON, because JSON needs no code generator on either side and can
//! be read with `jq` when a span comes out wrong. A protobuf document is
//! refused with 415 rather than half-parsed.
//!
//! One rule shapes the error handling: the agent is the important process and
//! this one is not. A body that will not parse, a client that dies mid
//! request, a second producer connecting while the first is still posting —
//! none of those is allowed to end the listener, because the trace you lose
//! is always the one from the run you were trying to understand.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::store::Store;

/// Where an OTLP producer expects to find a receiver on this machine.
pub const DEFAULT_ADDR: &str = "127.0.0.1:4318";

/// The only path this program serves.
const TRACES_PATH: &str = "/v1/traces";

/// A request line plus headers larger than this is not a trace document being
/// posted, and reading it into memory is the first thing an attack would ask
/// for.
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// The largest document accepted. An OTLP exporter batches a few hundred
/// spans per request; a hundred megabytes is not that, and a receiver that
/// allocates whatever the `content-length` claims is a receiver that can be
/// stopped with one request.
const MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;

/// How often the receiver says something on stderr while spans stream in.
/// Often enough to see that it is alive, rare enough that a long agent turn
/// does not bury the shell it was started in.
const PROGRESS_EVERY: u64 = 200;

/// Accept OTLP documents on `addr` until Ctrl-C.
///
/// `egress`, when there is one, turns the receiver into a forwarder as well
/// as a store. It runs as its own task and is reached through a doorbell
/// channel, never awaited here: the next OTLP endpoint is somewhere else on a
/// network, and a producer whose spans had to wait for it would be an agent
/// slowed down by its own tracing.
pub async fn receive(
    store: Arc<Store>,
    addr: SocketAddr,
    mirror: Option<&Path>,
    egress: Option<crate::otlp::Egress>,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("could not listen on {addr}"))?;
    serve(listener, store, mirror, egress).await
}

/// The listener loop, over an already bound socket.
pub async fn serve(
    listener: TcpListener,
    store: Arc<Store>,
    mirror: Option<&Path>,
    egress: Option<crate::otlp::Egress>,
) -> Result<()> {
    let mirror = match mirror {
        Some(path) => Some(open_mirror(path).await?),
        None => None,
    };
    let exporter = match egress {
        Some(egress) => Some(crate::otlp::spawn_live(Arc::clone(&store), egress)?),
        None => None,
    };

    let counters = Arc::new(Counters::default());
    let bound = listener
        .local_addr()
        .context("the listener has no local address")?;
    eprintln!("trace: receiving OTLP/HTTP on http://{bound}{TRACES_PATH}");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    // An accept that fails is per-connection (a descriptor
                    // limit, a client gone between SYN and accept) and says
                    // nothing about the next one.
                    Err(error) => {
                        eprintln!("trace: could not accept a connection: {error}");
                        continue;
                    }
                };
                let store = Arc::clone(&store);
                let mirror = mirror.clone();
                let counters = Arc::clone(&counters);
                let exporter = exporter.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle(stream, store, mirror, counters, exporter).await {
                        eprintln!("trace: connection from {peer} ended: {error}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    eprintln!("trace: could not listen for Ctrl-C: {error}");
                }
                break;
            }
        }
    }

    eprintln!(
        "trace: stopped after {} spans in {} documents ({} rejected)",
        counters.spans.load(Ordering::Relaxed),
        counters.documents.load(Ordering::Relaxed),
        counters.rejected.load(Ordering::Relaxed)
    );
    Ok(())
}

#[derive(Default)]
struct Counters {
    documents: AtomicU64,
    spans: AtomicU64,
    rejected: AtomicU64,
}

/// The append-only JSON copy, shared by every connection.
///
/// One handle behind a mutex rather than one per connection: two producers
/// appending through separate handles would interleave partial lines and ruin
/// the file for `jq`.
type Mirror = Arc<Mutex<tokio::fs::File>>;

async fn open_mirror(path: &Path) -> Result<Mirror> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("could not open {}", path.display()))?;
    eprintln!("trace: mirroring to {}", path.display());
    Ok(Arc::new(Mutex::new(file)))
}

/// One connection, which may carry several requests: an OTLP exporter keeps
/// the socket open between batches, and closing after every response would
/// make every batch pay for a new handshake.
async fn handle(
    stream: TcpStream,
    store: Arc<Store>,
    mirror: Option<Mirror>,
    counters: Arc<Counters>,
    exporter: Option<tokio::sync::mpsc::Sender<()>>,
) -> Result<()> {
    // Nagle would hold a small response back waiting for more to send, which
    // on loopback is pure latency added to every batch.
    let _ = stream.set_nodelay(true);
    let mut reader = BufReader::new(stream);

    loop {
        let request = match read_request(&mut reader).await? {
            Some(request) => request,
            // The client closed between requests, which is how a finished
            // exporter says goodbye.
            None => return Ok(()),
        };

        let response = match &request.problem {
            Some(problem) => problem.clone(),
            None => {
                let outcome = accept(
                    &request,
                    store.as_ref(),
                    mirror.as_ref(),
                    counters.as_ref(),
                    exporter.as_ref(),
                )
                .await;
                match outcome {
                    Ok(()) => Response::ok(),
                    Err(error) => {
                        counters.rejected.fetch_add(1, Ordering::Relaxed);
                        eprintln!("trace: rejected a document: {error}");
                        Response::error(400, &format!("the document could not be read: {error}"))
                    }
                }
            }
        };

        let close = request.close;
        write_response(reader.get_mut(), &response, close).await?;
        if close {
            return Ok(());
        }
    }
}

/// Parse, store, mirror and ring the forwarder. Every failure here is the
/// document's fault and becomes a 400.
async fn accept(
    request: &Request,
    store: &Store,
    mirror: Option<&Mirror>,
    counters: &Counters,
    exporter: Option<&tokio::sync::mpsc::Sender<()>>,
) -> Result<()> {
    let document: serde_json::Value =
        serde_json::from_slice(&request.body).context("the body is not JSON")?;
    let spans = store.insert_document(&document)?;

    if let Some(mirror) = mirror {
        // One document per line, so the file stays readable by `jq -c`
        // whatever the sender's formatting was.
        let line = serde_json::to_string(&document)?;
        let mut file = mirror.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        // Not flushed to disk per document: this is a debugging copy, the
        // database is the record, and an fsync per batch would make the
        // mirror the slowest thing in the receiver.
        file.flush().await?;
    }

    let stored = u64::try_from(spans).unwrap_or(u64::MAX);
    let documents = counters.documents.fetch_add(1, Ordering::Relaxed) + 1;
    let total = counters
        .spans
        .fetch_add(stored, Ordering::Relaxed)
        .saturating_add(stored);
    if total % PROGRESS_EVERY < stored {
        eprintln!("trace: {total} spans in {documents} documents");
    }

    if let Some(exporter) = exporter {
        // A full doorbell means the forwarder is already awake with work to
        // do; dropping the wake-up costs one poll interval and never blocks
        // the producer.
        let _ = exporter.try_send(());
    }
    Ok(())
}

/// One parsed request, or the answer it has already earned.
struct Request {
    body: Vec<u8>,
    close: bool,
    /// Set when the request is understood well enough to be refused: a wrong
    /// path, a wrong method, a wrong encoding. The body is drained but never
    /// parsed.
    problem: Option<Response>,
}

#[derive(Clone)]
struct Response {
    status: u16,
    body: String,
}

impl Response {
    fn ok() -> Self {
        Self {
            status: 200,
            // What an OTLP receiver returns when it kept everything: an empty
            // `partialSuccess` means no span was rejected.
            body: "{\"partialSuccess\":{}}".to_owned(),
        }
    }

    fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            body: serde_json::json!({ "error": message }).to_string(),
        }
    }
}

/// Read one request. `Ok(None)` means the peer closed cleanly.
async fn read_request(reader: &mut BufReader<TcpStream>) -> Result<Option<Request>> {
    let Some(head) = read_head(reader).await? else {
        return Ok(None);
    };

    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let version = parts.next().unwrap_or_default().to_owned();

    let mut content_length: Option<u64> = None;
    let mut content_type = String::new();
    let mut content_encoding = String::new();
    let mut connection = String::new();
    let mut transfer_encoding = String::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => content_length = value.parse().ok(),
            "content-type" => content_type = value.to_ascii_lowercase(),
            "content-encoding" => content_encoding = value.to_ascii_lowercase(),
            "connection" => connection = value.to_ascii_lowercase(),
            "transfer-encoding" => transfer_encoding = value.to_ascii_lowercase(),
            _ => {}
        }
    }

    // HTTP/1.0 keeps the connection only when it says so; 1.1 keeps it unless
    // it says not to.
    let mut close = if version.eq_ignore_ascii_case("HTTP/1.0") {
        !connection.contains("keep-alive")
    } else {
        connection.contains("close")
    };

    let path = target.split(['?', '#']).next().unwrap_or(&target);
    let media = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();

    // Every refusal is decided before a byte of the body is read, so the
    // reasons are in one place and in the order a client would fix them.
    let problem = if !method.eq_ignore_ascii_case("POST") {
        Some(Response::error(
            405,
            &format!("{method} is not accepted; OTLP is POST {TRACES_PATH}"),
        ))
    } else if path != TRACES_PATH {
        Some(Response::error(
            404,
            &format!("{path} is not served; this receiver accepts POST {TRACES_PATH}"),
        ))
    } else if !transfer_encoding.is_empty() {
        Some(Response::error(
            411,
            "a chunked body is not read; send content-length",
        ))
    } else if content_length.is_none() {
        Some(Response::error(411, "the request needs a content-length"))
    } else if content_length.is_some_and(|length| length > MAX_BODY_BYTES) {
        Some(Response::error(
            413,
            &format!("the body is past the {MAX_BODY_BYTES} byte limit; send smaller batches"),
        ))
    } else if media != "application/json" {
        let described = if media.is_empty() {
            "a request with no content-type".to_owned()
        } else {
            format!("`{media}`")
        };
        Some(Response::error(
            415,
            &format!(
                "{described} is not accepted: this receiver speaks OTLP/HTTP with JSON \
                 encoding, so send content-type: application/json"
            ),
        ))
    } else if !content_encoding.is_empty()
        && content_encoding != "identity"
        && !content_encoding.contains("gzip")
    {
        Some(Response::error(
            415,
            &format!(
                "`{content_encoding}` is not a content-encoding this receiver reads; use gzip \
                 or none"
            ),
        ))
    } else {
        None
    };

    if let Some(problem) = problem {
        // The body is read and thrown away rather than left on the socket.
        // Closing on unread bytes makes the kernel send a reset, and a reset
        // discards the response the client has not read yet — so the client
        // sees a dropped connection instead of the 415 explaining itself.
        match content_length {
            Some(length) if length <= MAX_BODY_BYTES => drain(reader, length).await?,
            _ => close = true,
        }
        return Ok(Some(Request {
            body: Vec::new(),
            close,
            problem: Some(problem),
        }));
    }

    let length = content_length.unwrap_or_default();
    let mut body = vec![0_u8; usize::try_from(length).unwrap_or(0)];
    reader
        .read_exact(&mut body)
        .await
        .context("the client closed before the body arrived")?;

    if content_encoding.contains("gzip") {
        body = match gunzip(&body) {
            Ok(body) => body,
            Err(error) => {
                return Ok(Some(Request {
                    body: Vec::new(),
                    close,
                    problem: Some(Response::error(
                        400,
                        &format!("the gzip body could not be read: {error}"),
                    )),
                }));
            }
        };
    }

    Ok(Some(Request {
        body,
        close,
        problem: None,
    }))
}

/// The request line and headers, up to the blank line.
async fn read_head(reader: &mut BufReader<TcpStream>) -> Result<Option<String>> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .context("could not read the request")?;
        if read == 0 {
            // The peer closed. Between requests that is a clean goodbye;
            // mid request line it is a client that died, and either way
            // there is nothing left to answer.
            return Ok(None);
        }
        if line == "\r\n" || line == "\n" {
            return Ok(Some(head));
        }
        head.push_str(line.trim_end_matches('\n').trim_end_matches('\r'));
        head.push_str("\r\n");
        if head.len() > MAX_HEADER_BYTES {
            return Err(anyhow::anyhow!("the request headers are too large"));
        }
    }
}

/// Read and discard the body of a request that is being refused.
async fn drain(reader: &mut BufReader<TcpStream>, length: u64) -> Result<()> {
    let mut sink = tokio::io::sink();
    tokio::io::copy(&mut reader.take(length), &mut sink)
        .await
        .context("the client closed before its body arrived")?;
    Ok(())
}

fn gunzip(body: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;

    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

async fn write_response(stream: &mut TcpStream, response: &Response, close: bool) -> Result<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        _ => "Error",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n",
        response.status,
        response.body.len()
    );
    if response.status == 405 {
        head.push_str("allow: POST\r\n");
    }
    head.push_str(if close {
        "connection: close\r\n\r\n"
    } else {
        "connection: keep-alive\r\n\r\n"
    });
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(response.body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    // Over a real socket, because the thing worth pinning is what a client
    // that is not this program gets back: `curl`, an OTel SDK and a
    // Collector all speak HTTP and none of them can be persuaded to call an
    // internal function.
    use std::net::SocketAddr;
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::serve;
    use crate::store::{Filter, Store};

    const DOCUMENT: &str = r#"{"resourceSpans":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"starkbot-neo"}}]},
        "scopeSpans":[{"scope":{"name":"starkbot-neo"},"spans":[
        {"traceId":"7f3a1c9e5d2b48a6913f0e7c4b5a6d2e","spanId":"a1b2c3d4e5f60718",
         "name":"invoke_agent","kind":1,
         "startTimeUnixNano":"1758240000000000000","endTimeUnixNano":"1758240004500000000",
         "attributes":[],"status":{"code":1}}]}]}]}"#;

    struct Receiver {
        _dir: tempfile::TempDir,
        store: Arc<Store>,
        addr: SocketAddr,
        task: tokio::task::JoinHandle<()>,
    }

    impl Receiver {
        async fn start() -> Self {
            let dir = tempfile::tempdir().expect("a temporary directory");
            let store =
                Arc::new(Store::open(&dir.path().join("trace.db")).expect("an open database"));
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a bound listener");
            let addr = listener.local_addr().expect("the bound address");
            let served = Arc::clone(&store);
            let task = tokio::spawn(async move {
                let _ = serve(listener, served, None, None).await;
            });
            Self {
                _dir: dir,
                store,
                addr,
                task,
            }
        }

        /// One request, one connection, closed by the server so the whole
        /// response can be read without parsing its length.
        async fn request(&self, head: &str, body: &[u8]) -> String {
            let mut stream = TcpStream::connect(self.addr).await.expect("a connection");
            let request = format!(
                "{head}\r\nhost: localhost\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
                body.len()
            );
            stream
                .write_all(request.as_bytes())
                .await
                .expect("the request head");
            stream.write_all(body).await.expect("the request body");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("the response");
            String::from_utf8_lossy(&response).into_owned()
        }

        async fn post(&self, body: &[u8]) -> String {
            self.request(
                "POST /v1/traces HTTP/1.1\r\ncontent-type: application/json",
                body,
            )
            .await
        }

        fn stored(&self) -> usize {
            self.store
                .spans(&Filter::default(), 100)
                .expect("the spans")
                .len()
        }
    }

    #[tokio::test]
    async fn a_posted_document_is_answered_and_stored() {
        let receiver = Receiver::start().await;
        let response = receiver.post(DOCUMENT.as_bytes()).await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.ends_with("{\"partialSuccess\":{}}"),
            "an OTLP client reads partialSuccess to learn nothing was dropped: {response}"
        );
        assert_eq!(receiver.stored(), 1);
        receiver.task.abort();
    }

    #[tokio::test]
    async fn a_gzip_body_is_read() {
        use std::io::Write;

        let receiver = Receiver::start().await;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(DOCUMENT.as_bytes())
            .expect("the compressed body");
        let body = encoder.finish().expect("the compressed body");

        let response = receiver
            .request(
                "POST /v1/traces HTTP/1.1\r\ncontent-type: application/json\r\n\
                 content-encoding: gzip",
                &body,
            )
            .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert_eq!(receiver.stored(), 1);
        receiver.task.abort();
    }

    #[tokio::test]
    async fn protobuf_is_refused_and_a_bad_body_does_not_kill_the_listener() {
        let receiver = Receiver::start().await;

        // OTLP's default encoding is protobuf, so this is the first thing a
        // stock exporter will try. It has to be told, not ignored.
        let refused = receiver
            .request(
                "POST /v1/traces HTTP/1.1\r\ncontent-type: application/x-protobuf",
                b"\x0a\x00",
            )
            .await;
        assert!(
            refused.starts_with("HTTP/1.1 415 Unsupported Media Type"),
            "{refused}"
        );
        assert!(refused.contains("JSON"), "the refusal says why: {refused}");

        let malformed = receiver.post(b"{\"resourceSpans\":").await;
        assert!(
            malformed.starts_with("HTTP/1.1 400 Bad Request"),
            "{malformed}"
        );

        let wrong_path = receiver
            .request(
                "POST /v1/logs HTTP/1.1\r\ncontent-type: application/json",
                b"{}",
            )
            .await;
        assert!(
            wrong_path.starts_with("HTTP/1.1 404 Not Found"),
            "{wrong_path}"
        );

        let wrong_method = receiver
            .request(
                "GET /v1/traces HTTP/1.1\r\ncontent-type: application/json",
                b"",
            )
            .await;
        assert!(
            wrong_method.starts_with("HTTP/1.1 405 Method Not Allowed"),
            "{wrong_method}"
        );

        // The whole point: after four refusals the receiver is still there.
        let accepted = receiver.post(DOCUMENT.as_bytes()).await;
        assert!(accepted.starts_with("HTTP/1.1 200 OK"), "{accepted}");
        assert_eq!(receiver.stored(), 1);
        receiver.task.abort();
    }
}
