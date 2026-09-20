//! The half of tracing that runs inside the agent, and therefore must not cost
//! the agent anything.
//!
//! An agent step already waits on a model, a browser and the accessibility API;
//! it must never also wait on a debugging tool. So [`emit`] does the smallest
//! possible amount of work on the caller's thread — stamp an envelope, push it
//! into a bounded queue — and one plain OS thread owns the socket. The queue is
//! bounded at [`QUEUE_CAPACITY`] and a full queue *drops* the record rather than
//! blocking: a collector that stops reading, or a slow disk behind it, would
//! otherwise apply backpressure straight into the agent's turn loop, which is
//! exactly the failure this crate must never cause. Drops are counted and the
//! count rides out on the next record that makes it through, so a gap in the
//! data is visible instead of silent.
//!
//! The writer is a `std::thread` and not a tokio task on purpose. `init` is
//! called from synchronous `main`s before any runtime exists, and the CLI
//! builds and drops runtimes around individual commands; a task would die with
//! the runtime that happened to be current when tracing started.
//!
//! Absence is normal. Nobody runs the collector most of the time, so a missing
//! socket, a refused connection or a collector that quits mid-run are all
//! ordinary states: the writer keeps draining and discarding, retries the
//! connect at most every [`RECONNECT_EVERY`], and says nothing. A tracing
//! producer that logged its own failures would be noisier than the thing it is
//! meant to observe.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

use crate::turn::current_turn;
use crate::wire::{Body, DEFAULT_SOCKET_NAME, Record, SOCKET_ENV, WIRE_VERSION};

/// Records in flight before [`emit`] starts dropping. A thousand covers the
/// burst a navigator run produces while the collector is writing to SQLite.
const QUEUE_CAPACITY: usize = 1024;

/// How often the writer may try the socket again. Reconnecting on every record
/// would turn a stopped collector into a `connect(2)` per agent step.
const RECONNECT_EVERY: Duration = Duration::from_secs(2);

/// Everything `init` decided, or `None` when there is nowhere to send records.
struct Producer {
    source: String,
    run: String,
    pid: u32,
    path: PathBuf,
    queue: SyncSender<Record>,
}

static PRODUCER: OnceLock<Option<Producer>> = OnceLock::new();

/// Sequence numbers are process-wide rather than per-`Producer` so the writer
/// thread and `emit` can both touch the counters without reaching through the
/// `OnceLock` that is still being initialized around them.
static SEQ: AtomicU64 = AtomicU64::new(1);
static DROPPED: AtomicU64 = AtomicU64::new(0);

/// Start tracing for this process. Idempotent: the second call, from another
/// front end or a test, keeps the first call's run id and writer.
pub fn init(source: &str) {
    PRODUCER.get_or_init(|| start(source));
}

/// Whether a writer is running. Callers use this to skip building an expensive
/// body they would only throw away.
#[must_use]
pub fn enabled() -> bool {
    matches!(PRODUCER.get(), Some(Some(_)))
}

/// The socket this process writes to, or would write to before [`init`].
#[must_use]
pub fn socket_path() -> Option<PathBuf> {
    match PRODUCER.get() {
        Some(producer) => producer.as_ref().map(|producer| producer.path.clone()),
        None => resolve_socket_path(),
    }
}

/// Record one thing. Never blocks, never panics, and does nothing at all before
/// [`init`] or without a collector.
pub fn emit(body: Body) {
    let Some(Some(producer)) = PRODUCER.get() else {
        return;
    };
    // Claim the outstanding drop count up front so this record carries it. If
    // the send then fails the count goes back, plus this record.
    let dropped = DROPPED.swap(0, Ordering::Relaxed);
    let record = Record {
        v: WIRE_VERSION,
        seq: SEQ.fetch_add(1, Ordering::Relaxed),
        ts_ms: now_ms(),
        run: producer.run.clone(),
        source: producer.source.clone(),
        pid: producer.pid,
        turn: current_turn(),
        dropped,
        body,
    };
    match producer.queue.try_send(record) {
        Ok(()) => {}
        Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
            DROPPED.fetch_add(dropped.saturating_add(1), Ordering::Relaxed);
        }
    }
}

fn start(source: &str) -> Option<Producer> {
    let path = resolve_socket_path()?;
    let (queue, records) = sync_channel::<Record>(QUEUE_CAPACITY);
    let writer_path = path.clone();
    // A process that cannot spawn a thread has bigger problems than tracing;
    // leave the producer unset and let every `emit` be a no-op.
    std::thread::Builder::new()
        .name("neo-trace".to_string())
        .spawn(move || write_records(&writer_path, &records))
        .ok()?;
    Some(Producer {
        source: source.to_string(),
        run: uuid::Uuid::now_v7().hyphenated().to_string(),
        pid: std::process::id(),
        path,
        queue,
    })
}

fn resolve_socket_path() -> Option<PathBuf> {
    if let Some(value) = std::env::var_os(SOCKET_ENV)
        && !value.is_empty()
    {
        return Some(PathBuf::from(value));
    }
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("com.starkbot.neo")
            .join(DEFAULT_SOCKET_NAME),
    )
}

/// The writer thread. It owns the stream, so a broken collector is handled in
/// one place and never observed by a caller.
fn write_records(path: &std::path::Path, records: &Receiver<Record>) {
    let mut stream: Option<UnixStream> = None;
    let mut last_attempt: Option<Instant> = None;
    while let Ok(record) = records.recv() {
        if stream.is_none()
            && last_attempt.is_none_or(|attempt| attempt.elapsed() >= RECONNECT_EVERY)
        {
            last_attempt = Some(Instant::now());
            stream = UnixStream::connect(path).ok();
        }
        let Some(open) = stream.as_mut() else {
            // Still no collector. Drain anyway, or the queue fills and `emit`
            // starts paying for a tool nobody is listening to.
            DROPPED.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        // Serialize into a string first: a body that fails halfway through
        // would otherwise leave half a JSON object on the wire and desynchronize
        // every line after it.
        let Ok(mut line) = serde_json::to_string(&record) else {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        line.push('\n');
        if open.write_all(line.as_bytes()).is_err() {
            stream = None;
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn now_ms() -> i64 {
    let nanos = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    (nanos / 1_000_000) as i64
}

#[cfg(test)]
mod tests {
    // A failed `expect` in a test is the test failing, which is the point.
    #![allow(clippy::expect_used)]
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;

    /// One test, not three: `init` runs once per process, so the no-op case,
    /// the socket path and the stream have to be observed in a fixed order
    /// inside a single test or they race each other through the `OnceLock`.
    #[test]
    fn the_writer_streams_ndjson_and_emitting_before_init_does_nothing() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join(DEFAULT_SOCKET_NAME);
        let listener = UnixListener::bind(&path).expect("a collector socket");

        assert!(!enabled(), "no writer before init");
        emit(Body::Log {
            level: "info".to_string(),
            message: "before init".to_string(),
        });
        assert!(!enabled(), "emitting does not start a writer");

        // Safety: this is the only test in the crate that reads or writes the
        // environment, and it does so before spawning the writer thread.
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(SOCKET_ENV, &path);
        }
        init("neo-cli");
        assert!(enabled(), "init with a socket path starts a writer");
        assert_eq!(socket_path(), Some(path));

        emit(Body::Log {
            level: "info".to_string(),
            message: "first".to_string(),
        });
        emit(Body::Log {
            level: "warn".to_string(),
            message: "second".to_string(),
        });

        let (client, _) = listener.accept().expect("the writer connects");
        let mut lines = BufReader::new(client).lines();
        let first = lines.next().expect("a first line").expect("readable");
        let second = lines.next().expect("a second line").expect("readable");
        let first: Record = serde_json::from_str(&first).expect("a record");
        let second: Record = serde_json::from_str(&second).expect("a record");

        // The pre-init emit never reached the socket and never took a number.
        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(
            first.body,
            Body::Log {
                level: "info".to_string(),
                message: "first".to_string(),
            }
        );
        assert_eq!(
            second.body,
            Body::Log {
                level: "warn".to_string(),
                message: "second".to_string(),
            }
        );
        assert_eq!(first.source, "neo-cli");
        assert_eq!(first.v, WIRE_VERSION);
        assert_eq!(first.pid, std::process::id());
        assert_eq!(first.run, second.run);
        assert!(!first.run.is_empty());
        assert_eq!(first.turn, None);
        assert_eq!(first.dropped, 0);
    }
}
