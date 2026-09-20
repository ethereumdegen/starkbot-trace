//! The half of the collector that is awake while the agent runs.
//!
//! Everything here is built around one rule: the agent is the important
//! process and this one is not. A producer that cannot connect carries on
//! silently, so the collector must be equally forgiving in the other
//! direction — a line that will not parse, a client that dies mid-record, a
//! second Neo connecting while the first is still talking. None of those is
//! allowed to end the listener, because the trace you lose is always the one
//! from the run you were trying to understand.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::store::Trace;

/// How often the listener says something on stderr while a run is streaming.
/// Often enough to see that it is alive, rare enough that a long agent turn
/// does not bury the shell it was started in.
const PROGRESS_EVERY: u64 = 200;

/// Accept trace connections on `socket` until Ctrl-C.
pub async fn serve(trace: Arc<Trace>, socket: &Path, mirror: Option<&Path>) -> Result<()> {
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("could not create {}", parent.display()))?;
    }

    // A Unix socket is a file, and killing the collector leaves that file
    // behind. `bind` on an existing path fails with EADDRINUSE, so without
    // this unlink every restart after a hard kill would refuse to start until
    // somebody deleted the file by hand.
    match tokio::fs::remove_file(socket).await {
        Ok(()) => eprintln!("trace: removed a stale socket at {}", socket.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("could not remove {}", socket.display()));
        }
    }

    let listener = UnixListener::bind(socket)
        .with_context(|| format!("could not listen on {}", socket.display()))?;
    let mirror = match mirror {
        Some(path) => Some(open_mirror(path).await?),
        None => None,
    };
    let total = Arc::new(AtomicU64::new(0));

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let trace = Arc::clone(&trace);
                    let mirror = mirror.clone();
                    let total = Arc::clone(&total);
                    tokio::spawn(async move {
                        if let Err(error) = drain(stream, trace, mirror, total).await {
                            eprintln!("trace: connection ended badly: {error}");
                        }
                    });
                }
                // An accept error is almost always a transient descriptor
                // shortage. Dropping the listener over it would take the
                // whole collector down with it.
                Err(error) => eprintln!("trace: could not accept a connection: {error}"),
            },
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    eprintln!("trace: could not listen for Ctrl-C: {error}");
                }
                break;
            }
        }
    }

    drop(listener);
    // Leaving the socket behind would make the next producer connect to
    // nothing and wait for a reader that never comes.
    if let Err(error) = tokio::fs::remove_file(socket).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("trace: could not remove {}: {error}", socket.display());
    }
    eprintln!(
        "trace: stopped after {} records",
        total.load(Ordering::Relaxed)
    );
    Ok(())
}

/// The append-only NDJSON copy, shared by every connection.
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
        .with_context(|| format!("could not open the mirror at {}", path.display()))?;
    Ok(Arc::new(Mutex::new(file)))
}

async fn drain(
    stream: UnixStream,
    trace: Arc<Trace>,
    mirror: Option<Mirror>,
    total: Arc<AtomicU64>,
) -> Result<()> {
    let peer = stream
        .peer_cred()
        .ok()
        .and_then(|cred| cred.pid())
        .map_or_else(|| "unknown pid".to_string(), |pid| format!("pid {pid}"));
    eprintln!("trace: {peer} connected");

    let mut lines = BufReader::new(stream).lines();
    let mut stored = 0_u64;
    let mut bad = 0_u64;
    let mut source = String::new();

    loop {
        // A producer killed mid-write gives a read error, not a clean end of
        // stream. That is a normal way for an agent to exit, so it closes the
        // connection instead of propagating.
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                eprintln!("trace: {peer} disconnected mid-line: {error}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let record: neo_trace::Record = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(error) => {
                bad += 1;
                // Only the first few, so a producer speaking a future wire
                // version cannot fill the terminal.
                if bad <= 3 {
                    eprintln!("trace: {peer} sent a line this build cannot read: {error}");
                }
                continue;
            }
        };
        if source.is_empty() {
            source = record.source.clone();
        }

        // The insert is a local WAL append of a few hundred bytes, so it runs
        // on this task rather than going through `spawn_blocking`: the hop
        // would cost more than the write.
        match trace.insert(&record) {
            Ok(_) => {
                stored += 1;
                let seen = total.fetch_add(1, Ordering::Relaxed) + 1;
                if seen.is_multiple_of(PROGRESS_EVERY) {
                    eprintln!("trace: {seen} records stored");
                }
            }
            Err(error) => {
                bad += 1;
                eprintln!("trace: could not store a record from {peer}: {error}");
            }
        }

        if let Some(mirror) = &mirror {
            let mut file = mirror.lock().await;
            if let Err(error) = write_line(&mut file, &line).await {
                eprintln!("trace: could not append to the mirror: {error}");
            }
        }
    }

    let who = if source.is_empty() {
        peer
    } else {
        format!("{source} ({peer})")
    };
    eprintln!("trace: {who} closed after {stored} records, {bad} rejected");
    Ok(())
}

async fn write_line(file: &mut tokio::fs::File, line: &str) -> std::io::Result<()> {
    file.write_all(line.as_bytes()).await?;
    file.write_all(b"\n").await
}

/// Where a producer is expected to be listening for us: the override first,
/// then Neo's own data directory.
pub fn default_socket() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(neo_trace::SOCKET_ENV) {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("HOME is not set, so there is no default socket path"))?;
    Ok(PathBuf::from(home)
        .join("Library")
        .join("Application Support")
        .join("com.starkbot.neo")
        .join(neo_trace::DEFAULT_SOCKET_NAME))
}
