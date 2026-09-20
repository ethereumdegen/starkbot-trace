#![forbid(unsafe_code)]
//! An OTLP receiver that is also a reader.
//!
//! This program shares no code with the agents it records — it shares a
//! protocol. Spans arrive over OTLP/HTTP, the way they would arrive at an
//! OpenTelemetry Collector, which means the producer can be restarted,
//! upgraded, rewritten or replaced with something else entirely without this
//! program knowing, and this one can be killed, moved or pointed at a
//! different database without any of that reaching the agent. It also means
//! the trace outlives the process that produced it, which is the only reason
//! any of this is useful after a crash.

mod otlp;
mod receiver;
mod report;
mod store;
mod watch;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use store::{Filter, Store};

#[derive(Parser)]
#[command(
    name = "starkbot-trace",
    version,
    about = "Receive and read OpenTelemetry traces from Starkbot Neo"
)]
struct Cli {
    /// The trace database. Defaults to `$STARKBOT_TRACE_DB`, else
    /// `~/Library/Application Support/com.starkbot.trace/trace.db`.
    // Global rather than declared on each subcommand: clap rejects the same
    // argument name in both places, and `receive --db` has to mean what
    // `tail --db` means.
    #[arg(long, global = true, value_name = "PATH")]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Accept OTLP/HTTP spans and store everything that arrives.
    Receive {
        /// Where to listen. An OTLP producer expects `127.0.0.1:4318`.
        #[arg(long, value_name = "ADDR", default_value = receiver::DEFAULT_ADDR)]
        addr: SocketAddr,
        /// Also append every raw document here, one per line, for `jq`.
        #[arg(long, value_name = "PATH")]
        mirror: Option<PathBuf>,
        /// Also forward each span to this OTLP endpoint as it arrives. The
        /// base URL, e.g. `http://localhost:4318`.
        #[arg(long, value_name = "URL")]
        otlp: Option<String>,
        /// An extra header on every forwarded request, `name: value`.
        /// Repeatable.
        #[arg(long = "header", value_name = "K: V")]
        header: Vec<String>,
    },
    /// The most recent spans.
    Tail {
        /// How many spans to show.
        #[arg(short = 'n', long = "limit", default_value_t = 40, value_name = "N")]
        limit: usize,
        /// Keep printing as new spans arrive.
        #[arg(long)]
        follow: bool,
        /// Only spans with this name, e.g. `invoke_agent`. Repeatable.
        #[arg(long, value_name = "NAME")]
        name: Vec<String>,
        /// Only this trace. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        trace: Option<String>,
        /// Only this turn. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        turn: Option<String>,
        /// Only this run. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        run: Option<String>,
        /// Only the last this many minutes.
        #[arg(long, value_name = "MINUTES")]
        since: Option<i64>,
        /// Only spans whose label, name or attributes contain this.
        #[arg(long, value_name = "TEXT")]
        grep: Option<String>,
        /// One JSON array instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Counts, token totals and latency percentiles.
    Stats {
        /// Only the last this many minutes.
        #[arg(long, value_name = "MINUTES")]
        since: Option<i64>,
        /// JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// One line per agent turn, newest first.
    Turns {
        /// How many turns to show.
        #[arg(short = 'n', long = "limit", default_value_t = 20, value_name = "N")]
        limit: usize,
        /// JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Everything that happened in one turn, as a waterfall. An id prefix is
    /// enough.
    Turn {
        /// The trace id, or as much of it as `turns` printed.
        id: String,
        /// JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// One line per process that has traced, newest first.
    Runs {
        /// How many runs to show.
        #[arg(short = 'n', long = "limit", default_value_t = 20, value_name = "N")]
        limit: usize,
        /// JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Re-post stored spans to another OTLP endpoint.
    Export {
        /// The endpoint's base URL, e.g. `http://localhost:4318`. Defaults
        /// to `$OTEL_EXPORTER_OTLP_ENDPOINT`.
        #[arg(long, value_name = "URL")]
        otlp: Option<String>,
        /// An extra header on every OTLP request, `name: value`. Repeatable.
        /// `$OTEL_EXPORTER_OTLP_HEADERS` is read too, in its `k=v,k2=v2`
        /// form; a flag of the same name wins.
        #[arg(long = "header", value_name = "K: V")]
        header: Vec<String>,
        /// Only the last this many minutes.
        #[arg(long, value_name = "MINUTES")]
        since: Option<i64>,
        /// Only this trace. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        trace: Option<String>,
        /// Keep exporting as new spans arrive.
        #[arg(long)]
        follow: bool,
        /// Print the OTLP document instead of sending it.
        #[arg(long)]
        dry_run: bool,
    },
    /// A live dashboard. q or Esc quits.
    Watch,
}

/// `main` stays synchronous so the reports and the dashboard run on the plain
/// thread they were written for; only the receiver and the exporter need a
/// reactor, and they build their own.
fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Receive {
            addr,
            mirror,
            otlp,
            header,
        } => {
            let database = database_path(cli.db)?;
            eprintln!("trace: database {}", database.display());
            // Built before the listener starts: a bad endpoint or an
            // unparseable header should stop the receiver here, not on the
            // first turn hours later.
            let egress = match otlp {
                Some(endpoint) => {
                    eprintln!("trace: forwarding to {endpoint}");
                    Some(otlp::Egress::new(Some(&endpoint), &header, false)?)
                }
                None => None,
            };
            let store = Arc::new(Store::open(&database)?);
            run_receive(store, addr, mirror, egress)
        }
        Command::Tail {
            limit,
            follow,
            name,
            trace,
            turn,
            run,
            since,
            grep,
            json,
        } => {
            let store = open(cli.db)?;
            let filter = Filter {
                run,
                trace,
                turn,
                names: name,
                since_ns: since.map(minutes_ago),
                text: grep,
            };
            report::tail(&store, &filter, limit, follow, json)
        }
        Command::Stats { since, json } => {
            let store = open(cli.db)?;
            report::stats(&store, since.map(minutes_ago), json)
        }
        Command::Turns { limit, json } => {
            let store = open(cli.db)?;
            report::turns(&store, limit, json)
        }
        Command::Turn { id, json } => {
            let store = open(cli.db)?;
            report::turn(&store, &id, json)
        }
        Command::Runs { limit, json } => {
            let store = open(cli.db)?;
            report::runs(&store, limit, json)
        }
        Command::Watch => {
            let store = open(cli.db)?;
            watch::watch(&store)
        }
        Command::Export {
            otlp,
            header,
            since,
            trace,
            follow,
            dry_run,
        } => {
            let store = open(cli.db)?;
            let filter = Filter {
                trace,
                since_ns: since.map(minutes_ago),
                ..Filter::default()
            };
            let egress = otlp::Egress::new(otlp.as_deref(), &header, dry_run)?;
            run_export(store, filter, egress, follow)
        }
    }
}

#[tokio::main]
async fn run_receive(
    store: Arc<Store>,
    addr: SocketAddr,
    mirror: Option<PathBuf>,
    egress: Option<otlp::Egress>,
) -> Result<()> {
    receiver::receive(store, addr, mirror.as_deref(), egress).await
}

/// The export needs a reactor for the same reason the receiver does and for
/// no other: `reqwest` is async.
#[tokio::main]
async fn run_export(
    store: Store,
    filter: Filter,
    egress: otlp::Egress,
    follow: bool,
) -> Result<()> {
    if follow {
        otlp::follow(&store, &filter, &egress).await
    } else {
        otlp::export(&store, &filter, &egress).await
    }
}

fn open(db: Option<PathBuf>) -> Result<Store> {
    Store::open(&database_path(db)?)
}

fn database_path(db: Option<PathBuf>) -> Result<PathBuf> {
    match db {
        Some(path) => Ok(path),
        None => Store::default_path().context("no --db and no default database path"),
    }
}

/// `--since` is minutes because that is how long ago the thing you are
/// looking for happened; the store speaks unix nanoseconds.
fn minutes_ago(minutes: i64) -> i64 {
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    now - minutes.saturating_mul(60_000_000_000)
}
