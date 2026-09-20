#![forbid(unsafe_code)]
//! A separate program from Neo on purpose.
//!
//! The agent's job is to finish a task; this one's job is to remember what it
//! did. Keeping them apart means the collector can be restarted, upgraded,
//! pointed at a different database or killed outright without any of that
//! reaching the agent — the producer side is fire-and-forget, so a missing
//! collector is simply a quiet one. It also means the trace survives the
//! process that produced it, which is the only reason any of this is useful
//! after a crash.

mod otlp;
mod report;
mod server;
mod store;
mod watch;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};

use store::{Filter, Trace};

#[derive(Parser)]
#[command(
    name = "starkbot-trace",
    version,
    about = "Collect and read Starkbot Neo execution traces"
)]
struct Cli {
    /// The trace database. Defaults to `$STARKBOT_TRACE_DB`, else
    /// `~/Library/Application Support/com.starkbot.trace/trace.db`.
    // Global rather than declared on each subcommand: clap rejects the same
    // argument name in both places, and `serve --db` has to mean what
    // `tail --db` means.
    #[arg(long, global = true, value_name = "PATH")]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Listen on the trace socket and store everything Neo sends.
    Serve {
        /// Defaults to `$STARKBOT_TRACE_SOCKET`, else
        /// `~/Library/Application Support/com.starkbot.neo/trace.sock`.
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        /// Also append every raw line here, for `jq`.
        #[arg(long, value_name = "PATH")]
        mirror: Option<PathBuf>,
        /// Also forward each turn to this OTLP endpoint as it closes. The
        /// base URL, e.g. `http://localhost:4318`.
        #[arg(long, value_name = "URL")]
        otlp: Option<String>,
        /// An extra header on every OTLP request, `name: value`. Repeatable.
        #[arg(long = "header", value_name = "K: V")]
        header: Vec<String>,
    },
    /// The most recent records.
    Tail {
        /// How many records to show.
        #[arg(short = 'n', long = "limit", default_value_t = 40, value_name = "N")]
        limit: usize,
        /// Keep printing as new records arrive.
        #[arg(long)]
        follow: bool,
        /// Only this record kind. Repeatable.
        #[arg(long, value_name = "KIND")]
        kind: Vec<String>,
        /// Only this turn. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        turn: Option<String>,
        /// Only this run. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        run: Option<String>,
        /// Only the last this many minutes.
        #[arg(long, value_name = "MINUTES")]
        since: Option<i64>,
        /// Only records whose label or body contains this.
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
    /// Everything that happened in one turn. An id prefix is enough.
    Turn {
        /// The turn id, or as much of it as `turns` printed.
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
    /// Map stored records to OpenTelemetry spans and send them over OTLP.
    Export {
        /// The collector's base URL, e.g. `http://localhost:4318`. Defaults
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
        /// Only this turn. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        turn: Option<String>,
        /// Only this run. An id prefix is enough.
        #[arg(long, value_name = "ID")]
        run: Option<String>,
        /// Keep exporting as new records arrive. Each turn goes out once it
        /// has closed.
        #[arg(long)]
        follow: bool,
        /// Print the OTLP payload instead of sending it.
        #[arg(long)]
        dry_run: bool,
    },
    /// A live dashboard. q or Esc quits.
    Watch,
}

/// `main` stays synchronous so the reports and the dashboard run on the plain
/// thread they were written for; only `serve` needs a reactor, and it builds
/// its own.
fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            socket,
            mirror,
            otlp,
            header,
        } => {
            let socket = match socket {
                Some(path) => path,
                None => server::default_socket()?,
            };
            let database = database_path(cli.db)?;
            eprintln!("trace: socket {}", socket.display());
            eprintln!("trace: database {}", database.display());
            // Built before the listener starts: a bad endpoint or an
            // unparseable header should stop the collector here, not on the
            // first turn hours later.
            let egress = match otlp {
                Some(endpoint) => {
                    eprintln!("trace: exporting to {endpoint}");
                    Some(otlp::Egress::new(Some(&endpoint), &header, false)?)
                }
                None => None,
            };
            let trace = Arc::new(Trace::open(&database)?);
            run_serve(trace, socket, mirror, egress)
        }
        Command::Tail {
            limit,
            follow,
            kind,
            turn,
            run,
            since,
            grep,
            json,
        } => {
            let trace = open(cli.db)?;
            let filter = Filter {
                run,
                turn,
                kinds: kind,
                since_ms: since.map(minutes_ago),
                text: grep,
            };
            report::tail(&trace, &filter, limit, follow, json)
        }
        Command::Stats { since, json } => {
            let trace = open(cli.db)?;
            report::stats(&trace, since.map(minutes_ago), json)
        }
        Command::Turns { limit, json } => {
            let trace = open(cli.db)?;
            report::turns(&trace, limit, json)
        }
        Command::Turn { id, json } => {
            let trace = open(cli.db)?;
            report::turn(&trace, &id, json)
        }
        Command::Runs { limit, json } => {
            let trace = open(cli.db)?;
            report::runs(&trace, limit, json)
        }
        Command::Watch => {
            let trace = open(cli.db)?;
            watch::watch(&trace)
        }
        Command::Export {
            otlp,
            header,
            since,
            turn,
            run,
            follow,
            dry_run,
        } => {
            let trace = open(cli.db)?;
            let filter = Filter {
                run,
                turn,
                kinds: Vec::new(),
                since_ms: since.map(minutes_ago),
                text: None,
            };
            let egress = otlp::Egress::new(otlp.as_deref(), &header, dry_run)?;
            run_export(trace, filter, egress, follow)
        }
    }
}

#[tokio::main]
async fn run_serve(
    trace: Arc<Trace>,
    socket: PathBuf,
    mirror: Option<PathBuf>,
    egress: Option<otlp::Egress>,
) -> Result<()> {
    server::serve(trace, &socket, mirror.as_deref(), egress).await
}

/// The export needs a reactor for the same reason `serve` does and for no
/// other: `reqwest` is async.
#[tokio::main]
async fn run_export(
    trace: Trace,
    filter: Filter,
    egress: otlp::Egress,
    follow: bool,
) -> Result<()> {
    if follow {
        otlp::follow(&trace, &filter, &egress).await
    } else {
        otlp::export(&trace, &filter, &egress).await
    }
}

fn open(db: Option<PathBuf>) -> Result<Trace> {
    Trace::open(&database_path(db)?)
}

fn database_path(db: Option<PathBuf>) -> Result<PathBuf> {
    match db {
        Some(path) => Ok(path),
        None => Trace::default_path(),
    }
}

/// `--since` is minutes because that is how long ago the thing you are
/// looking for happened; the store speaks epoch milliseconds.
fn minutes_ago(minutes: i64) -> i64 {
    let now = time::OffsetDateTime::now_utc().unix_timestamp() * 1_000;
    now - minutes.saturating_mul(60_000)
}
