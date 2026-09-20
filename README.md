# Starkbot Trace

Starkbot Trace records what a [Starkbot Neo](https://github.com/ethereumdegen/starkbot-neo)
agent actually did: every turn, every step the model chose, every inference
round trip, every navigator decision against a web page or a native macOS
window, and every `AppEvent` the core fanned out.

It is two crates:

- **`neo-trace`** — the producer, linked into Neo. It stamps a record and hands
  it to a background writer. Nothing else on the hot path.
- **`starkbot-trace`** — the collector, a separate binary. It owns the socket,
  the database and every report.

## Why it is a separate process

A trace that slows the thing it watches gets turned off. The producer never
does blocking I/O on a caller's thread: records go into a bounded queue
(1024), one writer thread owns the Unix socket, a full queue drops and counts
what it dropped, and a record that arrives while the collector is down is
discarded rather than buffered forever. With no collector listening, emitting
is a no-op — Neo runs exactly as it would without the crate.

A separate process also means the trace survives the run it describes: the
agent can crash, hang or be killed mid-turn and everything up to that moment
is already on disk.

## Run

```sh
cargo run --bin starkbot-trace -- serve            # listen, store, mirror
cargo run --bin starkbot-trace -- watch            # live dashboard
cargo run --bin starkbot-trace -- turns            # recent agent turns
cargo run --bin starkbot-trace -- turn <id>        # the waterfall of one turn
cargo run --bin starkbot-trace -- stats --since 60
cargo run --bin starkbot-trace -- tail --follow --kind jev_step
cargo run --bin starkbot-trace -- runs
```

Every report takes `--json`. `serve` accepts `--socket`, `--db` and `--mirror`
(an append-only NDJSON copy for `jq`).

Defaults:

| What | Where |
| --- | --- |
| socket | `$STARKBOT_TRACE_SOCKET`, else `~/Library/Application Support/com.starkbot.neo/trace.sock` |
| database | `$STARKBOT_TRACE_DB`, else `~/Library/Application Support/com.starkbot.trace/trace.db` |

## What a trace holds

Newline-delimited JSON, one `Record` per line: wire version, per-process
sequence number, timestamp, run id, source (`neo-cli`, `neo-tui`,
`neo-desktop`), pid, the turn id when the work belongs to one, the number of
records dropped before it, and a body tagged by `kind`:

| kind | what it records |
| --- | --- |
| `process_started` | which front end, which version, which argv |
| `turn_started` / `turn_finished` / `turn_failed` | one agent turn end to end |
| `turn_step` / `turn_step_finished` | the action the model chose, and what it observed |
| `inference` | provider, model, json mode, prompt size, duration, token usage, error |
| `surface_run` | one navigator run: surface, target, goal, outcome, steps |
| `jev_step` | one navigator decision: operation, confidence, candidates, staleness, the observe/decide/type/act split |
| `app_event` | anything the core published |
| `log` | a line with a level |

The collector denormalises duration, outcome, provider, model, operation,
surface and token counts at ingest, so `stats` and `turns` are index scans
rather than a JSON walk over the whole table.

## What it will not record

Credentials never reach a record: no keys, no tokens, no Keychain values. Text
typed into a field is counted, not kept — a login form holds a password, and a
trace that stored it would be a credential store nobody asked for. Prompts,
goals, observations and model answers *are* recorded; they are the point.

## Using it from another crate

```toml
neo-trace = { git = "https://github.com/ethereumdegen/starkbot-trace" }
```

```rust
neo_trace::init("my-app");
neo_trace::emit(neo_trace::Body::Log {
    level: "info".to_owned(),
    message: "started".to_owned(),
});

// Anything awaited inside a turn scope is attributed to that turn.
let turn = neo_trace::new_turn_id();
neo_trace::turn_scope(turn, async { do_work().await }).await;
```

## License

MIT.
