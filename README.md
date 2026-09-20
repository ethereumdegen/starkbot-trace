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

## OpenTelemetry

The Unix socket and the SQLite file are this tool's own shape: fast to write,
cheap to query, and readable by nothing else. OTLP is the other direction —
the same records mapped to OpenTelemetry spans and posted to whatever already
collects traces, so an agent turn lands next to the rest of your telemetry
instead of in a database only `starkbot-trace` can read.

Nothing about the socket changes. `neo-trace` gains no dependencies: the agent
still writes NDJSON to a local socket and the collector does the mapping, so a
backend that is slow, far away or down is the collector's problem and never
the agent's.

Two commands:

```sh
# Everything from the last ten hours, to a Collector on this machine.
starkbot-trace export --otlp http://localhost:4318 --since 600

# The mapping, on stdout, with no backend anywhere.
starkbot-trace export --dry-run --since 600 | jq '.resourceSpans[].scopeSpans[].spans[].name'

# Keep exporting as records land. A turn goes out when it closes.
starkbot-trace export --otlp http://localhost:4318 --follow

# Or collect and forward in one process.
starkbot-trace serve --otlp http://localhost:4318
```

`export` also takes `--turn`, `--run` and `--header 'name: value'` (repeatable,
for an endpoint that wants an API key). The endpoint is a base URL; `/v1/traces`
is appended, and an endpoint that already ends in it is left alone. The
standard variables are read as defaults:

| Variable | Meaning |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the base URL, when `--otlp` is not given |
| `OTEL_EXPORTER_OTLP_HEADERS` | extra headers as `k=v,k2=v2`; a `--header` of the same name wins |

The transport is OTLP over HTTP with JSON encoding, built by hand. There is no
`opentelemetry` SDK, no protobuf and no gRPC here: the payload is a documented,
stable schema, and a program whose job is to still be running in six months
does not need a code generator to post a JSON document.

### What a record becomes

| kind | span |
| --- | --- |
| `turn_started` + `turn_finished` / `turn_failed` | one root span, `invoke_agent`, spanning the whole turn |
| `turn_step` + `turn_step_finished` | one child span, `execute_tool`, with `gen_ai.tool.name` |
| `inference` | `chat {model}`, with `gen_ai.system`, `gen_ai.request.model` and the token counts |
| `surface_run` | `navigate {surface}` |
| `jev_step` | `jev {operation}`, under the navigator run that was open at the time |
| `app_event`, `log`, `process_started` | span events on the turn they happened in |

Trace ids are derived from the turn id, so re-exporting a turn updates a trace
rather than duplicating it. A record outside a turn gets a trace of its own.
Every span carries `starkbot.record_id`, the rowid, so a span in a backend can
be taken back to `starkbot-trace tail` and the record it came from. Attributes
follow the OpenTelemetry GenAI semantic conventions where those exist and
`starkbot.*` where they do not.

### Against a Collector on :4318

```yaml
# otel.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
```

```sh
docker run --rm -p 4318:4318 -v "$PWD/otel.yaml:/etc/otel.yaml" \
  otel/opentelemetry-collector:latest --config /etc/otel.yaml

starkbot-trace export --otlp http://localhost:4318 --since 600
# trace: 315 records mapped to 231 spans
```

Batches are at most 200 spans. A 429 or a 5xx is retried up to four times with
backoff, honouring `Retry-After`; a 4xx fails immediately and says why. A batch
that cannot be delivered is reported and the rest of the export continues, and
the command exits non-zero if anything failed.

## License

MIT.
