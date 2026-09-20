# Starkbot Trace

An OpenTelemetry receiver and a reader for agent traces. It accepts spans over
OTLP/HTTP, stores them in SQLite, and turns them back into something a person
can read: recent turns, one turn's waterfall, latency percentiles, token
totals, a live dashboard.

It records what a [Starkbot Neo](https://github.com/ethereumdegen/starkbot-neo)
agent actually did — every turn, every step the model chose, every inference
round trip, every navigator decision against a web page or a native macOS
window — and it does that the same way it would record any other instrumented
program:

```sh
starkbot-trace receive                                  # listens on 127.0.0.1:4318
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 neo   # and the agent exports to it
```

The two programs share a protocol and no code. Nothing in this repository is
linked into Neo, nothing in Neo is linked into this, and either side can be
replaced by anything else that speaks OTLP: point Neo at an OpenTelemetry
Collector instead and it will not notice, point this receiver at some other
OTel-instrumented program and it will store and display that too.

## Run

```sh
cargo run --bin starkbot-trace -- receive          # accept spans, store them
cargo run --bin starkbot-trace -- watch            # live dashboard
cargo run --bin starkbot-trace -- turns            # recent agent turns
cargo run --bin starkbot-trace -- turn <id>        # the waterfall of one trace
cargo run --bin starkbot-trace -- stats --since 60
cargo run --bin starkbot-trace -- tail --follow --name 'invoke_agent'
cargo run --bin starkbot-trace -- runs
```

Every report takes `--json`. `receive` accepts `--addr`, `--db`, `--mirror` (an
append-only copy of every document, one JSON object per line, for `jq`),
`--otlp` and `--header`.

Defaults:

| What | Where |
| --- | --- |
| listen address | `127.0.0.1:4318`, the OTLP/HTTP port |
| path | `POST /v1/traces`, `content-type: application/json`, `content-encoding: gzip` optional |
| database | `$STARKBOT_TRACE_DB`, else `~/Library/Application Support/com.starkbot.trace/trace.db` |

Only JSON encoding is accepted. OTLP's default is protobuf, and a protobuf
document is answered with 415 saying so, because JSON needs no code generator
on either side and can be read with `jq` when a span comes out wrong. A
Collector accepts OTLP/JSON on 4318 unmodified, so nothing downstream has to
know.

## What it understands

Any span is stored and displayed. These are the ones it also summarises,
following the OpenTelemetry GenAI semantic conventions where they exist:

| span | what it is | attributes it reads |
| --- | --- | --- |
| `invoke_agent` | one agent turn, the root of its trace | `starkbot.turn`, `starkbot.user_text`, `starkbot.steps`, `starkbot.answer`, `starkbot.exhausted`, `starkbot.asked` |
| `execute_tool` | one action the model chose | `gen_ai.tool.name`, `gen_ai.tool.call.id`, `starkbot.target`, `starkbot.goal`, `starkbot.thought`, `starkbot.observation` |
| `chat {model}` | one inference | `gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `starkbot.prompt_chars` |
| `navigate {surface}` | one navigator run against a browser or an app | `starkbot.surface`, `starkbot.target`, `starkbot.goal`, `starkbot.outcome`, `starkbot.steps` |
| `jev {OPERATION}` | one navigator decision | `starkbot.operation`, `starkbot.confidence`, `starkbot.candidates`, `starkbot.stale`, `starkbot.label`, `starkbot.typed_chars`, the observe/decide/type/act split |

The resource names the producer: `service.name`, `service.version`,
`process.pid`, `starkbot.surface` (`neo-cli`, `neo-tui`, `neo-desktop`) and
`starkbot.run`, a uuid v7 assigned once per process. A span event named
`app_event.{type}` is anything the agent's core published, and shows up as a
mark under its span in the waterfall.

A span with a name this build has never heard of is stored, labelled from its
attributes, and listed by `tail`, `stats` and the dashboard like any other; it
simply has no special summary. `status.code = 2` is a failure everywhere, with
the message shown beside the span.

Those attributes are lifted into columns at ingest — service, surface, run,
pid, turn, operation, model, system, token counts, duration — so `turns` and
`stats` are index scans rather than a JSON walk over every row. The attribute
map and the events are stored whole next to them.

## What it will not record

Nothing here can record what a producer never sends, and the producer is
careful: credentials never reach an attribute, and text typed into a field is
counted (`starkbot.typed_chars`), never kept — a login form holds a password,
and a trace that stored it would be a credential store nobody asked for.
Prompts are counted (`starkbot.prompt_chars`), not copied. Goals, thoughts,
observations and model answers *are* recorded; they are the point.

## Forwarding

A trace that only this tool can read is a trace nobody else will look at, so
stored spans can be posted on to another OTLP endpoint — a Collector, and from
there Jaeger, Tempo, Honeycomb or any of the AI-observability backends.

```sh
# Everything from the last ten hours, to a Collector on this machine.
starkbot-trace export --otlp http://localhost:4318 --since 600

# One trace, on stdout, with no backend anywhere.
starkbot-trace export --dry-run --trace 7f3a1c9e | jq '.resourceSpans[].scopeSpans[].spans[].name'

# Keep forwarding as spans land.
starkbot-trace export --otlp http://localhost:4318 --follow

# Or receive and forward in one process.
starkbot-trace receive --otlp http://localhost:4318
```

`export` takes `--since`, `--trace`, `--follow`, `--dry-run` and
`--header 'name: value'` (repeatable, for an endpoint that wants an API key).
The endpoint is a base URL; `/v1/traces` is appended, and an endpoint that
already ends in it is left alone. The standard variables are read as defaults:

| Variable | Meaning |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the base URL, when `--otlp` is not given |
| `OTEL_EXPORTER_OTLP_HEADERS` | extra headers as `k=v,k2=v2`; a `--header` of the same name wins |

Batches are at most 200 spans. A 429 or a 5xx is retried up to four times with
backoff, honouring `Retry-After`; a 4xx fails immediately and says why. A batch
that cannot be delivered is reported and the rest of the export continues, and
the command exits non-zero if anything failed.

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
# trace: 231 spans in 2 document(s)
```

## License

MIT.
