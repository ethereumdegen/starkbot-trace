//! The one shape the agent and the collector both agree on.
//!
//! The collector is a separate binary that outlives any single Neo process and
//! stores records it may not understand yet, so the envelope carries [`WIRE_VERSION`]
//! and the payload is an internally tagged enum: a collector built against an
//! older tag still reads `kind`, `run`, `turn` and the timings out of a body it
//! has no variant for. Everything here is plain data with no behaviour, so the
//! producer can build a record on a caller thread in a few allocations and hand
//! it straight to the writer.
//!
//! What is *not* here is as deliberate as what is. Prompts, answers, goals and
//! observations are the whole point of the tool and go in verbatim. Credentials,
//! OAuth tokens, key values and the literal text the navigator types into a
//! field never do — [`Body::JevStep`] carries `typed_chars`, a length, because a
//! password typed into a login form would otherwise land in a database nobody
//! thinks of as a secret store.

use serde::{Deserialize, Serialize};

/// Bumped when a field changes meaning, not when one is added. The collector
/// stores records whose version it does not know rather than dropping them.
pub const WIRE_VERSION: u32 = 1;

/// Absolute path of the collector's socket, overriding the default location.
pub const SOCKET_ENV: &str = "STARKBOT_TRACE_SOCKET";

/// The socket's name inside Neo's data directory, which is where the collector
/// puts it when nobody says otherwise.
pub const DEFAULT_SOCKET_NAME: &str = "trace.sock";

/// One thing that happened, with enough context to place it in a run and a turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub v: u32,
    /// Per-process, starting at 1. Gaps are real: a record the producer had to
    /// drop still consumed its number, so a reader can see the hole.
    pub seq: u64,
    pub ts_ms: i64,
    /// uuid v7, assigned once per process.
    pub run: String,
    /// `"neo-cli"`, `"neo-tui"`, `"neo-desktop"`, or whatever else called `init`.
    pub source: String,
    pub pid: u32,
    /// The agent turn this happened inside, when there is one.
    pub turn: Option<String>,
    /// Records lost to backpressure since the last record that reported them.
    /// Carried in-band because the collector cannot otherwise tell a quiet agent
    /// from a drowning one.
    pub dropped: u64,
    pub body: Body,
}

/// Everything worth recording, tagged by `kind` so the collector can filter on
/// it in SQL without deserializing the payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Body {
    ProcessStarted {
        version: String,
        args: Vec<String>,
    },
    TurnStarted {
        user_text: String,
        history_len: usize,
        max_steps: usize,
    },
    TurnStep {
        index: usize,
        thought: String,
        /// `"browse"`, `"app"`, `"answer"` or `"ask"`.
        action: String,
        target: Option<String>,
        goal: Option<String>,
    },
    TurnStepFinished {
        index: usize,
        action: String,
        observation: String,
        duration_ms: u64,
    },
    TurnFinished {
        steps: usize,
        exhausted: bool,
        asked: bool,
        text: String,
        duration_ms: u64,
    },
    TurnFailed {
        code: String,
        message: String,
        duration_ms: u64,
    },
    Inference {
        provider: String,
        model: String,
        json: bool,
        /// The prompt's size, not the prompt: a system prompt is stable and huge,
        /// and the interesting number is how it grew.
        prompt_chars: usize,
        duration_ms: u64,
        usage: serde_json::Value,
        ok: bool,
        error: Option<String>,
    },
    SurfaceRun {
        /// `"browser"` or `"app"`.
        surface: String,
        target: String,
        goal: String,
        outcome: String,
        steps: usize,
        duration_ms: u64,
        ok: bool,
        error: Option<String>,
    },
    JevStep {
        surface: String,
        target: String,
        index: usize,
        operation: String,
        confidence: f64,
        label: Option<String>,
        /// How much was typed, never what was typed.
        typed_chars: Option<usize>,
        candidates: usize,
        stale: bool,
        observe_ms: u64,
        jev_ms: u64,
        text_ms: u64,
        act_ms: u64,
        elapsed_ms: u64,
        usage: serde_json::Value,
    },
    /// A serialized `neo_core::AppEvent`, kept opaque so the producer does not
    /// depend on the event enum and does not have to change when it grows.
    AppEvent {
        event: serde_json::Value,
    },
    Log {
        level: String,
        message: String,
    },
}

impl Body {
    /// The value of the serialized `kind` tag, for callers that want to filter
    /// or count without going through serde.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProcessStarted { .. } => "process_started",
            Self::TurnStarted { .. } => "turn_started",
            Self::TurnStep { .. } => "turn_step",
            Self::TurnStepFinished { .. } => "turn_step_finished",
            Self::TurnFinished { .. } => "turn_finished",
            Self::TurnFailed { .. } => "turn_failed",
            Self::Inference { .. } => "inference",
            Self::SurfaceRun { .. } => "surface_run",
            Self::JevStep { .. } => "jev_step",
            Self::AppEvent { .. } => "app_event",
            Self::Log { .. } => "log",
        }
    }
}

#[cfg(test)]
mod tests {
    // A failed `expect` in a test is the test failing, which is the point.
    #![allow(clippy::expect_used)]
    use super::*;
    use serde_json::json;

    fn jev_step() -> Record {
        Record {
            v: WIRE_VERSION,
            seq: 7,
            ts_ms: 1_726_000_000_000,
            run: "0191f0a0-0000-7000-8000-000000000001".to_string(),
            source: "neo-cli".to_string(),
            pid: 4242,
            turn: Some("0191f0a0-0000-7000-8000-000000000002".to_string()),
            dropped: 3,
            body: Body::JevStep {
                surface: "app".to_string(),
                target: "LibreOffice".to_string(),
                index: 2,
                operation: "click".to_string(),
                confidence: 0.91,
                label: Some("Save".to_string()),
                typed_chars: Some(12),
                candidates: 37,
                stale: false,
                observe_ms: 40,
                jev_ms: 120,
                text_ms: 10,
                act_ms: 70,
                elapsed_ms: 240,
                usage: json!({ "input_tokens": 27, "output_tokens": 5 }),
            },
        }
    }

    #[test]
    fn a_record_round_trips_through_json() {
        let record = jev_step();
        let text = serde_json::to_string(&record).expect("a record serializes");
        let parsed: Record = serde_json::from_str(&text).expect("a record parses back");
        assert_eq!(parsed, record);
    }

    #[test]
    fn a_body_is_tagged_by_a_snake_case_kind() {
        let value = serde_json::to_value(jev_step()).expect("a record serializes");
        assert_eq!(
            value,
            json!({
                "v": 1,
                "seq": 7,
                "ts_ms": 1_726_000_000_000_i64,
                "run": "0191f0a0-0000-7000-8000-000000000001",
                "source": "neo-cli",
                "pid": 4242,
                "turn": "0191f0a0-0000-7000-8000-000000000002",
                "dropped": 3,
                "body": {
                    "kind": "jev_step",
                    "surface": "app",
                    "target": "LibreOffice",
                    "index": 2,
                    "operation": "click",
                    "confidence": 0.91,
                    "label": "Save",
                    "typed_chars": 12,
                    "candidates": 37,
                    "stale": false,
                    "observe_ms": 40,
                    "jev_ms": 120,
                    "text_ms": 10,
                    "act_ms": 70,
                    "elapsed_ms": 240,
                    "usage": { "input_tokens": 27, "output_tokens": 5 }
                }
            })
        );
    }

    #[test]
    fn kind_matches_the_serialized_tag() {
        let bodies = [
            Body::ProcessStarted {
                version: "0.0.1".to_string(),
                args: Vec::new(),
            },
            Body::TurnStarted {
                user_text: "hello".to_string(),
                history_len: 0,
                max_steps: 8,
            },
            Body::TurnStep {
                index: 0,
                thought: "look".to_string(),
                action: "browse".to_string(),
                target: None,
                goal: None,
            },
            Body::TurnStepFinished {
                index: 0,
                action: "browse".to_string(),
                observation: "done".to_string(),
                duration_ms: 1,
            },
            Body::TurnFinished {
                steps: 1,
                exhausted: false,
                asked: false,
                text: "ok".to_string(),
                duration_ms: 1,
            },
            Body::TurnFailed {
                code: "runtime".to_string(),
                message: "no".to_string(),
                duration_ms: 1,
            },
            Body::Inference {
                provider: "anthropic".to_string(),
                model: "claude".to_string(),
                json: true,
                prompt_chars: 10,
                duration_ms: 1,
                usage: json!({}),
                ok: true,
                error: None,
            },
            Body::SurfaceRun {
                surface: "app".to_string(),
                target: "Notes".to_string(),
                goal: "write".to_string(),
                outcome: "done".to_string(),
                steps: 1,
                duration_ms: 1,
                ok: true,
                error: None,
            },
            jev_step().body,
            Body::AppEvent { event: json!({}) },
            Body::Log {
                level: "info".to_string(),
                message: "up".to_string(),
            },
        ];
        for body in bodies {
            let kind = body.kind();
            let value = serde_json::to_value(&body).expect("a body serializes");
            assert_eq!(
                value.get("kind").and_then(serde_json::Value::as_str),
                Some(kind)
            );
        }
    }
}
