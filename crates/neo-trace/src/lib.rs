//! What Neo actually did, as it does it.
//!
//! The agent's own logs answer "did it work?". They do not answer "why did the
//! navigator click that, what did the model see, and where did the eleven
//! seconds go?" — the questions that matter when a turn goes wrong once in
//! twenty runs. This crate is the producer side of the answer: any Neo front
//! end calls [`init`] once, instrumented code calls [`emit`], and the records
//! stream as newline-delimited JSON over a Unix socket to `starkbot-trace`,
//! which stores and reports on them.
//!
//! Two properties are non-negotiable, because a debugging tool that changes
//! what it observes is worse than no tool. Tracing must never slow the agent
//! down — [`emit`] stamps an envelope and pushes it into a bounded queue that
//! drops under pressure, and one background thread owns the socket, so a slow
//! or absent collector costs a caller nothing (`client.rs`). And tracing must
//! never leak a secret — prompts, answers, goals and observations are recorded
//! because they are the point, but credentials, tokens, key values and the
//! text the navigator types into a field are not (`wire.rs`).
//!
//! Records are attributed to a turn through a task-local rather than a
//! parameter on every function, which is what makes instrumenting a nested
//! model call a one-line change (`turn.rs`).

mod client;
mod turn;
mod wire;

pub use client::{emit, enabled, init, socket_path};
pub use turn::{current_turn, new_turn_id, turn_scope};
pub use wire::{Body, DEFAULT_SOCKET_NAME, Record, SOCKET_ENV, WIRE_VERSION};
