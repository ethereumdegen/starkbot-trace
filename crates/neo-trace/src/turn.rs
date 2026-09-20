//! Which turn a record belongs to, without a parameter on every signature.
//!
//! A turn fans out: the loop asks the model for a step, the step launches a
//! navigator, the navigator asks the model again, and a dozen functions in
//! three crates end up emitting records. Threading a turn id through all of
//! them would change public signatures that have nothing to do with tracing,
//! and every new call site would be one more place to forget it. So the id
//! lives in a `tokio::task_local`: the turn wraps its whole future in
//! [`turn_scope`] and anything awaited inside — including a nested `ask_json`
//! or a navigator step — reads it back with [`current_turn`].
//!
//! Task-local, not thread-local, because tokio moves futures between worker
//! threads mid-turn; a thread-local would attribute half a turn to nothing.
//! Outside a scope — process start-up, a background refresh, the desktop's own
//! event stream — [`current_turn`] is simply `None`, which is a correct answer
//! and not an error.

tokio::task_local! {
    static TURN: String;
}

/// A fresh turn id. v7 so the collector can sort turns by id and get time order.
#[must_use]
pub fn new_turn_id() -> String {
    uuid::Uuid::now_v7().hyphenated().to_string()
}

/// The turn the current task is running inside, if any.
#[must_use]
pub fn current_turn() -> Option<String> {
    TURN.try_with(Clone::clone).ok()
}

/// Run `future` as part of `turn_id`. Everything it awaits sees that id.
pub async fn turn_scope<T>(turn_id: String, future: impl Future<Output = T>) -> T {
    TURN.scope(turn_id, future).await
}

#[cfg(test)]
mod tests {
    // A failed `expect` in a test is the test failing, which is the point.
    #![allow(clippy::expect_used)]
    use super::*;

    /// Two levels deep is the case that matters: this is how a navigator step
    /// inherits the turn the agent loop launched it from.
    async fn inner() -> Option<String> {
        current_turn()
    }

    async fn outer() -> Option<String> {
        inner().await
    }

    #[tokio::test]
    async fn a_scope_attributes_nested_work_and_ends_with_it() {
        assert_eq!(current_turn(), None);
        let turn = new_turn_id();
        let seen = turn_scope(turn.clone(), outer()).await;
        assert_eq!(seen, Some(turn));
        assert_eq!(current_turn(), None);
    }

    #[tokio::test]
    async fn turn_ids_are_unique() {
        assert_ne!(new_turn_id(), new_turn_id());
    }
}
