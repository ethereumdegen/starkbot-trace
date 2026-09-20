//! The promise this crate lives or dies by: a collector that stops reading
//! must cost the agent nothing.
//!
//! This is an integration test and not a unit test because `init` is
//! once-per-process, and the unit tests already spent that one initialization
//! on the happy path. Here the collector accepts the connection and then goes
//! to sleep, which is what a collector doing a slow SQLite write looks like
//! from the agent's side: the socket buffer fills, the writer thread blocks in
//! `write`, the bounded queue fills behind it, and `emit` has to keep
//! returning immediately and counting what it threw away.

// A failed `expect` in a test is the test failing, which is the point.
#![allow(clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixListener;
use std::time::{Duration, Instant};

use neo_trace::{Body, Record};

/// Far more than the queue plus the socket buffer can hold.
const RECORDS: usize = 20_000;

/// The last record the producer sends. The collector stops there instead of
/// waiting for a stream that only ends when the process does.
const SENTINEL: &str = "after the burst";

#[test]
fn a_stalled_collector_never_blocks_the_caller() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("trace.sock");
    let listener = UnixListener::bind(&path).expect("a collector socket");

    // Safety: this test binary holds a single test and sets the variable
    // before any writer thread exists to read it.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var(neo_trace::SOCKET_ENV, &path);
    }
    neo_trace::init("neo-tui");
    assert!(neo_trace::enabled());

    let collector = std::thread::spawn(move || {
        let (client, _) = listener.accept().expect("the writer connects");
        // Nobody is reading. This is the stall the producer has to survive.
        std::thread::sleep(Duration::from_millis(400));
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("a read deadline");
        let mut records = Vec::new();
        for line in BufReader::new(client).lines() {
            let Ok(line) = line else { break };
            let record: Record = serde_json::from_str(&line).expect("a record");
            let last = matches!(&record.body, Body::Log { message, .. } if message == SENTINEL);
            records.push(record);
            if last {
                break;
            }
        }
        records
    });

    let started = Instant::now();
    for index in 0..RECORDS {
        neo_trace::emit(Body::Log {
            level: "info".to_string(),
            message: format!("{index}: {}", "x".repeat(512)),
        });
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "{RECORDS} emits took {elapsed:?} behind a stalled collector"
    );

    // Once the collector catches up, the next record has to carry the count of
    // everything the queue refused while it was stalled.
    std::thread::sleep(Duration::from_millis(900));
    neo_trace::emit(Body::Log {
        level: "warn".to_string(),
        message: SENTINEL.to_string(),
    });

    let records = collector.join().expect("the collector thread");
    let last = records.last().expect("the sentinel arrives");
    assert!(
        matches!(&last.body, Body::Log { message, .. } if message == SENTINEL),
        "the producer keeps working after a stall"
    );
    assert!(
        records.len() < RECORDS,
        "a full queue drops records instead of blocking"
    );
    assert!(
        last.dropped > 0,
        "the drops are counted and reported in-band"
    );
    assert!(
        records.iter().all(|record| record.seq <= last.seq),
        "sequence numbers stay monotonic across a drop storm"
    );
}
