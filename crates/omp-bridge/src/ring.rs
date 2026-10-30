//! Bounded, independently cursor-addressed event history for one omp session.
//!
//! A cursor is the sequence number of the next event to read, initially zero.
//! Filtering never changes the producer's event filter or another reader's cursor.

use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

const MAX_EVENTS: usize = 2048;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_WAIT_MS: u64 = 25_000;

#[derive(Debug)]
struct Entry {
    cursor: u64,
    event: Value,
    bytes: usize,
}

#[derive(Debug, Default)]
struct State {
    entries: VecDeque<Entry>,
    bytes: usize,
    next: u64,
}

#[derive(Debug, Default)]
pub struct EventRing {
    state: Mutex<State>,
    changed: Condvar,
}

impl EventRing {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one frame. Oversized frames become explicit metadata markers.
    /// The byte limit includes each stored frame's JSON cursor envelope.
    pub fn push(&self, mut event: Value) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let cursor = state.next;
        let original_bytes = event.to_string().len();
        let mut bytes = envelope_bytes(cursor, &event);
        if bytes > MAX_BYTES {
            let kind: String = event_kind(&event).chars().take(256).collect();
            event = json!({
                "type": kind,
                "truncated": true,
                "originalBytes": original_bytes,
                "reason": "event_exceeds_ring_byte_limit"
            });
            bytes = envelope_bytes(cursor, &event);
        }
        while state.entries.len() >= MAX_EVENTS || state.bytes + bytes > MAX_BYTES {
            if let Some(old) = state.entries.pop_front() {
                state.bytes -= old.bytes;
            } else {
                break;
            }
        }
        state.entries.push_back(Entry { cursor, event, bytes });
        state.bytes += bytes;
        state.next += 1;
        drop(state);
        self.changed.notify_all();
    }

    /// Read a snapshot, waiting at most 25 seconds if no new frames exist.
    /// `nextCursor` advances past *all* observed frames, including filtered ones.
    /// `dropped` counts unavailable frames between the requested and oldest cursors.
    /// New non-matching frames also wake readers; an empty filtered page still
    /// carries a useful advanced cursor. Future cursors are explicitly rejected.
    pub fn read(&self, cursor: u64, kinds: &[String], wait_ms: u64) -> Value {
        let deadline = Instant::now() + Duration::from_millis(wait_ms.min(MAX_WAIT_MS));
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if cursor > state.next {
            return json!({
                "error": "cursor_ahead",
                "events": [],
                "requestedCursor": cursor,
                "oldestCursor": oldest(&state),
                "nextCursor": state.next,
                "dropped": 0,
                "gap": false
            });
        }
        while state.next == cursor {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (guard, timeout) = self.changed.wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner());
            state = guard;
            if timeout.timed_out() {
                break;
            }
        }
        let oldest_cursor = oldest(&state);
        let dropped = oldest_cursor.saturating_sub(cursor);
        let events: Vec<Value> = state.entries.iter()
            .filter(|entry| entry.cursor >= cursor)
            .filter(|entry| kinds.is_empty() || kinds.iter().any(|kind| kind == event_kind(&entry.event)))
            .map(|entry| json!({"cursor": entry.cursor, "event": entry.event}))
            .collect();
        json!({
            "events": events,
            "oldestCursor": oldest_cursor,
            "nextCursor": state.next,
            "dropped": dropped,
            "gap": dropped > 0
        })
    }
}

fn oldest(state: &State) -> u64 {
    state.entries.front().map_or(state.next, |entry| entry.cursor)
}

fn event_kind(event: &Value) -> &str {
    event.get("type").and_then(Value::as_str).unwrap_or("unknown")
}

fn envelope_bytes(cursor: u64, event: &Value) -> usize {
    json!({"cursor": cursor, "event": event}).to_string().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn count_overflow_reports_the_missing_prefix() {
        let ring = EventRing::new();
        for n in 0..MAX_EVENTS + 3 {
            ring.push(json!({"type": "notice", "n": n}));
        }
        let page = ring.read(0, &[], 0);
        assert_eq!(page["events"].as_array().unwrap().len(), MAX_EVENTS);
        assert_eq!(page["oldestCursor"], 3);
        assert_eq!(page["nextCursor"], MAX_EVENTS + 3);
        assert_eq!(page["dropped"], 3);
        assert_eq!(page["gap"], true);
        assert_eq!(page["events"][0]["event"]["n"], 3);
    }

    #[test]
    fn bytes_bound_evicts_before_count_bound() {
        let ring = EventRing::new();
        let payload = "x".repeat(MAX_BYTES / 2);
        ring.push(json!({"type": "message_end", "text": payload}));
        ring.push(json!({"type": "message_end", "text": payload}));
        let state = ring.state.lock().unwrap();
        assert!(state.bytes <= MAX_BYTES);
        assert_eq!(state.entries.len(), 1);
        drop(state);
        let page = ring.read(0, &[], 0);
        assert_eq!(page["dropped"], 1);
        assert_eq!(page["events"][0]["cursor"], 1);
    }

    #[test]
    fn oversized_frame_is_an_explicit_marker_and_keeps_its_cursor() {
        let ring = EventRing::new();
        let original = json!({"type": "agent_end", "text": "x".repeat(MAX_BYTES)});
        let size = original.to_string().len();
        ring.push(original);
        ring.push(json!({"type": "session_settled"}));
        let page = ring.read(0, &[], 0);
        assert_eq!(page["events"][0]["cursor"], 0);
        assert_eq!(page["events"][0]["event"]["type"], "agent_end");
        assert_eq!(page["events"][0]["event"]["truncated"], true);
        assert_eq!(page["events"][0]["event"]["originalBytes"], size);
        assert_eq!(page["events"][1]["cursor"], 1);
        assert_eq!(page["dropped"], 0);
    }

    #[test]
    fn filters_are_per_reader_and_advance_past_nonmatching_frames() {
        let ring = EventRing::new();
        ring.push(json!({"type": "notice"}));
        ring.push(json!({"type": "prompt_result"}));
        ring.push(json!({"type": "notice"}));
        let page = ring.read(0, &["prompt_result".into()], 0);
        assert_eq!(page["events"].as_array().unwrap().len(), 1);
        assert_eq!(page["events"][0]["cursor"], 1);
        assert_eq!(page["nextCursor"], 3);
        assert_eq!(ring.read(0, &[], 0)["events"].as_array().unwrap().len(), 3);
        assert_eq!(ring.read(3, &[], 0)["events"], json!([]));
        assert_eq!(ring.read(9, &[], 0)["error"], "cursor_ahead");
    }

    #[test]
    fn waiting_reader_is_woken_by_a_producer() {
        let ring = Arc::new(EventRing::new());
        let gate = Arc::new(Barrier::new(2));
        let reader_ring = ring.clone();
        let reader_gate = gate.clone();
        let reader = thread::spawn(move || {
            reader_gate.wait();
            reader_ring.read(0, &[], 2_000)
        });
        gate.wait();
        // Whether the producer arrives before or after wait_timeout, no wake is lost.
        ring.push(json!({"type": "session_settled"}));
        let page = reader.join().unwrap();
        assert_eq!(page["events"][0]["event"]["type"], "session_settled");
        assert_eq!(page["nextCursor"], 1);
    }

    #[test]
    fn empty_read_timeout_returns_without_an_event() {
        let ring = EventRing::new();
        let start = Instant::now();
        let page = ring.read(0, &[], 5);
        assert_eq!(page["events"], json!([]));
        assert_eq!(page["nextCursor"], 0);
        assert!(start.elapsed() >= Duration::from_millis(5));
    }
}
