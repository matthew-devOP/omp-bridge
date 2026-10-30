//! Read-only fleet observation and explicitly leased interactive attachment.
use std::collections::BTreeMap;
use std::io::{BufRead, IsTerminal};
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc};
use std::time::{Duration, Instant};
use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use omp_bridge::daemon::DaemonClient;

const LINE_WIDTH: usize = 120;

pub fn run_watch(session: Option<String>, socket: Option<PathBuf>, raw_json: bool, all: bool, cancel: Arc<AtomicBool>) -> Result<()> {
    if all == session.is_some() { bail!("choose a session or --all"); }
    let client = DaemonClient::connect_with_timeout(socket, "watch", Duration::from_secs(2))?;
    let start = Instant::now();
    let color = !raw_json && std::io::stdout().is_terminal();
    let mut cursors = BTreeMap::new();
    if let Some(session) = session { cursors.insert(session, 0); }
    while !cancel.load(Ordering::SeqCst) {
        if all {
            let list = client.call_with_timeout("omp_list", json!({}), Duration::from_secs(2))?;
            let sessions = list["sessions"].as_array().ok_or_else(||anyhow!("invalid_session_list"))?;
            let ids: Vec<&str> = sessions.iter().filter_map(|s|s["session"].as_str()).collect();
            cursors.retain(|id, _| ids.contains(&id.as_str()));
            for id in ids { cursors.entry(id.to_owned()).or_insert(0); }
        }
        // Zero-wait polling prevents an idle session delaying the rest of the fleet.
        for (id, cursor) in &mut cursors {
            if cancel.load(Ordering::SeqCst) { break; }
            match fetch_events(&client, id, *cursor) {
                Ok(page) => { emit_page(id, &page, start, color, raw_json); *cursor = page.next_cursor; }
                Err(error) if all && error.to_string().starts_with("unknown_session") => {},
                Err(error) => return Err(error),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

pub fn run_attach(session: String, socket: Option<PathBuf>, cancel: Arc<AtomicBool>) -> Result<()> {
    let client = DaemonClient::connect_with_timeout(socket, "attach", Duration::from_secs(2))?;
    let start = Instant::now();
    let color = std::io::stdout().is_terminal();
    let first = fetch_events(&client, &session, 0)?; // Validate before waiting for stdin.
    emit_page(&session, &first, start, color, false);
    let mut cursor = first.next_cursor;
    eprintln!("attached as {}: !take = take control (required before writes), plain line = steer, >line = follow-up, /quit = leave", client.client_id());
    let (tx, rx) = mpsc::channel();
    // A blocked terminal read must never keep the async runtime or daemon identity alive.
    // This std thread owns only the sender, and is detached on process exit.
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if tx.send(line).is_err() { break; }
        }
    });
    let mut has_control = false;
    while !cancel.load(Ordering::SeqCst) {
        let page = fetch_events(&client, &session, cursor)?;
        for envelope in &page.events {
            let event = &envelope["event"];
            if event["type"] == "controller_changed" && event["newClient"].as_str() != Some(client.client_id()) {
                has_control = false;
            }
        }
        emit_page(&session, &page, start, color, false);
        cursor = page.next_cursor;
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                let line = line?;
                let line = line.trim();
                if line == "/quit" { break; }
                if line.is_empty() { continue; }
                if line == "!take" {
                    has_control = false;
                    match client.call_with_timeout("omp_take_control", json!({"session":session,"reason":"operator attach"}), Duration::from_secs(2)) {
                        Ok(_) => { has_control = true; println!("take_control: accepted"); }
                        Err(error) => eprintln!("take_control: {error}"),
                    }
                    continue;
                }
                if !has_control { eprintln!("not controller — type !take to take control"); continue; }
                let (method, text) = match line.strip_prefix('>') {
                    Some(rest) => ("omp_follow_up", rest.trim()),
                    None => ("omp_steer", line),
                };
                match client.call_with_timeout(method, json!({"session":session,"text":text}), Duration::from_secs(2)) {
                    Ok(_) => println!("{method}: accepted"),
                    Err(error) => {
                        if error.to_string().starts_with("not_controller") { has_control = false; }
                        eprintln!("{method}: {error}; use !take explicitly if control was lost");
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {},
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct EventsPage {
    pub events: Vec<Value>,
    pub next_cursor: u64,
    pub dropped: u64,
}

fn fetch_events(client: &DaemonClient, session: &str, cursor: u64) -> Result<EventsPage> {
    parse_events_page(&client.call_with_timeout("omp_events", json!({"session":session,"cursor":cursor,"wait_ms":0}), Duration::from_secs(2))?)
}

fn parse_events_page(result: &Value) -> Result<EventsPage> {
    Ok(EventsPage {
        events: result["events"]
            .as_array()
            .ok_or_else(|| anyhow!("invalid_events_page"))?
            .clone(),
        next_cursor: result["nextCursor"]
            .as_u64()
            .ok_or_else(|| anyhow!("invalid_events_page"))?,
        dropped: result["dropped"].as_u64().unwrap_or(0),
    })
}

fn emit_page(session: &str, page: &EventsPage, start: Instant, color: bool, raw_json: bool) {
    if page.dropped > 0 {
        if raw_json {
            println!("{}", json!({"session":session,"type": "bridge_gap", "dropped": page.dropped}));
        } else {
            println!("{}", paint("90", &format!("   gap: {} events dropped", page.dropped), color));
        }
    }
    for envelope in &page.events {
        if raw_json {
            let mut tagged = envelope.clone();
            tagged["session"] = json!(session);
            println!("{tagged}");
        } else {
            println!("[{session}] {}", render_event(start, envelope, color));
        }
    }
}

fn render_event(start: Instant, envelope: &Value, color: bool) -> String {
    let event = &envelope["event"];
    let kind = event["type"].as_str().unwrap_or("?");
    let (code, body) = match kind {
        "agent_start" => ("32", String::new()),
        "message_end" => ("0", message_text(event)),
        k if k.starts_with("tool_execution") => ("33", tool_name(event)),
        "prompt_result" => ("36", format!("status={}", event["status"].as_str().unwrap_or("?"))),
        "session_settled" => ("36", String::new()),
        "controller_changed" => (
            "35",
            format!(
                "{} → {} ({})",
                event["previousClient"].as_str().unwrap_or("-"),
                event["newClient"].as_str().unwrap_or("-"),
                event["reason"].as_str().unwrap_or("-")
            ),
        ),
        // Earlier daemon builds used this name for the same transfer event.
        "bridge_control_transfer" => (
            "35",
            format!(
                "{} → {} ({})",
                event["from"].as_str().unwrap_or("-"),
                event["to"].as_str().unwrap_or("-"),
                event["reason"].as_str().unwrap_or("-")
            ),
        ),
        k if k.starts_with("bridge_") => ("90", String::new()),
        _ => ("0", String::new()),
    };
    let seconds = start.elapsed().as_secs_f64();
    let mut line = format!("{seconds:>7.1}s  {kind}  {body}");
    if line.chars().count() > LINE_WIDTH {
        line = line.chars().take(LINE_WIDTH - 1).collect::<String>() + "…";
    }
    paint(code, &line, color)
}

fn paint(code: &str, line: &str, color: bool) -> String {
    if color && code != "0" { format!("\x1b[{code}m{line}\x1b[0m") } else { line.to_owned() }
}

fn message_text(event: &Value) -> String {
    let message = &event["message"];
    if let Some(blocks) = message["content"].as_array() {
        for block in blocks {
            if let Some(text) = block["text"].as_str() {
                return text.replace('\n', " ");
            }
        }
    }
    if let Some(text) = message["content"].as_str() {
        return text.replace('\n', " ");
    }
    message["role"].as_str().unwrap_or("").to_owned()
}

fn tool_name(event: &Value) -> String {
    for key in ["toolName", "tool", "name"] {
        if let Some(name) = event[key].as_str() {
            return name.to_owned();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::io::{BufReader, Write};

    /// One fake-daemon exchange: read a line, assert with `check`, reply.
    fn serve_one(listener: &UnixListener, check: impl FnOnce(&Value) -> Value) {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        let reply = check(&request);
        writeln!(writer, "{reply}").unwrap();
    }

    #[test]
    fn hello_then_fetch_use_fresh_connections_and_advance_cursor() {
        let dir = std::env::temp_dir().join(format!("omp-bridge-watch-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("watch.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            serve_one(&listener, |request| {
                assert_eq!(request["method"], "hello");
                assert!(request["client_id"].is_null(), "hello carries no client_id");
                assert_eq!(request["params"]["name"], "watch");
                json!({"id": request["id"], "ok": true, "result": {"client_id": "client-9"}})
            });
            serve_one(&listener, |request| {
                assert_eq!(request["method"], "omp_events");
                assert_eq!(request["client_id"], "client-9");
                assert_eq!(request["params"]["cursor"], 5);
                assert_eq!(request["params"]["session"], "session-1");
                json!({"id": request["id"], "ok": true, "result": {
                    "events": [
                        {"cursor": 5, "event": {"type": "agent_start"}},
                        {"cursor": 6, "event": {"type": "session_settled"}}
                    ],
                    "nextCursor": 7, "oldestCursor": 0, "dropped": 0
                }})
            });
        });

        let path = path.to_string_lossy().into_owned();
        let client = DaemonClient::connect(Some(PathBuf::from(&path)), "watch").unwrap();
        assert_eq!(client.client_id(), "client-9");
        let page = fetch_events(&client, "session-1", 5).unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.next_cursor, 7);
        assert_eq!(page.dropped, 0);
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn malformed_event_page_is_rejected() {
        assert!(parse_events_page(&json!({"events":[],"nextCursor":"bad"})).is_err());
        assert!(parse_events_page(&json!({"events":{},"nextCursor":0})).is_err());
    }

    #[test]
    fn controller_changed_renders_parties_and_reason() {
        let start = Instant::now();
        let transfer = json!({"cursor": 3, "event": {
            "type": "controller_changed",
            "previousClient": "client-1", "newClient": "client-2",
            "reason": "handoff", "leaseEpoch": 2
        }});
        let line = render_event(start, &transfer, false);
        assert!(line.contains("client-1 → client-2 (handoff)"), "{line}");
    }
}
