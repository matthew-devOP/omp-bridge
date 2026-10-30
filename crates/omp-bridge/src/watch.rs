//! Human-facing daemon clients: `watch` streams a session's events, `attach` adds input.
//!
//! Both speak the daemon wire from docs/DAEMON-CONTRACT.md as plain clients:
//! one fresh connection per request (connect → one JSON line out → one JSON
//! line back → close), with the daemon-issued `client_id` reused across
//! requests. Everything here is blocking std I/O and self-contained.

use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

const POLL_WAIT_MS: u64 = 20_000;
const LINE_WIDTH: usize = 120;

pub fn run_watch(session: &str, socket: Option<String>, raw_json: bool) -> Result<()> {
    let path = socket_path(socket);
    let client = hello(&path, "watch")?;
    let start = Instant::now();
    let color = !raw_json && std::io::stdout().is_terminal();
    let mut cursor = 0u64;
    loop {
        let page = fetch_events(&path, &client, session, cursor, POLL_WAIT_MS)?;
        emit_page(&page, start, color, raw_json);
        cursor = page.next_cursor;
    }
}

pub fn run_attach(session: &str, socket: Option<String>) -> Result<()> {
    let path = socket_path(socket);
    let client = hello(&path, "attach")?;
    eprintln!(
        "attached as {client}: plain line = steer, >line = follow-up, !take = take control, /quit = leave"
    );

    let start = Instant::now();
    let color = std::io::stdout().is_terminal();
    let event_path = path.clone();
    let event_client = client.clone();
    let event_session = session.to_owned();
    std::thread::spawn(move || {
        let mut cursor = 0u64;
        loop {
            match fetch_events(&event_path, &event_client, &event_session, cursor, POLL_WAIT_MS) {
                Ok(page) => {
                    emit_page(&page, start, color, false);
                    cursor = page.next_cursor;
                }
                Err(error) => {
                    eprintln!("events: {error}");
                    break;
                }
            }
        }
    });

    let mut has_control = false;
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "/quit" {
            break;
        }
        if line == "!take" {
            match take_control(&path, &client, session) {
                Ok(()) => {
                    has_control = true;
                    println!("take_control: accepted");
                }
                Err(error) => println!("take_control: {error}"),
            }
            continue;
        }
        let (method, text) = match line.strip_prefix('>') {
            Some(rest) => ("omp_follow_up", rest.trim()),
            None => ("omp_steer", line),
        };
        // The lease is taken explicitly, and only once, before the first write.
        if !has_control {
            match take_control(&path, &client, session) {
                Ok(()) => has_control = true,
                Err(error) => {
                    println!("take_control: {error}");
                    continue;
                }
            }
        }
        match call(&path, Some(&client), method, json!({"session": session, "text": text})) {
            Ok(_) => println!("{method}: accepted"),
            Err(error) if error.to_string() == "not_controller" => {
                has_control = false;
                println!("not controller — type !take to take control");
            }
            Err(error) => println!("{method}: {error}"),
        }
    }
    Ok(())
}

fn take_control(path: &str, client: &str, session: &str) -> Result<()> {
    call(path, Some(client), "omp_take_control", json!({"session": session, "reason": "operator attach"}))
        .map(|_| ())
}

/// One request on a fresh connection: connect, one JSON line out, one line back.
/// `Ok` carries the reply's `result`; failures surface the short error `code`.
fn call(path: &str, client_id: Option<&str>, method: &str, params: Value) -> Result<Value> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let id = format!("r-{}", SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1);
    let mut frame = json!({"id": id, "method": method, "params": params});
    if let Some(client) = client_id {
        frame["client_id"] = json!(client);
    }
    let mut stream =
        UnixStream::connect(path).map_err(|_| anyhow!("daemon_unreachable: {path}"))?;
    writeln!(stream, "{frame}").map_err(|_| anyhow!("socket_write_failed"))?;
    stream.flush().map_err(|_| anyhow!("socket_write_failed"))?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(|_| anyhow!("socket_read_failed"))? == 0 {
        bail!("daemon_closed");
    }
    let reply: Value = serde_json::from_str(&line).map_err(|_| anyhow!("invalid_reply"))?;
    if reply["id"].as_str() != Some(id.as_str()) {
        bail!("reply_id_mismatch");
    }
    if reply["ok"] == json!(true) {
        return Ok(reply["result"].clone());
    }
    bail!("{}", reply["error"]["code"].as_str().unwrap_or("daemon_error"))
}

fn hello(path: &str, name: &str) -> Result<String> {
    let result = call(path, None, "hello", json!({"name": name}))?;
    result["client_id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("hello_missing_client_id"))
}

#[derive(Debug)]
pub struct EventsPage {
    pub events: Vec<Value>,
    pub next_cursor: u64,
    pub dropped: u64,
}

fn fetch_events(
    path: &str,
    client: &str,
    session: &str,
    cursor: u64,
    wait_ms: u64,
) -> Result<EventsPage> {
    let params = json!({"session": session, "cursor": cursor, "wait_ms": wait_ms});
    parse_events_page(&call(path, Some(client), "omp_events", params)?)
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

fn emit_page(page: &EventsPage, start: Instant, color: bool, raw_json: bool) {
    if page.dropped > 0 {
        if raw_json {
            println!("{}", json!({"type": "bridge_gap", "dropped": page.dropped}));
        } else {
            println!("{}", paint("90", &format!("   gap: {} events dropped", page.dropped), color));
        }
    }
    for envelope in &page.events {
        if raw_json {
            println!("{envelope}");
        } else {
            println!("{}", render_event(start, envelope, color));
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

fn socket_path(argument: Option<String>) -> String {
    if let Some(path) = argument {
        return path;
    }
    if let Ok(path) = std::env::var("OMP_BRIDGE_SOCKET")
        && !path.is_empty()
    {
        return path;
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR")
        && !dir.is_empty()
    {
        return std::path::Path::new(&dir).join("omp-bridge.sock").to_string_lossy().into_owned();
    }
    let home = std::env::var("HOME").ok().filter(|h| !h.is_empty()).unwrap_or_else(|| ".".into());
    std::path::Path::new(&home)
        .join(".local/state/omp-bridge/bridge.sock")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

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
                assert!(request.get("client_id").is_none(), "hello carries no client_id");
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
        let client = hello(&path, "watch").unwrap();
        assert_eq!(client, "client-9");
        let page = fetch_events(&path, &client, "session-1", 5, 1_000).unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.next_cursor, 7);
        assert_eq!(page.dropped, 0);
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn error_reply_surfaces_short_code_only() {
        let dir = std::env::temp_dir().join(format!("omp-bridge-watch-err-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("err.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            serve_one(&listener, |request| {
                json!({"id": request["id"], "ok": false, "error": {
                    "code": "not_controller", "message": "client-1 holds the lease"
                }})
            });
        });
        let path = path.to_string_lossy().into_owned();
        let error = call(&path, Some("client-2"), "omp_steer", json!({"session": "s", "text": "x"}))
            .unwrap_err()
            .to_string();
        assert_eq!(error, "not_controller");
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
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
