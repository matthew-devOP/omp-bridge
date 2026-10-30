//! Human-facing daemon clients: `watch` streams a session's events, `attach` adds input.
//!
//! Both speak the JSONL socket protocol from docs/DAEMON-CONTRACT.md as plain
//! clients; they never own sessions. Everything here is blocking std I/O.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

const POLL_WAIT_MS: u64 = 20_000;
const REPLY_TIMEOUT: Duration = Duration::from_secs(40);
const LINE_WIDTH: usize = 120;

pub fn run_watch(session: &str, socket: Option<String>, raw_json: bool) -> Result<()> {
    let path = socket_path(socket);
    let stream = connect(&path)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    hello_handshake(&mut writer, &mut reader, &format!("watch-{}", std::process::id()))?;
    let start = Instant::now();
    let color = !raw_json && std::io::stdout().is_terminal();
    let mut cursor = 0u64;
    let mut sequence = 0u64;
    loop {
        sequence += 1;
        let page = fetch_events(
            &mut writer,
            &mut reader,
            &format!("w-{sequence}"),
            session,
            cursor,
            POLL_WAIT_MS,
        )?;
        emit_page(&page, start, color, raw_json);
        cursor = page.next_cursor;
    }
}

pub fn run_attach(session: &str, socket: Option<String>) -> Result<()> {
    let path = socket_path(socket);
    let stream = connect(&path)?;
    let reader = BufReader::new(stream.try_clone()?);
    let mut hello_writer = stream.try_clone()?;
    // The handshake happens before the router thread exists, so it reads directly.
    let mut hello_reader = reader;
    let client_id = hello_handshake(
        &mut hello_writer,
        &mut hello_reader,
        &format!("attach-{}", std::process::id()),
    )?;
    eprintln!(
        "attached as {client_id}: plain line = steer, >line = follow-up, !take = take control, /quit = leave"
    );

    let writer = Arc::new(Mutex::new(hello_writer));
    let (event_tx, event_rx) = mpsc::channel::<Value>();
    let (action_tx, action_rx) = mpsc::channel::<Value>();
    std::thread::spawn(move || route_replies(hello_reader, &event_tx, &action_tx));

    let start = Instant::now();
    let color = std::io::stdout().is_terminal();
    let session_name = session.to_owned();
    let event_writer = writer.clone();
    std::thread::spawn(move || {
        let mut cursor = 0u64;
        let mut sequence = 0u64;
        loop {
            sequence += 1;
            let id = format!("w-{sequence}");
            let args = json!({"session": session_name, "cursor": cursor, "wait_ms": POLL_WAIT_MS});
            if send_request(&event_writer, &id, "events", args).is_err() {
                break;
            }
            let Ok(reply) = event_rx.recv_timeout(REPLY_TIMEOUT) else {
                eprintln!("events: reply_timeout");
                break;
            };
            if reply.get("bridgeEof").is_some() {
                eprintln!("daemon closed the connection");
                break;
            }
            match parse_events_page(&reply) {
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

    let action_sequence = AtomicU64::new(0);
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
        let (op, args) = if line == "!take" {
            ("take_control", json!({"session": session, "reason": "operator attach"}))
        } else if let Some(rest) = line.strip_prefix('>') {
            ("follow_up", json!({"session": session, "message": rest.trim()}))
        } else {
            ("steer", json!({"session": session, "message": line}))
        };
        let id = format!("a-{}", action_sequence.fetch_add(1, Ordering::Relaxed) + 1);
        send_request(&writer, &id, op, args)?;
        match action_rx.recv_timeout(REPLY_TIMEOUT) {
            Ok(reply) if reply.get("bridgeEof").is_some() => {
                eprintln!("daemon closed the connection");
                break;
            }
            Ok(reply) if reply["ok"] == json!(true) => {
                println!("{op}: accepted");
            }
            Ok(reply) => {
                let code = reply["error"].as_str().unwrap_or("error");
                if code == "not_controller" {
                    let holder = reply["controller"].as_str().unwrap_or("?");
                    println!("not controller (current: {holder}) — type !take to take control");
                } else {
                    println!("{op}: {code}");
                }
            }
            Err(_) => eprintln!("{op}: reply_timeout"),
        }
    }
    Ok(())
}

/// Read every reply line and route it by id prefix: `w-` to the events loop,
/// everything else to the action loop. A closed socket notifies both.
fn route_replies(
    mut reader: BufReader<UnixStream>,
    event_tx: &mpsc::Sender<Value>,
    action_tx: &mpsc::Sender<Value>,
) {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = value["id"].as_str().unwrap_or("");
        let target = if id.starts_with("w-") { event_tx } else { action_tx };
        if target.send(value).is_err() {
            break;
        }
    }
    let eof = json!({"bridgeEof": true});
    let _ = event_tx.send(eof.clone());
    let _ = action_tx.send(eof);
}

#[derive(Debug)]
pub struct EventsPage {
    pub events: VecDeque<Value>,
    pub next_cursor: u64,
    pub dropped: u64,
}

fn send_request(writer: &Arc<Mutex<UnixStream>>, id: &str, op: &str, args: Value) -> Result<()> {
    let mut guard = writer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    write_frame(&mut *guard, id, op, args)
}

fn write_frame(writer: &mut impl Write, id: &str, op: &str, args: Value) -> Result<()> {
    let frame = json!({"id": id, "op": op, "args": args});
    writeln!(writer, "{frame}").map_err(|_| anyhow!("socket_write_failed"))?;
    writer.flush().map_err(|_| anyhow!("socket_write_failed"))
}

fn read_reply(reader: &mut impl BufRead, id: &str) -> Result<Value> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).map_err(|_| anyhow!("socket_read_failed"))? == 0 {
            bail!("daemon_closed");
        }
        let value: Value = serde_json::from_str(&line).map_err(|_| anyhow!("invalid_reply"))?;
        if value["id"].as_str() == Some(id) {
            return Ok(value);
        }
    }
}

fn expect_ok(reply: Value) -> Result<Value> {
    if reply["ok"] == json!(true) {
        return Ok(reply["data"].clone());
    }
    bail!("{}", reply["error"].as_str().unwrap_or("daemon_error"))
}

pub fn hello_handshake(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    client: &str,
) -> Result<String> {
    write_frame(writer, "h-1", "hello", json!({"client": client}))?;
    let data = expect_ok(read_reply(reader, "h-1")?)?;
    Ok(data["clientId"].as_str().unwrap_or("client-?").to_owned())
}

pub fn fetch_events(
    writer: &mut impl Write,
    reader: &mut impl BufRead,
    id: &str,
    session: &str,
    cursor: u64,
    wait_ms: u64,
) -> Result<EventsPage> {
    let args = json!({"session": session, "cursor": cursor, "wait_ms": wait_ms});
    write_frame(writer, id, "events", args)?;
    let reply = read_reply(reader, id)?;
    parse_events_page(&reply)
}

fn parse_events_page(reply: &Value) -> Result<EventsPage> {
    if reply["ok"] != json!(true) {
        bail!("{}", reply["error"].as_str().unwrap_or("daemon_error"));
    }
    let data = &reply["data"];
    let events = data["events"]
        .as_array()
        .ok_or_else(|| anyhow!("invalid_events_page"))?
        .iter()
        .cloned()
        .collect();
    Ok(EventsPage {
        events,
        next_cursor: data["nextCursor"].as_u64().ok_or_else(|| anyhow!("invalid_events_page"))?,
        dropped: data["dropped"].as_u64().unwrap_or(0),
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

fn connect(path: &str) -> Result<UnixStream> {
    UnixStream::connect(path).map_err(|_| anyhow!("daemon_unreachable: {path}"))
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
    let dir = std::env::var("TMPDIR").ok().filter(|d| !d.is_empty()).unwrap_or_else(|| "/tmp".into());
    let uid = unsafe { libc::getuid() };
    std::path::Path::new(&dir)
        .join(format!("omp-bridge-{uid}.sock"))
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn handshake_and_fetch_advance_cursor_over_jsonl_socket() {
        let dir = std::env::temp_dir().join(format!("omp-bridge-watch-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("watch.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let hello: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(hello["op"], "hello");
            writeln!(writer, "{}", json!({"id": hello["id"], "ok": true, "data": {"clientId": "client-9"}}))
                .unwrap();
            line.clear();
            reader.read_line(&mut line).unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["op"], "events");
            assert_eq!(request["args"]["cursor"], 5);
            assert_eq!(request["args"]["session"], "session-1");
            writeln!(
                writer,
                "{}",
                json!({"id": request["id"], "ok": true, "data": {
                    "events": [
                        {"cursor": 5, "event": {"type": "agent_start"}},
                        {"cursor": 6, "event": {"type": "session_settled"}}
                    ],
                    "nextCursor": 7, "oldestCursor": 0, "dropped": 0
                }})
            )
            .unwrap();
        });

        let stream = UnixStream::connect(&path).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;
        let client = hello_handshake(&mut writer, &mut reader, "test").unwrap();
        assert_eq!(client, "client-9");
        let page = fetch_events(&mut writer, &mut reader, "w-1", "session-1", 5, 1_000).unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.next_cursor, 7);
        assert_eq!(page.dropped, 0);
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn control_transfer_renders_parties_and_reason() {
        let start = Instant::now();
        let transfer = json!({"cursor": 3, "event": {
            "type": "bridge_control_transfer", "from": "client-1", "to": "client-2", "reason": "handoff"
        }});
        let line = render_event(start, &transfer, false);
        assert!(line.contains("client-1 → client-2 (handoff)"), "{line}");
    }

    #[test]
    fn error_reply_surfaces_short_code() {
        let reply = json!({"id": "w-1", "ok": false, "error": "unknown_session"});
        let error = parse_events_page(&reply).unwrap_err().to_string();
        assert_eq!(error, "unknown_session");
    }
}
