//! Process-level integration against deterministic omp JSONL, with no account/network.
#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{atomic::{AtomicU64, Ordering}, mpsc::{self, Receiver}},
    thread,
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("bridge-test-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
}

fn bridge(args: &[&str], temp: &Temp) -> Child {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/mock_omp.py").canonicalize().unwrap();
    Command::new(env!("CARGO_BIN_EXE_omp-bridge"))
        .args(args)
        .env("OMP_BRIDGE_CMD", "python3")
        .env("OMP_BRIDGE_ARGS", json!([fixture]).to_string())
        .env("MOCK_OMP_LOG", temp.0.join("commands.jsonl"))
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit())
        .spawn().unwrap()
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        self.0.stdin.take();
        if self.0.try_wait().ok().flatten().is_some() { return; }
        // Daemon has no stdin lifecycle; SIGTERM exercises its normal cleanup.
        unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM); }
        let until = Instant::now() + Duration::from_secs(8);
        while Instant::now() < until {
            if self.0.try_wait().ok().flatten().is_some() { return; }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Mcp {
    process: Process,
    stdin: Option<ChildStdin>,
    frames: Receiver<Value>,
    next: u64,
}
impl Mcp {
    fn new(args: &[&str], temp: &Temp) -> Self {
        let mut child = bridge(args, temp);
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, frames) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let value: Value = serde_json::from_str(&line).expect("MCP stdout must be JSONL");
                if tx.send(value).is_err() { break; }
            }
        });
        let mut client = Self { process: Process(child), stdin, frames, next: 0 };
        let init = client.request("initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"integration","version":"1"}}));
        assert_eq!(init["serverInfo"]["name"], "omp-bridge");
        client.write(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        client
    }
    fn write(&mut self, frame: Value) {
        let input = self.stdin.as_mut().unwrap();
        writeln!(input, "{frame}").unwrap(); input.flush().unwrap();
    }
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next += 1;
        let id = self.next;
        self.write(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        loop {
            let value = self.frames.recv_timeout(Duration::from_secs(15)).expect("bounded MCP response deadline");
            if value.get("id") == Some(&json!(id)) {
                assert!(value.get("error").is_none(), "MCP error: {value}");
                return value["result"].clone();
            }
        }
    }
    fn raw_tool(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":args}))
    }
    fn tool(&mut self, name: &str, args: Value) -> Value {
        let raw = self.raw_tool(name, args);
        assert_ne!(raw["isError"], true, "{name}: {raw}");
        serde_json::from_str(raw["content"][0]["text"].as_str().expect("JSON tool text")).unwrap()
    }
    fn settle(&mut self, session: &str) -> Vec<Value> {
        let mut cursor = 0;
        let mut events = Vec::new();
        let end = Instant::now() + Duration::from_secs(10);
        while Instant::now() < end {
            let page = self.tool("omp_events", json!({"session":session,"cursor":cursor,"wait_ms":200}));
            assert_eq!(page["dropped"], 0);
            cursor = page["nextCursor"].as_u64().unwrap();
            events.extend(page["events"].as_array().unwrap().iter().map(|e| e["event"].clone()));
            let state = self.tool("omp_status", json!({"session":session}));
            if state["sessionSettled"] == true {
                let tail = self.tool("omp_events", json!({"session":session,"cursor":cursor,"wait_ms":100}));
                events.extend(tail["events"].as_array().unwrap().iter().map(|e| e["event"].clone()));
                return events;
            }
        }
        panic!("mock session did not settle");
    }
    fn wait_started(&mut self, session: &str) {
        let end = Instant::now() + Duration::from_secs(3);
        while Instant::now() < end {
            let page = self.tool("omp_events", json!({"session":session,"cursor":0,"kinds":["agent_start"],"wait_ms":100}));
            if !page["events"].as_array().unwrap().is_empty() { return; }
        }
        panic!("no agent_start from mock");
    }
}
impl Drop for Mcp {
    fn drop(&mut self) {
        self.stdin.take();
        // Give EOF cleanup a chance before Process's termination fallback.
        let end = Instant::now() + Duration::from_secs(3);
        while Instant::now() < end {
            if self.process.0.try_wait().ok().flatten().is_some() { break; }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn commands(temp: &Temp) -> Vec<Value> {
    fs::read_to_string(temp.0.join("commands.jsonl")).unwrap().lines()
        .map(|line| serde_json::from_str(line).unwrap()).collect()
}

#[test]
fn mcp_steers_promotes_without_resending_and_pages_history() {
    let temp = Temp::new();
    let mut client = Mcp::new(&["mcp"], &temp);
    let started = client.tool("omp_start", json!({"cwd":temp.0,"no_tools":true,"model":"mock-provider/mock-model"}));
    let session = started["session"].as_str().unwrap();
    client.tool("omp_send", json!({"session":session,"message":"original direction","wait":false}));
    client.wait_started(session);
    client.tool("omp_follow_up", json!({"session":session,"text":"promote exactly once"}));
    let promoted = client.tool("omp_promote_queued_message", json!({"session":session,"text":"promote exactly once"}));
    assert_eq!(promoted["promoted"], true);
    let absent = client.tool("omp_promote_queued_message", json!({"session":session,"text":"never queued"}));
    assert_eq!(absent["promoted"], false);
    client.tool("omp_steer", json!({"session":session,"text":"new visible direction"}));
    client.tool("omp_send", json!({"session":session,"message":"queued follow-up","streaming_behavior":"followUp","wait":false}));
    let events = client.settle(session);
    assert!(events.iter().any(|e| e["type"] == "queue_update"));
    let text = events.iter().filter(|e| e["type"] == "message_end" && e["message"]["role"] == "assistant")
        .map(|e| e["message"].to_string()).collect::<Vec<_>>().join("\n");
    assert!(text.contains("new visible direction"), "steering must affect the answer: {text}");
    assert!(text.contains("queued follow-up"), "follow-up must run after the first turn: {text}");
    let page = client.tool("omp_messages", json!({"session":session,"offset":0,"limit":2}));
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    assert!(page["totalMessages"].as_u64().unwrap() > 2);
    let next = client.tool("omp_messages", json!({"session":session,"offset":2,"limit":2}));
    assert_ne!(page["messages"], next["messages"]);
    let changed = client.tool("omp_set_model", json!({"session":session,"provider":"mock-provider","model_id":"another-model"}));
    assert_eq!(changed["model"]["id"], "another-model");
    client.tool("omp_compact", json!({"session":session,"instructions":"preserve decisions"}));
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        let p = client.tool("omp_events", json!({"session":session,"cursor":0,"kinds":["bridge_operation_result"],"wait_ms":50}));
        if p["events"].as_array().unwrap().iter().any(|e| e["event"]["outcome"].get("compaction").is_some()) { break; }
        assert!(Instant::now() < end, "compact outcome missing");
    }
    assert_eq!(client.tool("omp_stop", json!({"session":session}))["stopped"], true);
    let log = commands(&temp);
    assert_eq!(log.iter().filter(|x| x["type"] == "prompt").count(), 2, "no fallback duplicate prompt");
    assert_eq!(log.iter().filter(|x| x["type"] == "steer").count(), 1, "promotion must not fall back to steer");
    assert_eq!(log.iter().filter(|x| x["type"] == "follow_up").count(), 1);
    assert_eq!(log.iter().filter(|x| x["type"] == "get_messages").count(), 0, "history should use native pagination");
}

#[test]
fn two_mcp_clients_share_a_daemon_with_explicit_control_transfer() {
    let temp = Temp::new();
    let socket = temp.0.join("bridge.sock");
    let mut daemon = Process(bridge(&["serve", "--socket", socket.to_str().unwrap()], &temp));
    let until = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(daemon.0.try_wait().unwrap().is_none(), "daemon exited before binding");
        assert!(Instant::now() < until, "daemon socket deadline");
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(fs::metadata(&socket).unwrap().permissions().mode() & 0o777, 0o600);
    let args = ["mcp", "--daemon", "--socket", socket.to_str().unwrap()];
    let mut first = Mcp::new(&args, &temp);
    let mut second = Mcp::new(&args, &temp);
    let started = first.tool("omp_start", json!({"cwd":temp.0,"no_tools":true,"model":"mock-provider/mock-model"}));
    let session = started["session"].as_str().unwrap();
    let observer = second.tool("omp_list", json!({}));
    assert_eq!(observer["sessions"].as_array().unwrap().len(), 1);
    let rejected = second.raw_tool("omp_send", json!({"session":session,"message":"not owner","wait":false}));
    assert_eq!(rejected["isError"], true, "observer must not silently write");
    first.tool("omp_send", json!({"session":session,"message":"first controller task","wait":false}));
    first.wait_started(session);
    let prior = second.tool("omp_events", json!({"session":session,"cursor":0,"kinds":["agent_start"]}));
    assert!(!prior["events"].as_array().unwrap().is_empty(), "observer can replay prior events");
    second.tool("omp_take_control", json!({"session":session,"reason":"continue from second MCP client"}));
    let rejected = first.raw_tool("omp_steer", json!({"session":session,"text":"former controller"}));
    assert_eq!(rejected["isError"], true, "former controller must lose write access");
    second.tool("omp_follow_up", json!({"session":session,"text":"second controller follow-up"}));
    let events = first.settle(session);
    let transfers: Vec<_> = events.iter().filter(|e| e["type"] == "controller_changed").collect();
    assert!(!transfers.is_empty(), "first controller must observe transfer event");
    assert!(events.iter().any(|e| e["type"] == "message_end" && e.to_string().contains("second controller follow-up")));
    assert_eq!(second.tool("omp_stop", json!({"session":session}))["stopped"], true);
    assert!(first.tool("omp_list", json!({}))["sessions"].as_array().unwrap().is_empty());
}

#[test]
fn daemon_waits_for_delayed_first_bytes_after_accept() {
    use std::os::unix::net::UnixStream;

    let temp = Temp::new();
    let socket = temp.0.join("delayed.sock");
    let mut daemon = Process(bridge(&["serve", "--socket", socket.to_str().unwrap()], &temp));
    let until = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(daemon.0.try_wait().unwrap().is_none(), "daemon exited before binding");
        assert!(Instant::now() < until, "daemon socket deadline");
        thread::sleep(Duration::from_millis(10));
    }
    let mut stream = UnixStream::connect(&socket).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    // macOS can inherit O_NONBLOCK on accept. The daemon must switch accepted
    // sockets to blocking before reading, not mistake WouldBlock for EOF.
    thread::sleep(Duration::from_millis(60));
    writeln!(stream, "{}", json!({"id":"delayed-hello","method":"hello","params":{"name":"delayed-client"}})).unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    let count = BufReader::new(stream).read_line(&mut response).unwrap();
    assert!(count > 0, "daemon closed the connection before request bytes arrived");
    let response: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["id"], "delayed-hello");
    assert_eq!(response["ok"], true, "{response}");
    assert!(response["result"]["client_id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(daemon.0.try_wait().unwrap().is_none());
}

#[test]
fn watch_json_observes_attach_transfer_and_one_follow_up() {
    let temp = Temp::new();
    let socket = temp.0.join("human.sock");
    let mut daemon = Process(bridge(&["serve", "--socket", socket.to_str().unwrap()], &temp));
    let until = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(daemon.0.try_wait().unwrap().is_none(), "daemon exited before binding");
        assert!(Instant::now() < until, "daemon socket deadline");
        thread::sleep(Duration::from_millis(10));
    }
    let mut owner = Mcp::new(&["mcp", "--daemon", "--socket", socket.to_str().unwrap()], &temp);
    let started = owner.tool("omp_start", json!({"cwd":temp.0,"no_tools":true,"model":"mock-provider/mock-model"}));
    let session = started["session"].as_str().unwrap();

    let mut watcher = Process(bridge(&["watch", session, "--json", "--socket", socket.to_str().unwrap()], &temp));
    let watch_output = watcher.0.stdout.take().unwrap();
    let (watch_tx, watch_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(watch_output).lines() {
            let Ok(line) = line else { break };
            if watch_tx.send(line).is_err() { break; }
        }
    });

    let mut attach = Process(bridge(&["attach", session, "--socket", socket.to_str().unwrap()], &temp));
    let mut input = attach.0.stdin.take().unwrap();
    input.write_all(b">must not send before explicit take\n!take\n>attached follow-up exactly once\n/quit\n").unwrap();
    input.flush().unwrap();
    drop(input);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = attach.0.try_wait().unwrap() {
            assert!(status.success(), "attach must leave cleanly after /quit");
            break;
        }
        assert!(Instant::now() < deadline, "attach /quit exceeded deadline");
        thread::sleep(Duration::from_millis(10));
    }
    let output = BufReader::new(attach.0.stdout.take().unwrap()).lines().collect::<Result<Vec<_>, _>>().unwrap().join("\n");
    assert!(output.contains("take_control: accepted"), "{output}");
    assert!(output.contains("omp_follow_up: accepted"), "{output}");

    let deadline = Instant::now() + Duration::from_secs(5);
    let transfer = loop {
        let line = watch_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("watch --json must stream the actual transfer event");
        let envelope: Value = serde_json::from_str(&line).expect("--json must emit JSONL, not terminal colors");
        if envelope["event"]["type"] == "controller_changed" && envelope["event"]["reason"] == "operator attach" {
            break envelope;
        }
    };
    assert!(transfer["cursor"].as_u64().is_some(), "watch must preserve replay cursor");
    if transfer.get("session").is_some() { assert_eq!(transfer["session"], session); }
    assert_ne!(transfer["event"]["previousClient"], transfer["event"]["newClient"]);
    let rejected = owner.raw_tool("omp_steer", json!({"session":session,"text":"old controller cannot write"}));
    assert_eq!(rejected["isError"], true);
    let events = owner.settle(session);
    assert!(events.iter().any(|event| event["type"] == "message_end" && event["message"]["role"] == "assistant"
        && event.to_string().contains("attached follow-up exactly once")));
    let log = commands(&temp);
    assert_eq!(log.iter().filter(|frame| frame["type"] == "follow_up").count(), 1, "attach must neither auto-take nor resend");
    assert!(!log.iter().any(|frame| frame.to_string().contains("must not send before explicit take")), "input before !take must never reach omp");
    assert_eq!(log.iter().filter(|frame| frame["type"] == "steer").count(), 0, "denied write and follow-up must not fall back to steer");
    drop(watcher);
    owner.tool("omp_take_control", json!({"session":session,"reason":"test cleanup"}));
    assert_eq!(owner.tool("omp_stop", json!({"session":session}))["stopped"], true);
}

#[test]
fn fleet_json_does_not_block_on_idle_session_and_interrupt_preserves_sessions() {
    let temp = Temp::new();
    let socket = temp.0.join("fleet.sock");
    let mut daemon = Process(bridge(&["serve", "--socket", socket.to_str().unwrap()], &temp));
    let until = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(daemon.0.try_wait().unwrap().is_none(), "daemon exited before binding");
        assert!(Instant::now() < until, "daemon socket deadline");
        thread::sleep(Duration::from_millis(10));
    }
    let mut owner = Mcp::new(&["mcp", "--daemon", "--socket", socket.to_str().unwrap()], &temp);
    let idle = owner.tool("omp_start", json!({"cwd":temp.0,"no_tools":true,"model":"mock-provider/mock-model"}));
    let active = owner.tool("omp_start", json!({"cwd":temp.0,"no_tools":true,"model":"mock-provider/mock-model"}));
    let idle_id = idle["session"].as_str().unwrap();
    let active_id = active["session"].as_str().unwrap();
    let mut watcher = Process(bridge(&["watch", "--all", "--json", "--socket", socket.to_str().unwrap()], &temp));
    let output = watcher.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() { break; }
        }
    });
    let until = Instant::now() + Duration::from_secs(3);
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < 2 {
        let line = rx.recv_timeout(until.saturating_duration_since(Instant::now()))
            .expect("fleet must identify both sessions in JSON output");
        let envelope: Value = serde_json::from_str(&line).unwrap();
        let id = envelope["session"].as_str().expect("fleet JSON must label session");
        assert!(id == idle_id || id == active_id);
        seen.insert(id.to_owned());
    }
    let submitted = Instant::now();
    owner.tool("omp_send", json!({"session":active_id,"message":"fleet active task","wait":false}));
    let until = submitted + Duration::from_secs(2);
    loop {
        let line = rx.recv_timeout(until.saturating_duration_since(Instant::now()))
            .expect("idle first session must not block active session events");
        let envelope: Value = serde_json::from_str(&line).unwrap();
        if envelope["session"] == active_id && envelope["event"]["type"] == "agent_start" {
            break;
        }
    }
    assert!(submitted.elapsed() < Duration::from_secs(2));
    let interrupted = Instant::now();
    assert_eq!(unsafe { libc::kill(watcher.0.id() as i32, libc::SIGINT) }, 0);
    loop {
        if let Some(exit) = watcher.0.try_wait().unwrap() {
            assert!(exit.success(), "watch Ctrl-C should return a clean exit");
            break;
        }
        assert!(interrupted.elapsed() < Duration::from_secs(2), "watch cancellation must not wait on an idle long-poll");
        thread::sleep(Duration::from_millis(10));
    }
    let list = owner.tool("omp_list", json!({}));
    assert_eq!(list["sessions"].as_array().unwrap().len(), 2, "stopping a watcher must not stop agent sessions");
    assert_eq!(owner.tool("omp_stop", json!({"session":idle_id}))["stopped"], true);
    assert_eq!(owner.tool("omp_stop", json!({"session":active_id}))["stopped"], true);
}
