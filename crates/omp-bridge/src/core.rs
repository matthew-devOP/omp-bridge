//! Process-local session manager. Blocking methods are called on MCP blocking workers.
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};
use omp_rpc::{AbortCommand, Client, ClientOptions, Event, GetStateCommand, PromptCommand, SetEventFilterCommand};
use serde_json::{Value, json};

use crate::ring::EventRing;

const MAX_SESSIONS: usize = 4;
const MAX_OPERATIONS: usize = 16;
const MAX_PROMPT_BYTES: usize = 128 * 1024;
const MAX_WAIT_MS: u64 = 25_000;
const WORKER_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Default)]
pub struct Manager {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    sessions: Mutex<BTreeMap<String, Arc<Session>>>,
    counter: AtomicU64,
}

struct Session {
    id: String,
    model: String,
    cwd: PathBuf,
    pid: Option<u32>,
    protocol: u32,
    client: Mutex<Option<Arc<Client>>>,
    stopping: AtomicBool,
    busy: AtomicBool,
    disconnected: AtomicBool,
    ring: Arc<EventRing>,
    operations: Mutex<VecDeque<Arc<Operation>>>,
    settled: Mutex<Option<bool>>,
}

struct Operation {
    id: String,
    result: Mutex<Option<Value>>,
    changed: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn required<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("{key} must be a nonempty string"))
}

// Never echo arbitrary provider errors, command arguments, or stderr (which can contain credentials).
fn rpc_error(error: &omp_rpc::Error) -> &'static str {
    match error {
        omp_rpc::Error::Io(_) => "io_error",
        omp_rpc::Error::Json(_) => "invalid_json",
        omp_rpc::Error::Command { .. } => "command_rejected",
        omp_rpc::Error::Timeout { .. } => "timeout",
        omp_rpc::Error::Closed => "closed",
        omp_rpc::Error::Protocol(_) => "protocol_error",
        omp_rpc::Error::InvalidArgument(_) => "invalid_argument",
    }
}

impl Session {
    fn client(&self) -> Result<Arc<Client>> {
        if self.stopping.load(Ordering::SeqCst) { bail!("session_stopping"); }
        lock(&self.client).clone().ok_or_else(|| anyhow!("session_closed"))
    }

    fn summary(&self) -> Value {
        let operations: Vec<Value> = lock(&self.operations).iter().map(|op| {
            lock(&op.result).clone().unwrap_or_else(|| json!({
                "operationId":op.id,"status":"running_or_unknown"
            }))
        }).collect();
        json!({"session":self.id,"model":self.model,"cwd":self.cwd,
            "pid":self.pid,"protocolVersion":self.protocol,
            "busy":self.busy.load(Ordering::SeqCst),
            "stopping":self.stopping.load(Ordering::SeqCst),
            "disconnected":self.disconnected.load(Ordering::SeqCst),
            "sessionSettled":*lock(&self.settled),"operations":operations})
    }
}

impl Manager {
    fn id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.inner.counter.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn session(&self, args: &Value) -> Result<Arc<Session>> {
        lock(&self.inner.sessions).get(required(args,"session")?).cloned()
            .ok_or_else(|| anyhow!("unknown_session"))
    }

    pub fn start(&self, args: Value) -> Result<Value> {
        let model = args.get("model").and_then(Value::as_str).map(str::to_owned)
            .or_else(|| std::env::var("OMP_BRIDGE_MODEL").ok())
            .unwrap_or_else(|| "opencode-go/deepseek-v4.1-flash".into());
        if model.is_empty() || model.len() > 512 || model.starts_with('-') { bail!("invalid_model"); }
        let cwd = match args.get("cwd").and_then(Value::as_str) {
            Some(cwd) => PathBuf::from(cwd), None => std::env::current_dir()?,
        }.canonicalize().map_err(|_| anyhow!("cwd_not_found"))?;
        if !cwd.is_dir() { bail!("cwd_not_directory"); }
        let executable = std::env::var("OMP_BRIDGE_CMD").unwrap_or_else(|_| "omp".into());
        if executable.is_empty() { bail!("OMP_BRIDGE_CMD must name one executable"); }
        let prefix: Vec<String> = serde_json::from_str(&std::env::var("OMP_BRIDGE_ARGS").unwrap_or_else(|_| "[]".into()))
            .map_err(|_| anyhow!("OMP_BRIDGE_ARGS must be a JSON array of strings"))?;
        let mut process = Command::new(executable);
        process.args(prefix).args(["--mode","rpc","--no-ui","--no-session","--model",&model,
            "--no-extensions","--no-skills","--no-rules","--no-context-files","--no-lsp","--no-pty","--no-title"])
            .current_dir(&cwd).stderr(Stdio::null());
        if args.get("no_tools").and_then(Value::as_bool).unwrap_or(false) { process.arg("--no-tools"); }

        // Holding the registry lock during spawn prevents racing past the session cap.
        let mut sessions = lock(&self.inner.sessions);
        if sessions.len() >= MAX_SESSIONS { bail!("session_limit (4); stop a session first"); }
        let (client, receiver) = Client::spawn(process, ClientOptions {
            default_timeout: Duration::from_secs(5), ..ClientOptions::default()
        }).map_err(|e| anyhow!("omp_start: {}",rpc_error(&e)))?;
        // Fixed once: bounded bridge ring and no token-by-token deltas. Preserve complete
        // message_end + agent_end for the SDK prompt collector's assistant-text reconstruction.
        client.call(&SetEventFilterCommand { events: Some([
            "agent_start","agent_end","message_end","tool_execution_start","tool_execution_end"
        ].into_iter().map(str::to_owned).collect()) })
            .map_err(|e| anyhow!("event_filter: {}",rpc_error(&e)))?;
        let session = Arc::new(Session {
            id:self.id("session"), model, cwd, pid:client.pid(), protocol:client.protocol_version(),
            client:Mutex::new(Some(Arc::new(client))), stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false), disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()), operations:Mutex::new(VecDeque::new()), settled:Mutex::new(None),
        });
        let weak = Arc::downgrade(&session);
        thread::spawn(move || {
            for event in receiver {
                let Some(session) = weak.upgrade() else { break; };
                let value = match event {
                    Event::Notification(n) => serde_json::to_value(n).unwrap_or(Value::Null),
                    Event::UnmatchedResponse(r) => serde_json::to_value(r).unwrap_or(Value::Null),
                    Event::Undecodable { .. } => json!({"type":"bridge_decode_error"}),
                };
                match value.get("type").and_then(Value::as_str) {
                    Some("prompt_result") => {
                        *lock(&session.settled) = value.get("sessionSettled").and_then(Value::as_bool);
                    }
                    Some("session_settled") => *lock(&session.settled) = Some(true),
                    Some("agent_start") => *lock(&session.settled) = Some(false),
                    _ => {}
                }
                session.ring.push(value);
            }
            if let Some(session) = weak.upgrade() {
                session.disconnected.store(true,Ordering::SeqCst);
                session.ring.push(json!({"type":"bridge_disconnected"}));
            }
        });
        let result = session.summary();
        sessions.insert(session.id.clone(),session);
        Ok(result)
    }

    pub fn send(&self, args: Value) -> Result<Value> {
        if args.get("mode").and_then(Value::as_str).unwrap_or("prompt") != "prompt" {
            bail!("unsupported_mode: MVP accepts normal prompt only");
        }
        let message = required(&args,"message")?.to_owned();
        if message.len()>MAX_PROMPT_BYTES { bail!("prompt_too_large (128 KiB maximum)"); }
        let session = self.session(&args)?;
        let client = session.client()?;
        if session.busy.compare_exchange(false,true,Ordering::SeqCst,Ordering::SeqCst).is_err() { bail!("busy"); }
        *lock(&session.settled)=None;
        let op=Arc::new(Operation {id:self.id("operation"),result:Mutex::new(None),changed:Condvar::new()});
        {
            let mut operations=lock(&session.operations);
            if operations.len()>=MAX_OPERATIONS { operations.pop_front(); }
            operations.push_back(op.clone());
        }
        let task_op=op.clone();
        let task_session=session.clone();
        thread::spawn(move || {
            let outcome=client.prompt_and_wait(&PromptCommand {message,images:None,streaming_behavior:None},WORKER_TIMEOUT);
            let value=match outcome {
                Ok(turn) => {
                    if let Some(ref result)=turn.result { *lock(&task_session.settled)=Some(result.session_settled); }
                    let (text,truncated)=bounded_text(turn.assistant_text);
                    json!({"operationId":task_op.id,"status":"finished",
                        "assistant_text":text,"assistantTextTruncated":truncated,"result":turn.result})
                },
                Err(ref error) => json!({"operationId":task_op.id,
                    "status":if matches!(error,omp_rpc::Error::Timeout{..}) {"running_or_unknown"} else {"failed"},
                    "error":rpc_error(error),"admission":"unknown","retrySafe":false}),
            };
            // An SDK timeout does not prove the model stopped. Keep the session guarded
            // against another normal prompt until it is explicitly aborted or stopped.
            if value["status"]!="running_or_unknown" { task_session.busy.store(false,Ordering::SeqCst); }
            *lock(&task_op.result)=Some(value.clone());
            task_op.changed.notify_all();
            task_session.ring.push(json!({"type":"bridge_operation_result","operationId":task_op.id,"outcome":value}));
        });
        if !args.get("wait").and_then(Value::as_bool).unwrap_or(true) {
            return Ok(json!({"session":session.id,"operationId":op.id,"status":"running_or_unknown","admission":"pending"}));
        }
        let timeout=args.get("timeout_ms").and_then(Value::as_u64).unwrap_or(MAX_WAIT_MS).min(MAX_WAIT_MS);
        let guard=lock(&op.result);
        let (guard,_)=op.changed.wait_timeout_while(guard,Duration::from_millis(timeout),|result|result.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.clone().unwrap_or_else(|| json!({"session":session.id,"operationId":op.id,
            "status":"running_or_unknown","timedOut":true,"aborted":false,"retrySafe":false})))
    }

    pub fn events(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        let kinds:Vec<String>=match args.get("kinds") {
            Some(value) if !value.is_null()=>serde_json::from_value(value.clone()).map_err(|_|anyhow!("kinds must be an array of strings"))?,
            _=>vec![],
        };
        Ok(session.ring.read(args.get("cursor").and_then(Value::as_u64).unwrap_or(0),&kinds,
            args.get("wait_ms").and_then(Value::as_u64).unwrap_or(0).min(MAX_WAIT_MS)))
    }

    pub fn status(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        let mut summary=session.summary();
        match session.client()?.call(&GetStateCommand {}) {
            Ok(state)=>summary["state"]=serde_json::to_value(state)?,
            Err(error)=>{summary["state"]=Value::Null;summary["stateError"]=json!(rpc_error(&error));}
        }
        Ok(summary)
    }

    pub fn abort(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        session.client()?.call(&AbortCommand {}).map_err(|e|anyhow!("abort: {}",rpc_error(&e)))?;
        // Acknowledgement requests cancellation, not proof that the agent settled.
        Ok(json!({"session":session.id,"abortRequested":true,"sessionSettled":*lock(&session.settled)}))
    }

    pub fn stop(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        session.stopping.store(true,Ordering::SeqCst);
        let Some(client)=lock(&session.client).take() else { bail!("stop_already_in_progress"); };
        if session.busy.load(Ordering::SeqCst) { let _=client.call(&AbortCommand {}); }
        let deadline=Instant::now()+Duration::from_secs(5);
        let mut client=client;
        loop {
            match Arc::try_unwrap(client) {
                Ok(client)=>{
                    let result=client.close();
                    if result.is_ok() {
                        lock(&self.inner.sessions).remove(&session.id);
                        return Ok(json!({"session":session.id,"stopped":true}));
                    }
                    return Ok(json!({"session":session.id,"stopped":false,"error":"teardown_unconfirmed"}));
                }
                Err(shared)=>client=shared,
            }
            if Instant::now()>=deadline {
                *lock(&session.client)=Some(client);
                return Ok(json!({"session":session.id,"stopped":false,"status":"stopping","error":"active_call_pending","retrySafe":true}));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn list(&self)->Value {
        json!({"sessions":lock(&self.inner.sessions).values().map(|s|s.summary()).collect::<Vec<_>>(),"scope":"this_bridge_process"})
    }

    pub fn shutdown(&self) {
        let ids:Vec<String>=lock(&self.inner.sessions).keys().cloned().collect();
        for session in ids { let _=self.stop(json!({"session":session})); }
    }
}

fn bounded_text(text:Option<String>)->(Option<String>,bool) {
    let Some(mut text)=text else {return (None,false);};
    const MAX:usize=64*1024;
    if text.len()<=MAX {return (Some(text),false);}
    let mut end=MAX;
    while !text.is_char_boundary(end) {end-=1;}
    text.truncate(end);
    (Some(text),true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead,BufReader,Write};
    #[test]
    fn empty_registry_and_invalid_requests_are_local() {
        let manager=Manager::default();
        assert_eq!(manager.list()["sessions"],json!([]));
        assert!(manager.status(json!({"session":"missing"})).is_err());
        assert!(manager.send(json!({"message":"","session":"missing"})).is_err());
        assert!(manager.send(json!({"mode":"steer","message":"x","session":"missing"})).is_err());
        assert!(manager.start(json!({"model":"--bad"})).is_err());
        manager.shutdown();
    }

    #[test]
    fn timeout_does_not_resend_or_abort_and_busy_guards_prompt() {
        let (client_read,mut server_write)=std::io::pipe().unwrap();
        let (server_read,client_write)=std::io::pipe().unwrap();
        writeln!(server_write,"{}",json!({"type":"ready"})).unwrap();
        let (client,_events)=Client::from_io(client_read,client_write,ClientOptions::default()).unwrap();
        let manager=Manager::default();
        let session=Arc::new(Session {
            id:"test".into(),model:"fake".into(),cwd:PathBuf::from("/tmp"),pid:None,protocol:1,
            client:Mutex::new(Some(Arc::new(client))),stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false),disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()),operations:Mutex::new(VecDeque::new()),settled:Mutex::new(None),
        });
        lock(&manager.inner.sessions).insert("test".into(),session.clone());
        let result=manager.send(json!({"session":"test","message":"hello","timeout_ms":0})).unwrap();
        assert_eq!(result["status"],"running_or_unknown");
        assert_eq!(result["aborted"],false);
        assert!(manager.send(json!({"session":"test","message":"duplicate"})).unwrap_err().to_string().contains("busy"));
        let mut reader=BufReader::new(server_read);
        let mut line=String::new();
        reader.read_line(&mut line).unwrap();
        let request:Value=serde_json::from_str(&line).unwrap();
        assert_eq!(request["type"],"prompt");
        writeln!(server_write,"{}",json!({"type":"response","id":request["id"],"command":"prompt","success":true,"data":{"agentInvoked":true}})).unwrap();
        writeln!(server_write,"{}",json!({"type":"prompt_result","id":request["id"],"agentInvoked":true,"status":"completed","sessionSettled":true})).unwrap();
        let op=lock(&session.operations).back().unwrap().clone();
        let (outcome,_)=op.changed.wait_timeout_while(lock(&op.result),Duration::from_secs(2),|r|r.is_none()).unwrap();
        let outcome=outcome.as_ref().expect("worker completed");
        assert_eq!(outcome["operationId"],result["operationId"]);
        assert_eq!(outcome["result"]["sessionSettled"],true);
        assert!(!session.busy.load(Ordering::SeqCst));
        drop(server_write);
        assert_eq!(manager.stop(json!({"session":"test"})).unwrap()["stopped"],true);
    }

    #[test]
    fn output_truncation_preserves_utf8() {
        let (text,truncated)=bounded_text(Some("🦀".repeat(20_000)));
        assert!(truncated);
        assert!(text.unwrap().len()<=64*1024);
    }
}
