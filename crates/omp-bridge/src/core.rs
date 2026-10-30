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
use omp_rpc::{AbortCommand, Client, ClientOptions, Event, GetStateCommand, PromptCommand, SetEventFilterCommand,
    SteerCommand, FollowUpCommand, PromoteQueuedMessageCommand, SetModelCommand,
    GetMessagesPageCommand, StreamingBehavior};
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
    model: Mutex<String>,
    cwd: PathBuf,
    pid: Option<u32>,
    protocol: u32,
    client: Mutex<Option<Arc<Client>>>,
    stopping: AtomicBool,
    busy: AtomicBool,
    timed_out: AtomicBool,
    generation: AtomicU64,
    admission: Mutex<()>,
    disconnected: AtomicBool,
    ring: Arc<EventRing>,
    operations: Mutex<VecDeque<Arc<Operation>>>,
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
    fn record_event(&self, value: Value) {
        // Receiver delivery can lag behind prompt_and_wait and even the next send.
        // Events are historical evidence, never authoritative current-session state.
        self.ring.push(value);
    }
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
        let busy=self.busy.load(Ordering::SeqCst);
        let state=if self.stopping.load(Ordering::SeqCst) {"stopping"}
            else if self.disconnected.load(Ordering::SeqCst) {"unknown"}
            else if busy && !self.timed_out.load(Ordering::SeqCst) {"running"}
            else {"unknown"};
        json!({"session":self.id,"model":*lock(&self.model),"cwd":self.cwd,"status":state,
            "pid":self.pid,"protocolVersion":self.protocol,
            "busy":busy,
            "stopping":self.stopping.load(Ordering::SeqCst),
            "disconnected":self.disconnected.load(Ordering::SeqCst),
            "sessionSettled":Value::Null,"operations":operations})
    }
}

impl Manager {
    fn id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.inner.counter.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn session(&self, args: &Value) -> Result<Arc<Session>> {
        lock(&self.inner.sessions).get(session_id(args)?).cloned()
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
            "agent_start","agent_end","message_end","tool_execution_start","tool_execution_end",
            "queue_update","auto_compaction_start","auto_compaction_end","model_changed"
        ].into_iter().map(str::to_owned).collect()) })
            .map_err(|e| anyhow!("event_filter: {}",rpc_error(&e)))?;
        let session = Arc::new(Session {
            id:self.id("session"), model:Mutex::new(model), cwd, pid:client.pid(), protocol:client.protocol_version(),
            client:Mutex::new(Some(Arc::new(client))), stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false), timed_out:AtomicBool::new(false), generation:AtomicU64::new(0), admission:Mutex::new(()), disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()), operations:Mutex::new(VecDeque::new()),
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
                session.record_event(value);
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
        if let Some(behavior)=streaming_behavior(&args)? {
            // This is ONE prompt RPC with explicit busy-session admission behavior.
            // Never retry it as steer/follow_up after timeout or unsupported protocol.
            let ack=client.call(&PromptCommand {message,images:None,streaming_behavior:Some(behavior)})
                .map_err(|e|anyhow!("queued_prompt: {}; admission_unknown; retrySafe=false",rpc_error(&e)))?;
            return Ok(json!({"session":session.id,"admitted":true,"agentInvoked":ack.agent_invoked,
                "queue":behavior.as_str(),"status":"admitted","waitIgnored":true}));
        }
        {
            let _admission=lock(&session.admission);
            if session.busy.compare_exchange(false,true,Ordering::SeqCst,Ordering::SeqCst).is_err() { bail!("busy"); }
            session.generation.fetch_add(1,Ordering::SeqCst);
            session.timed_out.store(false,Ordering::SeqCst);
        }
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
            {
                let _admission=lock(&task_session.admission);
                if value["status"]!="running_or_unknown" { task_session.busy.store(false,Ordering::SeqCst); }
                else { task_session.timed_out.store(true,Ordering::SeqCst); }
            }
            *lock(&task_op.result)=Some(value.clone());
            task_op.changed.notify_all();
            task_session.ring.push(json!({"type":"bridge_operation_result","operationId":task_op.id,"outcome":value}));
        });
        if !args.get("wait").and_then(Value::as_bool).unwrap_or(true) {
            return Ok(json!({"session":session.id,"operationId":op.id,"status":"running_or_unknown","admission":"pending"}));
        }
        self.wait_operation(json!({"session":session.id,"operationId":op.id,"timeout_ms":args.get("timeout_ms")}))
    }

    /// Wait outside daemon lease admission locks. Waiting never retransmits or cancels work.
    pub fn wait_operation(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let operation=required(&args,"operationId")?;
        let op=lock(&session.operations).iter().find(|op|op.id==operation).cloned()
            .ok_or_else(||anyhow!("unknown_operation"))?;
        let timeout=args.get("timeout_ms").and_then(Value::as_u64).unwrap_or(MAX_WAIT_MS).min(MAX_WAIT_MS);
        let guard=lock(&op.result);
        let (guard,_)=op.changed.wait_timeout_while(guard,Duration::from_millis(timeout),|result|result.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.clone().unwrap_or_else(|| json!({"session":session.id,"operationId":op.id,
            "status":"running_or_unknown","timedOut":true,"aborted":false,"retrySafe":false})))
    }

    pub fn emit_event(&self,session:&str,event:Value)->Result<()> {
        self.session(&json!({"session":session}))?.record_event(event);
        Ok(())
    }

    pub fn steer(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let message=control_text(&args)?;
        session.client()?.call(&SteerCommand {message,images:None})
            .map_err(|e|anyhow!("steer: {}; admission_unknown; retrySafe=false",rpc_error(&e)))?;
        Ok(json!({"session":session.id,"admitted":true,"queue":"steering"}))
    }

    pub fn follow_up(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let message=control_text(&args)?;
        session.client()?.call(&FollowUpCommand {message,images:None})
            .map_err(|e|anyhow!("follow_up: {}; admission_unknown; retrySafe=false",rpc_error(&e)))?;
        Ok(json!({"session":session.id,"admitted":true,"queue":"followUp"}))
    }

    pub fn promote_queued_message(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let message=control_text(&args)?;
        let result=session.client()?.call(&PromoteQueuedMessageCommand {message})
            .map_err(|e|anyhow!("promote_queued_message: {}; no_fallback_attempted",rpc_error(&e)))?;
        Ok(json!({"session":session.id,"promoted":result.promoted}))
    }

    pub fn set_model(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let provider=required(&args,"provider")?.to_owned();
        let model_id=required(&args,"model_id")?.to_owned();
        if provider.len()>256 || model_id.len()>512 {bail!("invalid_model");}
        let model=session.client()?.call(&SetModelCommand{provider:provider.clone(),model_id:model_id.clone()})
            .map_err(|e|anyhow!("set_model: {}",rpc_error(&e)))?;
        *lock(&session.model)=format!("{provider}/{model_id}");
        Ok(json!({"session":session.id,"model":model}))
    }

    /// Compaction is a long-running operation. It never holds the daemon lease lock
    /// while the model is summarizing; callers observe its result in events/status.
    pub fn compact(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let client=session.client()?;
        let instructions=args.get("instructions").and_then(Value::as_str).map(str::to_owned);
        if instructions.as_ref().is_some_and(|s|s.len()>MAX_PROMPT_BYTES) {bail!("instructions_too_large");}
        {
            let _admission=lock(&session.admission);
            if session.busy.compare_exchange(false,true,Ordering::SeqCst,Ordering::SeqCst).is_err() {bail!("busy");}
            session.generation.fetch_add(1,Ordering::SeqCst);
            session.timed_out.store(false,Ordering::SeqCst);
        }
        let op=Arc::new(Operation{id:self.id("operation"),result:Mutex::new(None),changed:Condvar::new()});
        {
            let mut operations=lock(&session.operations);
            if operations.len()>=MAX_OPERATIONS {operations.pop_front();}
            operations.push_back(op.clone());
        }
        let task_op=op.clone();let task_session=session.clone();
        thread::spawn(move || {
            let outcome=client.call(&LongCompact {custom_instructions:instructions});
            let value=match outcome {
                Ok(result)=>json!({"operationId":task_op.id,"status":"finished","compaction":result}),
                Err(ref e)=>json!({"operationId":task_op.id,"status":if matches!(e,omp_rpc::Error::Timeout{..}) {"running_or_unknown"} else {"failed"},"error":rpc_error(e),"retrySafe":false}),
            };
            {
                let _admission=lock(&task_session.admission);
                if value["status"]!="running_or_unknown" {task_session.busy.store(false,Ordering::SeqCst);}
                else {task_session.timed_out.store(true,Ordering::SeqCst);}
            }
            *lock(&task_op.result)=Some(value.clone());task_op.changed.notify_all();
            task_session.record_event(json!({"type":"bridge_operation_result","operationId":task_op.id,"outcome":value}));
        });
        Ok(json!({"session":session.id,"operationId":op.id,"status":"running_or_unknown","admission":"pending"}))
    }

    /// Native stable cursor pagination. Offset is an optional compatibility adapter
    /// that walks bounded pages, rather than loading the entire conversation.
    pub fn messages(&self,args:Value)->Result<Value> {
        let session=self.session(&args)?;
        let client=session.client()?;
        let limit=args.get("limit").and_then(Value::as_u64).unwrap_or(50).clamp(1,200);
        let mut cursor=args.get("cursor").and_then(Value::as_str).map(str::to_owned);
        let mut skip=args.get("offset").and_then(Value::as_u64).unwrap_or(0);
        if skip>10_000 {bail!("offset_too_large; use cursor pagination");}
        if skip>0 && cursor.is_some() {bail!("use offset or cursor, not both");}
        while skip>0 {
            let page=client.call(&GetMessagesPageCommand{cursor,limit:Some(skip.min(200) as i64)})
                .map_err(|e|anyhow!("messages: {}",rpc_error(&e)))?;
            skip=skip.saturating_sub(page.messages.len() as u64);
            cursor=page.next_cursor;
            if cursor.is_none() {return Ok(json!({"session":session.id,"messages":[],"totalMessages":page.total_messages,"nextCursor":Value::Null}));}
        }
        let page=client.call(&GetMessagesPageCommand{cursor,limit:Some(limit as i64)})
            .map_err(|e|anyhow!("messages: {}",rpc_error(&e)))?;
        Ok(json!({"session":session.id,"messages":page.messages,"totalMessages":page.total_messages,"nextCursor":page.next_cursor}))
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
        let generation=session.generation.load(Ordering::SeqCst);
        let mut summary=session.summary();
        match session.client()?.call(&GetStateCommand {}) {
            Ok(state)=>{
                let _admission=lock(&session.admission);
                let same_generation=generation==session.generation.load(Ordering::SeqCst);
                if same_generation && session.timed_out.load(Ordering::SeqCst) && state.is_settled {
                    session.busy.store(false,Ordering::SeqCst);
                }
                summary=session.summary();
                if same_generation && !session.busy.load(Ordering::SeqCst) {
                    summary["sessionSettled"]=json!(state.is_settled);
                    summary["status"]=json!(if state.is_settled {"settled"} else if state.is_streaming {"running"} else {"unknown"});
                }
                summary["state"]=json!({
                    "sessionId":state.session_id,"model":state.model,
                    "isStreaming":state.is_streaming,"isCompacting":state.is_compacting,
                    "isSettled":state.is_settled,"hasPendingAsyncWork":state.has_pending_async_work,
                    "messageCount":state.message_count,"queuedMessageCount":state.queued_message_count,
                    "sessionFile":state.session_file
                });
            },
            Err(error)=>{summary["state"]=Value::Null;summary["stateError"]=json!(rpc_error(&error));}
        }
        Ok(summary)
    }

    pub fn abort(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        session.client()?.call(&AbortCommand {}).map_err(|e|anyhow!("abort: {}",rpc_error(&e)))?;
        // Acknowledgement requests cancellation, not proof that the agent settled.
        let state=self.status(args).unwrap_or_else(|_|session.summary());
        Ok(json!({"session":session.id,"abortRequested":true,"sessionSettled":state["sessionSettled"]}))
    }

    pub fn stop(&self,args: Value)->Result<Value> {
        let session=self.session(&args)?;
        session.stopping.store(true,Ordering::SeqCst);
        let Some(client)=lock(&session.client).take() else { bail!("stop_already_in_progress"); };
        if session.busy.load(Ordering::SeqCst) { let _=client.call(&AbortCommand {}); }
        let started=Instant::now();
        let deadline=started+Duration::from_secs(5);
        let mut sent_term=false;
        let mut sent_kill=false;
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
            #[cfg(unix)]
            if let Some(pid)=session.pid {
                // Client still owns its unreaped Child here. Its spawn creates a new
                // process group led by this PID; we never signal an arbitrary caller PID.
                // Closing pipes wakes outstanding SDK calls so Arc can be unwrapped.
                if started.elapsed()>=Duration::from_millis(500) && !sent_term {
                    unsafe {libc::kill(-(pid as i32),libc::SIGTERM);}
                    sent_term=true;
                }
                if started.elapsed()>=Duration::from_millis(1500) && !sent_kill {
                    unsafe {libc::kill(-(pid as i32),libc::SIGKILL);}
                    sent_kill=true;
                }
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


fn session_id(args:&Value)->Result<&str> {
    args.get("session").or_else(||args.get("session_id")).and_then(Value::as_str)
        .filter(|s|!s.is_empty()).ok_or_else(||anyhow!("session must be a nonempty string"))
}
fn control_text(args:&Value)->Result<String> {
    let message=required(args,"text")?;
    if message.len()>MAX_PROMPT_BYTES {bail!("text_too_large (128 KiB maximum)");}
    Ok(message.to_owned())
}
fn streaming_behavior(args:&Value)->Result<Option<StreamingBehavior>> {
    match args.get("streaming_behavior").or_else(||args.get("streamingBehavior")) {
        None|Some(Value::Null)=>Ok(None),
        Some(Value::String(s)) if s=="steer"=>Ok(Some(StreamingBehavior::Steer)),
        Some(Value::String(s)) if s=="followUp"=>Ok(Some(StreamingBehavior::FollowUp)),
        _=>bail!("streaming_behavior must be steer or followUp"),
    }
}
#[derive(serde::Serialize)]
struct LongCompact {
    #[serde(rename="customInstructions",skip_serializing_if="Option::is_none")]
    custom_instructions:Option<String>,
}
impl omp_rpc::Command for LongCompact {
    const NAME:&'static str="compact";
    const TIMEOUT_MS:Option<u64>=Some(120_000);
    type Output=omp_rpc::CompactionResult;
    fn decode(data:Option<Value>)->std::result::Result<Self::Output,serde_json::Error> {
        serde_json::from_value(data.unwrap_or_else(||json!({})))
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
            id:"test".into(),model:Mutex::new("fake".into()),cwd:PathBuf::from("/tmp"),pid:None,protocol:1,
            client:Mutex::new(Some(Arc::new(client))),stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false),timed_out:AtomicBool::new(false),generation:AtomicU64::new(0),admission:Mutex::new(()),disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()),operations:Mutex::new(VecDeque::new()),
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


    fn fake_manager() -> (Manager, std::io::PipeWriter, BufReader<std::io::PipeReader>) {
        let (client_read,mut server_write)=std::io::pipe().unwrap();
        let (server_read,client_write)=std::io::pipe().unwrap();
        writeln!(server_write,"{}",json!({"type":"ready"})).unwrap();
        let (client,_)=Client::from_io(client_read,client_write,ClientOptions::default()).unwrap();
        let session=Arc::new(Session {
            id:"test".into(),model:Mutex::new("fake".into()),cwd:PathBuf::from("/tmp"),pid:None,protocol:1,
            client:Mutex::new(Some(Arc::new(client))),stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false),timed_out:AtomicBool::new(false),generation:AtomicU64::new(0),admission:Mutex::new(()),disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()),operations:Mutex::new(VecDeque::new()),
        });
        let manager=Manager::default();
        lock(&manager.inner.sessions).insert("test".into(),session);
        (manager,server_write,BufReader::new(server_read))
    }
    fn read_request(reader:&mut BufReader<std::io::PipeReader>)->Value {
        let mut line=String::new();reader.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }
    fn reply(writer:&mut std::io::PipeWriter,request:&Value,data:Value) {
        writeln!(writer,"{}",json!({"type":"response","id":request["id"],"command":request["type"],"success":true,"data":data})).unwrap();
    }

    #[test]
    fn streaming_prompt_and_promotion_are_single_rpc_without_fallback() {
        let (manager,mut writer,mut reader)=fake_manager();
        let server=thread::spawn(move || {
            let prompt=read_request(&mut reader);
            assert_eq!(prompt["type"],"prompt");assert_eq!(prompt["streamingBehavior"],"followUp");
            reply(&mut writer,&prompt,json!({"agentInvoked":true}));
            let promotion=read_request(&mut reader);
            assert_eq!(promotion["type"],"promote_queued_message");
            reply(&mut writer,&promotion,json!({"promoted":false}));
            let next=read_request(&mut reader);
            assert_eq!(next["type"],"follow_up","a false promotion must not fallback to steer");
            reply(&mut writer,&next,json!({}));
            let steer=read_request(&mut reader);assert_eq!(steer["type"],"steer");
            reply(&mut writer,&steer,json!({}));
        });
        let queued=manager.send(json!({"session":"test","message":"later","streaming_behavior":"followUp","wait":true})).unwrap();
        assert_eq!(queued["status"],"admitted");assert!(queued.get("operationId").is_none());
        assert_eq!(manager.promote_queued_message(json!({"session":"test","text":"later"})).unwrap()["promoted"],false);
        assert_eq!(manager.follow_up(json!({"session_id":"test","text":"next"})).unwrap()["admitted"],true);
        assert_eq!(manager.steer(json!({"session":"test","text":"change"})).unwrap()["queue"],"steering");
        server.join().unwrap();manager.shutdown();
    }

    #[test]
    fn message_pagination_uses_native_cursor_and_bounded_limit() {
        let (manager,mut writer,mut reader)=fake_manager();
        let server=thread::spawn(move || {
            let first=read_request(&mut reader);assert_eq!(first["type"],"get_messages_page");
            assert_eq!(first["limit"],200);
            reply(&mut writer,&first,json!({"messages":[],"totalMessages":500,"nextCursor":"opaque-next"}));
            let second=read_request(&mut reader);assert_eq!(second["cursor"],"opaque-next");assert_eq!(second["limit"],2);
            reply(&mut writer,&second,json!({"messages":[],"totalMessages":500}));
        });
        let first=manager.messages(json!({"session":"test","limit":9999})).unwrap();
        assert_eq!(first["nextCursor"],"opaque-next");
        let second=manager.messages(json!({"session":"test","limit":2,"cursor":first["nextCursor"]})).unwrap();
        assert!(second["nextCursor"].is_null());
        server.join().unwrap();manager.shutdown();
    }

    #[test]
    fn compact_is_observable_async_operation() {
        let (manager,mut writer,mut reader)=fake_manager();
        let server=thread::spawn(move || {
            let compact=read_request(&mut reader);assert_eq!(compact["type"],"compact");
            assert_eq!(compact["customInstructions"],"retain decisions");
            reply(&mut writer,&compact,json!({"summary":"compact summary","firstKeptEntryId":"entry-1","tokensBefore":123}));
        });
        let admitted=manager.compact(json!({"session":"test","instructions":"retain decisions"})).unwrap();
        assert_eq!(admitted["admission"],"pending");
        let result=manager.wait_operation(json!({"session":"test","operationId":admitted["operationId"],"timeout_ms":1000})).unwrap();
        assert_eq!(result["compaction"]["tokensBefore"],123);assert_eq!(result["status"],"finished");
        server.join().unwrap();manager.shutdown();
    }

    #[test]
    fn output_truncation_preserves_utf8() {
        let (text,truncated)=bounded_text(Some("🦀".repeat(20_000)));
        assert!(truncated);
        assert!(text.unwrap().len()<=64*1024);
    }

    #[test]
    fn stale_terminal_events_cannot_settle_a_new_prompt() {
        let session=Session {
            id:"race".into(),model:Mutex::new("fake".into()),cwd:PathBuf::from("/tmp"),pid:None,protocol:2,
            client:Mutex::new(None),stopping:AtomicBool::new(false),
            busy:AtomicBool::new(true),timed_out:AtomicBool::new(false),generation:AtomicU64::new(2),
            admission:Mutex::new(()),disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()),operations:Mutex::new(VecDeque::new()),
        };
        // The first prompt completed in the SDK; its receiver events arrive after
        // prompt two has already been admitted. They must remain historical only.
        session.record_event(json!({"type":"prompt_result","id":"old","sessionSettled":true}));
        session.record_event(json!({"type":"session_settled"}));
        let state=session.summary();
        assert_eq!(state["status"],"running");
        assert_eq!(state["sessionSettled"],Value::Null);
        assert_eq!(state["busy"],true);
        session.busy.store(false,Ordering::SeqCst);
        assert_eq!(session.summary()["sessionSettled"],Value::Null);
    }

    #[cfg(unix)]
    #[test]
    fn stop_reaps_a_child_even_when_abort_never_settles() {
        let mut process=Command::new("python3");
        process.args(["-u","-c",r#"
import sys,json
print(json.dumps({'type':'ready'}),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    print(json.dumps({'type':'response','id':request['id'],'command':request['type'],'success':True,'data':{'agentInvoked':True}}),flush=True)
"#]);
        let (client,_events)=Client::spawn(process,ClientOptions::default()).unwrap();
        let pid=client.pid();
        let session=Arc::new(Session {
            id:"process-test".into(),model:Mutex::new("fake".into()),cwd:PathBuf::from("/tmp"),pid,protocol:1,
            client:Mutex::new(Some(Arc::new(client))),stopping:AtomicBool::new(false),
            busy:AtomicBool::new(false),timed_out:AtomicBool::new(false),generation:AtomicU64::new(0),admission:Mutex::new(()),disconnected:AtomicBool::new(false),
            ring:Arc::new(EventRing::new()),operations:Mutex::new(VecDeque::new()),
        });
        let manager=Manager::default();
        lock(&manager.inner.sessions).insert(session.id.clone(),session);
        manager.send(json!({"session":"process-test","message":"hang","wait":false})).unwrap();
        let result=manager.stop(json!({"session":"process-test"})).unwrap();
        assert_eq!(result["stopped"],true);
        assert_eq!(manager.list()["sessions"],json!([]));
        // A live process group would return 0; confirmed teardown must have removed it.
        assert_eq!(unsafe {libc::kill(-(pid.unwrap() as i32),0)},-1);
    }
}
