//! Opt-in shared-session daemon. The Unix socket is the local trust boundary;
//! controller leases coordinate cooperating clients of the same OS account.
use crate::core::Manager;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    os::{fd::{AsRawFd, FromRawFd}, unix::{ffi::OsStrExt, fs::{DirBuilderExt, MetadataExt, PermissionsExt}, net::{UnixListener, UnixStream}}},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_CLIENTS: usize = 256;
const MAX_CONNECTIONS: usize = 64;
static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static BIND_LOCK: Mutex<()> = Mutex::new(());

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn default_socket() -> PathBuf {
    if let Some(path) = std::env::var_os("OMP_BRIDGE_SOCKET").filter(|v| !v.is_empty()) {
        return PathBuf::from(path);
    }
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir).join("omp-bridge.sock");
    }
    if let Some(home) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(home).join(".local/state/omp-bridge/bridge.sock");
    }
    std::env::temp_dir().join(format!("omp-bridge-{}", unsafe { libc::geteuid() })).join("bridge.sock")
}

#[derive(Serialize, Deserialize)]
struct Request {
    id: String,
    #[serde(default)]
    client_id: Option<String>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Clone)]
pub struct DaemonClient { identity:Arc<ClientIdentity> }
struct ClientIdentity { socket:PathBuf, client_id:String }
impl Drop for ClientIdentity {
    fn drop(&mut self) {
        // Best effort, bounded cleanup. A crashed or disconnected controller never
        // destroys sessions or silently transfers its lease to another client.
        let deadline=Instant::now()+Duration::from_millis(180);
        let request=Request{id:next_request_id(),client_id:Some(self.client_id.clone()),method:"goodbye".into(),params:json!({})};
        let _=exchange_before(&self.socket,&request,deadline);
    }
}
impl DaemonClient {
    pub fn connect(socket: Option<PathBuf>, name: &str) -> Result<Self> {
        let socket = socket.unwrap_or_else(default_socket);
        let response = exchange(&socket, &Request {
            id: next_request_id(), client_id: None, method: "hello".into(),
            params: json!({"name":name}),
        })?;
        let client_id = response["client_id"].as_str().ok_or_else(|| anyhow!("invalid_daemon_hello"))?.to_owned();
        Ok(Self {identity:Arc::new(ClientIdentity {socket,client_id})})
    }

    pub fn call(&self, method: &str, params: Value) -> Result<Value> {
        exchange(&self.identity.socket, &Request {
            id:next_request_id(), client_id:Some(self.identity.client_id.clone()), method:method.into(), params,
        })
    }

    pub fn client_id(&self) -> &str { &self.identity.client_id }
    pub fn socket(&self) -> &Path { &self.identity.socket }
}

fn next_request_id() -> String {
    format!("{}-{}",std::process::id(), REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

fn exchange(socket: &Path, request: &Request) -> Result<Value> {
    exchange_before(socket,request,Instant::now()+Duration::from_secs(75))
}

fn exchange_before(socket:&Path,request:&Request,deadline:Instant)->Result<Value> {
    let mut stream=connect_before(socket,deadline).context("daemon_unavailable")?;
    stream.set_write_timeout(Some(remaining(deadline)?.min(Duration::from_secs(5))))?;
    write_frame(&mut stream,&serde_json::to_value(request)?)?;
    stream.set_read_timeout(Some(remaining(deadline)?))?;
    let response=read_frame(&mut BufReader::new(stream))?;
    if response.get("id").and_then(Value::as_str) != Some(&request.id) { bail!("daemon_response_id_mismatch"); }
    if response["ok"] == true {return Ok(response.get("result").cloned().unwrap_or(Value::Null));}
    let code=response["error"]["code"].as_str().unwrap_or("daemon_error");
    let message=response["error"]["message"].as_str().unwrap_or("request failed");
    bail!("{code}: {message}")
}

fn remaining(deadline:Instant)->Result<Duration> {
    deadline.checked_duration_since(Instant::now()).filter(|v|!v.is_zero()).ok_or_else(||anyhow!("daemon_timeout"))
}

fn connect_before(path:&Path,deadline:Instant)->Result<UnixStream> {
    let mut address:libc::sockaddr_un=unsafe {std::mem::zeroed()};
    let path=path.as_os_str().as_bytes();
    if path.len()>=address.sun_path.len() || path.contains(&0) {bail!("invalid_socket_path");}
    address.sun_family=libc::AF_UNIX as libc::sa_family_t;
    for (destination,source) in address.sun_path.iter_mut().zip(path) {*destination=*source as libc::c_char;}
    let length=(std::mem::offset_of!(libc::sockaddr_un,sun_path)+path.len()+1) as libc::socklen_t;
    #[cfg(any(target_os="macos",target_os="ios",target_os="freebsd"))]
    {address.sun_len=length as u8;}
    let fd=unsafe {libc::socket(libc::AF_UNIX,libc::SOCK_STREAM,0)};
    if fd<0 {return Err(std::io::Error::last_os_error().into());}
    let stream=unsafe {UnixStream::from_raw_fd(fd)};
    if unsafe {libc::fcntl(fd,libc::F_SETFD,libc::FD_CLOEXEC)}<0 {return Err(std::io::Error::last_os_error().into());}
    stream.set_nonblocking(true)?;
    let result=unsafe {libc::connect(fd,&address as *const _ as *const libc::sockaddr,length)};
    if result<0 {
        let error=std::io::Error::last_os_error();
        if !matches!(error.raw_os_error(),Some(libc::EINPROGRESS)|Some(libc::EAGAIN)) {return Err(error.into());}
        let mut event=libc::pollfd{fd,events:libc::POLLOUT,revents:0};
        let timeout=remaining(deadline)?.as_millis().max(1).min(i32::MAX as u128) as i32;
        if unsafe {libc::poll(&mut event,1,timeout)}<=0 {bail!("daemon_connect_timeout");}
        let mut error_code:libc::c_int=0;
        let mut error_len=std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        if unsafe {libc::getsockopt(fd,libc::SOL_SOCKET,libc::SO_ERROR,&mut error_code as *mut _ as *mut libc::c_void,&mut error_len)}<0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if error_code!=0 {return Err(std::io::Error::from_raw_os_error(error_code).into());}
    }
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn read_frame(reader: &mut impl BufRead) -> Result<Value> {
    let mut frame=Vec::new();
    let bytes=reader.take((MAX_FRAME+1) as u64).read_until(b'\n',&mut frame)?;
    if bytes==0 { bail!("empty_frame"); }
    if bytes>MAX_FRAME { bail!("frame_too_large"); }
    if frame.last()!=Some(&b'\n') { bail!("incomplete_frame"); }
    serde_json::from_slice(&frame).map_err(|_|anyhow!("invalid_json"))
}

fn write_frame(writer:&mut impl Write, value:&Value)->Result<()> {
    let bytes=serde_json::to_vec(value)?;
    if bytes.len()+1>MAX_FRAME { bail!("frame_too_large"); }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[derive(Clone, Serialize)]
struct Lease {
    #[serde(rename="clientId")]
    client_id:String,
    #[serde(rename="clientName")]
    client_name:String,
    #[serde(rename="leaseEpoch")]
    epoch:u64,
}

struct SessionControl { admission:Mutex<()>, lease:Mutex<Lease> }
#[derive(Default)]
struct Control {
    clients:BTreeMap<String,String>,
    leases:BTreeMap<String,Arc<SessionControl>>,
}

struct DaemonState {
    manager:Manager,
    control:Mutex<Control>,
    closing:AtomicBool,
}

impl DaemonState {
    fn new()->Self {Self {manager:Manager::default(),control:Mutex::new(Control::default()),closing:AtomicBool::new(false)}}

    fn session_control(&self,session:&str)->Result<Arc<SessionControl>> {
        lock(&self.control).leases.get(session).cloned().ok_or_else(||anyhow!("unknown_session"))
    }

    fn ensure_open(&self)->Result<()> {
        if self.closing.load(Ordering::SeqCst) {bail!("daemon_shutting_down");}
        Ok(())
    }

    fn dispatch(&self, request:Request)->Result<Value> {
        if request.id.len()>128 || request.method.len()>128 {bail!("invalid_request");}
        let mut params=request.params;
        if params.is_null() {params=json!({});}
        if !params.is_object() {bail!("invalid_params");}
        if params.get("session").is_none()
            && let Some(id)=params.get("session_id").cloned() {params["session"]=id;}
        let mut control=lock(&self.control);
        self.ensure_open()?;
        if request.method=="hello" {
            if control.clients.len()>=MAX_CLIENTS {bail!("client_limit");}
            let name=params["name"].as_str().unwrap_or("client");
            if name.is_empty() || name.len()>64 || name.chars().any(char::is_control) {bail!("invalid_client_name");}
            let client_id=random_id()?;
            control.clients.insert(client_id.clone(),name.into());
            return Ok(json!({"client_id":client_id,"name":name,"protocolVersion":1,"version":env!("CARGO_PKG_VERSION")}));
        }
        let client_id=request.client_id.ok_or_else(||anyhow!("unknown_client"))?;
        let client_name=control.clients.get(&client_id).cloned().ok_or_else(||anyhow!("unknown_client"))?;
        if request.method=="goodbye" {
            control.clients.remove(&client_id);
            return Ok(json!({"disconnected":true,"sessionsPreserved":true}));
        }
        drop(control); // No RPC, handshake, event wait or teardown holds the registry.
        match request.method.as_str() {
            "ping"=>Ok(json!({"protocolVersion":1,"version":env!("CARGO_PKG_VERSION"),"scope":"shared_daemon"})),
            "omp_list"=>{
                let mut result=self.manager.list();
                result["scope"]=json!("shared_daemon");
                let leases=lock(&self.control).leases.clone();
                if let Some(sessions)=result["sessions"].as_array_mut() {
                    for session in sessions {
                        let id=session["session"].as_str().unwrap_or("");
                        let lease=leases.get(id).map(|entry|json!(*lock(&entry.lease))).unwrap_or(Value::Null);
                        session["controller"]=lease;
                    }
                }
                Ok(result)
            }
            "omp_start"=>{
                let mut result=self.manager.start(params)?;
                let session=result["session"].as_str().ok_or_else(||anyhow!("start_missing_session"))?.to_owned();
                let lease=Lease {client_id:client_id.clone(),client_name,epoch:1};
                let mut control=lock(&self.control);
                if self.closing.load(Ordering::SeqCst) {
                    drop(control);
                    let _=self.manager.stop(json!({"session":session}));
                    bail!("daemon_shutting_down");
                }
                control.leases.insert(session.clone(),Arc::new(SessionControl{admission:Mutex::new(()),lease:Mutex::new(lease.clone())}));
                drop(control);
                self.manager.emit_event(&session,json!({"type":"controller_changed","previousClient":Value::Null,
                    "newClient":client_id,"reason":"session_started","leaseEpoch":1,"timestampMs":timestamp_ms()}))?;
                result["controller"]=json!(lease);
                Ok(result)
            }
            "omp_take_control"=>{
                let session=session_id(&params)?;
                let reason=params["reason"].as_str().map(str::trim).filter(|s|!s.is_empty()).ok_or_else(||anyhow!("reason_required"))?;
                if reason.len()>1024 {bail!("reason_too_long");}
                let entry=self.session_control(session)?;
                let _admission=lock(&entry.admission);
                self.ensure_open()?;
                let previous=lock(&entry.lease).clone();
                if previous.client_id==client_id {return Ok(json!({"session":session,"controller":previous,"changed":false}));}
                let lease=Lease{client_id:client_id.clone(),client_name,epoch:previous.epoch+1};
                self.manager.emit_event(session,json!({"type":"controller_changed","previousClient":previous.client_id,
                    "newClient":client_id,"reason":reason,"leaseEpoch":lease.epoch,"timestampMs":timestamp_ms()}))?;
                *lock(&entry.lease)=lease.clone();
                Ok(json!({"session":session,"controller":lease,"changed":true}))
            }
            "omp_status"|"omp_events"|"omp_messages"=>{
                let session=session_id(&params)?.to_owned();
                let mut result=match request.method.as_str() {
                    "omp_status"=>self.manager.status(params)?,
                    "omp_events"=>bounded_events(self.manager.events(params)?),
                    _=>self.manager.messages(params)?,
                };
                if request.method=="omp_status" {
                    result["controller"]=self.session_control(&session).ok().map(|entry|json!(*lock(&entry.lease))).unwrap_or(Value::Null);
                }
                Ok(result)
            }
            "omp_send"|"omp_steer"|"omp_follow_up"|"omp_promote_queued_message"|"omp_set_model"|"omp_compact"|"omp_abort"|"omp_stop"=>{
                let session=session_id(&params)?.to_owned();
                let entry=self.session_control(&session)?;
                let admission=lock(&entry.admission);
                self.ensure_open()?;
                if lock(&entry.lease).client_id!=client_id {bail!("not_controller");}
                if request.method=="omp_send" {
                    let wait=params["wait"].as_bool().unwrap_or(true);
                    let timeout=params.get("timeout_ms").cloned().unwrap_or(json!(25000));
                    params["wait"]=json!(false);
                    let result=self.manager.send(params)?;
                    drop(admission);
                    if wait && let Some(id)=result["operationId"].as_str() {
                        return self.manager.wait_operation(json!({"session":session,"operationId":id,"timeout_ms":timeout}));
                    }
                    return Ok(result);
                }
                let result=match request.method.as_str() {
                    "omp_steer"=>self.manager.steer(params),
                    "omp_follow_up"=>self.manager.follow_up(params),
                    "omp_promote_queued_message"=>self.manager.promote_queued_message(params),
                    "omp_set_model"=>self.manager.set_model(params),
                    "omp_compact"=>self.manager.compact(params),
                    "omp_abort"=>self.manager.abort(params),
                    _=>{
                        self.manager.emit_event(&session,json!({"type":"session_stop_requested","clientId":client_id,"timestampMs":timestamp_ms()}))?;
                        self.manager.stop(params)
                    },
                }?;
                if request.method=="omp_stop" && result["stopped"]==true {lock(&self.control).leases.remove(&session);}
                Ok(result)
            }
            _=>Err(anyhow!("unknown_method")),
        }
    }
}

fn session_id(params:&Value)->Result<&str> {
    params["session"].as_str().filter(|s|!s.is_empty()).ok_or_else(||anyhow!("session_required"))
}

fn timestamp_ms()->u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn random_id()->Result<String> {
    let mut bytes=[0u8;24];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b|format!("{b:02x}")).collect())
}

// A daemon frame has a stricter limit than the ring. Paginate before serializing,
// preserving replay cursor and replacing a single huge event with an explicit marker.
fn bounded_events(mut value:Value)->Value {
    const PAGE_BYTES:usize=768*1024;
    let Some(events)=value["events"].as_array().cloned() else {return value;};
    let mut page=Vec::new();
    let mut bytes=0;
    for mut entry in events {
        let size=serde_json::to_vec(&entry).map(|v|v.len()).unwrap_or(PAGE_BYTES+1);
        if size>PAGE_BYTES {
            entry["event"]=json!({"type":"bridge_event_truncated","truncated":true,"originalBytes":size,
                "originalType":entry["event"]["type"]});
        }
        let size=serde_json::to_vec(&entry).map(|v|v.len()).unwrap_or(0);
        if bytes+size>PAGE_BYTES {
            value["nextCursor"]=entry["cursor"].clone();
            value["hasMore"]=json!(true);
            break;
        }
        bytes+=size;
        page.push(entry);
    }
    value["events"]=json!(page);
    value
}

fn error_code(error:&anyhow::Error)->&str {
    let text=error.to_string();
    match text.as_str() {
        "not_controller"=>"not_controller",
        "unknown_client"=>"unknown_client",
        "unknown_session"=>"unknown_session",
        "unknown_method"=>"unknown_method",
        "daemon_shutting_down"=>"daemon_shutting_down",
        "frame_too_large"=>"frame_too_large",
        "reason_required"=>"reason_required",
        "client_limit"=>"client_limit",
        _=>"request_failed",
    }
}

fn handle_connection(mut stream:UnixStream,state:Arc<DaemonState>)->Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let frame=read_frame(&mut BufReader::new(stream.try_clone()?));
    let (id,outcome)=match frame {
        Ok(frame)=>{
            let id=frame.get("id").cloned().unwrap_or(Value::Null);
            let outcome=serde_json::from_value::<Request>(frame).map_err(|_|anyhow!("invalid_request")).and_then(|r|state.dispatch(r));
            (id,outcome)
        },
        Err(error)=>(Value::Null,Err(error)),
    };
    let response=match outcome {
        Ok(result)=>json!({"id":id,"ok":true,"result":result}),
        Err(error)=>json!({"id":id,"ok":false,"error":{"code":error_code(&error),"message":error.to_string()}}),
    };
    if serde_json::to_vec(&response)?.len()+1>MAX_FRAME {
        return write_frame(&mut stream,&json!({"id":id,"ok":false,"error":{"code":"response_too_large","message":"Narrow the event filter or message page."}}));
    }
    write_frame(&mut stream,&response)
}

struct SocketGuard {path:PathBuf,device:u64,inode:u64}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(meta)=fs::symlink_metadata(&self.path)
            && meta.dev()==self.device && meta.ino()==self.inode {let _=fs::remove_file(&self.path);}
    }
}

pub fn serve(socket:Option<PathBuf>,shutdown:Arc<AtomicBool>)->Result<()> {
    let socket=socket.unwrap_or_else(default_socket);
    let parent=socket.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    if !parent.exists() {
        let mut builder=fs::DirBuilder::new();
        builder.recursive(true).mode(0o700).create(parent)?;
    }
    let metadata=fs::symlink_metadata(parent)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {bail!("socket_parent_must_be_directory");}
    if fs::symlink_metadata(&socket).is_ok() {bail!("socket_path_exists: refusing to replace an existing socket or symlink");}
    let listener={
        let _binding=lock(&BIND_LOCK);
        // Bind is atomic with respect to an existing pathname. The temporary mask
        // only narrows new-file access; it prevents a pre-chmod exposure window.
        let old=unsafe {libc::umask(0o077)};
        let result=UnixListener::bind(&socket);
        unsafe {libc::umask(old)};
        result.context("socket_bind_failed")?
    };
    let meta=fs::symlink_metadata(&socket)?;
    let _socket_guard=SocketGuard {path:socket.clone(),device:meta.dev(),inode:meta.ino()};
    fs::set_permissions(&socket,fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let state=Arc::new(DaemonState::new());
    let active=Arc::new(AtomicUsize::new(0));
    let mut workers=Vec::new();
    let mut accept_error=None;
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((mut stream,_))=>{
                // On BSD/macOS the accepted socket inherits the listener's
                // non-blocking flag; the request reader expects blocking IO.
                stream.set_nonblocking(false)?;
                if active.load(Ordering::SeqCst)>=MAX_CONNECTIONS {
                    let _=write_frame(&mut stream,&json!({"id":null,"ok":false,"error":{"code":"connection_limit","message":"Too many concurrent requests"}}));
                    continue;
                }
                active.fetch_add(1,Ordering::SeqCst);
                let state=state.clone();let active=active.clone();
                workers.push(thread::spawn(move || {let _=handle_connection(stream,state);active.fetch_sub(1,Ordering::SeqCst);}));
                let mut i=0;
                while i<workers.len() {
                    if workers[i].is_finished() {let _=workers.swap_remove(i).join();} else {i+=1;}
                }
            },
            Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>{
                let mut ready=libc::pollfd{fd:listener.as_raw_fd(),events:libc::POLLIN,revents:0};
                unsafe {libc::poll(&mut ready,1,20);}
            },
            Err(error)=>{accept_error=Some(error);break;}
        }
    }
    state.closing.store(true,Ordering::SeqCst);
    drop(listener);
    state.manager.shutdown();
    for worker in workers {let _=worker.join();}
    if let Some(error)=accept_error {return Err(error.into());}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn framing_rejects_overflow_and_partial_json() {
        assert!(read_frame(&mut std::io::Cursor::new(vec![b'x';MAX_FRAME+1])).is_err());
        assert!(read_frame(&mut std::io::Cursor::new(b"{}".to_vec())).is_err());
        assert_eq!(read_frame(&mut std::io::Cursor::new(b"{}\n".to_vec())).unwrap(),json!({}));
    }
    #[test]
    fn hello_registers_distinct_clients_and_unknown_clients_are_rejected() {
        let state=DaemonState::new();
        let call=||Request{id:"1".into(),client_id:None,method:"hello".into(),params:json!({"name":"test"})};
        let a=state.dispatch(call()).unwrap();let b=state.dispatch(call()).unwrap();
        assert_ne!(a["client_id"],b["client_id"]);
        let result=state.dispatch(Request{id:"2".into(),client_id:Some("unregistered".into()),method:"omp_list".into(),params:json!({})});
        assert_eq!(result.unwrap_err().to_string(),"unknown_client");
    }
    #[test]
    fn client_registry_is_bounded_and_goodbye_releases_a_slot() {
        let state=DaemonState::new();
        let mut first=String::new();
        for i in 0..MAX_CLIENTS {
            let response=state.dispatch(Request{id:i.to_string(),client_id:None,method:"hello".into(),params:json!({"name":"test"})}).unwrap();
            if i==0 {first=response["client_id"].as_str().unwrap().to_owned();}
        }
        let hello=||Request{id:"more".into(),client_id:None,method:"hello".into(),params:json!({"name":"test"})};
        assert_eq!(state.dispatch(hello()).unwrap_err().to_string(),"client_limit");
        assert_eq!(state.dispatch(Request{id:"bye".into(),client_id:Some(first.clone()),method:"goodbye".into(),params:json!({})}).unwrap()["disconnected"],true);
        assert!(state.dispatch(hello()).is_ok());
        assert_eq!(state.dispatch(Request{id:"old".into(),client_id:Some(first),method:"ping".into(),params:json!({})}).unwrap_err().to_string(),"unknown_client");
    }
    #[test]
    fn socket_permissions_client_lifetime_and_owned_cleanup() {
        let dir=PathBuf::from("/tmp").join(format!("omp-d-{}",&random_id().unwrap()[..16]));
        let socket=dir.join("bridge.sock");
        let shutdown=Arc::new(AtomicBool::new(false));
        let server_shutdown=shutdown.clone();
        let server_socket=socket.clone();
        let server=thread::spawn(move || serve(Some(server_socket),server_shutdown));
        let deadline=std::time::Instant::now()+Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now()<deadline {thread::sleep(Duration::from_millis(10));}
        if !socket.exists() {
            shutdown.store(true,Ordering::SeqCst);
            panic!("daemon startup failed: {:?}",server.join().unwrap());
        }
        assert_eq!(fs::metadata(&socket).unwrap().permissions().mode()&0o777,0o600);
        assert_eq!(fs::metadata(&dir).unwrap().permissions().mode()&0o777,0o700);
        let a=DaemonClient::connect(Some(socket.clone()),"writer").unwrap();
        let b=DaemonClient::connect(Some(socket.clone()),"observer").unwrap();
        assert_ne!(a.client_id(),b.client_id());
        assert_eq!(a.call("ping",json!({})).unwrap()["protocolVersion"],1);
        assert_eq!(b.call("omp_list",json!({})).unwrap()["sessions"],json!([]));
        assert!(serve(Some(socket.clone()),Arc::new(AtomicBool::new(true))).is_err());
        assert!(a.call("ping",json!({})).is_ok());
        drop(a);drop(b);
        shutdown.store(true,Ordering::SeqCst);
        server.join().unwrap().unwrap();
        assert!(!socket.exists());
        fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn last_client_drop_releases_registration_after_clone_lifetime() {
        let dir=PathBuf::from("/tmp").join(format!("omp-drop-{}",&random_id().unwrap()[..12]));
        let socket=dir.join("bridge.sock");
        let shutdown=Arc::new(AtomicBool::new(false));
        let worker_shutdown=shutdown.clone();let worker_socket=socket.clone();
        let server=thread::spawn(move ||serve(Some(worker_socket),worker_shutdown));
        let deadline=Instant::now()+Duration::from_secs(5);
        while !socket.exists() && Instant::now()<deadline {thread::sleep(Duration::from_millis(5));}
        for _ in 0..MAX_CLIENTS+20 {
            let client=DaemonClient::connect(Some(socket.clone()),"temporary").unwrap();
            let copy=client.clone();
            drop(client);
            assert_eq!(copy.call("ping",json!({})).unwrap()["protocolVersion"],1);
            drop(copy); // Last clone sends goodbye, preventing cap exhaustion.
        }
        shutdown.store(true,Ordering::SeqCst);server.join().unwrap().unwrap();
        fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn client_drop_has_a_short_deadline_when_daemon_does_not_reply() {
        let dir=PathBuf::from("/tmp").join(format!("omp-drop-{}",&random_id().unwrap()[..12]));
        fs::create_dir(&dir).unwrap();let socket=dir.join("bridge.sock");
        let listener=UnixListener::bind(&socket).unwrap();
        let server=thread::spawn(move || {let (_stream,_)=listener.accept().unwrap();thread::sleep(Duration::from_millis(400));});
        let client=DaemonClient{identity:Arc::new(ClientIdentity{socket:socket.clone(),client_id:"test-client".into()})};
        let start=Instant::now();drop(client);
        assert!(start.elapsed()<Duration::from_millis(300));
        server.join().unwrap();fs::remove_file(socket).unwrap();fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn unrelated_client_reads_do_not_wait_for_a_session_admission_lock() {
        let state=Arc::new(DaemonState::new());
        let hello=state.dispatch(Request{id:"hello".into(),client_id:None,method:"hello".into(),params:json!({})}).unwrap();
        let client=hello["client_id"].as_str().unwrap().to_owned();
        let session=Arc::new(SessionControl{admission:Mutex::new(()),lease:Mutex::new(Lease{client_id:client.clone(),client_name:"test".into(),epoch:1})});
        lock(&state.control).leases.insert("slow-session".into(),session.clone());
        let (tx,rx)=std::sync::mpsc::channel();
        let worker=thread::spawn(move || {let _guard=lock(&session.admission);tx.send(()).unwrap();thread::sleep(Duration::from_millis(250));});
        rx.recv().unwrap();
        let start=Instant::now();
        assert!(state.dispatch(Request{id:"list".into(),client_id:Some(client),method:"omp_list".into(),params:json!({})}).is_ok());
        assert!(start.elapsed()<Duration::from_millis(150));
        worker.join().unwrap();
    }
    #[test]
    fn existing_symlink_is_never_removed() {
        let dir=PathBuf::from("/tmp").join(format!("omp-d-{}",&random_id().unwrap()[..16]));
        fs::create_dir(&dir).unwrap();
        let target=dir.join("keep");fs::write(&target,b"keep").unwrap();
        let socket=dir.join("bridge.sock");
        std::os::unix::fs::symlink(&target,&socket).unwrap();
        assert!(serve(Some(socket.clone()),Arc::new(AtomicBool::new(true))).is_err());
        assert!(fs::symlink_metadata(&socket).unwrap().file_type().is_symlink());
        assert_eq!(fs::read(&target).unwrap(),b"keep");
        fs::remove_file(socket).unwrap();fs::remove_file(target).unwrap();fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn event_page_preserves_cursor_when_wire_budget_is_reached() {
        let event=|cursor|json!({"cursor":cursor,"event":{"type":"message_end","data":"x".repeat(500*1024)}});
        let page=bounded_events(json!({"events":[event(2),event(3)],"nextCursor":4}));
        assert_eq!(page["events"].as_array().unwrap().len(),1);
        assert_eq!(page["nextCursor"],3);
        assert_eq!(page["hasMore"],true);
    }
    #[test]
    fn huge_event_is_an_explicit_replay_marker() {
        let page=bounded_events(json!({"events":[{"cursor":1,"event":{"type":"message_end","data":"x".repeat(MAX_FRAME)}}],"nextCursor":2}));
        assert_eq!(page["events"][0]["event"]["truncated"],true);
        assert_eq!(page["nextCursor"],2);
    }
}
