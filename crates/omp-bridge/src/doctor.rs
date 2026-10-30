//! Readiness diagnostics without opening authentication files.
use crate::{core::Manager, daemon::{DaemonClient, default_socket}};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::{env, fs, os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt}, path::{Path, PathBuf}, sync::atomic::{AtomicU64, Ordering}, thread, time::{Duration, Instant}};

static TEMP_ID:AtomicU64=AtomicU64::new(1);
const MODEL:&str="opencode-go/deepseek-v4.1-flash";
const PROBE:&str="OMP_BRIDGE_DOCTOR_OK";

struct ProbeGuard { manager:Manager, directory:PathBuf }
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        self.manager.shutdown();
        // Only remove this empty directory, never recursive caller content.
        let _=fs::remove_dir(&self.directory);
    }
}

fn executable(command:&str, search_path:Option<&std::ffi::OsStr>)->Option<PathBuf> {
    let candidate=Path::new(command);
    if command.contains(std::path::MAIN_SEPARATOR) {
        return is_executable(candidate).then(||candidate.to_path_buf());
    }
    env::split_paths(search_path?).map(|dir|dir.join(command)).find(|path|is_executable(path))
}
fn is_executable(path:&Path)->bool {
    fs::metadata(path).map(|m|m.is_file() && m.permissions().mode()&0o111!=0).unwrap_or(false)
}
fn socket_check(socket:&Path)->Value {
    let metadata=match fs::symlink_metadata(socket) {
        Ok(value)=>value,
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return json!({"status":"absent","requiredForPrivateMode":false}),
        Err(_)=>return json!({"status":"error","code":"socket_metadata_unavailable"}),
    };
    if !metadata.file_type().is_socket() {
        return json!({"status":"error","code":"not_a_unix_socket"});
    }
    let permissions=metadata.permissions().mode()&0o777;
    let owned=metadata.uid()==unsafe {libc::geteuid()};
    if permissions!=0o600 || !owned {
        return json!({"status":"error","code":"socket_not_private","permissions":format!("{permissions:04o}"),"ownedByCurrentUser":owned});
    }
    match DaemonClient::connect(Some(socket.to_path_buf()),"doctor") {
        Ok(client)=>{
            let ping=client.call("ping",json!({}));
            let _=client.call("goodbye",json!({}));
            match ping {
                Ok(value)=>json!({"status":"ok","permissions":"0600","protocolVersion":value["protocolVersion"],"version":value["version"]}),
                Err(_)=>json!({"status":"error","code":"daemon_ping_failed","permissions":"0600"}),
            }
        },
        Err(_)=>json!({"status":"error","code":"daemon_unreachable","permissions":"0600"}),
    }
}
fn live_verified(value:&Value)->bool {
    value["status"]=="finished" && value["result"]["status"]=="completed"
        && value["result"]["sessionSettled"]==true
        && value["assistant_text"].as_str().is_some_and(|text|text.trim()==PROBE)
}

/// Check executable resolution, an isolated RPC-v2 handshake, and optional daemon.
/// `live=true` explicitly opts into one model inference; without it authentication
/// remains unknown, even after a successful handshake. No credential files are read.
pub fn run(socket:Option<PathBuf>,live:bool)->Result<Value> {
    let command=env::var("OMP_BRIDGE_CMD").unwrap_or_else(|_|"omp".into());
    let resolved=executable(&command,env::var_os("PATH").as_deref());
    let label=Path::new(&command).file_name().and_then(|s|s.to_str()).unwrap_or("omp");
    let daemon=socket_check(&socket.unwrap_or_else(default_socket));
    let mut report=json!({
        "version":env!("CARGO_PKG_VERSION"),
        "executable":{"status":if resolved.is_some(){"ok"}else{"missing"},"name":label},
        "rpc":{"status":"not_checked"},
        "authentication":{"status":"unknown","provider":"opencode-go","reason":"handshake does not prove inference access; use --live for one minimal prompt"},
        "daemon":daemon,
        "liveRequested":live,"promptsSent":0,
        "privateModeReady":false,"sharedModeReady":false,"inferenceVerified":false,
    });
    if resolved.is_none(){return Ok(report);}
    let directory=env::temp_dir().join(format!("omp-doctor-{}-{}",std::process::id(),TEMP_ID.fetch_add(1,Ordering::Relaxed)));
    fs::DirBuilder::new().mode(0o700).create(&directory).map_err(|_|anyhow!("doctor_temp_directory_unavailable"))?;
    let guard=ProbeGuard {manager:Manager::default(),directory};
    let started=match guard.manager.start(json!({"cwd":guard.directory,"model":MODEL,"no_tools":true})) {
        Ok(value)=>value,
        Err(_)=>{
            report["rpc"]=json!({"status":"error","code":"handshake_failed"});
            return Ok(report);
        }
    };
    let Some(session)=started["session"].as_str() else {
        report["rpc"]=json!({"status":"error","code":"invalid_start_result"});
        return Ok(report);
    };
    let status=guard.manager.status(json!({"session":session}));
    let state_valid=status.as_ref().is_ok_and(|value|value["state"].is_object());
    let protocol_ok=started["protocolVersion"]==2;
    let observed_model=status.as_ref().ok().map(|value|json!({"id":value["state"]["model"]["id"],"provider":value["state"]["model"]["provider"]})).unwrap_or(Value::Null);
    report["rpc"]=json!({"status":if protocol_ok&&state_valid{"ok"}else{"error"},"protocolVersion":started["protocolVersion"],"stateRead":state_valid,"requestedModel":MODEL,"observedModel":observed_model});
    report["privateModeReady"]=json!(protocol_ok&&state_valid);
    report["sharedModeReady"]=json!(protocol_ok&&state_valid&&report["daemon"]["status"]=="ok");
    if live && protocol_ok && state_valid {
        report["promptsSent"]=json!(1);
        let sent=guard.manager.send(json!({"session":session,"message":"Respond exactly OMP_BRIDGE_DOCTOR_OK. Do not use tools.","wait":true,"timeout_ms":25000}));
        match sent {
            Ok(mut outcome)=>{
                let operation_id=outcome["operationId"].as_str().map(str::to_owned);
                let deadline=Instant::now()+Duration::from_secs(35);
                while outcome["status"]=="running_or_unknown" && Instant::now()<deadline {
                    thread::sleep(Duration::from_millis(100));
                    let Ok(status)=guard.manager.status(json!({"session":session})) else {break;};
                    if let Some(value)=status["operations"].as_array().and_then(|items|items.iter().find(|v|v["operationId"].as_str()==operation_id.as_deref())) {
                        outcome=value.clone();
                    }
                }
                let verified=live_verified(&outcome);
                report["inferenceVerified"]=json!(verified);
                report["authentication"]=if verified {
                    json!({"status":"verified_by_inference","provider":"opencode-go","model":MODEL})
                } else {
                    json!({"status":"unverified","provider":"opencode-go","reason":"probe did not return the exact settled result; this does not identify authentication as the cause"})
                };
            },
            Err(_)=>report["authentication"]=json!({"status":"unverified","provider":"opencode-go","reason":"probe failed; authentication is not isolated as the cause"}),
        }
    }
    let stopped=guard.manager.stop(json!({"session":session})).map(|v|v["stopped"]==true).unwrap_or(false);
    report["cleanupConfirmed"]=json!(stopped);
    if !stopped {
        report["privateModeReady"]=json!(false);
        report["sharedModeReady"]=json!(false);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_exact_settled_probe_verifies_inference() {
        let mut value=json!({"status":"finished","result":{"status":"completed","sessionSettled":true},"assistant_text":PROBE});
        assert!(live_verified(&value));
        value["result"]["sessionSettled"]=json!(false);assert!(!live_verified(&value));
        value["result"]["sessionSettled"]=json!(true);value["assistant_text"]=json!("different");assert!(!live_verified(&value));
    }
    #[test]
    fn executable_search_is_explicit_and_does_not_need_environment_mutation() {
        let found=executable("sh",Some(std::ffi::OsStr::new("/bin:/usr/bin")));
        assert!(found.is_some());
        assert!(executable("omp-bridge-definitely-not-an-executable",Some(std::ffi::OsStr::new("/bin"))).is_none());
        assert!(executable("/",None).is_none());
    }
    #[test]
    fn absent_socket_does_not_claim_daemon_ready() {
        let path=env::temp_dir().join(format!("omp-doctor-missing-{}",std::process::id()));
        assert_ne!(socket_check(&path)["status"],"ok");
    }
}
