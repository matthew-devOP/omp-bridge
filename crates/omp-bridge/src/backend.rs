use crate::{core::Manager, daemon::DaemonClient};
use anyhow::{Result, bail};
use serde_json::Value;

#[derive(Clone)]
pub enum Backend { Local(Manager), Shared(DaemonClient) }
impl Backend {
    pub fn call(&self, method:&str, args:Value)->Result<Value> {
        match self {
            Self::Shared(client)=>client.call(method,args),
            Self::Local(manager)=>local_call(manager,method,args),
        }
    }
}
pub fn local_call(m:&Manager,method:&str,args:Value)->Result<Value> {
    match method {
        "omp_start"=>m.start(args),"omp_send"=>m.send(args),
        "omp_steer"=>m.steer(args),"omp_follow_up"=>m.follow_up(args),
        "omp_promote_queued_message"=>m.promote_queued_message(args),
        "omp_set_model"=>m.set_model(args),"omp_compact"=>m.compact(args),
        "omp_messages"=>m.messages(args),"omp_events"=>m.events(args),
        "omp_status"=>m.status(args),"omp_abort"=>m.abort(args),
        "omp_stop"=>m.stop(args),"omp_list"=>Ok(m.list()),
        "omp_take_control"=>bail!("private_session: control transfer requires mcp --daemon"),
        _=>bail!("unknown_method"),
    }
}
