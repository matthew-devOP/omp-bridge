mod core;
mod ring;
mod mcp;
use clap::{Parser, Subcommand};
use rmcp::ServiceExt;
use serde_json::json;
use std::io::{self, Write};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc};
use std::time::Duration;

#[derive(Parser)]
#[command(version, about="Control Oh My Pi via structured RPC: MCP server or interactive REPL")]
struct Cli { #[command(subcommand)] command: Mode }
#[derive(Subcommand)]
enum Mode {
    /// MCP over stdin/stdout. Diagnostics go to stderr.
    Mcp,
    /// Interactive local session; not an attachment to another MCP client's session.
    Repl {
        #[arg(long)] model: Option<String>,
        #[arg(long)] cwd: Option<String>,
        #[arg(long)] no_tools: bool,
    },
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manager=core::Manager::default();
    match Cli::parse().command {
        Mode::Mcp => {
            let service=mcp::Bridge{manager:manager.clone()}.serve((tokio::io::stdin(),tokio::io::stdout())).await?;
            tokio::select! {
                result=service.waiting() => { if let Err(e)=result { eprintln!("MCP transport closed: {e}"); } },
                _=tokio::signal::ctrl_c() => {},
            }
        },
        Mode::Repl{model,cwd,no_tools} => {
            let m=manager.clone();
            let cancelled=Arc::new(AtomicBool::new(false));
            let worker_cancel=cancelled.clone();
            let mut worker=tokio::task::spawn_blocking(move || repl(m,model,cwd,no_tools,worker_cancel));
            tokio::select! {
                result=&mut worker => { if let Err(e)=result? { eprintln!("REPL: {e}"); } },
                _=tokio::signal::ctrl_c() => {
                    cancelled.store(true,Ordering::SeqCst);
                    let cleanup=manager.clone();
                    tokio::task::spawn_blocking(move || cleanup.shutdown()).await?;
                    let _=worker.await;
                }
            }
        }
    }
    tokio::task::spawn_blocking(move || manager.shutdown()).await?;
    Ok(())
}
fn repl(m:core::Manager, model:Option<String>, cwd:Option<String>, no_tools:bool,cancelled:Arc<AtomicBool>)->anyhow::Result<()> {
    let started=m.start(json!({"model":model,"cwd":cwd,"no_tools":no_tools}))?;
    println!("{started}");
    let session=started["session"].as_str().ok_or_else(||anyhow::anyhow!("missing session id"))?;
    println!("Type a prompt or /status /events /abort /quit. Waiting timeout never cancels work.");
    let (tx,rx)=mpsc::channel();
    // A detached input reader permits cancellation while stdin is idle.
    // It owns no sessions; process exit releases it after manager cleanup.
    std::thread::spawn(move || loop {
        let mut line=String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0)|Err(_)=>break,
            Ok(_)=>if tx.send(line).is_err(){break;},
        }
    });
    let mut cursor=0;
    loop {
        if cancelled.load(Ordering::SeqCst){return Ok(());}
        print!("omp> "); io::stdout().flush()?;
        let line=loop {
            if cancelled.load(Ordering::SeqCst){return Ok(());}
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line)=>break Some(line),
                Err(mpsc::RecvTimeoutError::Timeout)=>continue,
                Err(_)=>break None,
            }
        };
        let Some(line)=line else {break;};
        let line=line.trim(); if line.is_empty(){continue;}
        if line=="/quit" {break;}
        let result=match line {
            "/status"=>m.status(json!({"session":session})),
            "/abort"=>m.abort(json!({"session":session})),
            "/events"=>m.events(json!({"session":session,"cursor":cursor})),
            _=>m.send(json!({"session":session,"message":line,"wait":true})),
        };
        match result { Ok(v)=>{if line=="/events" {cursor=v["nextCursor"].as_u64().unwrap_or(cursor);} println!("{}",serde_json::to_string_pretty(&v)?);},Err(e)=>eprintln!("{e}") }
    }
    println!("{}",m.stop(json!({"session":session}))?);
    Ok(())
}
