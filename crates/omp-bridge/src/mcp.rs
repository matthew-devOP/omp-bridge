use crate::backend::Backend;
use rmcp::{tool, tool_router, tool_handler, ServerHandler, handler::server::wrapper::Parameters, model::{CallToolResult, ContentBlock}};
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};
use serde_json::Value;

#[derive(Clone)]
pub struct Bridge { pub backend: Backend }

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Start {
    /// Existing working directory for the agent. Defaults to bridge working directory.
    pub cwd: Option<String>,
    /// Provider/model identifier, passed at process startup.
    pub model: Option<String>,
    /// Disable tools for a minimal inference-only smoke test. Default false.
    pub no_tools: Option<bool>,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Session { #[serde(alias="session_id")] pub session: String }
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SendArgs {
    #[serde(alias="session_id")]
    pub session: String,
    pub message: String,
    /// Normal prompt by default; use dedicated steer/follow-up tools while active.
    pub mode: Option<String>,
    /// Queue a prompt exactly once while streaming: steer or followUp.
    pub streaming_behavior: Option<String>,
    /// Wait for result up to timeout_ms; false returns an operationId immediately.
    pub wait: Option<bool>,
    /// At most 25000 ms. Timeout never aborts or retries the prompt.
    pub timeout_ms: Option<u64>,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Events {
    #[serde(alias="session_id")]
    pub session: String,
    /// Next event cursor to read, initially zero.
    pub cursor: Option<u64>,
    /// Filter this read only; empty means all kinds.
    pub kinds: Option<Vec<String>>,
    /// Long-poll at most 25000 ms.
    pub wait_ms: Option<u64>,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct TextCommand {
    #[serde(alias="session_id")] pub session:String,
    /// Message to steer or queue, exactly once.
    #[serde(alias="message")] pub text:String,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct TakeControl {
    #[serde(alias="session_id")] pub session:String,
    /// Human-readable explanation recorded in the controller transfer event.
    pub reason:String,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SetModel {
    #[serde(alias="session_id")] pub session:String,
    pub provider:String,
    pub model_id:String,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Compact {
    #[serde(alias="session_id")] pub session:String,
    pub instructions:Option<String>,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Messages {
    #[serde(alias="session_id")] pub session:String,
    /// Stable page cursor returned by the previous call.
    pub cursor:Option<String>,
    /// Legacy offset for starting without a cursor.
    pub offset:Option<u64>,
    /// Bounded page size, maximum 100.
    pub limit:Option<u64>,
}
async fn run(f: impl FnOnce() -> anyhow::Result<Value> + Send + 'static) -> CallToolResult {
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(v)) => CallToolResult::success(vec![ContentBlock::text(v.to_string())]),
        Ok(Err(e)) => CallToolResult::error(vec![ContentBlock::text(e.to_string())]),
        Err(_) => CallToolResult::error(vec![ContentBlock::text("bridge worker failed")]),
    }
}
#[tool_router]
impl Bridge {
    #[tool(description="Start an Oh My Pi RPC v2 session. Private by default; daemon mode shares sessions with an exclusive controller lease.")]
    async fn omp_start(&self, Parameters(p): Parameters<Start>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_start",serde_json::to_value(p)?)).await
    }
    #[tool(description="Send a prompt. Returns a bridge operationId; timeout means running_or_unknown, never implicit abort or retry.")]
    async fn omp_send(&self, Parameters(p): Parameters<SendArgs>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_send",serde_json::to_value(p)?)).await
    }
    #[tool(description="Read bounded session events with a cursor and explicit dropped-history indication. Inspect prompt_result.sessionSettled or session_settled to establish settlement.")]
    async fn omp_events(&self, Parameters(p): Parameters<Events>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_events",serde_json::to_value(p)?)).await
    }
    #[tool(description="Get authoritative agent state and bridge operation outcomes. Completion and session settlement are distinct.")]
    async fn omp_status(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_status",serde_json::to_value(p)?)).await
    }
    #[tool(description="Abort the current agent turn without stopping the process.")]
    async fn omp_abort(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_abort",serde_json::to_value(p)?)).await
    }
    #[tool(description="Stop the session. stopped=true only after confirmed process teardown.")]
    async fn omp_stop(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_stop",serde_json::to_value(p)?)).await
    }
    #[tool(description="List sessions: private to this process by default, shared when connected to a daemon.")]
    async fn omp_list(&self) -> CallToolResult {
        let b=self.backend.clone(); run(move || b.call("omp_list",serde_json::json!({}))).await
    }
    #[tool(description="Steer a running agent at its next step. Sends exactly one RPC steer, never a duplicate fallback prompt.")]
    async fn omp_steer(&self,Parameters(p):Parameters<TextCommand>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_steer",serde_json::to_value(p)?)).await
    }
    #[tool(description="Queue a follow-up for when the agent would otherwise stop. Admission does not mean completion.")]
    async fn omp_follow_up(&self,Parameters(p):Parameters<TextCommand>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_follow_up",serde_json::to_value(p)?)).await
    }
    #[tool(description="Promote a matching queued follow-up into steering without sending it again. Inspect promoted, not object truthiness.")]
    async fn omp_promote_queued_message(&self,Parameters(p):Parameters<TextCommand>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_promote_queued_message",serde_json::to_value(p)?)).await
    }
    #[tool(description="Explicitly take the exclusive controller lease for a shared daemon session. Every transfer records its reason in the event stream.")]
    async fn omp_take_control(&self,Parameters(p):Parameters<TakeControl>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_take_control",serde_json::to_value(p)?)).await
    }
    #[tool(description="Switch the session model by provider and model_id. Requires the controller lease in daemon mode.")]
    async fn omp_set_model(&self,Parameters(p):Parameters<SetModel>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_set_model",serde_json::to_value(p)?)).await
    }
    #[tool(description="Compact conversation context with optional instructions; invokes the model and may consume provider quota.")]
    async fn omp_compact(&self,Parameters(p):Parameters<Compact>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_compact",serde_json::to_value(p)?)).await
    }
    #[tool(description="Read paginated message history with a stable cursor. Available to observers as well as the controller.")]
    async fn omp_messages(&self,Parameters(p):Parameters<Messages>)->CallToolResult {
        let b=self.backend.clone();run(move||b.call("omp_messages",serde_json::to_value(p)?)).await
    }

}

#[tool_handler]
impl ServerHandler for Bridge {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        rmcp::model::ServerConfig::new(rmcp::model::ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new("omp-bridge",env!("CARGO_PKG_VERSION")))
            .with_instructions("Control Oh My Pi agents: start, prompt, steer, queue follow-ups, inspect replay/history, switch models, compact, stop. Daemon mode allows observers and explicit exclusive controller transfer. Timeouts never retry prompts.")
    }
}
