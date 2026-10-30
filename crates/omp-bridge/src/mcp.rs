use crate::core::Manager;
use rmcp::{tool, tool_router, tool_handler, ServerHandler, handler::server::wrapper::Parameters, model::{CallToolResult, ContentBlock}};
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};
use serde_json::Value;

#[derive(Clone)]
pub struct Bridge { pub manager: Manager }

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
pub struct Session { pub session: String }
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SendArgs {
    pub session: String,
    pub message: String,
    /// Only prompt supported in MVP.
    pub mode: Option<String>,
    /// Wait for result up to timeout_ms; false returns an operationId immediately.
    pub wait: Option<bool>,
    /// At most 25000 ms. Timeout never aborts or retries the prompt.
    pub timeout_ms: Option<u64>,
}
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Events {
    pub session: String,
    /// Next event cursor to read, initially zero.
    pub cursor: Option<u64>,
    /// Filter this read only; empty means all kinds.
    pub kinds: Option<Vec<String>>,
    /// Long-poll at most 25000 ms.
    pub wait_ms: Option<u64>,
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
    #[tool(description="Start an isolated Oh My Pi RPC v2 session. Each MCP client owns separate sessions.")]
    async fn omp_start(&self, Parameters(p): Parameters<Start>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.start(serde_json::to_value(p)?)).await
    }
    #[tool(description="Send a prompt. Returns a bridge operationId; timeout means running_or_unknown, never implicit abort or retry.")]
    async fn omp_send(&self, Parameters(p): Parameters<SendArgs>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.send(serde_json::to_value(p)?)).await
    }
    #[tool(description="Read bounded session events with a cursor and explicit dropped-history indication. Inspect prompt_result.sessionSettled or session_settled to establish settlement.")]
    async fn omp_events(&self, Parameters(p): Parameters<Events>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.events(serde_json::to_value(p)?)).await
    }
    #[tool(description="Get authoritative agent state and bridge operation outcomes. Completion and session settlement are distinct.")]
    async fn omp_status(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.status(serde_json::to_value(p)?)).await
    }
    #[tool(description="Abort the current agent turn without stopping the process.")]
    async fn omp_abort(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.abort(serde_json::to_value(p)?)).await
    }
    #[tool(description="Stop the session. stopped=true only after confirmed process teardown.")]
    async fn omp_stop(&self, Parameters(p): Parameters<Session>) -> CallToolResult {
        let m=self.manager.clone(); run(move || m.stop(serde_json::to_value(p)?)).await
    }
    #[tool(description="List sessions owned by this bridge process, not sessions of other MCP clients.")]
    async fn omp_list(&self) -> CallToolResult {
        let m=self.manager.clone(); run(move || Ok(m.list())).await
    }
}

#[tool_handler]
impl ServerHandler for Bridge {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        rmcp::model::ServerConfig::new(rmcp::model::ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new("omp-bridge",env!("CARGO_PKG_VERSION")))
            .with_instructions("Own process-local Oh My Pi sessions. Start, send a prompt, inspect events/status for settlement, then stop. Timeout never aborts or retries a prompt.")
    }
}
