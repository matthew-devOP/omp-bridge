# omp-bridge

A Rust CLI that bridges **Codex CLI** and **Claude Code** to agents running under the
**Oh My Pi (omp)** harness, over omp's native RPC mode.

From either client, an agent can start an omp session, send it tasks, stream its
events, abort a turn, and shut it down. Sessions run isolated by default
(`--no-session`, extensions/skills/rules/context-files/LSP/PTY/title generation off)
so the bridge drives a clean, reproducible agent.

> Status: built during a one-hour timed challenge (2026-10-30). Expect MVP scope.
> The task, the team and the end-to-end timeline are in [docs/CHALLENGE.md](docs/CHALLENGE.md) (tag `v1`).

## Why

Before this bridge, driving a Codex agent on another machine meant tmux plumbing:
`capture-pane` to read, `send-keys` to type, `respawn-pane` to recover. It works,
but it is fragile — no structured results, no event stream, no clean abort.

omp ships an RPC mode (`omp --mode rpc --no-ui`): newline-delimited JSON over
stdio (a custom JSONL protocol, not JSON-RPC 2.0), with protocol-v2 framing
(1 MiB frames, `rpc_chunk` reassembly), distinct `prompt_result` /
`session_settled` signals, and full session control. `omp-bridge` wraps that in
an MCP stdio server so any MCP client gets structured control instead of tmux
scraping.

## Architecture

```
Codex CLI ─┐                       ┌─ omp --mode rpc --no-ui ── OpenCode Go model
           ├─ MCP (stdio) ── omp-bridge ── omp RPC (JSONL/stdio)
Claude Code┘      │
                  └─ up to 4 concurrent omp sessions per bridge process
                     (process-local: each MCP client controls its own agents)
```

- `crates/omp-bridge` — the binary: `omp-bridge mcp` (MCP stdio server) and
  `omp-bridge repl` (interactive demo).
- `vendor/omp-rpc` — the Rust client for omp's RPC protocol, vendored from
  [Annonymous-MTW/oh-my-pi](https://github.com/Annonymous-MTW/oh-my-pi)
  (`sdk/rust/omp-rpc`, MIT) at commit `6f3fbc94c567c0ac619c013dd15d718356eecba1`;
  see `vendor/omp-rpc/PROVENANCE.md`.
- `site/` — the Astro presentation page.

## MCP tools (14 — actual schemas in `evidence/v2/mcp-tool-schemas.json`)

| Tool | What it does |
| --- | --- |
| `omp_start` | Spawn an omp RPC session (model passed in argv), negotiate protocol v2. `no_tools: true` gives an inference-only session. |
| `omp_send` | Send a prompt. Returns a bridge `operationId`; with `wait` (default) it blocks up to `timeout_ms` (max 25 s). On timeout the status is `running_or_unknown` — the turn is **never** aborted or re-sent. `assistant_text` capped at 64 KiB with an explicit marker. Accepts `streaming_behavior: "steer" \| "followUp"` to queue into a live run exactly once. |
| `omp_steer` | Redirect the agent **mid-turn** on omp's native steer queue — the running task changes direction without losing the turn |
| `omp_follow_up` | Queue a message for after the current turn yields |
| `omp_promote_queued_message` | Promote a queued message ahead of the queue |
| `omp_take_control` | Daemon mode: take the write lease for a shared session, with a mandatory reason — every reader sees a `controller_changed` event |
| `omp_events` | Long-poll (`cursor`, `kinds`, `wait_ms`) the session's ring buffer — `{cursor, event}` envelopes, bounded at 2048 entries / 2 MiB, with explicit `dropped` gaps |
| `omp_status` | Session state: `running` / `settled` / `unknown`, model, pending work — whitelisted payload, never a fabricated answer |
| `omp_messages` | The session transcript, paginated (`offset`, `limit`) |
| `omp_set_model` | Switch the live session to another catalog model (`provider`, `model_id`) |
| `omp_compact` | Compact the session context (invokes the model; async `operationId`) |
| `omp_abort` | Abort the current turn (session stays alive) |
| `omp_stop` | Tear the session down (confirmed teardown or an error — never a fabricated `stopped`) |
| `omp_list` | Sessions of this bridge process — or of the shared daemon, with per-session controller info |

## Shared sessions: the daemon and the control lease (v2)

```bash
omp-bridge serve --socket /path/dir/bridge.sock   # daemon on a private 0600 Unix socket
omp-bridge mcp --daemon --socket …                # MCP proxy: many clients, shared sessions
omp-bridge watch session-1 [--json]               # live colored event stream (read-only)
omp-bridge watch --all                            # fleet view: every session, round-robin
omp-bridge attach session-1                       # watch + input: line=steer, >line=follow-up, !take=take control
omp-bridge doctor [--live]                        # check omp, auth, protocol v2, daemon socket
```

One client holds the **write lease** per session; everyone else reads events freely.
`omp_take_control` transfers it explicitly — with a reason — and emits a
`controller_changed` event (`previousClient`, `newClient`, `reason`, `leaseEpoch`)
that every observer sees. No silent takeovers; the lease survives disconnects.
Without `--daemon` the bridge keeps v1 behavior: process-local sessions.
Full wire contract: [`docs/DAEMON-CONTRACT.md`](docs/DAEMON-CONTRACT.md).

## Build

```bash
cargo build --release --locked   # workspace root; vendor/omp-rpc resolves by path
cargo test                       # 40 tests: ring bounds, lease atomicity, timeout-never-aborts,
                                 # fleet watch round-robin, mock-omp integration (CI runs these + clippy -D warnings)
```

The binary lands at `target/release/omp-bridge`. Try it without an MCP client:

```bash
OMP_BRIDGE_CMD=... ./target/release/omp-bridge repl --model opencode-go/deepseek-v4.1-flash
```

## Configuration

| Env var | Meaning |
| --- | --- |
| `OMP_BRIDGE_CMD` | Path to the omp executable (default: `omp` on PATH). A single executable path — never interpreted by a shell. |
| `OMP_BRIDGE_ARGS` | Optional JSON array of prefix arguments (e.g. `["/path/to/cli.ts"]` when `OMP_BRIDGE_CMD` is `bun`) |
| `OMP_BRIDGE_MODEL` | Default model when `omp_start` gets none (fallback: `opencode-go/deepseek-v4.1-flash`) |

## Registering the bridge

Both clients need `OMP_BRIDGE_CMD` (or `omp` on PATH) so the bridge can find omp.

**Claude Code:**

```bash
claude mcp add omp-bridge \
  --env OMP_BRIDGE_CMD=/path/to/omp \
  -- /path/to/omp-bridge mcp
```

**Codex CLI** (`~/.codex/config.toml`):

```toml
[mcp_servers.omp_bridge]
command = "/path/to/omp-bridge"
args = ["mcp"]
env = { OMP_BRIDGE_CMD = "/path/to/omp" }
```

## Demo

**v2 — mid-turn steer, real timed** ([`demo/v2/steer-live.json`](demo/v2/steer-live.json)):
a live agent is steered mid-turn (`admitted, queue: "steering"`), visibly changes
direction, then answers a queued follow-up; first `prompt_result` reports
`sessionSettled: false` (yield ≠ settled), the authoritative settle comes from `omp_status`.

**v2 — cross-client control handoff** ([`evidence/v2/cross-client.json`](evidence/v2/cross-client.json),
transcript in [`demo/v2/cross-client-transcript.md`](demo/v2/cross-client-transcript.md)):
Codex CLI starts a shared agent through the daemon (lease 1), Claude Code attaches,
reads the backlog and takes control (`controller_changed`, lease 2), its follow-up is
answered exactly `CROSS_E2E_HANDOFF_OK`, then Codex **observes the transfer event**,
takes the lease back (lease 3) and stops the session with a confirmed teardown.

The v1 runs, both live on 2026-10-30, each from its own machine and MCP client:

**Codex CLI (i9)** — transcript in
[`demo/codex-i9-e2e-transcript.txt`](demo/codex-i9-e2e-transcript.txt), machine-readable
proof in [`evidence/codex-i9-final.json`](evidence/codex-i9-final.json): exact
`OMP_BRIDGE_FINAL_OK` reply, `prompt_result.sessionSettled` + `session_settled`
observed, teardown confirmed with `stopped: true`.

**Claude Code (M2)** — full transcript in
[`demo/claude-m2-e2e-transcript.txt`](demo/claude-m2-e2e-transcript.txt):

1. `omp_start` (model `opencode-go/deepseek-v4.1-flash`) → `session-1`, protocol v2
2. `omp_send` (prompt, wait) → status `finished`, `sessionSettled: true`, assistant text exactly `BRIDGE_E2E_OK`
3. `omp_events` (cursor 0) → 9 events, no drops: `ready` → `agent_start` → `message_end` → `agent_end` → `prompt_result` → `session_settled`
4. `omp_status` → idle, settled, no pending operations
5. `omp_stop` → `{"stopped": true}` — confirmed process teardown

## License

MIT — see [LICENSE](LICENSE). Vendored `omp-rpc` is MIT, © Stencil Labs / the
oh-my-pi contributors; see `vendor/omp-rpc/PROVENANCE.md`.
