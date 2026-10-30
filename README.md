# omp-bridge

A Rust CLI that bridges **Codex CLI** and **Claude Code** to agents running under the
**Oh My Pi (omp)** harness, over omp's native RPC mode.

From either client, an agent can start an omp session, send it tasks, stream its
events, abort a turn, and shut it down. Sessions run isolated by default
(`--no-session`, extensions/skills/rules/context-files/LSP/PTY/title generation off)
so the bridge drives a clean, reproducible agent.

> Status: built during a one-hour timed challenge (2026-10-30). Expect MVP scope.

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

## MCP tools

| Tool | What it does |
| --- | --- |
| `omp_start` | Spawn an omp RPC session (model passed in argv), negotiate protocol v2. `no_tools: true` gives an inference-only session. |
| `omp_send` | Send a prompt (MVP supports `mode: "prompt"` only). Returns a bridge `operationId`; with `wait` (default) it blocks up to `timeout_ms` (max 25 s). On timeout the status is `running_or_unknown` — the turn is **never** aborted or re-sent; follow up with `omp_events`/`omp_status`/`omp_abort`. `assistant_text` is capped at 64 KiB with an explicit truncation marker. |
| `omp_events` | Long-poll (`cursor`, `kinds`, `wait_ms`) the session's ring buffer — `{cursor, event}` envelopes, bounded at 2048 entries / 2 MiB, with explicit `dropped` gaps |
| `omp_status` | Session state: `running` / `settled` / `unknown`, model, pending work |
| `omp_abort` | Abort the current turn (session stays alive) |
| `omp_stop` | Tear the session down (confirmed teardown or an error — never a fabricated `stopped`) |
| `omp_list` | List this bridge process's sessions |

## Build

```bash
cargo build --release --locked   # workspace root; vendor/omp-rpc resolves by path
cargo test                       # 11 tests: bounded ring, busy guard, timeout-never-aborts
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

Both goal criteria ran live on 2026-10-30, each from its own machine and MCP client.

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
