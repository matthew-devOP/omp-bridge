# omp-bridge

A Rust CLI that bridges **Codex CLI** and **Claude Code** to agents running under the
**Oh My Pi (omp)** harness, over omp's native RPC mode.

From either client, an agent can start an omp session, send it tasks, stream its
events, abort a turn, and shut it down — with all of omp's harness controls intact.

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
                  └─ one omp session per bridge process (sessions are
                     process-local: each MCP client controls its own agents)
```

- `crates/omp-bridge` — the binary: `omp-bridge mcp` (MCP stdio server) and
  `omp-bridge repl` (interactive demo).
- `vendor/omp-rpc` — the Rust client for omp's RPC protocol, vendored from
  [oh-my-pi](https://github.com/can1357/oh-my-pi) (`sdk/rust/omp-rpc`, MIT);
  see `vendor/omp-rpc/PROVENANCE.md` for the exact commit.
- `site/` — the Astro presentation page.

## MCP tools

| Tool | What it does |
| --- | --- |
| `omp_start` | Spawn an omp RPC session (model passed in argv), negotiate protocol v2 |
| `omp_send` | Send a task. Synchronous by default for `prompt` (waits on `prompt_result`); `steer`/`follow_up` return `admitted`/`queued`. A timeout does **not** abort the turn. |
| `omp_events` | Long-poll the session's event ring buffer (cursor + explicit `dropped` gaps) |
| `omp_status` | Session state: `running` / `settled` / `unknown`, model, pending work |
| `omp_abort` | Abort the current turn (session stays alive) |
| `omp_stop` | Tear the session down (confirmed teardown or an error — never a fabricated `stopped`) |
| `omp_list` | List this bridge process's sessions |

## Configuration

| Env var | Meaning |
| --- | --- |
| `OMP_BRIDGE_CMD` | Path to the omp executable (default: `omp` on PATH). A single executable path — never interpreted by a shell. |
| `OMP_BRIDGE_ARGS` | Optional JSON array of prefix arguments (e.g. `["/path/to/cli.ts"]` when `OMP_BRIDGE_CMD` is `bun`) |

## Registering the bridge

**Claude Code:**

```bash
claude mcp add omp-bridge -- /path/to/omp-bridge mcp
```

**Codex CLI** (`~/.codex/config.toml`):

```toml
[mcp_servers.omp_bridge]
command = "/path/to/omp-bridge"
args = ["mcp"]
```

## Demo

_(added at the end of the challenge: transcript / GIF)_

## License

MIT — see [LICENSE](LICENSE). Vendored `omp-rpc` is MIT, © Stencil Labs / the
oh-my-pi contributors; see `vendor/omp-rpc/PROVENANCE.md`.
