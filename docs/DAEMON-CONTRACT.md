# omp-bridge daemon contract (v2)

Agreed interface between the daemon (`omp-bridge serve`) and every socket client
(`omp-bridge mcp --daemon`, `omp-bridge watch`, `omp-bridge attach`). Fixed first,
implemented in parallel. Owner of `serve`/lease/proxy: Codex side. Owner of
`watch`/`attach`: Claude side. Changes to this file require the other side's ack.

## Transport

- Unix domain socket, permissions **0600**, path from `--socket <path>` or
  `OMP_BRIDGE_SOCKET`, default `${TMPDIR:-/tmp}/omp-bridge-$UID.sock`.
- Newline-delimited JSON, UTF-8, one object per line, both directions
  (same style as omp RPC; no Content-Length framing).
- One connection = one client. The daemon never pushes unsolicited frames:
  clients poll `events` (long-poll server-side, like `omp_events`).

## Frames

Client → daemon:

```json
{"id":"c-1","op":"hello","args":{"client":"claude-m2"}}
{"id":"c-2","op":"start","args":{"model":"...","no_tools":false}}
{"id":"c-3","op":"send","args":{"session":"session-1","message":"...","wait":true,"timeout_ms":25000}}
```

Daemon → client (always a reply to an `id`):

```json
{"id":"c-2","ok":true,"data":{"session":"session-1","controller":"client-1",...}}
{"id":"c-3","ok":false,"error":"not_controller","controller":"client-2"}
```

- `hello` is mandatory first; reply carries `{clientId}`. Any other first frame → `error:"hello_required"`.
- `op` values map 1:1 onto the Manager verbs: `start`, `send`, `steer`, `follow_up`,
  `events`, `status`, `abort`, `stop`, `list`, `take_control`, `set_model`,
  `compact`, `messages`. Args identical to the MCP tool inputs.
- Unknown op → `error:"unknown_op"`. Errors are short codes, never raw provider text.

## Lease (controller)

- Per session the daemon tracks `controller: Option<clientId>`.
- `start` makes the caller controller of the new session.
- **Write ops require the lease:** `send`, `steer`, `follow_up`, `abort`, `stop`,
  `set_model`, `compact`. Non-controller → `error:"not_controller"` + current controller.
- **Read ops are free:** `events`, `status`, `messages`, `list`.
- `take_control {session, reason}` transfers the lease unconditionally to the caller
  and pushes a ring event every reader sees:

```json
{"type":"bridge_control_transfer","session":"session-1","from":"client-1","to":"client-2","reason":"operator handoff"}
```

- A client disconnecting releases its lease (controller → null, plus a
  `bridge_control_transfer` with `to: null, reason: "disconnect"`). Sessions keep running.

## MCP proxy mode

`omp-bridge mcp --daemon [--socket p]` serves the same MCP tools but forwards every
call over the socket instead of owning sessions. New MCP tools in this mode (and
in-process where they make sense):

- `omp_steer {session, message}` → RPC `steer`
- `omp_follow_up {session, message}` → RPC `follow_up`
- `omp_take_control {session, reason}` (daemon mode only; in-process returns `error:"no_daemon"`)
- `omp_set_model {session, model}`; `omp_compact {session}`; `omp_messages {session, cursor?}`

Default stays Q1: without `--daemon`, sessions are process-local, exactly as v1.

## watch / attach (human view)

- `omp-bridge watch <session> [--socket p] [--json]`: hello → `events` long-poll loop
  from cursor 0, colored one-line-per-event rendering on stdout (`--json` = raw frames).
- `omp-bridge attach <session>`: watch + stdin lines; plain line = `steer`,
  `>text` = `follow_up`, `!take` = `take_control`. Attach does NOT auto-take the
  lease; it takes it on first write command (or `!take`).
