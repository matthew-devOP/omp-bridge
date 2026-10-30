# omp-bridge daemon contract (v2 — final)

Agreed interface between the daemon (`omp-bridge serve`) and every socket client
(`omp-bridge mcp --daemon`, `omp-bridge watch`, `omp-bridge attach`). The daemon
side owns this contract; the terminal clients implement it as written.

## Transport

- Unix domain socket. Default path: `$XDG_RUNTIME_DIR/omp-bridge.sock`, falling
  back to `$HOME/.local/state/omp-bridge/bridge.sock`. Override with `--socket`
  or `OMP_BRIDGE_SOCKET`. Parent dir created private (0700) when the bridge
  creates it; the socket itself is 0600. Symlinked or pre-existing live sockets
  are rejected; the daemon removes only its own socket on shutdown.
- Newline-delimited JSON, UTF-8, max 1 MiB per frame.
- **One request per connection:** connect → one request line → one reply line →
  close. An operation may block its connection up to 25 s (long-poll `omp_events`,
  `wait=true` sends); other clients stay concurrent. EOF kills the request only —
  never the daemon or its sessions.

## Frames

```json
{"id":"r-1","client_id":"c-7f3a","method":"omp_status","params":{"session":"session-1"}}
{"id":"r-1","ok":true,"result":{...}}
{"id":"r-1","ok":false,"error":{"code":"not_controller","message":"client c-91 holds the lease"}}
```

- `hello` is the only method accepted without `client_id`; `params.name` is a
  display label (`codex`, `claude`, `watch` — never a person). The reply issues a
  daemon-generated `client_id` the process reuses across connections. No
  filesystem credential.
- `ping` returns daemon version and protocol (1). `omp_list` includes a
  controller summary per session.
- Error codes are short machine strings; messages carry no provider output.

## Lease (controller)

- `omp_start` leases the new session to the caller. One controller per session.
- **Mutations require the lease:** `omp_send`, `omp_steer`, `omp_follow_up`,
  `omp_promote_queued_message`, `omp_set_model`, `omp_compact`, `omp_abort`,
  `omp_stop`. Others read freely: `omp_events`, `omp_status`, `omp_messages`, `omp_list`.
- `omp_take_control {session, reason}` — reason mandatory and non-empty —
  transfers the lease explicitly (even mid-prompt) and pushes a ring event:

```json
{"type":"controller_changed","previousClient":"c-91","newClient":"c-7f3a","reason":"operator handoff","leaseEpoch":2}
```

- No silent takeover. The lease survives socket disconnects and client crashes
  (sessions keep running); it ends only on transfer or session stop.
- Admission + lease check are atomic; the lease lock is never held across a
  prompt wait (admit under lock, wait outside).
- The socket permissions are the trust boundary; client ids prevent accidental
  mixing, not hostile same-UID isolation.

## MCP tools over the daemon

`omp-bridge mcp --daemon` proxies the same tools over this wire; without
`--daemon` sessions stay process-local exactly as v1. New tools:
`omp_steer {session, text}`, `omp_follow_up {session, text}`,
`omp_promote_queued_message {session, text}`, `omp_take_control {session, reason}`,
`omp_set_model {session, provider, model_id}`, `omp_compact {session, instructions?}`,
`omp_messages {session, offset?, limit?}`. `omp_send` gains
`streaming_behavior: "steer" | "followUp"` (queues into a live run exactly once —
no fallback resend); `wait: true` is honored only for a normal idle prompt.

## watch / attach (human view)

- `omp-bridge watch <session> [--socket p] [--json]`: `hello` once, then an
  `omp_events` long-poll loop from cursor 0; colored one-line-per-event rendering,
  explicit gap markers on `dropped`; `--json` emits raw frames, one per line.
- `omp-bridge attach <session>`: watch plus stdin — plain line → `omp_steer`,
  `>text` → `omp_follow_up`, `!take` → `omp_take_control`. Attach takes the lease
  explicitly (on `!take`, or automatically right before its first write, with
  reason `operator attach`) — never silently.
- Rust seam once the daemon lands: `daemon::DaemonClient::connect(socket, name)`,
  `client.call(method, params)`, `daemon::default_socket()`.
