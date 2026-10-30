# Cross-client control handoff — real transcript (M2, 2026-10-30)

Daemon: `omp-bridge serve` on a private 0600 Unix socket. Two real MCP clients on the same machine: Codex CLI 0.162.1 (`[mcp_servers.omp_bridge]`, daemon mode) and Claude Code (`claude mcp add omp-daemon … mcp --daemon`). Shared session `session-3`, model `opencode-go/deepseek-v4.1-flash`. Phases below are the verbatim client reports (paths-free); wall-clock phase boundaries recorded in evidence/v2/cross-client.json.

## Phase A — Codex starts the shared agent (lease epoch 1)
```
controller clientId: fc6596d7bedc85af4eef2fc99f79d7f62c4a7edb1ded56a2
lease epoch: 1
tokens used
18,983
session id: session-3
operationId: operation-4
controller clientId: fc6596d7bedc85af4eef2fc99f79d7f62c4a7edb1ded56a2
lease epoch: 1
```

## Phase B — Claude attaches, reads the backlog, takes control (epoch 2), follow-up
```
All five steps completed on session-3 (nothing was stopped, no new session started):

**1) Initial omp_events (cursor 0, wait 2000ms):** 10 events (cursors 0–9), `nextCursor: 10`, `dropped: 0`. The session had already finished counting 1–40 and emitted `CROSS_E2E_BASE_DONE`.

**2) omp_take_control:** succeeded — `changed: true`, new controller `clientName: "mcp"`, `clientId: e56dbbca1174f569dc2b3bd0e0b7eac8b5f2f868ed25df17`, `leaseEpoch: 2`.

**3) omp_follow_up:** admitted to the `followUp` queue.

**4) Poll from cursor 10:** one poll (wait 20000ms) was enough — events 10–17 arrived, `nextCursor: 18`.

The `controller_changed` event, raw:
```json
{"leaseEpoch":2,"newClient":"e56dbbca1174f569dc2b3bd0e0b7eac8b5f2f868ed25df17","previousClient":"fc6596d7bedc85af4eef2fc99f79d7f62c4a7edb1ded56a2","reason":"operator handoff: Claude taking over mid-count","timestampMs":1793327711272,"type":"controller_changed"}
```

The assistant's reply text (exactly as requested):
```
CROSS_E2E_HANDOFF_OK
```
(from `message_end` at cursor 15, model `deepseek-v4.1-flash`, followed by `agent_end` and `session_settled` at cursor 17.)

**5) omp_status:** `sessionSettled: true` (also `status: "settled"`, `busy: false`, controller still the mcp client at leaseEpoch 2). I did not call omp_stop, per your instruction — the session is left running for the other client to close.
```

## Phase C — Codex observes the transfer, takes the lease back (epoch 3), stops
```
{"session":"session-3","stopped":true}
```
```json
{"leaseEpoch":2,"newClient":"e56dbbca1174f569dc2b3bd0e0b7eac8b5f2f868ed25df17","previousClient":"fc6596d7bedc85af4eef2fc99f79d7f62c4a7edb1ded56a2","reason":"operator handoff: Claude taking over mid-count","timestampMs":1793327711272,"type":"controller_changed"}
```

New lease epoch: `3`

Stop result:
```json
{"session":"session-3","stopped":true}
```
```
