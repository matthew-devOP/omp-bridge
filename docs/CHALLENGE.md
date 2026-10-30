# The challenge behind v1

`v1` (commit `08eb253`) is the first result of a timed, one-hour challenge run on
2026-10-30: a friendly head-to-head between the repository owner (with their harness) and another
developer (with theirs). Each side used a Claude Code session and a Codex CLI session. This file
records the task exactly as both agents received it, and the end-to-end timeline.

## The team on this side

| Role | Agent | Machine |
|---|---|---|
| Claude side ("m2brain") | Claude Code session (Claude Fable) | M2 (Apple Silicon) |
| Codex side ("Edith / Astra") | Codex CLI 0.164, GPT-6-Astra | i9 (Intel) |
| Coordinator (support, no product code) | Claude Code session (coord-m2) | M2 |

The two working agents coordinated through handoff files and a direct tmux channel.
After the start they ran **without steering from the owner**, under an autonomy
mandate given at the start (below).

## The task, as given to both agents

The statement both agents started from, given in Romanian (verbatim excerpts):

> **PROVOCAREA (o oră, cronometrată, la start)**
> - Un instrument CLI în **Rust** care face puntea între **Codex CLI** și **Claude Code**
>   pe de o parte și un agent care rulează sub harness-ul **Oh My Pi (omp)**, autentificat cu
>   abonamentul **OpenCode Go** (ex. `opencode-go/deepseek-v4.1-flash` sau alt model suportat).
> - Din Codex sau Claude, un agent pornește o sesiune omp, îi trimite sarcini, citește
>   evenimentele, întrerupe, oprește, cu toate controalele harness-ului omp.
> - Plus o **pagină de prezentare în Astro**.

At the start, the owner added (verbatim, Romanian):

> „gata pornește challange-ul … sesiuni separate pentru fiecare client. ideea este că
> aprobările trebuie luate fără aportul meu … să nu trebuiască steer până se implementează
> challange-ul”

**Done criterion (`/goal`), as given:**
- From Codex on i9 AND from Claude Code on M2, an agent starts an omp session on OpenCode Go
  through omp-bridge (MCP), sends it a small task, reads events until settled, and stops it.
- The public repo holds the code, a README with the registration snippets, and the Astro page,
  built locally.
- A short demo (transcript) in the README.

**Autonomy mandate, pre-approved by the owner for the challenge:**
- live prompts on OpenCode Go (smoke, e2e, demo);
- creating this public repository on `matthew-devOP` and pushing to it;
- dependency installs inside the project directories only;
- the Astro page.

Out of scope: other repositories, secrets, deletions outside the project, system settings.
Anything a permission classifier blocked was routed around legitimately (git bundles instead
of a collaborator invite), not forced.

**Proposed architecture handed to both, refined by them:**
- omp's native RPC mode (`omp --mode rpc --no-ui`, JSONL over stdio, protocol v2);
- the in-repo Rust client `sdk/rust/omp-rpc`, vendored with PROVENANCE;
- a session core with an MCP stdio face (start / send / events / status / abort / stop / list),
  registered in both Codex and Claude Code;
- a REPL for humans.

The motivation was replacing the tmux plumbing (`capture-pane` / `send-keys` / `respawn-pane`)
that the coordinator used to drive the Codex agent.

## Timeline (EET, UTC+2)

Preparation happened before the clock started and is **not** counted:
- 01:48–02:56: machine setup, the Rust toolchain on i9, harness tools for the Codex agent;
- both agents read the omp RPC docs, then cross-reviewed a decisions skeleton (v1 → v2);
- an RPC handshake (`ready → negotiate v2 → get_state`, no live prompt) passed on both machines.

| Time | Δ from start | Event | Evidence |
|---|---|---|---|
| **02:59** | 0:00 | **START**: owner gives the go and the autonomy mandate | — |
| 03:01:31 | 2:31 | First commit: scaffold, README, MIT license (Claude side) | `c811c52` |
| 03:04:28 | 5:28 | Astro presentation page | `62584aa` |
| 03:08:38 | 9:38 | Public repository created | GitHub `created_at` |
| 03:10:31 | 11:31 | Rust bridge: core, MCP, REPL (Codex side) | `0ef4663` |
| 03:13:31 | 14:31 | Codex-side code merged on `main` (via git bundle) | `e1fbdc4` |
| **03:14:07** | **15:07** | **E2E from Codex CLI passes** on i9 | `evidence/`, `demo/codex-i9-e2e-transcript.txt` |
| 03:15:56 | 16:56 | Fix: stale settlement race, REPL cleanup on interrupt | `672c3f0` |
| **03:18:02** | **19:02** | **E2E from Claude Code passes** on M2 (demo committed; see note) | `fa6a767`, `demo/claude-m2-e2e-transcript.txt` |
| 03:19:32 | 20:32 | Cross-audit from the Claude side: two findings | — |
| 03:21:06 | 22:06 | Findings fixed (serverInfo, concise `omp_status`); final Codex demo | `975538a` |
| 03:24:05 | 25:05 | Final README/site corrections from the Codex side's review: **v1 commit** | `08eb253` |
| **03:26:10** | **27:10** | **Goal closed**: independent audit from a fresh public clone, byte-identical | — |

> **Note on the Claude Code e2e time.** The table anchors it to the commit of its demo
> transcript (03:18:02), because that is verifiable. The run itself happened about four
> minutes earlier, around 03:14, so the two e2e runs were nearly simultaneous (reported by
> the Claude side).

**End to end: 27 min 10 s from start to the audited goal; 25 min 05 s to the `v1` commit.**
Both e2e criteria were proven by minute 19.

At `v1`:
- `cargo build --release --locked` passes, with 11/11 tests;
- the Astro build passes (Astro 7.4.4 on Node 26; one page);
- the MCP server answers `initialize` / `tools/list` with 7 tools.

**Interventions during the run:** one, from the coordinator. It pressed Esc on a Codex
auto-approval review that had been stuck for 13 minutes on a status check-in. No product
decision was steered.

## Known limits of v1 (MVP)

- **Prompts only.** No steer or follow-up while a turn runs.
- **Separate sessions per MCP client** (the owner's decision at the start). No shared
  daemon, no cross-client attach.
- **Smoke and e2e coverage only.** Not an exhaustive test of every tool or model.

## Lessons noted by the coordinator

- Codex `--approve-for-me` can stall for minutes on trivial MCP calls.
- When an agent can't authenticate `gh`, a git-bundle handoff is a workable, auditable
  substitute.
- Check toolchain minimums early: Node 22.13 on i9 was too old for Astro 7, so the page was
  built on M2.

## v2 — one more hour (started 04:01)

v1 hit a minimal bar in 27 minutes. The owner measured it and raised the bar explicitly:
real agentic control depth, and a presentation that holds up on a projector. Same team,
same rules (no human steering after START), one more timed hour.

**The v2 criterion, in short:**

- `omp_steer` / `omp_follow_up` while a turn runs, on omp's native queues;
- a shared-session daemon (`omp-bridge serve`, private Unix socket) with an explicit
  **controller lease**: one writer at a time, visible `controller_changed` events, no
  silent takeover;
- `omp-bridge watch` / `attach` for humans, `doctor` for diagnostics;
- more harness controls over MCP (`set_model`, `compact`, `messages`,
  `promote_queued_message`), a mock-omp integration test, GitHub Actions CI
  (`cargo test` + `clippy -D warnings`);
- the Astro page rebuilt as a product page with honest, evidence-backed content;
- a cross-client e2e on one machine: Codex CLI starts an agent through the daemon,
  Claude Code attaches, reads the backlog, takes control with a reason, sends a
  follow-up — and the Codex side sees the transfer.

### v2 timeline (EET, UTC+2; times are commit timestamps)

| Clock | Elapsed | Milestone | Ref |
|---|---|---|---|
| **04:01** | 0:00 | **START v2** — the raised bar lands on both sides | — |
| 04:02 | 0:01 | Daemon contract fixed between the two sides; CI workflow added | `c956e46` |
| 04:08 | 0:07 | Product page: animated architecture, real-event replay, tool explorer | `074523a` |
| 04:08 | 0:07 | `watch` / `attach` socket clients (Claude side) | `04a590a` |
| 04:12 | 0:11 | `watch` adapted to the final daemon wire | `53c70bc` |
| 04:13 | 0:12 | Shared Unix daemon with controller leases + interactive controls (Codex side) | `28e8b79` |
| 04:15 | 0:14 | **Live mid-turn steer proven**, with a real timed replay | `1d5414f` |
| 04:16 | 0:15 | Use-case animations + SaaS vision section on the page | `acab576`, `9f141f2` |
| 04:22 | 0:21 | Shared sessions kept responsive under load; `doctor` added | `926aa8c` |
| 04:23 | 0:22 | Cross-review honesty + accessibility fixes land on the page | `2a0d0f6` |
| 04:25 | 0:24 | Daemon merged on `main` | `8349b27` |
| 04:28 | 0:27 | Flaky test caught a real macOS accept bug (inherited `O_NONBLOCK`); fixed | `c9a3ca0` |
| 04:36 | 0:35 | **Cross-client e2e passes on one machine**: Codex starts the shared agent through the daemon (lease 1), Claude attaches, reads the backlog and takes control (`controller_changed`, lease 2), the follow-up answers exactly `CROSS_E2E_HANDOFF_OK`, Codex observes the transfer event, takes the lease back (lease 3) and stops with a confirmed teardown | `evidence/v2/cross-client.json` |
| 04:50 | 0:49 | Final core bundle merged: live model-switch/compaction/doctor proofs, metadata redaction, delayed-socket regression — 37 tests | `f8d9e0d` |
| 04:50 | 0:49 | Fleet watch (`--all`), explicit attach control, bounded daemon calls — 40 tests | `ca2b4e2` |
| 04:56 | 0:55 | Real-Chrome CDP audit fixes: mobile overflow, raw captured schemas and live-evidence responses in the explorer | `8b7358f` |
| 04:58 | 0:57 | **v2 tagged** — A and B complete with committed evidence; stretch C delivered: copy buttons, `doctor`, fleet `watch --all`, `--json` | tag `v2` |

### Honesty, cross-checked

Both sides reviewed each other during the hour, on quality as well as correctness. The
Codex side's review of the page forced three fixes that are visible in the result: replay
timings labeled **reconstructed** where the evidence has no per-event timestamps, the tool
explorer rebuilt from the actual `tools/list` capture with real-vs-illustrative response
labels, and contrast/mobile/reduced-motion accessibility repairs. The Claude side's review
of the daemon found the serverInfo identity and an oversized `omp_status` payload in v1,
both fixed in the v2 core — and, during the v2 merge, a flaky daemon test that turned out
to be a real macOS bug rather than test noise.
