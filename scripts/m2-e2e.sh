#!/bin/sh
# M2 end-to-end check for the challenge /goal criterion, Claude Code side.
#
# Prereqs: a built omp-bridge binary, omp authenticated (opencode-go), and the
# oh-my-pi clone at ~/challange-andrei/oh-my-pi. Run from the repo root:
#
#   sh scripts/m2-e2e.sh /path/to/omp-bridge
#
# Steps it performs:
#   1. registers the bridge in Claude Code (user scope, name omp-bridge)
#   2. prints the env the bridge needs on M2
#   3. prints the prompt to paste into a fresh `claude` session; the agent must
#      omp_start -> omp_send a small task -> omp_events until settled -> omp_stop.
#
# The actual MCP calls are made by the Claude agent, not this script — the goal
# criterion is "from Claude Code, an agent drives omp through the bridge".
set -eu

BRIDGE="${1:?usage: sh scripts/m2-e2e.sh /path/to/omp-bridge}"
OMP_CLI="$HOME/challange-andrei/oh-my-pi/packages/coding-agent/src/cli.ts"
BUN="$(command -v bun)"

[ -x "$BRIDGE" ] || { echo "not executable: $BRIDGE" >&2; exit 1; }
[ -f "$OMP_CLI" ] || { echo "missing omp clone: $OMP_CLI" >&2; exit 1; }

claude mcp remove omp-bridge --scope user >/dev/null 2>&1 || true
claude mcp add omp-bridge --scope user \
  --env OMP_BRIDGE_CMD="$BUN" \
  --env OMP_BRIDGE_ARGS="[\"$OMP_CLI\"]" \
  -- "$BRIDGE" mcp

echo
echo "Registered. Verify with: claude mcp list"
echo
echo "Then, in a fresh claude session, paste:"
echo
cat <<'PROMPT'
Using ONLY the omp-bridge MCP tools: start an omp session with model
opencode-go/deepseek-v4.1-flash, send it the prompt "Reply with exactly
BRIDGE_E2E_OK and nothing else", read events until the session is settled,
show me the assistant text and the final status, then stop the session and
confirm the teardown result.
PROMPT
