#!/usr/bin/env python3
"""Capture real bridge MCP responses against the offline JSONL fixture.

These are executed integration results, explicitly not live-provider evidence.
The complete tools/list schemas are captured from the same running binary.
"""
import datetime
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from smoke_mcp import MCP


def main():
    root = Path(__file__).resolve().parents[1]
    os.environ["OMP_BRIDGE_CMD"] = sys.executable
    os.environ["OMP_BRIDGE_ARGS"] = json.dumps([str(root / "tests/fixtures/mock_omp.py")])
    started = time.monotonic()
    client = MCP(root / "target/debug/omp-bridge", started + 30)
    session = None
    capture = {
        "source": "executed_mock_integration",
        "provider": "offline JSONL fixture; no model subscription or network",
        "timestampUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "records": [],
    }
    try:
        capture["initialize"] = client.request("initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "mock-evidence-capture", "version": "1"},
        })
        client.notify("notifications/initialized")
        capture["toolsListResponse"] = client.request("tools/list", {})
        schemas = {item["name"]: item["inputSchema"] for item in capture["toolsListResponse"]["tools"]}

        def recorded(name, arguments):
            begin = time.monotonic()
            raw = client.request("tools/call", {"name": name, "arguments": arguments})
            if raw.get("isError"):
                raise RuntimeError("unexpected tool error: " + name)
            value = json.loads(raw["content"][0]["text"])
            capture["records"].append({
                "tool": name, "source": "executed_mock_integration",
                "inputSchema": schemas[name], "request": arguments,
                "mcpResult": raw, "response": value,
                "elapsedMs": round((time.monotonic() - begin) * 1000, 3),
            })
            return value

        session = client.tool("omp_start", {
            "cwd": "/tmp", "no_tools": True, "model": "mock-provider/mock-model",
        })["session"]
        recorded("omp_list", {})
        client.tool("omp_send", {"session": session, "message": "offline capture task", "wait": False})
        until = time.monotonic() + 3
        while time.monotonic() < until:
            page = client.tool("omp_events", {"session": session, "cursor": 0, "kinds": ["agent_start"], "wait_ms": 20})
            if page["events"]:
                break
        else:
            raise RuntimeError("fixture did not start")
        client.tool("omp_follow_up", {"session": session, "text": "promote this queued input"})
        promoted = recorded("omp_promote_queued_message", {"session": session, "text": "promote this queued input"})
        assert promoted["promoted"] is True
        aborted = recorded("omp_abort", {"session": session})
        assert aborted["abortRequested"] is True
        stopped = client.tool("omp_stop", {"session": session})
        assert stopped["stopped"] is True
        capture["teardown"] = stopped
        session = None
        capture["passed"] = True
    finally:
        if session:
            client.tool("omp_stop", {"session": session})
        capture["bridgeExitCode"] = client.close()
    target = root / "evidence/v2/mock-tool-responses.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(capture, indent=2) + "\n")
    print(json.dumps({"passed": capture["passed"], "tools": [r["tool"] for r in capture["records"]],
                      "schemasCaptured": len(capture["toolsListResponse"]["tools"]), "bridgeExitCode": capture["bridgeExitCode"]}))


if __name__ == "__main__":
    main()
