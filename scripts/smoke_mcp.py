#!/usr/bin/env python3
"""One-prompt MCP/OpenCode Go smoke. Requires an authenticated local omp.

This tests MCP over the bridge's stdio transport; it is not evidence that a
particular interactive Codex or Claude session has loaded the MCP server.
No credentials or model transcript are printed or written to the evidence file.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time


class SmokeError(Exception):
    pass


class MCP:
    def __init__(self, binary, deadline):
        self.deadline = deadline
        self.sequence = 0
        self.frames = queue.Queue()
        self.process = subprocess.Popen(
            [str(binary), "mcp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, encoding="utf-8", bufsize=1,
            env=os.environ.copy(),
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        try:
            for line in self.process.stdout:
                try:
                    self.frames.put(json.loads(line))
                except (ValueError, TypeError):
                    self.frames.put(SmokeError("non_json_mcp_stdout"))
                    return
        except (OSError, UnicodeError):
            self.frames.put(SmokeError("mcp_reader_failed"))
        finally:
            self.frames.put(SmokeError("mcp_stdout_closed"))

    def notify(self, method, params=None):
        frame = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            frame["params"] = params
        try:
            self.process.stdin.write(json.dumps(frame) + "\n")
            self.process.stdin.flush()
        except (OSError, ValueError):
            raise SmokeError("mcp_write_failed") from None

    def request(self, method, params=None, deadline=None):
        self.sequence += 1
        request_id = self.sequence
        frame = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            frame["params"] = params
        try:
            self.process.stdin.write(json.dumps(frame) + "\n")
            self.process.stdin.flush()
        except (OSError, ValueError):
            raise SmokeError("mcp_write_failed") from None
        end = min(self.deadline, deadline or self.deadline)
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise SmokeError("mcp_request_timeout")
            try:
                response = self.frames.get(timeout=remaining)
            except queue.Empty:
                raise SmokeError("mcp_request_timeout") from None
            if isinstance(response, Exception):
                raise response
            if not isinstance(response, dict):
                raise SmokeError("invalid_mcp_frame")
            if response.get("id") != request_id:
                continue  # Notifications and responses to expired requests.
            if "error" in response:
                raise SmokeError("mcp_protocol_error")
            if "result" not in response:
                raise SmokeError("mcp_response_missing_result")
            return response["result"]

    def tool(self, name, arguments, expect_error=False, deadline=None):
        result = self.request("tools/call", {"name": name, "arguments": arguments}, deadline)
        if expect_error:
            if result.get("isError") is not True:
                raise SmokeError("missing_expected_tool_error")
            return {"expectedToolError": True}
        if result.get("isError"):
            # Never copy arbitrary provider errors into the public evidence.
            raise SmokeError("tool_failed_" + name)
        for content in result.get("content", []):
            if content.get("type") == "text":
                try:
                    value = json.loads(content["text"])
                except (ValueError, KeyError):
                    raise SmokeError("tool_response_not_json") from None
                if isinstance(value, dict):
                    return value
        raise SmokeError("tool_response_missing_json_object")

    def close(self):
        try:
            self.process.stdin.close()  # Bridge runs Manager.shutdown on EOF.
        except (OSError, ValueError):
            pass
        remaining = max(0.1, min(4.0, self.deadline - time.monotonic()))
        try:
            self.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=1)
        return self.process.returncode


def run(binary, output):
    started = time.monotonic()
    deadline = started + 90
    work_deadline = deadline - 12  # Reserve time to stop and reap the bridge.
    evidence = {
        "test": "mcp_stdio_one_prompt",
        "timestampUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "passed": False,
        "promptsSent": 0,
        "expectedToolError": False,
        "eventSettlementObserved": False,
        "stopped": False,
    }
    mcp = None
    session = None
    failure = None
    with tempfile.TemporaryDirectory(prefix="omp-bridge-smoke-") as cwd:
        try:
            mcp = MCP(binary, deadline)
            initialized = mcp.request("initialize", {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "omp-bridge-smoke", "version": "0.1.0"},
            }, work_deadline)
            evidence["mcpProtocolVersion"] = initialized.get("protocolVersion")
            mcp.notify("notifications/initialized")
            listed = mcp.request("tools/list", {}, work_deadline)
            names = {tool["name"] for tool in listed.get("tools", [])}
            expected = {"omp_start", "omp_send", "omp_events", "omp_status",
                        "omp_abort", "omp_stop", "omp_list"}
            if not expected.issubset(names):
                raise SmokeError("missing_mcp_tools")
            evidence["tools"] = sorted(expected)
            evidence.update(mcp.tool("omp_status", {"session": "smoke-nonexistent"},
                                     expect_error=True, deadline=work_deadline))
            launch = mcp.tool("omp_start", {"cwd": cwd, "no_tools": True,
                "model": "opencode-go/deepseek-v4.1-flash"}, deadline=work_deadline)
            session = launch.get("session")
            if not isinstance(session, str) or not session:
                raise SmokeError("start_missing_session")
            evidence["ompProtocolVersion"] = launch.get("protocolVersion")
            evidence["model"] = launch.get("model")
            if evidence["ompProtocolVersion"] != 2:
                raise SmokeError("omp_protocol_not_v2")
            evidence["promptsSent"] = 1
            outcome = mcp.tool("omp_send", {
                "session": session, "message": "Reply exactly OMP_BRIDGE_OK. Do not use tools.",
                "mode": "prompt", "wait": True, "timeout_ms": 25000,
            }, deadline=work_deadline)
            operation_id = outcome.get("operationId")
            if not operation_id:
                raise SmokeError("missing_operation_id")
            evidence["operationId"] = operation_id
            cursor = 0
            observed_kinds = set()
            exact_response = False
            completed = False
            settled = False

            def observe_outcome(value):
                nonlocal exact_response, completed, settled
                if value.get("operationId") != operation_id:
                    return
                exact_response |= value.get("assistant_text", "").strip() == "OMP_BRIDGE_OK" if isinstance(value.get("assistant_text"), str) else False
                result = value.get("result") or {}
                completed |= result.get("status") == "completed"
                settled |= result.get("sessionSettled") is True

            observe_outcome(outcome)
            while time.monotonic() < work_deadline:
                page = mcp.tool("omp_events", {"session": session, "cursor": cursor,
                    "wait_ms": 1000}, deadline=work_deadline)
                if page.get("gap") or page.get("dropped", 0):
                    raise SmokeError("smoke_event_history_dropped")
                cursor = page["nextCursor"]
                for entry in page.get("events", []):
                    event = entry.get("event", {})
                    kind = event.get("type", "unknown")
                    observed_kinds.add(kind)
                    if kind == "prompt_result":
                        evidence["eventSettlementObserved"] |= event.get("sessionSettled") is True
                    elif kind == "session_settled":
                        evidence["eventSettlementObserved"] = True
                    elif kind == "bridge_operation_result":
                        value = event.get("outcome", {})
                        observe_outcome(value)
                        if value.get("operationId") == operation_id:
                            evidence["eventSettlementObserved"] |= (value.get("result") or {}).get("sessionSettled") is True
                state = mcp.tool("omp_status", {"session": session}, deadline=work_deadline)
                for value in state.get("operations", []):
                    observe_outcome(value)
                if completed and settled and exact_response and evidence["eventSettlementObserved"]:
                    break
                if any(value.get("operationId") == operation_id and value.get("status") == "failed"
                       for value in state.get("operations", [])):
                    raise SmokeError("prompt_operation_failed")
            evidence.update({"eventKinds": sorted(observed_kinds), "completed": completed,
                             "sessionSettled": settled, "expectedResponseMatched": exact_response})
            if not (completed and settled and exact_response and evidence["eventSettlementObserved"]):
                raise SmokeError("prompt_evidence_incomplete")
        except SmokeError as error:
            failure = str(error)
        except (OSError, ValueError, KeyError, TypeError):
            failure = "local_smoke_error"
        finally:
            if mcp is not None:
                if session is not None:
                    try:
                        stopped = mcp.tool("omp_stop", {"session": session}, deadline=deadline - 3)
                        if not stopped.get("stopped") and stopped.get("retrySafe"):
                            stopped = mcp.tool("omp_stop", {"session": session}, deadline=deadline - 3)
                        evidence["stopped"] = stopped.get("stopped") is True
                    except (SmokeError, OSError, ValueError):
                        evidence["stopped"] = False
                evidence["bridgeExitCode"] = mcp.close()
    evidence["durationSeconds"] = round(time.monotonic() - started, 3)
    evidence["passed"] = failure is None and evidence["stopped"] and evidence.get("bridgeExitCode") == 0
    if not evidence["passed"]:
        evidence["error"] = failure or "shutdown_not_confirmed"
    serialized = json.dumps(evidence, indent=2, sort_keys=True)
    if output:
        Path(output).write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/debug/omp-bridge")
    parser.add_argument("--output", type=Path, help="Optional sanitized JSON evidence file")
    args = parser.parse_args()
    sys.exit(run(args.binary, args.output))
