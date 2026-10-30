#!/usr/bin/env python3
"""Deterministic process-level omp RPC fixture. No network or authentication.

The fixture models admission, an active turn, steering/follow-up queues and
exactly-once promotion. It intentionally delays turns so integration tests can
exercise concurrent control while stdout remains a real JSONL event stream.
"""
import json
import os
import sys
import threading
import time

output_lock = threading.Lock()
state_lock = threading.RLock()
messages = []
steering = []
follow_up = []
active = False
aborted = False
pending_ids = []
model_id = "mock-model"
provider = "mock-provider"
filter_kinds = None


def emit(value):
    with output_lock:
        sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
        sys.stdout.flush()


def event(kind, **fields):
    if filter_kinds is None or kind in filter_kinds or kind in ("prompt_result", "session_settled", "ready"):
        emit(dict(type=kind, **fields))


def respond(request, data=None):
    emit({"type": "response", "id": request.get("id"), "command": request["type"],
          "success": True, "data": {} if data is None else data})


def reject(request, reason):
    emit({"type": "response", "id": request.get("id"), "command": request["type"],
          "success": False, "error": reason, "code": "invalid_argument"})


def model():
    return {"id": model_id, "name": model_id, "api": "mock", "provider": provider,
            "baseUrl": "http://invalid.test", "reasoning": False, "input": ["text"],
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
            "contextWindow": 8192, "maxTokens": 2048}


def message(role, text):
    result = {"role": role, "timestamp": int(time.time() * 1000)}
    if role == "user":
        result["content"] = text
    else:
        result.update(content=[{"type": "text", "text": text}], api="mock",
                      provider=provider, model=model_id, usage={}, stopReason="stop")
    return result


def queue_event():
    event("queue_update", steering=[item[0] for item in steering],
          followUp=[item[0] for item in follow_up], liveSteered=0)


def run_turn(initial, request_id):
    global active, aborted
    event("agent_start")
    with state_lock:
        first = message("user", initial)
        messages.append(first)
        event("message_end", message=first)
    # Deliberately enough time to admit separate client requests, without relying
    # on the test racing a sub-millisecond mock response.
    time.sleep(0.8)
    with state_lock:
        inputs = [initial]
        ids = [request_id] if request_id else []
        while steering:
            text, queued_id = steering.pop(0)
            inputs.append(text)
            if queued_id:
                ids.append(queued_id)
            user = message("user", text)
            messages.append(user)
            event("message_end", message=user)
        answer = message("assistant", "ABORTED" if aborted else " | ".join(inputs))
        messages.append(answer)
        event("message_end", message=answer)
        event("agent_end", messages=[answer])
        next_item = follow_up.pop(0) if follow_up and not aborted else None
        settled = next_item is None
        active = not settled
        for ident in ids:
            event("prompt_result", id=ident, agentInvoked=True,
                  status="aborted" if aborted else "completed", sessionSettled=settled)
        queue_event()
        if settled:
            event("session_settled")
        aborted = False
    if next_item:
        run_turn(*next_item)


def start_turn(text, request_id):
    global active
    active = True
    threading.Thread(target=run_turn, args=(text, request_id), daemon=True).start()


emit({"type": "ready", "protocolVersion": 1, "supportedProtocolVersions": [1, 2],
      "maxFrameBytes": 1048576, "maxReassembledFrameBytes": 67108864})
for raw in sys.stdin:
    request = json.loads(raw)
    log = os.environ.get("MOCK_OMP_LOG")
    if log:
        with open(log, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(request) + "\n")
    kind = request["type"]
    with state_lock:
        if kind == "negotiate_protocol":
            respond(request, {"protocolVersion": 2})
        elif kind == "set_event_filter":
            filter_kinds = request.get("events")
            respond(request, {"events": filter_kinds})
        elif kind == "get_state":
            respond(request, {"sessionId": "mock-session", "model": model(),
                "isStreaming": active, "isCompacting": False,
                "isSettled": not active and not steering and not follow_up,
                "hasPendingAsyncWork": bool(follow_up), "messageCount": len(messages),
                "queuedMessageCount": len(steering) + len(follow_up),
                "queuedMessages": {"steering": [], "followUp": []}})
        elif kind == "prompt":
            behavior = request.get("streamingBehavior")
            if active and behavior not in ("steer", "followUp"):
                reject(request, "active prompt requires streamingBehavior")
                continue
            respond(request, {"agentInvoked": True})
            if active:
                queue = steering if behavior == "steer" else follow_up
                queue.append((request["message"], request.get("id")))
                queue_event()
            else:
                start_turn(request["message"], request.get("id"))
        elif kind in ("steer", "follow_up"):
            respond(request)
            if active:
                queue = steering if kind == "steer" else follow_up
                queue.append((request["message"], None))
                queue_event()
            else:
                start_turn(request["message"], None)
        elif kind == "promote_queued_message":
            index = next((i for i, item in enumerate(follow_up) if item[0] == request["message"]), None)
            promoted = index is not None
            if promoted:
                steering.append(follow_up.pop(index))
                queue_event()
            respond(request, {"promoted": promoted})
        elif kind == "get_messages":
            respond(request, {"messages": messages})
        elif kind == "get_messages_page":
            offset = int(request.get("cursor") or "0")
            limit = min(100, max(1, int(request.get("limit") or 20)))
            end = min(offset + limit, len(messages))
            data = {"messages": messages[offset:end], "totalMessages": len(messages)}
            if end < len(messages):
                data["nextCursor"] = str(end)
            respond(request, data)
        elif kind == "set_model":
            model_id = request["modelId"]
            provider = request["provider"]
            respond(request, model())
        elif kind == "compact":
            respond(request, {"summary": "mock compacted", "firstKeptEntryId": "mock-first", "tokensBefore": 123})
        elif kind == "abort":
            aborted = True
            steering.clear()
            follow_up.clear()
            respond(request)
        else:
            reject(request, "unsupported command")
