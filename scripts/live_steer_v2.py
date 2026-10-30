#!/usr/bin/env python3
"""Real MCP steer/follow-up demonstration; captures visible, sanitized events.

Requires an authenticated omp. One primary prompt, one steer, one follow-up.
Replay timestamps are measured observer arrival times, not invented model times.
"""
import argparse
import datetime
import json
from pathlib import Path
import tempfile
import time
from smoke_mcp import MCP, SmokeError

MODEL = 'opencode-go/deepseek-v4.1-flash'
PROMPT = ('This is an interactive control demonstration. First call the bash tool exactly once '
          'with the command sleep 10. Do not read or write any files and do not use other tools. '
          'After that tool finishes, reply exactly BASELINE_RED. If a later user instruction '
          'changes the requested final reply, obey that newer instruction instead.')
STEER = ('Change direction now: cancel the requested BASELINE_RED answer. After the current '
         'sleep command finishes, reply exactly STEERED_BLUE. Do not call any additional tools.')
FOLLOW = ('For the next follow-up turn, reply exactly FOLLOW_UP_GREEN. Do not call any tools.')


def visible_text(message):
    if not isinstance(message, dict) or message.get('role') != 'assistant':
        return None
    content = message.get('content', [])
    if isinstance(content, str):
        return content
    return ''.join(c.get('text', '') for c in content
                   if isinstance(c, dict) and c.get('type') == 'text') or None


def safe_event(event):
    kind = event.get('type', 'unknown')
    out = {'type': kind}
    if kind == 'message_end':
        text = visible_text(event.get('message'))
        if text:
            # This controlled task should only emit short public markers. Do not
            # export unexpected arbitrary model text, paths, or hidden reasoning.
            markers = [m for m in ('BASELINE_RED', 'STEERED_BLUE', 'FOLLOW_UP_GREEN') if m in text]
            out.update(role='assistant', markers=markers,
                       text=text if text.strip() in markers else '[non-marker visible text omitted]')
    elif kind == 'queue_update':
        for key in ('steering', 'followUp'):
            value = event.get(key)
            if isinstance(value, list):
                out[key] = ['steer request' if STEER in str(x) else
                            'follow-up request' if FOLLOW in str(x) else 'queued message'
                            for x in value]
    elif kind in ('tool_execution_start', 'tool_execution_end'):
        out['toolName'] = event.get('toolName', event.get('tool_name', 'unknown'))
        if isinstance(event.get('isError'), bool):
            out['isError'] = event['isError']
    elif kind == 'prompt_result':
        for key in ('status', 'sessionSettled', 'agentInvoked'):
            if key in event:
                out[key] = event[key]
    elif kind == 'bridge_operation_result':
        value = event.get('outcome') or {}
        out['status'] = value.get('status')
        result = value.get('result') or {}
        out['sessionSettled'] = result.get('sessionSettled')
    return out


def run(binary, evidence_path, replay_path):
    started = time.monotonic()
    deadline = started + 150
    mcp = MCP(binary, deadline)
    session = None
    events = []
    tools = []
    observed_texts = []
    seen = set()
    cursor = 0
    settled = False
    stopped = False
    steer_sent = follow_sent = False
    tool_started_before_steer = False
    failure = None
    evidence = {'test': 'live_mcp_steer_follow_up', 'real': True,
                'timestampUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'model': MODEL, 'primaryPrompts': 0, 'steerCalls': 0, 'followUpCalls': 0,
                'passed': False, 'timestampMeaning': 'observer arrival, monotonic milliseconds'}

    def record(kind, value):
        events.append({'atMs': round((time.monotonic() - started) * 1000),
                       'source': 'client' if kind == 'tool_call' else 'agent',
                       'event': {'type': kind, **value}})

    def call(name, args):
        record('tool_call', {'tool': name})
        result = mcp.tool(name, args)
        tools.append(name)
        return result

    def read_events(wait=100):
        nonlocal cursor, settled
        page = call('omp_events', {'session': session, 'cursor': cursor, 'wait_ms': wait})
        if page.get('gap') or page.get('dropped'):
            raise SmokeError('event_history_gap')
        cursor = page['nextCursor']
        batch = []
        for entry in page['events']:
            event = entry['event']
            kind = event.get('type')
            seen.add(kind)
            text = visible_text(event.get('message')) if kind == 'message_end' else None
            if text:
                observed_texts.append(text.strip())
            record('rpc_event', {'cursor': entry['cursor'], **safe_event(event)})
            batch.append(event)
            if kind == 'session_settled':
                settled = True
            elif kind == 'agent_start':
                settled = False
        return batch

    with tempfile.TemporaryDirectory(prefix='omp-bridge-live-') as cwd:
        try:
            info = mcp.request('initialize', {'protocolVersion': '2025-11-25', 'capabilities': {},
                'clientInfo': {'name': 'live-steer-demo', 'version': '2'}})
            mcp.notify('notifications/initialized')
            evidence['mcpServer'] = info.get('serverInfo')
            launch = call('omp_start', {'cwd': cwd, 'model': MODEL, 'no_tools': False})
            session = launch['session']
            evidence['ompProtocolVersion'] = launch['protocolVersion']
            admission = call('omp_send', {'session': session, 'message': PROMPT, 'wait': False})
            evidence['primaryPrompts'] = 1
            evidence['operationId'] = admission.get('operationId')
            while time.monotonic() < deadline - 20:
                batch = read_events(300)
                if any(e.get('type') == 'tool_execution_start' for e in batch):
                    tool_started_before_steer = True
                    break
                if settled:
                    raise SmokeError('primary_finished_before_tool_started')
            if not tool_started_before_steer:
                raise SmokeError('no_active_tool_to_steer')
            steered = call('omp_steer', {'session': session, 'text': STEER})
            steer_sent = steered.get('admitted') is True
            evidence['steerCalls'] = 1
            evidence['steerAdmission'] = steered
            followed = call('omp_follow_up', {'session': session, 'text': FOLLOW})
            follow_sent = followed.get('admitted') is True
            evidence['followUpCalls'] = 1
            evidence['followUpAdmission'] = followed
            # Admission is not completion. Continue until both visible markers
            # have reached the transcript and the whole session is settled.
            while time.monotonic() < deadline - 15:
                read_events(500)
                state = call('omp_status', {'session': session})
                state_settled = (state.get('state') or {}).get('isSettled') is True
                if state_settled and 'STEERED_BLUE' in observed_texts and 'FOLLOW_UP_GREEN' in observed_texts:
                    settled = True
                    break
            evidence.update(toolStartedBeforeSteer=tool_started_before_steer,
                            steeringAdmitted=steer_sent, followUpAdmitted=follow_sent,
                            changedDirection='STEERED_BLUE' in observed_texts,
                            baselineSuppressed='BASELINE_RED' not in observed_texts,
                            followUpCompleted='FOLLOW_UP_GREEN' in observed_texts,
                            sessionSettled=settled, sessionSettledSource='omp_status.state.isSettled',
                            queueUpdateObserved='queue_update' in seen,
                            eventKinds=sorted(seen))
            if not all(evidence[k] for k in ('toolStartedBeforeSteer', 'steeringAdmitted',
                'followUpAdmitted', 'changedDirection', 'baselineSuppressed',
                'followUpCompleted', 'sessionSettled', 'queueUpdateObserved')):
                raise SmokeError('interaction_acceptance_not_met')
        except Exception as error:
            failure = str(error) if isinstance(error, SmokeError) else type(error).__name__
        finally:
            if session:
                try:
                    stopped = call('omp_stop', {'session': session}).get('stopped') is True
                except Exception:
                    failure = failure or 'cleanup_failed'
            code = mcp.close()
            evidence.update(stopped=stopped, bridgeExitCode=code,
                            durationMs=round((time.monotonic() - started) * 1000),
                            passed=failure is None and stopped and code == 0)
            if failure:
                evidence['failure'] = failure
            # The admission responses contain only synthetic session IDs and no paths.
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            replay_path.parent.mkdir(parents=True, exist_ok=True)
            evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
            replay_path.write_text(json.dumps({'real': True, 'timestampMeaning': evidence['timestampMeaning'],
                'model': MODEL, 'events': events}, indent=2) + '\n')
    print(json.dumps(evidence, indent=2))
    return 0 if evidence['passed'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=Path('target/debug/omp-bridge'))
    parser.add_argument('--evidence', type=Path, default=Path('evidence/v2/steer-live.json'))
    parser.add_argument('--replay', type=Path, default=Path('demo/v2/steer-live.json'))
    args = parser.parse_args()
    raise SystemExit(run(args.binary.resolve(), args.evidence, args.replay))
