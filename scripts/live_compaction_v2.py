#!/usr/bin/env python3
"""Exercise actual MCP compaction with a temporary, disclosed test-only overlay.

The default20k retained tokens intentionally prevents tiny-history compaction.
This test sets keepRecentTokens=0 and methodOrder=[soft] only for the spawned
process. It never reads or edits the user's config or credential files.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import tempfile
import time
from smoke_mcp import MCP, SmokeError

MODEL = 'opencode-go/deepseek-v4.1-flash'
OVERLAY = 'compaction:\n  keepRecentTokens: 0\n  methodOrder: [soft]\n'
PROMPTS = [
    ('Remember the public demo project code BLUE_ORBIT. It is a local CLI bridge. '
     'Respond exactly NOTE_STORED. Do not use tools.', 'NOTE_STORED'),
    ('Remember the demo target is local only, no hosting or deployment. '
     'Respond exactly SECOND_NOTE_STORED. Do not use tools.', 'SECOND_NOTE_STORED'),
]


def run(binary, output):
    start = time.monotonic()
    evidence = {'test': 'live_mcp_compaction_with_test_overlay', 'real': True, 'passed': False,
        'timestampUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'model': MODEL,
        'testOnlyOverlay': {'compaction': {'keepRecentTokens': 0, 'methodOrder': ['soft']}},
        'overlayScope': 'temporary --config file passed only to the spawned omp process',
        'defaultRetentionUnchanged': True, 'promptsSent': 0, 'compactCalls': 0, 'examples': []}
    mcp = None
    session = None
    failure = None
    original_args = os.environ.get('OMP_BRIDGE_ARGS')
    with tempfile.TemporaryDirectory(prefix='omp-compact-test-') as cwd:
        overlay = Path(cwd) / 'test-overlay.yml'
        overlay.write_text(OVERLAY)
        args = json.loads(original_args or '[]')
        os.environ['OMP_BRIDGE_ARGS'] = json.dumps([*args, '--config', str(overlay)])
        try:
            deadline = time.monotonic() + 180
            mcp = MCP(binary, deadline)
            mcp.request('initialize', {'protocolVersion': '2025-11-25', 'capabilities': {},
                'clientInfo': {'name': 'compaction-demo', 'version': '2'}})
            mcp.notify('notifications/initialized')
            launch = mcp.tool('omp_start', {'cwd': cwd, 'model': MODEL, 'no_tools': True})
            session = launch['session']
            for prompt, marker in PROMPTS:
                result = mcp.tool('omp_send', {'session': session, 'message': prompt, 'wait': True})
                evidence['promptsSent'] += 1
                operation = result['operationId']
                end = time.monotonic() + 35
                while result.get('status') not in ('finished', 'failed') and time.monotonic() < end:
                    state = mcp.tool('omp_status', {'session': session})
                    result = next((x for x in state.get('operations', []) if x['operationId'] == operation), result)
                    time.sleep(.15)
                if result.get('assistant_text', '').strip() != marker:
                    raise SmokeError('seed_prompt_not_completed')
            history = mcp.tool('omp_messages', {'session': session, 'limit': 10})
            evidence['historyBeforeCount'] = history.get('totalMessages')
            call = {'session': session, 'instructions': 'Summarize only the demo project facts in one short paragraph. Preserve BLUE_ORBIT and local-only.'}
            admission = mcp.tool('omp_compact', call)
            evidence['compactCalls'] = 1
            evidence['examples'].append({'tool': 'omp_compact', 'call': call, 'response': admission})
            operation = admission['operationId']
            outcome = None
            while time.monotonic() < deadline - 15:
                state = mcp.tool('omp_status', {'session': session})
                outcome = next((x for x in state.get('operations', []) if x['operationId'] == operation), None)
                if outcome and outcome.get('status') in ('finished', 'failed'):
                    break
                time.sleep(.2)
            evidence['operationStatus'] = outcome.get('status') if outcome else 'not_observed'
            evidence['compactionSucceeded'] = bool(outcome and outcome.get('status') == 'finished')
            evidence['outcome'] = {k: outcome[k] for k in ('operationId', 'status', 'error', 'retrySafe') if outcome and k in outcome}
            if outcome and isinstance(outcome.get('compaction'), dict):
                compact = outcome['compaction']
                evidence['outcome']['compaction'] = {k: v for k, v in compact.items()
                    if k in ('tokensBefore', 'tokensAfter', 'firstKeptEntryId')}
                summary = compact.get('summary')
                if isinstance(summary, str):
                    evidence['summaryCharacters'] = len(summary)
                    evidence['summaryPreservesProjectCode'] = 'BLUE_ORBIT' in summary
                    evidence['summaryPreservesLocalOnly'] = 'local' in summary.lower()
            evidence['examples'].append({'tool': 'omp_status', 'call': {'session': session},
                'response': {'session': session, 'busy': state.get('busy'),
                             'sessionSettled': state.get('sessionSettled'), 'operations': [evidence['outcome']]}})
            after = mcp.tool('omp_messages', {'session': session, 'limit': 10})
            evidence['historyAfterCount'] = after.get('totalMessages')
            evidence['compactionSummaryInHistory'] = any(m.get('role') == 'compactionSummary' for m in after.get('messages', []))
            evidence['passed'] = evidence['compactionSucceeded']
        except Exception as error:
            failure = str(error) if isinstance(error, SmokeError) else type(error).__name__
        finally:
            stopped = False
            if mcp:
                if session:
                    try:
                        stopped = mcp.tool('omp_stop', {'session': session}).get('stopped') is True
                    except Exception:
                        failure = failure or 'stop_failed'
                evidence['bridgeExitCode'] = mcp.close()
            if original_args is None:
                os.environ.pop('OMP_BRIDGE_ARGS', None)
            else:
                os.environ['OMP_BRIDGE_ARGS'] = original_args
            evidence['stopped'] = stopped
            evidence['durationMs'] = round((time.monotonic() - start) * 1000)
            evidence['passed'] = evidence['passed'] and stopped and failure is None
            if failure:
                evidence['failure'] = failure
    evidence['temporaryOverlayRemoved'] = not overlay.exists()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({k: v for k, v in evidence.items() if k != 'examples'}, indent=2))
    return 0 if evidence['passed'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=Path('target/debug/omp-bridge'))
    parser.add_argument('--output', type=Path, default=Path('evidence/v2/compaction-live.json'))
    args = parser.parse_args()
    raise SystemExit(run(args.binary.resolve(), args.output))
