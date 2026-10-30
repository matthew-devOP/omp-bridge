#!/usr/bin/env python3
"""Validate model switching/history/compaction with a real authenticated omp.

One inference prompt and one compaction request. Catalog discovery reads RPC
metadata, never authentication files. Public evidence includes only safe fields.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import tempfile
import threading
import time
from smoke_mcp import MCP, SmokeError

INITIAL = 'opencode-go/deepseek-v4.1-flash'
ALTERNATIVE = 'deepseek-v4-pro'
MARKER = 'A4_CONTROL_OK'


def discover_models():
    executable = os.environ.get('OMP_BRIDGE_CMD', 'omp')
    prefix = json.loads(os.environ.get('OMP_BRIDGE_ARGS', '[]'))
    with tempfile.TemporaryDirectory(prefix='omp-controls-catalog-') as cwd:
        process = subprocess.Popen([executable, *prefix, '--mode', 'rpc', '--no-ui',
            '--no-session', '--model', INITIAL, '--no-tools', '--no-extensions',
            '--no-skills', '--no-rules', '--no-context-files', '--no-lsp', '--no-pty',
            '--no-title'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, cwd=cwd, start_new_session=True)
        frames = queue.Queue()
        def read():
            for line in process.stdout:
                try:
                    frames.put(json.loads(line))
                except ValueError:
                    frames.put({'type': 'decode_error'})
            frames.put({'type': 'eof'})
        threading.Thread(target=read, daemon=True).start()
        deadline = time.monotonic() + 60
        def receive(predicate):
            while time.monotonic() < deadline:
                value = frames.get(timeout=max(.1, deadline - time.monotonic()))
                if value.get('type') in ('eof', 'decode_error'):
                    raise SmokeError('catalog_transport_failed')
                if value.get('type') == 'rpc_chunk':
                    raise SmokeError('catalog_chunking_requires_sdk')
                if predicate(value):
                    return value
            raise SmokeError('catalog_timeout')
        try:
            receive(lambda v: v.get('type') == 'ready')
            process.stdin.write(json.dumps({'id': 'protocol', 'type': 'negotiate_protocol',
                                            'protocolVersion': 2}) + '\n')
            process.stdin.flush()
            negotiated = receive(lambda v: v.get('id') == 'protocol')
            if not negotiated.get('success'):
                raise SmokeError('catalog_protocol_negotiation_failed')
            process.stdin.write(json.dumps({'id': 'models', 'type': 'get_available_models'}) + '\n')
            process.stdin.flush()
            response = receive(lambda v: v.get('id') == 'models')
            return [{'provider': model['provider'], 'id': model['id']}
                    for model in response.get('data', {}).get('models', [])
                    if model.get('provider') == 'opencode-go']
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)


def safe_message(message):
    text = ''.join(c.get('text', '') for c in message.get('content', [])
                   if isinstance(c, dict) and c.get('type') == 'text')
    return {'role': message.get('role'), 'text': MARKER if MARKER in text else '[visible text omitted]'}


def run(binary, output):
    started = time.monotonic()
    evidence = {'test': 'live_mcp_harness_controls', 'real': True, 'passed': False,
                'timestampUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'promptsSent': 0, 'compactCalls': 0, 'examples': []}
    mcp = None
    session = None
    failure = None
    stopped = False
    try:
        catalog = discover_models()
        evidence['catalogSource'] = 'RPC get_available_models'
        evidence['availableOpenCodeGoModels'] = catalog
        if not any(m['id'] == ALTERNATIVE for m in catalog):
            raise SmokeError('alternative_not_available')
        deadline = time.monotonic() + 180
        mcp = MCP(binary, deadline)
        mcp.request('initialize', {'protocolVersion': '2025-11-25', 'capabilities': {},
            'clientInfo': {'name': 'live-controls-demo', 'version': '2'}})
        mcp.notify('notifications/initialized')
        listed = mcp.request('tools/list', {})
        schemas = {tool['name']: tool.get('inputSchema') for tool in listed['tools']}
        with tempfile.TemporaryDirectory(prefix='omp-controls-') as cwd:
            launch = mcp.tool('omp_start', {'cwd': cwd, 'model': INITIAL, 'no_tools': True})
            session = launch['session']
            switch_args = {'session': session, 'provider': 'opencode-go', 'model_id': ALTERNATIVE}
            switched = mcp.tool('omp_set_model', switch_args)
            model = switched['model']
            evidence['modelSwitched'] = model.get('id') == ALTERNATIVE and model.get('provider') == 'opencode-go'
            evidence['examples'].append({'tool': 'omp_set_model', 'schema': schemas['omp_set_model'],
                'call': switch_args, 'response': {'session': session,
                    'model': {k: model[k] for k in ('provider', 'id', 'name') if k in model}}})
            if not evidence['modelSwitched']:
                raise SmokeError('model_switch_not_confirmed')
            prompt = mcp.tool('omp_send', {'session': session,
                'message': 'Reply exactly A4_CONTROL_OK. Do not use tools.', 'wait': True})
            evidence['promptsSent'] = 1
            operation = prompt['operationId']
            while prompt.get('status') != 'finished' and time.monotonic() < deadline - 100:
                state = mcp.tool('omp_status', {'session': session})
                prompt = next((op for op in state['operations'] if op['operationId'] == operation), prompt)
                if prompt.get('status') == 'failed':
                    break
                time.sleep(.15)
            evidence['alternativeInferencePassed'] = prompt.get('assistant_text', '').strip() == MARKER
            if not evidence['alternativeInferencePassed']:
                raise SmokeError('alternative_inference_failed_or_timed_out')
            pages = []
            cursor = None
            for _ in range(5):
                args = {'session': session, 'limit': 1}
                if cursor:
                    args['cursor'] = cursor
                result = mcp.tool('omp_messages', args)
                page = {'session': session, 'messages': [safe_message(m) for m in result['messages']],
                        'totalMessages': result.get('totalMessages'), 'nextCursor': result.get('nextCursor')}
                pages.append(page)
                evidence['examples'].append({'tool': 'omp_messages', 'schema': schemas['omp_messages'],
                    'call': args, 'response': page})
                cursor = result.get('nextCursor')
                if not cursor:
                    break
            evidence['paginatedHistoryPassed'] = len(pages) >= 2 and all(len(p['messages']) <= 1 for p in pages)
            compact_args = {'session': session, 'instructions': 'Keep only the A4_CONTROL_OK marker.'}
            compact = mcp.tool('omp_compact', compact_args)
            evidence['compactCalls'] = 1
            evidence['examples'].append({'tool': 'omp_compact', 'schema': schemas['omp_compact'],
                                         'call': compact_args, 'response': compact})
            compact_id = compact['operationId']
            result = None
            while time.monotonic() < deadline - 12:
                state = mcp.tool('omp_status', {'session': session})
                result = next((op for op in state['operations'] if op['operationId'] == compact_id), None)
                if result and result.get('status') in ('finished', 'failed'):
                    break
                time.sleep(.2)
            evidence['compactionOperationObserved'] = result is not None
            evidence['compactionStatus'] = result.get('status') if result else 'not_observed'
            evidence['compactionSucceeded'] = bool(result and result.get('status') == 'finished')
            safe_result = {k: result[k] for k in ('operationId', 'status', 'error', 'retrySafe') if result and k in result}
            if result and isinstance(result.get('compaction'), dict):
                safe_result['compaction'] = {k: v for k, v in result['compaction'].items()
                    if k in ('tokensBefore', 'tokensAfter', 'firstKeptEntryId')}
            evidence['compactionOutcome'] = safe_result
            if not evidence['compactionSucceeded']:
                evidence['compactionLimitation'] = ('One-turn history may be too small to compact; '
                    'the bridge exposed the actual asynchronous failure without pretending success.')
            evidence['examples'].append({'tool': 'omp_status', 'schema': schemas['omp_status'],
                'call': {'session': session}, 'response': {'session': session,
                'busy': state.get('busy'), 'sessionSettled': state.get('sessionSettled'),
                'operations': [safe_result]}})
            evidence['passed'] = evidence['modelSwitched'] and evidence['alternativeInferencePassed'] and evidence['paginatedHistoryPassed'] and evidence['compactionSucceeded']
    except Exception as error:
        failure = str(error) if isinstance(error, SmokeError) else type(error).__name__
    finally:
        if mcp:
            if session:
                try:
                    stopped = mcp.tool('omp_stop', {'session': session}).get('stopped') is True
                except Exception:
                    failure = failure or 'cleanup_failed'
            evidence['bridgeExitCode'] = mcp.close()
        evidence['stopped'] = stopped
        evidence['durationMs'] = round((time.monotonic() - started) * 1000)
        if failure:
            evidence['failure'] = failure
        evidence['passed'] = evidence['passed'] and stopped and failure is None
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({k: v for k, v in evidence.items() if k not in ('availableOpenCodeGoModels', 'examples')}, indent=2))
    return 0 if evidence['passed'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=Path('target/debug/omp-bridge'))
    parser.add_argument('--output', type=Path, default=Path('evidence/v2/controls-live.json'))
    args = parser.parse_args()
    raise SystemExit(run(args.binary.resolve(), args.output))
