"""Explicit live verification through remote Agent Bridge MCP, not worker CLIs."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def unpack(result):
    structured = getattr(result, 'structured_content', getattr(result, 'structuredContent', None))
    if structured is not None:
        return structured
    for item in result.content:
        if item.type == 'text':
            return json.loads(item.text)
    raise ValueError('Missing MCP result')


async def run(args):
    parameters = StdioServerParameters(command='/usr/bin/ssh', args=[
        '-T', args.host, '.local/bin/agent-bridge'])
    with open(os.devnull, 'w') as diagnostics:
        await connect(parameters, diagnostics, args)


async def connect(parameters, diagnostics, args):
    async with stdio_client(parameters, errlog=diagnostics) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            await session.initialize()

            async def call(name, arguments):
                result = unpack(await session.call_tool(name, arguments))
                if not result.get('ok'):
                    raise RuntimeError(str(result.get('error', 'MCP call failed'))[:500])
                return result

            pinned = {'gateway/kimi-k2.7-code': 'pool-kimi-k27',
                      'gateway/glm-5.3-flash': 'pool-glm-flash',
                      'gateway/kimi-k3': 'pool-kimi-k3',
                      'opencode-go/muse-spark-1.3-contributor': 'pool-muse-go'}
            args.agent = pinned[args.model]
            inventory = await call('list_agents', {})
            rows = [a for a in inventory['agents'] if a['agent'] == args.agent]
            print(json.dumps({'host': args.host, 'agent': args.agent,
                              'available': bool(rows and rows[0]['available'])}), flush=True)
            if not args.allow_live_dispatch:
                return
            if not rows or not rows[0]['available']:
                raise RuntimeError('Requested remote Bridge agent unavailable')
            result = await call('dispatch_task', {
                'agent': args.agent,
                'cwd': args.cwd,
                'message': 'Connectivity smoke only. No tools, file reads/writes, shell, delegation, or external actions. Reply exactly POOL_SMOKE_OK.',
                'title': 'Subscription pool connectivity smoke', 'user_requested': True})
            task, owner = result['task_id'], result['session_id']
            print(json.dumps({'host': args.host, 'task_id': task, 'session_id': owner,
                              'model': args.model}), flush=True)
            try:
                for _ in range(12):
                    result = await call('wait_task', {'task_id': task, 'timeout_sec': 30})
                    if result.get('status') in ('completed', 'failed', 'cancelled'):
                        break
                result = await call('get_result', {'task_id': task})
                # Bounded test prompt only. Never print whole runtime/transcript.
                text = json.dumps(result)
                print(json.dumps({'host': args.host, 'model': args.model,
                                  'sentinel': 'POOL_SMOKE_OK' in text,
                                  'status': result.get('status'),
                                  'observed_model': result.get('observed_model'),
                                  'error': str(result.get('error') or '')[:500]}), flush=True)
                if 'POOL_SMOKE_OK' not in text or result.get('status') != 'completed':
                    raise RuntimeError('Remote Bridge model smoke did not complete')
            finally:
                await call('end_session', {'session_id': owner})


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', choices=['mac-studio', 'mac-mini'], required=True)
    p.add_argument('--cwd', required=True, help='Absolute repository path on remote host')
    p.add_argument('--model', default='gateway/kimi-k2.7-code', choices=[
        'gateway/kimi-k2.7-code', 'gateway/glm-5.3-flash', 'gateway/kimi-k3',
        'opencode-go/muse-spark-1.3-contributor'])
    p.add_argument('--allow-live-dispatch', action='store_true')
    args = p.parse_args()
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
