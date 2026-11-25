import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import test from 'node:test'

import { AsyncTasks } from '../desktop_host/async_tasks.js'
import { apply } from '../desktop_host/index.js'
import { ensureWorkspaceRules } from '../desktop_host/workspace_rules.js'

async function directory(t) {
  const path = await mkdtemp(join(tmpdir(), 'maibot-async-'))
  t.after(async () => {
    assert.equal(dirname(path), tmpdir())
    assert.ok(path.split(/[\\/]/).at(-1).startsWith('maibot-async-'))
    await rm(path, { recursive: true, force: true })
  })
  return path
}

const origin = { stream: 'test', streamId: 'real-chat-stream', context: {}, sender: 'maibot', timeout: 60, task: 'test' }

test('submission persists immediately, execution survives submitter disconnect, notifications require ack', async (t) => {
  const root = await directory(t)
  let release, started
  const began = new Promise((resolve) => { started = resolve })
  const queue = new AsyncTasks(root, async () => {
    started()
    return await new Promise((resolve) => { release = resolve })
  })
  await queue.initialize()
  const accepted = await queue.submit(origin)
  await began
  assert.equal(accepted.status, 'accepted')
  assert.equal(queue.notification(), null)
  assert.equal(await queue.acknowledge(accepted.jobId), false)
  assert.ok(await readFile(join(root, `${accepted.jobId}.json`), 'utf8'))
  release({ status: 'completed', final: 'only the answer', files: [] })
  await queue.worker
  const reloaded = new AsyncTasks(root, () => { throw new Error('must not rerun completed tasks') })
  await reloaded.initialize()
  assert.equal(reloaded.notification().final, 'only the answer')
  assert.equal(reloaded.notification().streamId, origin.streamId)
  assert.equal(reloaded.notification().request, origin.task)
  assert.equal(reloaded.notification().jobId, accepted.jobId)
  assert.equal(await reloaded.acknowledge(accepted.jobId), true)
  assert.equal(await reloaded.acknowledge(accepted.jobId), true)
  assert.equal(reloaded.notification(), null)
})

test('restart marks running work interrupted without re-executing side effects; queued work resumes', async (t) => {
  const root = await directory(t)
  const id = '11111111-1111-4111-8111-111111111111'
  const waiting = '22222222-2222-4222-8222-222222222222'
  for (const [jobId, status] of [[id, 'running'], [waiting, 'queued']]) {
    await writeFile(join(root, `${jobId}.json`), JSON.stringify({ id: jobId, status, input: origin, createdAt: 1 }))
  }
  let calls = 0
  const queue = new AsyncTasks(root, async () => { calls++; return { status: 'completed', final: 'done' } })
  await queue.initialize()
  assert.equal(queue.notification().status, 'failed')
  assert.match(queue.jobs.get(id).result.error, /中断/)
  queue.start()
  await queue.worker
  assert.equal(calls, 1)
  assert.equal(queue.jobs.get(waiting).status, 'completed')
})

test('one undeliverable notification does not starve another chat', async (t) => {
  const queue = new AsyncTasks(await directory(t), async () => ({ status: 'completed', final: 'done' }))
  await queue.initialize()
  await queue.submit(origin)
  await queue.submit({ ...origin, streamId: 'other-chat' })
  while (queue.worker) await queue.worker
  const first = queue.notification()
  const second = queue.notification()
  assert.notEqual(first.jobId, second.jobId)
  assert.equal(queue.notification().jobId, first.jobId)
})

test('real HTTP accepts before completion, checks authentication, retains files-only results and low effort', async (t) => {
  const root = await directory(t)
  const token = 'a'.repeat(64)
  await writeFile(join(root, 'token'), token)
  let listener, qqListener, dispose, session, requestId, turn = 0
  let submittedPrompt
  let cancelled = 0
  const selections = []
  const restrictions = []
  const ctx = {
    sessions: { get: () => session },
    agents: { get: () => ({ cancel() { cancelled++ } }) },
    permissionPresets: { set() {}, current: () => 'workspace-write' },
    approval: { overrideOf: () => 'never' },
    llm: { listModels: async () => [{ id: 'test-model' }], resolveCallConfig: async (config) => config },
    tools: { get: (name) => ['notify_qq', 'send_to_qq'].includes(name) ? {} : undefined },
    on(event, callback) {
      if (event === 'session/event') listener = callback
      if (event === 'tools/result') qqListener = callback
      return () => {}
    },
    effect(factory) { dispose = factory() },
    sessionController: {
      async create() { session = { id: 'test-session', append() {} }; return { sessionId: session.id } },
      agents: {
        resolveAgent: async () => ({ agent: { ctx: { tools: { restrict(filter) { restrictions.push(filter); return () => {} } } } } }),
        selectForNextRequest(agent, selected) { selections.push(selected) },
      },
      async prompt(input) {
        submittedPrompt = input.content[0].text
        requestId = input.requestId
        turn++
        listener(session, { type: 'turn/start', data: { turn } })
        listener(session, { type: 'user/message', data: { source: { kind: 'user', rpcId: requestId } } })
      },
    },
  }
  await apply(ctx, {
    workspaceRoot: root, cwd: root, stateDir: root, senderId: 'maibot', tokenFile: join(root, 'token'),
    modelProvider: 'test', model: 'test-model', reasoningEffort: 'low',
  })
  t.after(() => dispose())
  const endpoint = JSON.parse(await readFile(join(root, 'endpoint.json'), 'utf8'))
  const base = `http://127.0.0.1:${endpoint.port}/.maibot-desktop-bridge`
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
  assert.equal((await fetch(`${base}/notifications`)).status, 401)
  const response = await fetch(`${base}/tasks`, { method: 'POST', headers, body: JSON.stringify(origin) })
  assert.equal(response.status, 202)
  const accepted = await response.json()
  // Acceptance is already observable while the agent has not produced any result.
  assert.equal((await (await fetch(`${base}/notifications`, { headers })).json()).notification, null)
  while (!requestId) await new Promise((resolve) => setTimeout(resolve, 5))
  assert.equal(submittedPrompt, '【MaiBot桥接任务】 delivery=maibot\ntest')
  assert.match(await readFile(join(root, 'AGENT.md'), 'utf8'), /MaiBot 桥接规则/)
  assert.match(await readFile(join(root, 'AGENTS.md'), 'utf8'), /MaiBot 桥接规则/)
  assert.equal(selections[0].reasoningEffort, 'low')
  assert.deepEqual(restrictions, [{ deny: ['notify_qq', 'send_to_qq'] }])
  assert.equal(cancelled, 0)
  listener(session, { type: 'tool/call', data: { turn, name: 'present', arguments: JSON.stringify({ files: ['screen.png'] }) } })
  listener(session, { type: 'turn/end', data: { turn, reason: { kind: 'completed' } } })
  let item
  for (let attempt = 0; attempt < 100; attempt++) {
    item = (await (await fetch(`${base}/notifications`, { headers })).json()).notification
    if (item) break
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  assert.equal(item.jobId, accepted.jobId)
  assert.equal(item.final, '')
  assert.deepEqual(item.files, [join(root, 'screen.png')])
  assert.equal((await fetch(`${base}/notifications/${item.jobId}/ack`, { method: 'POST', headers })).status, 200)
  assert.equal((await (await fetch(`${base}/notifications`, { headers })).json()).notification, null)

  for (const [outcome, expected] of [
    [{ isError: false, value: { detail: '文本已发送' } }, 'sent'],
    [{ isError: false, value: { detail: '文本已排队；等你下次说话时补发' } }, 'queued'],
    [{ isError: true, error: { message: 'failed' } }, 'none'],
  ]) {
    requestId = undefined
    const submitted = await (await fetch(`${base}/tasks`, {
      method: 'POST', headers, body: JSON.stringify({ ...origin, delivery: 'qq' }),
    })).json()
    while (!requestId) await new Promise((resolve) => setTimeout(resolve, 5))
    qqListener({ name: 'notify_qq', agent: { session }, arguments: { path: join(root, 'direct.png') } }, outcome)
    if (expected !== 'none') {
      listener(session, { type: 'tool/call', data: { turn, name: 'present', arguments: JSON.stringify({ files: ['direct.png'] }) } })
    }
    listener(session, { type: 'assistant/message', data: { turn, message: { content: [{ type: 'text', text: '任务已完成。我通过 notify_qq 发送了以下回复……' }] } } })
    listener(session, { type: 'turn/end', data: { turn, reason: { kind: 'completed' } } })
    for (let attempt = 0; attempt < 100; attempt++) {
      item = (await (await fetch(`${base}/notifications`, { headers })).json()).notification
      if (item?.jobId === submitted.jobId) break
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    assert.equal(item.qqDelivery, expected)
    assert.equal(item.delivery, 'qq')
    assert.equal(item.final, '')
    assert.deepEqual(item.files, [])
    assert.equal(item.status, expected === 'none' ? 'failed' : 'completed')
    await fetch(`${base}/notifications/${item.jobId}/ack`, { method: 'POST', headers })
  }
  assert.equal(restrictions.length, 1) // Explicit QQ delivery never inherits a stale deny mask.
})

test('workspace rule updates preserve existing instructions and never duplicate managed sections', async (t) => {
  const root = await directory(t)
  const original = '\uFEFF# Existing rules\r\nKeep these exact bytes.\r\n'
  await writeFile(join(root, 'AGENTS.md'), original, 'utf8')
  await ensureWorkspaceRules(root, 'First bridge rule')
  await ensureWorkspaceRules(root, 'Updated bridge rule')
  const first = await readFile(join(root, 'AGENTS.md'))
  assert.ok(first.subarray(0, Buffer.byteLength(original)).equals(Buffer.from(original)))
  assert.ok(!first.toString('utf8').includes('First bridge rule'))
  assert.equal(first.toString('utf8').split('MAIBOT_BRIDGE_RULES_START').length, 2)
  await ensureWorkspaceRules(root, 'Updated bridge rule')
  assert.ok(first.equals(await readFile(join(root, 'AGENTS.md'))))
})

test('malformed managed sections are exposed instead of overwritten', async (t) => {
  const root = await directory(t)
  const malformed = '<!-- MAIBOT_BRIDGE_RULES_START -->\nUser text without a closing marker'
  await writeFile(join(root, 'AGENTS.md'), malformed, 'utf8')
  await assert.rejects(ensureWorkspaceRules(root, 'new rule'), /Malformed/)
  assert.equal(await readFile(join(root, 'AGENTS.md'), 'utf8'), malformed)
})
