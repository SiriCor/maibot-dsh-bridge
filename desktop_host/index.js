import { AsyncTasks } from './async_tasks.js'
import { ensureWorkspaceRules } from './workspace_rules.js'
import { timingSafeEqual, randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { mkdirSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from 'node:fs'
import { join, resolve, relative as relativeTo, isAbsolute } from 'node:path'

export const name = 'maibot-desktop-host-bridge'
export const inject = ['sessions', 'sessionController', 'permissionPresets', 'approval', 'agents', 'llm', 'tools']

const HOST = '127.0.0.1'
const BASE = '/.maibot-desktop-bridge'
const ROUTE = `${BASE}/tasks`
const MAX_BODY = 256 * 1024
const MAX_REPLY = 64 * 1024
const MIN_TIMEOUT = 10
const MAX_TIMEOUT = 1800
const STREAM = /^[A-Za-z0-9_-]{1,96}$/
const MAX_FILES = 32
const TASK_PREFIX = '【MaiBot桥接任务】'
const BRIDGE_RULES = readFileSync(new URL('./AGENT.md', import.meta.url), 'utf8')

function reply(res, status, value) {
  const body = JSON.stringify(value)
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(body),
  })
  res.end(body)
}

async function readJson(req) {
  let size = 0
  const chunks = []
  for await (const chunk of req) {
    size += chunk.length
    if (size > MAX_BODY) throw new Error('request-too-large')
    chunks.push(chunk)
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'))
}

function textOf(message) {
  const blocks = message?.content
  if (!Array.isArray(blocks)) return ''
  let text = ''
  for (const block of blocks) {
    if (block?.type === 'text' && typeof block.text === 'string') text += block.text
  }
  return text
}

// `present` is the only tool that declares user-facing deliverables. Its
// arguments arrive as the model's raw JSON string, so a malformed call is
// simply skipped instead of failing the whole run.
function presentedPaths(argumentsText) {
  let parsed
  try {
    parsed = JSON.parse(argumentsText)
  } catch {
    return []
  }
  const files = parsed?.files
  if (!Array.isArray(files)) return []
  const paths = []
  for (const entry of files) {
    const value = typeof entry === 'string' ? entry : entry?.path
    if (typeof value === 'string' && value.trim()) paths.push(value.trim())
  }
  return paths
}

// Deliverables stay inside the session directory: a present call naming a path
// elsewhere is dropped rather than forwarded to MaiBot.
function resolvePresented(cwd, value) {
  if (typeof cwd !== 'string' || !cwd) return undefined
  const absolute = resolve(cwd, value)
  const relative = relativeTo(cwd, absolute)
  if (!relative || relative.startsWith('..') || isAbsolute(relative)) return undefined
  return absolute
}

function writeEndpoint(file, value) {
  const temporary = `${file}.${process.pid}.tmp`
  writeFileSync(temporary, JSON.stringify(value), { encoding: 'utf8', mode: 0o600 })
  renameSync(temporary, file)
}

export async function apply(ctx, config = {}) {
  const { workspaceRoot, senderId, tokenFile, stateDir } = config
  const port = config.port ?? 0
  if (![workspaceRoot, senderId, tokenFile, stateDir].every((value) => typeof value === 'string' && value.length)
      || !Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error('maibot bridge needs workspaceRoot, senderId, tokenFile, stateDir and an optional port')
  }

  const root = resolve(workspaceRoot)
  mkdirSync(root, { recursive: true })
  const realRoot = realpathSync.native(root)

  // Optional pin: every bridged session runs in one fixed directory instead of
  // a per-stream subdirectory of workspaceRoot. An empty value keeps the old
  // per-stream layout.
  const forcedCwd = typeof config.cwd === 'string' && config.cwd.trim()
    ? realpathSync.native((() => {
      const dir = resolve(config.cwd.trim())
      mkdirSync(dir, { recursive: true })
      return dir
    })())
    : undefined

  // Optional pin: the model every bridged session runs on. Host defaults stay
  // untouched, so other Desktop sessions keep their own selection.
  const pinProvider = typeof config.modelProvider === 'string' ? config.modelProvider.trim() : ''
  const pinModel = typeof config.model === 'string' ? config.model.trim() : ''
  const pinReasoningEffort = typeof config.reasoningEffort === 'string' ? config.reasoningEffort.trim() : ''
  if ((pinProvider && !pinModel) || (!pinProvider && pinModel)) {
    throw new Error('maibot bridge needs both modelProvider and model, or neither')
  }
  if (pinProvider && pinModel) {
    const known = await ctx.llm.listModels(pinProvider).catch(() => [])
    if (!known.some((entry) => entry.id === pinModel)) {
      throw new Error(`maibot bridge model pin unavailable: ${pinProvider}/${pinModel}`)
    }
  }

  if (forcedCwd !== undefined) await ensureWorkspaceRules(forcedCwd, BRIDGE_RULES)

  const state = resolve(stateDir)
  mkdirSync(state, { recursive: true })
  const endpointFile = join(state, 'endpoint.json')

  // Provisioned separately with a verified Windows owner-only ACL; POSIX mode bits
  // do not restrict inherited Windows permissions.
  const token = readFileSync(tokenFile, 'utf8').trim()
  if (!/^[a-f0-9]{64}$/.test(token)) throw new Error('invalid maibot bridge token file')
  const expected = Buffer.from(token, 'ascii')

  const pending = new Map()
  const streams = new Map()
  let busy = false
  let closing = false

  const cancelSession = (sessionId) => {
    try {
      ctx.agents.get(sessionId)?.cancel({ kind: 'user' }, { keepInbox: false })
    } catch {
      // The agent already stopped; nothing to cancel.
    }
  }

  const detachEvents = ctx.on('session/event', (session, event) => {
    const run = pending.get(session?.id)
    if (!run || run.done) return
    if (event?.type === 'turn/start') {
      if (Number.isSafeInteger(event.data?.turn)) run.activeTurn = event.data.turn
    } else if (event?.type === 'user/message') {
      if (event.data?.source?.kind === 'user' && event.data.source.rpcId === run.requestId
          && Number.isSafeInteger(run.activeTurn)) {
        run.promptTurn = run.activeTurn
        run.final = ''
        run.files = []
      }
    } else if (event?.type === 'request/header') {
      const actual = event.data?.header?.config
      ctx.logger?.info?.(`maibot bridge v13 request session=${session.id} effort=${actual?.reasoningEffort ?? '(unset)'} expected=${pinReasoningEffort || '(default)'} elapsedMs=${Date.now() - run.startedAt}`)
    } else if (event?.type === 'tool/call') {
      if (!Number.isSafeInteger(run.promptTurn) || run.promptTurn !== event.data?.turn) return
      if (event.data?.name !== 'present') return
      for (const path of presentedPaths(event.data?.arguments)) {
        const absolute = resolvePresented(run.cwd, path)
        if (absolute && run.files.length < MAX_FILES && !run.files.includes(absolute)) run.files.push(absolute)
      }
    } else if (event?.type === 'assistant/message') {
      if (!Number.isSafeInteger(run.promptTurn) || run.promptTurn !== event.data?.turn || event.data?.interrupted) return
      const text = textOf(event.data?.message)
      if (text) run.final = text.slice(0, MAX_REPLY)
    } else if (event?.type === 'turn/end' && Number.isSafeInteger(run.promptTurn) && run.promptTurn === event.data?.turn) {
      const kind = event.data?.reason?.kind
      if (kind === 'completed' && run.delivery === 'qq' && run.qqDelivery === 'none') {
        run.finish({ status: 'failed', error: 'DSH 未通过 QQ 发送请求内容。' })
        return
      }
      if (kind === 'completed' && run.delivery === 'qq' && run.files.some((path) => !run.qqFiles.has(path))) {
        run.finish({ status: 'failed', error: 'DSH 尚未通过 QQ 发送全部请求的文件。' })
        return
      }
      if (kind === 'completed' && (run.final || run.files.length || run.qqDelivery !== 'none')) {
        run.finish({ status: 'completed', final: run.final,
          files: run.files.filter((path) => !run.qqFiles.has(path)), qqDelivery: run.qqDelivery })
      }
      else run.finish({ status: 'failed', error: `Desktop turn ended: ${kind ?? 'unknown'} without final text` })
    }
  }, { global: true })

  const detachQqResults = ctx.on('tools/result', (execution, result) => {
    const run = pending.get(execution.agent?.session.id)
    if (!run || run.done || !Number.isSafeInteger(run.promptTurn)
        || execution.name !== 'notify_qq' || result.isError !== false) return
    // The rendered tool card only says "notify QQ", even when delivery was queued.
    // Read the actual plugin result so a queued notification is never called delivered.
    const detail = result.value?.detail
    const status = typeof detail !== 'string' ? 'accepted'
      : detail.includes('已排队') ? 'queued' : detail.includes('已发送') ? 'sent' : 'accepted'
    const severity = { none: 0, sent: 1, accepted: 2, queued: 3 }
    if (severity[status] > severity[run.qqDelivery]) run.qqDelivery = status
    if (typeof execution.arguments?.path === 'string') {
      const path = resolvePresented(run.cwd, execution.arguments.path)
      if (path) run.qqFiles.add(path)
    }
  }, { global: true })

  async function openSession(stream, cwd) {
    const live = streams.get(stream)
    if (live && ctx.sessions.get(live) && ctx.agents.get(live)) return live
    const created = await ctx.sessionController.create({ sessionId: `maibot-${randomUUID()}`, cwd })
    const session = ctx.sessions.get(created.sessionId)
    if (!session) throw new Error('Desktop session unavailable')
    // This channel never elevates: the session is pinned to the workspace-write
    // preset and every approval ask resolves as a rejection.
    ctx.permissionPresets.set(session, 'workspace-write')
    if (ctx.permissionPresets.current(session) !== 'workspace-write') {
      throw new Error('workspace-write could not be enforced')
    }
    session.append('approval/policy', { policy: 'never' })
    if (ctx.approval.overrideOf(session) !== 'never') throw new Error('approval denial could not be enforced')
    streams.set(stream, created.sessionId)
    return created.sessionId
  }

  async function runTask(input, res) {
    if (typeof input?.sender !== 'string' || input.sender !== senderId
        || typeof input.task !== 'string' || !input.task.trim() || input.task.length > 24000
        || typeof input.stream !== 'string' || !STREAM.test(input.stream)
        || !Number.isInteger(input.timeout) || input.timeout < MIN_TIMEOUT || input.timeout > MAX_TIMEOUT) {
      return { status: 'failed', error: 'invalid request' }
    }
    if (closing) return { status: 'failed', error: 'Desktop bridge unloaded' }
    if (busy) return { status: 'failed', error: 'Desktop bridge busy' }
    busy = true

    let sessionId
    let run
    let releaseQqRestriction
    let abortReason
    const abort = (reason) => {
      abortReason = reason
      if (sessionId) cancelSession(sessionId)
      run?.finish({ status: 'failed', error: reason })
    }
    const onClose = () => {
      if (!run?.done) abort('client disconnected')
    }
    const timer = setTimeout(() => abort('Desktop task timed out'), input.timeout * 1000)
    res.on('close', onClose)

    try {
      const cwd = forcedCwd ?? join(realRoot, input.stream)
      if (forcedCwd === undefined) {
        mkdirSync(cwd, { recursive: true })
        if (realpathSync.native(cwd) !== cwd) throw new Error('workspace path is not a real directory')
      }

      if (forcedCwd === undefined) await ensureWorkspaceRules(cwd, BRIDGE_RULES)
      sessionId = await openSession(input.stream, cwd)
      const recipient = await ctx.sessionController.agents.resolveAgent(sessionId)
      if ('error' in recipient) throw recipient.error
      if (input.delivery !== 'qq') {
        const deny = ['notify_qq', 'send_to_qq'].filter((name) => ctx.tools.get(name) !== undefined)
        if (deny.length) releaseQqRestriction = recipient.agent.ctx.tools.restrict({ deny })
      }
      // Apply the configured effort on every request, including reused sessions.
      if (pinProvider && pinModel) {
        // Session-local selection only: selectForNextRequest records the choice on
        // this session and never rewrites the host-wide default model.
        const resolved = await ctx.llm.resolveCallConfig({
          provider: pinProvider,
          model: pinModel,
          ...(pinReasoningEffort ? { reasoningEffort: pinReasoningEffort } : {}),
        })
        if (pinReasoningEffort && resolved.reasoningEffort !== pinReasoningEffort) {
          throw new Error(`Bridge reasoning effort mismatch: requested=${pinReasoningEffort} resolved=${resolved.reasoningEffort}`)
        }
        const target = await ctx.sessionController.agents.resolveAgent(sessionId)
        if ('error' in target) throw target.error
        ctx.sessionController.agents.selectForNextRequest(target.agent, {
          provider: resolved.provider,
          model: resolved.model,
          ...resolved.reasoningEffort === undefined ? {} : { reasoningEffort: resolved.reasoningEffort },
        })
      }
      if (closing || abortReason) {
        if (closing) abortReason = 'Desktop bridge unloaded'
        cancelSession(sessionId)
        return { status: 'failed', error: abortReason, sessionId }
      }

      const result = new Promise((settle) => {
        run = {
          final: '',
          files: [],
          qqDelivery: 'none',
          delivery: input.delivery ?? 'maibot',
          qqFiles: new Set(),
          startedAt: Date.now(),
          cwd,
          requestId: `maibot-${randomUUID()}`,
          done: false,
          finish(value) {
            if (run.done) return
            run.done = true
            pending.delete(sessionId)
            settle({ ...value, sessionId })
          },
        }
      })
      pending.set(sessionId, run)

      void Promise.resolve().then(async () => {
        if (run.done || closing || abortReason) return
        await ctx.sessionController.prompt({
          content: [{ type: 'text', text: `${TASK_PREFIX} delivery=${input.delivery ?? 'maibot'}\n${input.task}` }],
          requestId: run.requestId,
          sessionId,
          mode: 'queue',
        }, new AbortController().signal)
        if (run.done || closing || abortReason) cancelSession(sessionId)
      }).catch((error) => {
        streams.delete(input.stream)
        cancelSession(sessionId)
        run.finish({ status: 'failed', error: `Desktop rejected prompt: ${String(error)}` })
      })

      return await result
    } finally {
      clearTimeout(timer)
      releaseQqRestriction?.()
      res.off('close', onClose)
      if (sessionId) pending.delete(sessionId)
      busy = false
    }
  }

  const tasks = new AsyncTasks(join(state, 'tasks'), runTask, ctx.logger)
  await tasks.initialize()

  const server = createServer((req, res) => {
    void (async () => {
      const remote = req.socket.remoteAddress
      if (remote !== HOST && remote !== `::ffff:${HOST}`) return reply(res, 403, { status: 'failed' })
      const route = req.url?.split('?')[0]
      const supplied = req.headers.authorization?.replace(/^Bearer /, '') ?? ''
      const candidate = Buffer.from(supplied, 'ascii')
      if (candidate.length !== expected.length || !timingSafeEqual(candidate, expected)) {
        return reply(res, 401, { status: 'failed' })
      }
      if (closing) return reply(res, 503, { status: 'failed', error: 'Desktop bridge unloaded' })
      if (req.method === 'GET' && route === `${BASE}/notifications`) {
        return reply(res, 200, { notification: tasks.notification() })
      }
      const ack = route?.match(/^\/\.maibot-desktop-bridge\/notifications\/([a-f0-9-]+)\/ack$/)
      if (req.method === 'POST' && ack) {
        const accepted = await tasks.acknowledge(ack[1])
        return reply(res, accepted ? 200 : 409, { acknowledged: accepted })
      }
      if (req.method !== 'POST' || route !== ROUTE) return reply(res, 404, { status: 'failed' })
      let input
      try {
        input = await readJson(req)
      } catch {
        return reply(res, 400, { status: 'failed', error: 'invalid JSON or oversized request' })
      }
      if (input?.sender !== senderId || typeof input.task !== 'string' || !input.task.trim()
          || input.task.length > 24000 || typeof input.stream !== 'string' || !STREAM.test(input.stream)
          || typeof input.streamId !== 'string' || !input.streamId || input.streamId.length > 256
          || !Number.isInteger(input.timeout) || input.timeout < MIN_TIMEOUT || input.timeout > MAX_TIMEOUT
          || (input.delivery !== undefined && !['maibot', 'qq', 'to_assistant'].includes(input.delivery))
          || !input.context || typeof input.context !== 'object' || Array.isArray(input.context)
          || Object.keys(input.context).some((key) => !['user_id', 'group_id', 'platform'].includes(key))
          || Object.values(input.context).some((value) => typeof value !== 'string' || value.length > 128)) {
        return reply(res, 400, { status: 'failed', error: 'invalid request' })
      }
      const accepted = await tasks.submit(input)
      if (!res.destroyed) reply(res, 202, accepted)
    })().catch((error) => {
      if (!res.destroyed) reply(res, 500, { status: 'failed', error: String(error) })
    })
  })
  server.keepAliveTimeout = 5000

  await new Promise((settle, fail) => {
    server.once('error', fail)
    server.listen(port, HOST, () => {
      server.off('error', fail)
      settle()
    })
  })
  const bound = server.address()
  if (typeof bound === 'string' || !bound) throw new Error('Desktop bridge bound an unexpected address')
  writeEndpoint(endpointFile, { version: 2, host: HOST, port: bound.port, pid: process.pid })

  ctx.effect(() => () => {
    closing = true
    tasks.close()
    detachEvents()
    detachQqResults()
    server.close()
    rmSync(endpointFile, { force: true })
    streams.clear()
    for (const [sessionId, run] of pending) {
      cancelSession(sessionId)
      run.finish({ status: 'failed', error: 'Desktop bridge unloaded' })
    }
  })

  tasks.start()
  ctx.logger?.info?.(`maibot desktop bridge v13 listening on ${HOST}:${bound.port}`
    + ` cwd=${forcedCwd ?? '(per-stream)'} model=${pinProvider ? `${pinProvider}/${pinModel}` : '(default)'} effort=${pinReasoningEffort || '(default)'}`)
}
