import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import { mkdir, readFile, readdir, rename, unlink, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const JOB_ID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/
const TERMINAL = new Set(['completed', 'failed'])
const MAX_JOBS = 200

/** Durable submission and notification queues; no HTTP response owns an agent run. */
export class AsyncTasks {
  constructor(directory, execute, logger) {
    this.directory = directory
    this.execute = execute
    this.logger = logger
    this.jobs = new Map()
    this.closed = false
    this.worker = undefined
    this.admission = Promise.resolve()
  }

  async save(job) {
    const file = join(this.directory, `${job.id}.json`)
    const temporary = `${file}.tmp`
    await writeFile(temporary, JSON.stringify(job), { encoding: 'utf8', mode: 0o600 })
    await rename(temporary, file)
  }

  async initialize() {
    await mkdir(this.directory, { recursive: true })
    for (const name of await readdir(this.directory)) {
      if (!name.endsWith('.json') || !JOB_ID.test(name.slice(0, -5))) continue
      const job = JSON.parse(await readFile(join(this.directory, name), 'utf8'))
      if (job.id !== name.slice(0, -5) || !['queued', 'running', 'completed', 'failed'].includes(job.status)) {
        throw new Error(`Invalid persisted bridge task: ${name}`)
      }
      // Never repeat an interrupted task: it may already have changed external state.
      if (job.status === 'running') {
        job.status = 'failed'
        job.result = { status: 'failed', error: 'DSH 宿主在任务执行期间重启，任务已中断。' }
        await this.save(job)
      }
      this.jobs.set(job.id, job)
    }
  }

  async submit(input) {
    // Serialize admission so concurrent submissions cannot exceed the durable queue bound.
    const admitted = this.admission.then(async () => {
      if (this.closed) throw new Error('Desktop bridge unloaded')
      if (this.jobs.size >= MAX_JOBS) throw new Error('Desktop bridge queue full')
      const job = { id: randomUUID(), createdAt: Date.now(), status: 'queued', input }
      await this.save(job)
      this.jobs.set(job.id, job)
      this.start()
      return { status: 'accepted', jobId: job.id }
    })
    this.admission = admitted.catch(() => {})
    return admitted
  }

  start() {
    if (this.closed || this.worker) return
    this.worker = this.drain().catch((error) => {
      // Storage failures stop the worker and remain visible; never pretend delivery succeeded.
      this.closed = true
      this.logger?.error?.(`maibot async task worker stopped: ${String(error)}`)
    }).finally(() => {
      this.worker = undefined
      if (!this.closed && [...this.jobs.values()].some((job) => job.status === 'queued')) this.start()
    })
  }

  async drain() {
    while (!this.closed) {
      const job = [...this.jobs.values()].filter((item) => item.status === 'queued')
        .sort((a, b) => a.createdAt - b.createdAt)[0]
      if (!job) return
      job.status = 'running'
      await this.save(job)
      let result
      try {
        // The run has its own timeout. Disconnecting the submitter never cancels it.
        result = await this.execute(job.input, new EventEmitter())
      } catch (error) {
        this.logger?.error?.(`maibot task ${job.id} failed: ${String(error)}`)
        result = { status: 'failed', error: 'DSH 任务执行失败，请检查宿主日志。' }
      }
      // Publish only after the terminal record is durable. Otherwise an ack could
      // delete the old file while the final rename recreates it behind the consumer.
      const completed = { ...job, result, status: result.status === 'completed' ? 'completed' : 'failed' }
      await this.save(completed)
      this.jobs.set(job.id, completed)
    }
  }

  notification() {
    const ready = [...this.jobs.values()].filter((item) => TERMINAL.has(item.status))
      .sort((a, b) => a.createdAt - b.createdAt)
    // A file that cannot be delivered must not block every other chat's results.
    const previous = ready.findIndex((item) => item.id === this.lastNotificationId)
    const job = ready[(previous + 1) % (ready.length || 1)]
    if (!job) return null
    this.lastNotificationId = job.id
    const qqDelivery = job.result.qqDelivery ?? 'none'
    return {
      jobId: job.id, streamId: job.input.streamId, context: job.input.context,
      status: job.status, final: qqDelivery === 'none' ? (job.result.final ?? '') : '', files: job.result.files ?? [],
      delivery: job.input.delivery ?? 'maibot', qqDelivery,
      request: job.input.task,
      error: job.status === 'failed' ? job.result.error : undefined,
    }
  }

  async acknowledge(id) {
    if (!JOB_ID.test(id)) return false
    const job = this.jobs.get(id)
    if (!job) return true // A repeated acknowledgement after a lost response is harmless.
    if (!TERMINAL.has(job.status)) return false
    try {
      await unlink(join(this.directory, `${id}.json`))
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
    this.jobs.delete(id)
    return true
  }

  close() {
    this.closed = true
  }
}
