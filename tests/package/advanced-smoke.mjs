import assert from 'node:assert/strict'
import { fork } from 'node:child_process'
import { once } from 'node:events'
import { randomUUID } from 'node:crypto'
import { mkdtemp, rm, readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createServer } from 'node:net'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { WorkflowsModule, getWorkflowToken } from 'better-workflows'
import { createWorkflowsAdmin } from 'better-workflows/admin'
import { sqlite } from 'better-workflows/sqlite'
import { postgres } from 'better-workflows/postgres'
import { Batch, Child, Even, Odd, queues, setAudit } from './advanced-contracts.mjs'

const distribution = new URL('../../dist/', import.meta.url)
const declarations = (await readdir(distribution)).filter((name) => name.endsWith('.d.mts'))
assert.ok(declarations.length > 0)
for (const name of declarations) {
  assert.doesNotMatch(
    await readFile(new URL(name, distribution), 'utf8'),
    /from\s+['"](?:effect|@effect\/)/u,
    `${name} must not expose private engine types`
  )
}

const url = process.env.WORKFLOWS_TEST_POSTGRES_URL
const dir = await mkdtemp(join(tmpdir(), 'bw-advanced-'))
const namespace = `advanced-${randomUUID()}`
const storage = url
  ? postgres({ connectionString: url })
  : sqlite({ filename: join(dir, 'db.sqlite'), runtime: 'node' })
const events = []
setAudit((event) => events.push(event))
const workers = []
let app
const admin = await createWorkflowsAdmin({ namespace, storage })
async function startWorker(kind) {
  const child = fork(new URL('./distributed-worker.mjs', import.meta.url), [], {
    env: { ...process.env, WORKFLOW_NAMESPACE: namespace, WORKFLOW_WORKER: kind },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc']
  })
  workers.push(child)
  let log = ''
  child.stdout.on('data', (chunk) => {
    log += String(chunk)
  })
  child.stderr.on('data', (chunk) => {
    log += String(chunk)
  })
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Worker bootstrap timed out: ${log}`)), 20000)
    child.on('message', (value) => {
      if (value.event === 'ready') {
        clearTimeout(timeout)
        resolve()
      } else events.push(value)
    })
    child.once('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })
    child.once('exit', (code) => {
      clearTimeout(timeout)
      if (code !== 0) reject(new Error(log))
    })
  })
}
async function port() {
  const server = createServer()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const value = server.address().port
  await new Promise((resolve) => server.close(resolve))
  return value
}
try {
  assert.equal((await admin.migrations.run()).valid, true)
  assert.equal((await admin.migrations.validate()).valid, true)
  if (url) await Promise.all([startWorker('even'), startWorker('odd')])
  const options = {
    namespace,
    storage,
    migrations: 'validate',
    pollInterval: '20ms',
    lease: { duration: '3s', refreshInterval: '800ms' }
  }
  if (url) {
    options.topology = 'distributed'
    options.cluster = { address: { host: '127.0.0.1', port: await port() } }
    options.execution = { activities: { enabled: false } }
  }
  class App {}
  Module({
    imports: [
      WorkflowsModule.forRoot(options),
      WorkflowsModule.forFeature({
        name: 'advanced',
        queues,
        workflows: [Batch, Child],
        activities: url ? [] : [Even, Odd],
        activityContracts: url ? [Even, Odd] : []
      })
    ]
  })(App)
  app = await NestFactory.createApplicationContext(App, { logger: false, abortOnError: false })
  const client = app.get(getWorkflowToken(Batch))
  const handle = await client.start('one')
  assert.deepEqual(
    await handle.result({ timeout: '45s' }),
    Array.from({ length: 12 }, (_, index) => index * 2)
  )
  assert.equal(events.length, 24)
  assert.equal(
    new Set(events.filter((event) => event.event === 'start').map((event) => event.index)).size,
    12
  )
  if (url)
    assert.equal(
      new Set(events.map((event) => event.pid)).size,
      2,
      'both OS worker processes must execute'
    )
  // All child processes use this host's wall clock. End-before-start resolves millisecond ties.
  events.sort((a, b) => a.at - b.at || (a.event === b.event ? 0 : a.event === 'end' ? -1 : 1))
  let active = 0
  let peak = 0
  const keys = new Map()
  for (const event of events) {
    const delta = event.event === 'start' ? 1 : -1
    active += delta
    peak = Math.max(peak, active)
    const keyed = (keys.get(event.tenant) ?? 0) + delta
    keys.set(event.tenant, keyed)
    assert.ok(active >= 0 && active <= 2, `global active=${active}`)
    assert.ok(keyed >= 0 && keyed <= 1, `key ${event.tenant} active=${keyed}`)
  }
  assert.equal(active, 0)
  assert.equal(peak, 2)
  assert.equal((await client.start('one')).created, false)
  await new Promise((resolve) => setTimeout(resolve, 150))
  const plan = await admin.retention.preview({ before: new Date(Date.now() - 20).toISOString() })
  assert.equal(plan.blocked.length, 0)
  assert.equal(plan.candidates.length, 13)
  assert.equal((await admin.retention.prune(plan, { confirm: true })).deleted, 13)
  await assert.rejects(client.start('one'), { code: 'EXECUTION_PRUNED' })
  console.log(
    `${process.version}: ${url ? 'PostgreSQL with TWO OS worker processes' : 'SQLite'} advanced map/children/global/per-key/migrations/retention smoke passed`
  )
} finally {
  await app?.close()
  for (const worker of workers) {
    if (worker.exitCode !== null || worker.signalCode !== null) continue
    const exited = once(worker, 'exit')
    const timeout = setTimeout(() => worker.kill('SIGKILL'), 3000)
    if (worker.connected) worker.send('stop')
    else worker.kill('SIGKILL')
    await exited
    clearTimeout(timeout)
  }
  await admin.close()
  await rm(dir, { recursive: true, force: true })
}
