import { expect, test } from 'bun:test'
import { spawn } from 'node:child_process'
import { Database } from 'bun:sqlite'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { once } from 'node:events'

interface WorkerMessage {
  readonly type: string
  readonly executionId?: string
  readonly count?: number
}

function worker(
  directory: string,
  mode: string,
  options: { namespace?: string; url?: string } = {}
) {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    WORKFLOW_DATABASE: join(directory, 'schedule.sqlite'),
    WORKFLOW_MODE: mode
  }
  if (options.namespace !== undefined) env.WORKFLOW_NAMESPACE = options.namespace
  if (options.url !== undefined) env.WORKFLOW_DATABASE_URL = options.url
  const child = spawn(process.execPath, ['tests/fixtures/schedule-crash-worker.ts'], {
    cwd: process.cwd(),
    env,
    stdio: ['ignore', 'pipe', 'pipe', 'ipc']
  })

  let logs = ''
  child.stdout!.on('data', (chunk) => {
    logs += String(chunk)
  })
  child.stderr!.on('data', (chunk) => {
    logs += String(chunk)
  })
  const message = new Promise<WorkerMessage>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`Schedule worker timed out: ${logs}`))
    }, 15_000)
    child.once('message', (value) => {
      clearTimeout(timeout)
      // SAFETY: both ends of this IPC channel use the fixture's response protocol.
      resolve(value as WorkerMessage)
    })
    child.once('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })
    child.once('exit', (code, signal) => {
      clearTimeout(timeout)
      if (code !== 0 && signal !== 'SIGKILL')
        reject(new Error(`Schedule worker exited ${code ?? 'null'} ${logs}`))
    })
  })
  return { child, message }
}

const postgresUrl = process.env['WORKFLOWS_TEST_POSTGRES_URL']

test.skipIf(!postgresUrl)(
  'competing PostgreSQL scheduler processes materialize one occurrence',
  async () => {
    if (!postgresUrl) return
    const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-postgres-'))
    const namespace = `schedule-pg-${process.pid}-${Date.now()}`
    const prepare = worker(directory, 'prepare', { namespace, url: postgresUrl })
    let first: ReturnType<typeof worker> | undefined
    let second: ReturnType<typeof worker> | undefined
    try {
      expect((await prepare.message).type).toBe('ready')
      first = worker(directory, 'observe', { namespace, url: postgresUrl })
      second = worker(directory, 'observe', { namespace, url: postgresUrl })
      const observed = await Promise.all([first.message, second.message])
      expect(observed.every((message) => message.type === 'observed')).toBe(true)
      expect(new Set(observed.map((message) => message.executionId)).size).toBe(1)
      expect(observed.map((message) => message.count)).toEqual([1, 1])
    } finally {
      prepare.child.kill('SIGKILL')
      first?.child.kill('SIGKILL')
      second?.child.kill('SIGKILL')
      await rm(directory, { recursive: true, force: true })
    }
  },
  30_000
)

test('a scheduler restart after a committed occurrence does not duplicate its execution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-crash-'))
  const prepare = worker(directory, 'prepare')
  let first: ReturnType<typeof worker> | undefined
  let second: ReturnType<typeof worker> | undefined
  try {
    expect((await prepare.message).type).toBe('ready')
    first = worker(directory, 'wait')
    const checkpoint = await first.message
    expect(checkpoint.type).toBe('checkpoint')
    expect(checkpoint.executionId).toBeString()
    const firstExit = once(first.child, 'exit')
    first.child.kill('SIGKILL')
    expect((await firstExit)[1]).toBe('SIGKILL')
    second = worker(directory, 'recover')
    const recovered = await second.message
    expect(recovered.type).toBe('complete')
    expect(recovered.executionId).toBe(checkpoint.executionId)
    expect(recovered.count).toBe(1)
    const database = new Database(join(directory, 'schedule.sqlite'))
    try {
      expect(
        database
          .query<{ occurrences: number; executions: number }, []>(
            `SELECT
               (SELECT COUNT(*) FROM better_workflows_schedule_occurrences) AS occurrences,
               (SELECT COUNT(*) FROM better_workflows_runs) AS executions`
          )
          .get()
      ).toEqual({ occurrences: 1, executions: 1 })
    } finally {
      database.close()
    }
  } finally {
    prepare.child.kill('SIGKILL')
    first?.child.kill('SIGKILL')
    second?.child.kill('SIGKILL')
    await rm(directory, { recursive: true, force: true })
  }
}, 20_000)

test('competing scheduler processes materialize one occurrence and one execution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-schedule-concurrency-'))
  const prepare = worker(directory, 'prepare')
  let first: ReturnType<typeof worker> | undefined
  let second: ReturnType<typeof worker> | undefined
  try {
    expect((await prepare.message).type).toBe('ready')
    first = worker(directory, 'observe')
    second = worker(directory, 'observe')
    const observed = await Promise.all([first.message, second.message])
    expect(observed.every((message) => message.type === 'observed')).toBe(true)
    expect(new Set(observed.map((message) => message.executionId)).size).toBe(1)
    expect(observed.map((message) => message.count)).toEqual([1, 1])
    const database = new Database(join(directory, 'schedule.sqlite'))
    try {
      expect(
        database.query('SELECT COUNT(*) AS count FROM better_workflows_schedule_occurrences').get()
      ).toEqual({ count: 1 })
      expect(database.query('SELECT COUNT(*) AS count FROM better_workflows_runs').get()).toEqual({
        count: 1
      })
    } finally {
      database.close()
    }
  } finally {
    prepare.child.kill('SIGKILL')
    first?.child.kill('SIGKILL')
    second?.child.kill('SIGKILL')
    await rm(directory, { recursive: true, force: true })
  }
}, 20_000)
