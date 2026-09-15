import { test, expect } from 'bun:test'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { readDeadlines, expireDeadlines } from './deadline-snapshot'

interface Message {
  type: string
  executionId: string
  created?: boolean
  result?: string
}
interface Call {
  id: string
  attempt: number
  key: string
}

function worker(directory: string, scenario: string, mode: string) {
  const child = spawn(process.execPath, ['tests/fixtures/structured-crash-worker.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      WORKFLOW_DATABASE: join(directory, 'db.sqlite'),
      WORKFLOW_LOG: join(directory, 'calls.jsonl'),
      WORKFLOW_SCENARIO: scenario,
      WORKFLOW_MODE: mode
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc']
  })
  let logs = ''
  child.stdout!.on('data', (chunk) => {
    logs += String(chunk)
  })
  child.stderr!.on('data', (chunk) => {
    logs += String(chunk)
  })
  const message = new Promise<Message>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`Worker timed out: ${logs}`))
    }, 12000)
    child.once('message', (value) => {
      clearTimeout(timeout)
      // Safety: both ends of this IPC channel use the fixture's Message protocol.
      resolve(value as Message)
    })
    child.once('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })
    child.once('exit', (code, signal) => {
      clearTimeout(timeout)
      if (code !== 0 && signal !== 'SIGKILL') reject(new Error(`Worker exited ${code}: ${logs}`))
    })
  })
  return { child, message }
}

for (const scenario of ['map', 'child', 'saga']) {
  test(`SIGKILL recovers structured ${scenario} without duplicating completed effects`, async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bw-structured-crash-'))
    const first = worker(dir, scenario, 'wait')
    let second: ReturnType<typeof worker> | undefined
    try {
      const checkpoint = await first.message
      expect(checkpoint.type).toBe('checkpoint')
      const exited = once(first.child, 'exit')
      first.child.kill('SIGKILL')
      expect((await exited)[1]).toBe('SIGKILL')
      const deadlines = readDeadlines(join(dir, 'db.sqlite'), checkpoint.executionId)
      if (scenario === 'saga') {
        expect(deadlines).toHaveLength(1)
        expect(deadlines[0]).toMatchObject({ kind: 'retry', attempt: 1 })
        await expireDeadlines(deadlines)
        expect(deadlines[0]!.deadline).toBeLessThanOrEqual(Date.now())
      }
      second = worker(dir, scenario, 'recover')
      const complete = await second.message
      expect(complete.type).toBe('complete')
      expect(complete.executionId).toBe(checkpoint.executionId)
      expect(complete.created).toBe(false)
      expect(complete.result).toBe(
        scenario === 'map' ? 'a,b,c,d' : scenario === 'child' ? 'structured-id' : 'ORDER_FAILED'
      )
      const calls: Call[] = (await readFile(join(dir, 'calls.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      if (scenario === 'saga') {
        // Recovery must reuse the original due instant, regardless of process startup speed.
        expect(readDeadlines(join(dir, 'db.sqlite'), checkpoint.executionId)).toEqual(
          deadlines.map((deadline) => ({ ...deadline, delivered: 1 }))
        )
        expect(calls.map((call) => call.id)).toEqual([
          'forward-a',
          'forward-b',
          'undo-b',
          'undo-b',
          'undo-a'
        ])
        const retried = calls.filter((call) => call.id === 'undo-b')
        expect(retried.map((call) => call.attempt)).toEqual([1, 2])
        expect(new Set(retried.map((call) => call.key)).size).toBe(1)
      } else {
        expect(calls.map((call) => call.id).sort()).toEqual(
          scenario === 'map' ? ['a', 'b', 'c', 'd'] : ['structured-id']
        )
        expect(calls.every((call) => call.attempt === 1)).toBe(true)
      }
    } finally {
      first.child.kill('SIGKILL')
      second?.child.kill('SIGKILL')
      await rm(dir, { recursive: true, force: true })
    }
  }, 20000)
}
