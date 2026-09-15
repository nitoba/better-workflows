import { expect, test } from 'bun:test'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { once } from 'node:events'

function worker(directory: string, scenario: string, mode: string) {
  const child = spawn(process.execPath, ['tests/fixtures/crash-worker.ts'], {
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
  const message = new Promise<{
    type: string
    executionId: string
    created?: boolean
    result?: string
  }>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`Worker timed out: ${logs}`))
    }, 12000)
    child.once('message', (value) => {
      clearTimeout(timeout)
      resolve(value as { type: string; executionId: string; created?: boolean; result?: string })
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

for (const scenario of ['signal', 'timer', 'retry']) {
  test(`SIGKILL recovery preserves ${scenario} state and completed activity results`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'workflow-crash-'))
    const first = worker(directory, scenario, 'wait')
    let second: ReturnType<typeof worker> | undefined
    try {
      const checkpoint = await first.message
      expect(checkpoint.type).toBe('checkpoint')
      const firstExit = once(first.child, 'exit')
      first.child.kill('SIGKILL')
      expect((await firstExit)[1]).toBe('SIGKILL')
      // Let the original persisted deadline expire while no process exists.
      if (scenario !== 'signal') await new Promise((resolve) => setTimeout(resolve, 2100))
      const start = Date.now()
      second = worker(directory, scenario, 'recover')
      const done = await second.message
      expect(done.type).toBe('complete')
      expect(done.executionId).toBe(checkpoint.executionId)
      expect(done.created).toBe(false)
      expect(done.result).toBe('crash-id')
      if (scenario !== 'signal') expect(Date.now() - start).toBeLessThan(1800)
      const calls = (await readFile(join(directory, 'calls.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      expect(calls.map((call) => call.attempt)).toEqual(scenario === 'retry' ? [1, 2] : [1])
      expect(new Set(calls.map((call) => call.key)).size).toBe(1)
    } finally {
      first.child.kill('SIGKILL')
      second?.child.kill('SIGKILL')
      await rm(directory, { recursive: true, force: true })
    }
  }, 20000)
}
