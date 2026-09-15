import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { Type } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { WorkflowsModule, getWorkflowToken } from '../src'
import type { WorkflowClass, WorkflowsOptions, WorkflowClient } from '../src'
import { sqlite } from '../src/sqlite'

export interface TestAppOptions {
  readonly providers?: readonly Type[]
  readonly execution?: WorkflowsOptions['execution']
  readonly queues?: WorkflowsOptions['queues']
  readonly filename?: string
}

export async function testApp<W extends WorkflowClass>(workflow: W, options: TestAppOptions = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-'))
  const root: WorkflowsOptions = {
    namespace: 'integration',
    storage: sqlite({ filename: options.filename ?? join(directory, 'workflows.sqlite') }),
    queues: options.queues ?? { work: { concurrency: 2 } },
    pollInterval: '20ms',
    lease: { duration: '1500ms', refreshInterval: '400ms' }
  }
  const configured = options.execution ? { ...root, execution: options.execution } : root
  const module = await Test.createTestingModule({
    imports: [WorkflowsModule.forRoot(configured), WorkflowsModule.forFeature([workflow])],
    providers: [workflow, ...(options.providers ?? [])]
  }).compile()
  try {
    await module.init()
  } catch (error) {
    await module.close()
    await rm(directory, { recursive: true, force: true })
    throw error
  }
  return {
    module,
    directory,
    filename: root.storage.driver === 'sqlite' ? root.storage.filename : '',
    client: module.get<WorkflowClient<W>>(getWorkflowToken(workflow)),
    async close() {
      await module.close()
      await rm(directory, { recursive: true, force: true })
    }
  }
}

export async function eventually<T>(
  read: () => Promise<T>,
  predicate: (value: T) => boolean,
  timeout = 5000
): Promise<T> {
  const until = Date.now() + timeout
  while (true) {
    const value = await read()
    if (predicate(value)) return value
    if (Date.now() >= until) throw new Error('Condition did not become true')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
