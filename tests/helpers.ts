import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { Type } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { defineQueue, WorkflowsModule, getWorkflowToken } from '../src'
import type { WorkflowClass, WorkflowsOptions, WorkflowClient } from '../src'
import { sqlite } from '../src/sqlite'
import { WORKFLOW_METADATA, ACTIVITIES_METADATA } from '../src/decorators'

export interface TestAppOptions {
  readonly providers?: readonly Type[]
  readonly activityContracts?: readonly Type[]
  readonly execution?: WorkflowsOptions['execution']
  readonly observability?: WorkflowsOptions['observability']
  readonly queues?: WorkflowsOptions['queues']
  readonly deadLetter?: WorkflowsOptions['deadLetter']
  readonly filename?: string
}

export async function testApp<W extends WorkflowClass>(workflow: W, options: TestAppOptions = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-'))
  const root: WorkflowsOptions = {
    namespace: 'integration',
    storage: sqlite({ filename: options.filename ?? join(directory, 'workflows.sqlite') }),
    pollInterval: '20ms',
    lease: { duration: '1500ms', refreshInterval: '400ms' }
  }
  let configured: WorkflowsOptions = root
  if (options.execution) configured = { ...configured, execution: options.execution }
  if (options.observability) configured = { ...configured, observability: options.observability }
  if (options.deadLetter) configured = { ...configured, deadLetter: options.deadLetter }
  const module = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot(configured),
      WorkflowsModule.forFeature({
        name: 'integration',
        workflows: [
          ...new Set([
            workflow,
            ...(options.providers ?? []).filter((provider): provider is WorkflowClass =>
              Reflect.hasOwnMetadata(WORKFLOW_METADATA, provider)
            )
          ])
        ],
        activities: (options.providers ?? []).filter((provider) =>
          Reflect.hasOwnMetadata(ACTIVITIES_METADATA, provider)
        ),
        activityContracts: options.activityContracts ?? [],
        providers: (options.providers ?? []).filter(
          (provider) =>
            !Reflect.hasOwnMetadata(WORKFLOW_METADATA, provider) &&
            !Reflect.hasOwnMetadata(ACTIVITIES_METADATA, provider)
        ),
        queues: options.queues ?? [{ queue: defineQueue('work'), concurrency: 2 }]
      })
    ]
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
