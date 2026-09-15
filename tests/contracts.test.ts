import { expect, test } from 'bun:test'
import { Inject, Injectable, Module, Scope } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { Cause, Effect, Exit } from 'effect'
import { ConnectionError, SqlError } from 'effect/unstable/sql/SqlError'
import { z } from 'zod'
import {
  defineQueue,
  Activities,
  Activity,
  Workflow,
  WorkflowsModule,
  getWorkflowToken
} from '../src'
import type { WorkflowClient } from '../src'
import { sqlite } from '../src/sqlite'
import { promised } from '../src/internal/effects'

@Workflow({
  name: 'versioned',
  version: 1,
  input: z.string(),
  output: z.number(),
  idempotencyKey: (value) => value
})
class VersionOne {
  async run(_input: string): Promise<number> {
    return 1
  }
}

@Workflow({
  name: 'versioned',
  version: 2,
  input: z.string(),
  output: z.string(),
  idempotencyKey: (value) => value
})
class VersionTwo {
  async run(_input: string): Promise<string> {
    return 'two'
  }
}

test('version upgrades preserve deduplication but cannot reinterpret a prior output type', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'versions',
        storage: sqlite({ filename: ':memory:' }),
        queues: [],
        pollInterval: '20ms'
      }),
      WorkflowsModule.forFeature({ name: 'versions', workflows: [VersionOne, VersionTwo] })
    ]
  }).compile()
  try {
    await app.init()
    const one = app.get<WorkflowClient<typeof VersionOne>>(getWorkflowToken(VersionOne))
    const two = app.get<WorkflowClient<typeof VersionTwo>>(getWorkflowToken(VersionTwo))
    const original = await one.start('order-1')
    expect(await original.result({ timeout: '3s' })).toBe(1)
    const existing = await two.start('order-1')
    expect(existing.created).toBe(false)
    expect(existing.executionId).toBe(original.executionId)
    expect((await existing.describe()).version).toBe(1)
    await expect(existing.result()).rejects.toMatchObject({ code: 'RESULT_VERSION_MISMATCH' })
    expect(await (await two.start('order-2')).result({ timeout: '3s' })).toBe('two')
  } finally {
    await app.close()
  }
})

@Injectable()
class Settings {
  readonly namespace = 'async-root'
}
@Module({ providers: [Settings], exports: [Settings] })
class SettingsModule {}

test('forRootAsync resolves dependencies from imported Nest modules', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRootAsync({
        imports: [SettingsModule],
        inject: [Settings],
        useFactory: async (settings: Settings) => ({
          namespace: settings.namespace,
          storage: sqlite({ filename: ':memory:' }),
          queues: []
        })
      }),
      WorkflowsModule.forFeature({ name: 'versions', workflows: [VersionOne] })
    ]
  }).compile()
  try {
    await app.init()
    const client = app.get<WorkflowClient<typeof VersionOne>>(getWorkflowToken(VersionOne))
    expect(await (await client.start('configured')).result({ timeout: '3s' })).toBe(1)
  } finally {
    await app.close()
  }
})

@Injectable({ scope: Scope.REQUEST })
class RequestState {}
@Activities()
class RequestActivity {
  constructor(@Inject(RequestState) readonly request: RequestState) {}
  @Activity({
    name: 'request-dependent',
    version: 1,
    queue: defineQueue('work'),
    input: z.string(),
    output: z.string()
  })
  async execute(input: string): Promise<string> {
    return input
  }
}

test('request-scoped dependency trees fail at bootstrap instead of losing their request during replay', async () => {
  const app = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        namespace: 'scopes',
        storage: sqlite({ filename: ':memory:' })
      }),
      WorkflowsModule.forFeature({
        name: 'scoped',
        activities: [RequestActivity],
        providers: [RequestState],
        queues: [{ queue: defineQueue('work'), concurrency: 1 }]
      })
    ]
  }).compile()
  try {
    await expect(app.init()).rejects.toMatchObject({ code: 'UNSUPPORTED_SCOPE' })
  } finally {
    await app.close().catch((error) => {
      // Nest rethrows a failed initialization from close; no runtime was opened.
      expect(error).toMatchObject({ code: 'UNSUPPORTED_SCOPE' })
    })
  }
})

test('SQL infrastructure errors crossing an async callback remain defects, not business results', async () => {
  const error = new SqlError({
    reason: new ConnectionError({ cause: new Error('connection lost') })
  })
  const exit = await Effect.runPromiseExit(
    promised(async () => {
      throw error
    })
  )
  expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(error)
})
