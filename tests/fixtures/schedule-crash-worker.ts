import 'reflect-metadata'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import { Interval, Workflow, WorkflowsAdmin, WorkflowsModule } from '../../src'
import { sqlite } from '../../src/sqlite'
import { postgres } from '../../src/postgres'
import { WorkflowsTestingModule } from '../../src/testing'

const filename = process.env['WORKFLOW_DATABASE']!
const mode = process.env['WORKFLOW_MODE'] ?? 'wait'
const namespace = process.env['WORKFLOW_NAMESPACE'] ?? 'schedule-crash-tests'
const connectionString = process.env['WORKFLOW_DATABASE_URL']
const storage = connectionString
  ? postgres({ connectionString, maxConnections: 4 })
  : sqlite({ filename, runtime: 'node' })
const Input = z.object({ id: z.string() })

@Interval({ name: 'crash.schedule', every: '1h', input: { id: 'schedule-crash' } })
@Workflow({
  name: 'crash.schedule-workflow',
  version: 1,
  input: Input,
  output: z.string(),
  idempotencyKey: (input) => input.id
})
class ScheduleCrashWorkflow {
  async run(input: z.infer<typeof Input>): Promise<string> {
    return input.id
  }
}

const options = {
  namespace,
  storage,
  pollInterval: '10ms' as const,
  lease: { duration: '500ms' as const, refreshInterval: '100ms' as const },
  execution: {
    workflows: { enabled: false },
    activities: { enabled: false },
    schedules: { enabled: mode !== 'prepare' }
  }
}
const root =
  mode === 'prepare'
    ? WorkflowsTestingModule.forRoot({
        ...options,
        // The prepared cursor is just ahead of the real clock, so observers do not
        // create a historical misfire backlog while they compete for one deadline.
        initialTime: Date.now() - 60 * 60 * 1_000 + 1_000
      })
    : WorkflowsModule.forRoot(options)

@Module({
  imports: [
    root,
    WorkflowsModule.forFeature({ name: 'schedule-crash', workflows: [ScheduleCrashWorkflow] })
  ]
})
class AppModule {}

const sleep = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds))

async function untilOccurrence(admin: WorkflowsAdmin) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    const page = await admin.listScheduleOccurrences('crash.schedule', { state: 'started' })
    if (page.occurrences.length > 0) return page.occurrences[0]!
    await sleep(10)
  }
  throw new Error('Schedule occurrence was not materialized before the test deadline')
}

const app = await NestFactory.createApplicationContext(AppModule, { logger: false })
const admin = app.get(WorkflowsAdmin)

try {
  if (mode === 'prepare') {
    process.send?.({ type: 'ready' })
    await new Promise(() => undefined)
  } else if (mode === 'wait') {
    const occurrence = await untilOccurrence(admin)
    process.send?.({
      type: 'checkpoint',
      executionId: occurrence.executionId,
      count: (await admin.listScheduleOccurrences('crash.schedule', { state: 'started' }))
        .occurrences.length
    })
    await new Promise(() => undefined)
  } else {
    const occurrence = await untilOccurrence(admin)
    process.send?.({
      type: mode === 'observe' ? 'observed' : 'complete',
      executionId: occurrence.executionId,
      count: (await admin.listScheduleOccurrences('crash.schedule', { state: 'started' }))
        .occurrences.length
    })
    await app.close()
    process.disconnect?.()
  }
} catch (error) {
  console.error(error)
  process.exitCode = 1
  await app.close().catch(() => undefined)
  process.disconnect?.()
}
