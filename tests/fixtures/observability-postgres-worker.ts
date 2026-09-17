import 'reflect-metadata'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  Activity,
  Activities,
  defineQueue,
  getWorkflowToken,
  Workflow,
  WorkflowsModule
} from '../../src'
import type { WorkflowClient, WorkflowContext } from '../../src'
import { otlp } from '../../src/observability'
import { postgres } from '../../src/postgres'

const connectionString = process.env['WORKFLOWS_TEST_POSTGRES_URL']
const namespace = process.env['WORKFLOWS_TRACE_NAMESPACE']
const endpoint = process.env['WORKFLOWS_TRACE_ENDPOINT']
const role = process.env['WORKFLOWS_TRACE_ROLE']
const secret = process.env['WORKFLOWS_TRACE_SECRET']
const TraceQueue = defineQueue('observability-postgres-traces')

@Activities()
class PostgresTraceActivities {
  @Activity({
    name: 'observability.postgres.echo',
    version: 1,
    queue: TraceQueue,
    input: z.string(),
    output: z.string()
  })
  async echo(value: string): Promise<string> {
    return `processed:${value}`
  }
}

@Workflow({
  name: 'observability.postgres.workflow',
  version: 1,
  input: z.string(),
  output: z.string(),
  idempotencyKey: (value) => value
})
class PostgresTraceWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(PostgresTraceActivities).echo(input, { stepId: 'echo' })
  }
}

if (!connectionString || !namespace || !endpoint || !role)
  throw new Error('PostgreSQL tracing fixture requires connection, namespace, endpoint and role')

const root = WorkflowsModule.forRoot({
  namespace,
  storage: postgres({ connectionString, maxConnections: 4 }),
  queues: [{ queue: TraceQueue, concurrency: 1 }],
  execution:
    role === 'orchestrator'
      ? { workflows: { enabled: true }, activities: { enabled: false } }
      : { workflows: { enabled: false }, activities: { enabled: true } },
  observability: otlp({
    serviceName: `observability-${role}`,
    endpoint,
    traces: { exportInterval: '5ms' },
    metrics: false,
    logs: false,
    shutdownTimeout: '1s'
  })
})

const feature =
  role === 'orchestrator'
    ? WorkflowsModule.forFeature({
        name: 'observability-postgres-orchestrator',
        workflows: [PostgresTraceWorkflow],
        activityContracts: [PostgresTraceActivities]
      })
    : WorkflowsModule.forFeature({
        name: 'observability-postgres-worker',
        activities: [PostgresTraceActivities]
      })

@Module({ imports: [root, feature] })
class AppModule {}

try {
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false })
  if (role === 'orchestrator') {
    const client = app.get<WorkflowClient<typeof PostgresTraceWorkflow>>(
      getWorkflowToken(PostgresTraceWorkflow)
    )
    const handle = await client.start(secret ?? 'SUPER_SECRET_TEST_VALUE')
    process.send?.({ type: 'started', executionId: handle.executionId })
    const result = await handle.result({ timeout: '20s' })
    process.send?.({ type: 'complete', executionId: handle.executionId, result })
    await app.close()
    process.disconnect?.()
  } else {
    process.send?.({ type: 'ready' })
    await new Promise<void>((resolve) => {
      process.once('message', (message) => {
        // SAFETY: the parent test sends only the fixture stop protocol.
        if ((message as { readonly type?: string }).type === 'stop') resolve()
      })
    })
    await app.close()
    process.send?.({ type: 'stopped' })
    process.disconnect?.()
  }
} catch (error) {
  console.error(error)
  process.exitCode = 1
  process.disconnect?.()
}
