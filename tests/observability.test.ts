import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { createServer as createTcpServer } from 'node:net'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { expect, test } from 'bun:test'
import { Effect, ManagedRuntime } from 'effect'
import { otlp } from '../src/observability'
import { Activity, Activities, Workflow, WorkflowsAdmin, defineQueue } from '../src'
import type { WorkflowContext, WorkflowsOptions } from '../src'
import { WorkflowError } from '../src/errors'
import { otlpLayer, otlpResource } from '../src/internal/otlp'
import { TelemetryService } from '../src/internal/telemetry'
import { eventually, testApp } from './helpers'
import { z } from 'zod'

const TraceQueue = defineQueue('observability-traces')

@Activities()
class OtlpTraceActivity {
  @Activity({
    name: 'observability.echo',
    version: 1,
    queue: TraceQueue,
    input: z.string(),
    output: z.string()
  })
  async echo(value: string): Promise<string> {
    return value
  }
}

@Workflow({ name: 'observability.workflow', version: 1, input: z.string(), output: z.string() })
class OtlpTraceWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(OtlpTraceActivity).echo(input, { stepId: 'echo' })
  }
}

@Workflow({ name: 'observability.continued', version: 1, input: z.number(), output: z.number() })
class OtlpContinuationWorkflow {
  async run(input: number, context: WorkflowContext): Promise<number> {
    if (input === 0) return context.continueAsNew(1)
    return input
  }
}

const DeadLetterTraceQueue = defineQueue('observability-dead-letters')

@Activities()
class RecoverableActivities {
  @Activity({
    name: 'observability.recoverable',
    version: 1,
    queue: DeadLetterTraceQueue,
    input: z.string(),
    output: z.string()
  })
  async recover(value: string): Promise<string> {
    return `recovered:${value}`
  }
}

@Activities()
class IncompatibleActivities {
  @Activity({
    name: 'observability.incompatible',
    version: 1,
    queue: DeadLetterTraceQueue,
    input: z.string(),
    output: z.string()
  })
  async run(value: string): Promise<string> {
    return value
  }
}

@Workflow({
  name: 'observability.dead-letter-owner',
  version: 1,
  input: z.string(),
  output: z.string()
})
class OtlpDeadLetterWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(RecoverableActivities).recover(input, { stepId: 'recover' })
  }
}

type DeadLetterTestApp = Awaited<ReturnType<typeof testApp<typeof OtlpDeadLetterWorkflow>>>

interface OtlpTraceSpan {
  readonly name: string
  readonly traceId: string
  readonly spanId: string
  readonly parentSpanId?: string
  readonly attributes?: ReadonlyArray<{
    readonly key: string
    readonly value?: {
      readonly stringValue?: string
      readonly intValue?: string | number
      readonly doubleValue?: number
      readonly boolValue?: boolean
    }
  }>
}

interface OtlpTracePayload {
  readonly resourceSpans?: ReadonlyArray<{
    readonly scopeSpans?: ReadonlyArray<{ readonly spans?: ReadonlyArray<OtlpTraceSpan> }>
  }>
}

async function collectOtlpRequests(
  run: (endpoint: string) => Promise<void>
): Promise<ReadonlyArray<{ readonly url: string; readonly body: string }>> {
  const requests: Array<{ readonly url: string; readonly body: string }> = []
  const server = createServer((request, response) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk: string) => {
      body += chunk
    })
    request.on('end', () => {
      requests.push({ url: request.url ?? '', body })
      response.statusCode = 200
      response.end()
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })

  // SAFETY: the server was successfully bound to an ephemeral TCP address above.
  const port = (server.address() as AddressInfo).port
  try {
    await run(`http://127.0.0.1:${port}`)
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
  return requests
}

function exportedOtlpSpans(
  requests: ReadonlyArray<{ readonly url: string; readonly body: string }>
): ReadonlyArray<OtlpTraceSpan> {
  return requests
    .filter((request) => request.url === '/v1/traces')
    .flatMap((request) => {
      // SAFETY: this test server only records JSON OTLP trace responses from the exporter.
      const payload = JSON.parse(request.body) as OtlpTracePayload
      return (
        payload.resourceSpans?.flatMap(
          (resource) => resource.scopeSpans?.flatMap((scope) => scope.spans ?? []) ?? []
        ) ?? []
      )
    })
}

function spanAttribute(span: OtlpTraceSpan, key: string): string | number | boolean | undefined {
  const value = span.attributes?.find((attribute) => attribute.key === key)?.value
  if (!value) return undefined
  if (value.stringValue !== undefined) return value.stringValue
  if (value.intValue !== undefined) return Number(value.intValue)
  if (value.doubleValue !== undefined) return value.doubleValue
  return value.boolValue
}

interface FixtureMessage {
  readonly type: string
  readonly executionId?: string
  readonly result?: string
}

function nextFixtureMessage(child: ChildProcess, timeoutMs = 20_000): Promise<FixtureMessage> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('PostgreSQL tracing fixture did not respond in time'))
    }, timeoutMs)
    child.once('message', (message) => {
      clearTimeout(timeout)
      // SAFETY: the fixture and this test use the bounded FixtureMessage IPC protocol.
      resolve(message as FixtureMessage)
    })
    child.once('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })
    child.once('exit', (code, signal) => {
      clearTimeout(timeout)
      reject(new Error(`PostgreSQL tracing fixture exited ${code ?? 'null'} ${signal ?? ''}`))
    })
  })
}

function spawnTraceFixture(
  role: 'orchestrator' | 'worker',
  endpoint: string,
  namespace: string,
  secret: string,
  clusterPort: number
): ChildProcess {
  const child = spawn(process.execPath, ['tests/fixtures/observability-postgres-worker.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      WORKFLOWS_TRACE_ENDPOINT: endpoint,
      WORKFLOWS_TRACE_NAMESPACE: namespace,
      WORKFLOWS_TRACE_ROLE: role,
      WORKFLOWS_TRACE_CLUSTER_PORT: String(clusterPort),
      WORKFLOWS_TRACE_SECRET: secret
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc']
  })
  child.stdout?.on('data', (chunk) => process.stdout.write(chunk))
  child.stderr?.on('data', (chunk) => process.stderr.write(chunk))
  return child
}

async function freeTcpPort(): Promise<number> {
  const server = createTcpServer()
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  // SAFETY: the server was successfully bound to an ephemeral TCP address above.
  const port = (server.address() as AddressInfo).port
  await new Promise<void>((resolve) => server.close(() => resolve()))
  return port
}

async function waitForFixtureExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return
  await new Promise<void>((resolve) => child.once('exit', () => resolve()))
}

function invalidConfiguration(action: () => void): void {
  try {
    action()
    throw new Error('expected invalid configuration')
  } catch (error) {
    expect(error).toBeInstanceOf(WorkflowError)
    // SAFETY: the assertion above verifies the thrown value is a WorkflowError.
    expect((error as WorkflowError).code).toBe('INVALID_CONFIGURATION')
  }
}

test('otlp configuration is public and validates local settings', () => {
  const options = otlp({
    serviceName: 'reports-worker',
    endpoint: 'http://otel-collector:4318',
    traces: true,
    metrics: { enabled: true, exportInterval: '10s' },
    logs: { enabled: true, level: 'info' },
    headers: { Authorization: 'Bearer secret' },
    attributes: { 'deployment.environment.name': 'test' }
  })

  expect(options.kind).toBe('otlp')
  expect(Object.isFrozen(options)).toBe(true)
  expect(Object.isFrozen(options.headers)).toBe(true)
  expect(Object.isFrozen(options.attributes)).toBe(true)

  invalidConfiguration(() => otlp({ serviceName: 'worker', endpoint: 'not-a-url' }))
  invalidConfiguration(() =>
    otlp({ serviceName: 'worker', endpoint: 'http://localhost:4318', exportInterval: 0 })
  )
  invalidConfiguration(() =>
    otlp({
      serviceName: 'worker',
      endpoint: 'http://localhost:4318',
      // SAFETY: this test intentionally supplies an invalid runtime value.
      logs: { enabled: true, level: 'verbose' as never }
    })
  )
})

test('otlp resources contain deployment identity but no storage secrets', () => {
  const options: WorkflowsOptions = {
    namespace: 'reports',
    topology: 'distributed',
    storage: {
      driver: 'postgres',
      connectionString: 'postgres://user:secret@db.internal/reports',
      maxConnections: 5
    },
    execution: { workflows: { enabled: false }, activities: { enabled: false } },
    observability: otlp({
      serviceName: 'reports-worker',
      serviceVersion: '2026.9.1',
      endpoint: 'https://otel-collector:4318',
      attributes: { 'deployment.environment.name': 'test' }
    })
  }

  const resource = otlpResource(options)
  expect(resource.serviceName).toBe('reports-worker')
  expect(resource.serviceVersion).toBe('2026.9.1')
  expect(resource.attributes['deployment.environment.name']).toBe('test')
  expect(resource.attributes['better_workflows.namespace']).toBe('reports')
  expect(resource.attributes['better_workflows.version']).toBe('0.1.0-alpha.7')
  expect(resource.attributes['better_workflows.topology']).toBe('distributed')
  expect(resource.attributes['better_workflows.storage.driver']).toBe('postgres')
  expect(resource.attributes['better_workflows.role']).toBe('producer')
  expect(JSON.stringify(resource)).not.toContain('secret')
  expect(JSON.stringify(resource)).not.toContain('db.internal')
})

test('collector failures are isolated from the managed runtime', async () => {
  const options: WorkflowsOptions = {
    namespace: 'offline-collector',
    storage: { driver: 'sqlite', filename: ':memory:', runtime: 'auto' },
    observability: otlp({
      serviceName: 'offline-worker',
      endpoint: 'http://127.0.0.1:1',
      traces: true,
      metrics: { enabled: true, exportInterval: '1ms' },
      logs: { enabled: true, level: 'info' },
      shutdownTimeout: '20ms'
    })
  }
  const runtime = ManagedRuntime.make(otlpLayer(options))
  try {
    const telemetry = await runtime.runPromise(TelemetryService)
    telemetry.count('workflowStarted')
    await runtime.runPromise(Effect.logInfo('collector is offline'))
  } finally {
    await runtime.dispose()
  }
})

test('otlp metrics export the runtime registry', async () => {
  const requests: Array<{ readonly url: string; readonly body: string }> = []
  const server = createServer((request, response) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk: string) => {
      body += chunk
    })
    request.on('end', () => {
      requests.push({ url: request.url ?? '', body })
      response.statusCode = 200
      response.end()
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })

  // SAFETY: the server was successfully bound to an ephemeral TCP address above.
  const port = (server.address() as AddressInfo).port
  const options: WorkflowsOptions = {
    namespace: 'export-test',
    storage: { driver: 'sqlite', filename: ':memory:', runtime: 'auto' },
    observability: otlp({
      serviceName: 'export-test',
      endpoint: `http://127.0.0.1:${port}`,
      traces: false,
      metrics: { enabled: true, exportInterval: '5ms' },
      logs: false,
      shutdownTimeout: '1s'
    })
  }
  const runtime = ManagedRuntime.make(otlpLayer(options))
  try {
    const telemetry = await runtime.runPromise(TelemetryService)
    telemetry.count('workflowStarted')
    await new Promise((resolve) => setTimeout(resolve, 25))
  } finally {
    await runtime.dispose()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }

  const metricsRequest = requests.find((request) => request.url === '/v1/metrics')
  expect(metricsRequest).toBeDefined()
  // SAFETY: the preceding assertion verifies that the collector received a metrics request.
  const payload = JSON.parse(metricsRequest!.body) as {
    readonly resourceMetrics: ReadonlyArray<{
      readonly resource: { readonly attributes: ReadonlyArray<{ readonly key: string }> }
      readonly scopeMetrics: ReadonlyArray<{
        readonly metrics: ReadonlyArray<{ readonly name: string }>
      }>
    }>
  }
  const resourceAttributes = payload.resourceMetrics[0]?.resource.attributes.map(
    (entry) => entry.key
  )
  const metricNames = payload.resourceMetrics[0]?.scopeMetrics.flatMap((scope) =>
    scope.metrics.map((metric) => metric.name)
  )
  expect(resourceAttributes).toContain('better_workflows.namespace')
  expect(resourceAttributes).toContain('better_workflows.version')
  expect(metricNames).toContain('better_workflows.workflow.started')
})

test('activity spans use the persisted dispatch context without exporting payloads', async () => {
  const requests: Array<{ readonly url: string; readonly body: string }> = []
  const server = createServer((request, response) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk: string) => {
      body += chunk
    })
    request.on('end', () => {
      requests.push({ url: request.url ?? '', body })
      response.statusCode = 200
      response.end()
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })

  // SAFETY: the server was successfully bound to an ephemeral TCP address above.
  const port = (server.address() as AddressInfo).port
  const secret = 'SUPER_SECRET_TEST_VALUE'
  const app = await testApp(OtlpTraceWorkflow, {
    providers: [OtlpTraceActivity],
    queues: [{ queue: TraceQueue, concurrency: 1 }],
    observability: otlp({
      serviceName: 'trace-propagation-test',
      endpoint: `http://127.0.0.1:${port}`,
      traces: { exportInterval: '5ms' },
      metrics: false,
      logs: false,
      shutdownTimeout: '1s'
    })
  })
  try {
    const handle = await app.client.start(secret)
    expect(await handle.result({ timeout: '5s' })).toBe(secret)
  } finally {
    await app.close()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }

  const spans = requests
    .filter((request) => request.url === '/v1/traces')
    .flatMap((request) => {
      // SAFETY: this test server only records JSON OTLP trace responses sent by the exporter.
      const payload = JSON.parse(request.body) as OtlpTracePayload
      return (
        payload.resourceSpans?.flatMap(
          (resource) => resource.scopeSpans?.flatMap((scope) => scope.spans ?? []) ?? []
        ) ?? []
      )
    })
  const dispatch = spans.find((span) => span.name === 'better-workflows.activity.dispatch')
  const execute = spans.find((span) => span.name === 'better-workflows.activity.execute')
  expect(dispatch).toBeDefined()
  expect(execute).toBeDefined()
  // SAFETY: both preceding assertions verify that the expected spans were exported.
  expect(execute!.traceId).toBe(dispatch!.traceId)
  expect(execute!.parentSpanId).toBe(dispatch!.spanId)
  expect(requests.map((request) => request.body).join('\n')).not.toContain(secret)
})

test('continue-as-new spans retain chain and generation correlation', async () => {
  const requests = await collectOtlpRequests(async (endpoint) => {
    const app = await testApp(OtlpContinuationWorkflow, {
      observability: otlp({
        serviceName: 'continuation-tracing-test',
        endpoint,
        traces: { exportInterval: '5ms' },
        metrics: false,
        logs: false,
        shutdownTimeout: '1s'
      })
    })
    try {
      const handle = await app.client.start(0)
      expect(await handle.result({ timeout: '5s' })).toBe(1)
    } finally {
      await app.close()
    }
  })

  const rounds = exportedOtlpSpans(requests).filter(
    (span) => span.name === 'better-workflows.workflow.round'
  )
  expect(rounds).toHaveLength(2)
  const chainIds = rounds.map((span) => spanAttribute(span, 'better_workflows.execution.chain_id'))
  const generations = rounds.map((span) =>
    spanAttribute(span, 'better_workflows.execution.generation')
  )
  const executionIds = rounds.map((span) => spanAttribute(span, 'better_workflows.execution.id'))
  expect(new Set(chainIds).size).toBe(1)
  expect(new Set(generations)).toEqual(new Set([0, 1]))
  for (const executionId of executionIds) expect(executionId).toEqual(expect.any(String))
  expect(new Set(executionIds).size).toBe(2)
})

test('dead-letter tracing correlates create, requeue and restored execution without payloads', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'better-workflows-dlq-tracing-'))
  const filename = join(directory, 'workflows.sqlite')
  const secret = 'SUPER_SECRET_TEST_VALUE'
  let incompatibleApp: DeadLetterTestApp | undefined
  let operatorApp: DeadLetterTestApp | undefined
  let restoredApp: DeadLetterTestApp | undefined

  const requests = await collectOtlpRequests(async (endpoint) => {
    const telemetry = otlp({
      serviceName: 'dead-letter-tracing-test',
      endpoint,
      traces: { exportInterval: '5ms' },
      metrics: false,
      logs: false,
      shutdownTimeout: '1s'
    })
    try {
      incompatibleApp = await testApp(OtlpDeadLetterWorkflow, {
        filename,
        providers: [IncompatibleActivities],
        activityContracts: [RecoverableActivities],
        queues: [{ queue: DeadLetterTraceQueue, concurrency: 1 }],
        observability: telemetry
      })
      const handle = await incompatibleApp.client.start(secret)
      const blocked = await eventually(
        () => handle.describe(),
        (snapshot) => snapshot.status === 'blocked' && snapshot.blockedOn !== undefined
      )
      expect(blocked.blockedOn).toBeDefined()
      const deadLetters = await eventually(
        () => incompatibleApp!.module.get(WorkflowsAdmin).listDeadLetters({ state: 'open' }),
        (page) => page.deadLetters.length === 1
      )
      const deadLetter = deadLetters.deadLetters[0]!
      await incompatibleApp.close()
      incompatibleApp = undefined

      operatorApp = await testApp(OtlpDeadLetterWorkflow, {
        filename,
        execution: { workflows: { enabled: false }, activities: { enabled: false } },
        activityContracts: [RecoverableActivities],
        queues: [{ queue: DeadLetterTraceQueue, concurrency: 1 }],
        observability: telemetry
      })
      await operatorApp.module.get(WorkflowsAdmin).requeueDeadLetter(deadLetter.id)
      await operatorApp.close()
      operatorApp = undefined

      restoredApp = await testApp(OtlpDeadLetterWorkflow, {
        filename,
        providers: [RecoverableActivities],
        queues: [{ queue: DeadLetterTraceQueue, concurrency: 1 }],
        observability: telemetry
      })
      const restored = restoredApp.client.getHandle(handle.executionId)
      expect(await restored.result({ timeout: '5s' })).toBe(`recovered:${secret}`)
    } finally {
      await restoredApp?.close()
      restoredApp = undefined
      await operatorApp?.close()
      operatorApp = undefined
      await incompatibleApp?.close()
      incompatibleApp = undefined
    }
  })
  await rm(directory, { recursive: true, force: true })

  const spans = exportedOtlpSpans(requests)
  const created = spans.find((span) => span.name === 'better-workflows.dead_letter.create')
  const requeued = spans.find((span) => span.name === 'better-workflows.dead_letter.requeue')
  const executed = spans.find((span) => span.name === 'better-workflows.activity.execute')
  expect(created).toBeDefined()
  expect(requeued).toBeDefined()
  expect(executed).toBeDefined()
  // SAFETY: these assertions verify that all expected DLQ and execution spans were exported.
  expect(spanAttribute(created!, 'better_workflows.execution.id')).toBeDefined()
  expect(spanAttribute(requeued!, 'better_workflows.execution.id')).toBe(
    spanAttribute(created!, 'better_workflows.execution.id')
  )
  expect(spanAttribute(executed!, 'better_workflows.execution.id')).toBe(
    spanAttribute(created!, 'better_workflows.execution.id')
  )
  expect(spanAttribute(created!, 'better_workflows.dead_letter.id')).toBe(
    spanAttribute(requeued!, 'better_workflows.dead_letter.id')
  )
  expect(requests.map((request) => request.body).join('\n')).not.toContain(secret)
})

const postgresConnectionString = process.env['WORKFLOWS_TEST_POSTGRES_URL']

test.skipIf(!postgresConnectionString)(
  'PostgreSQL tracing propagates activity context across worker processes',
  async () => {
    if (!postgresConnectionString) return
    const namespace = `observability-pg-${process.pid}-${Date.now()}`
    const secret = 'SUPER_SECRET_TEST_VALUE'
    const [orchestratorPort, workerPort] = await Promise.all([freeTcpPort(), freeTcpPort()])
    const requests = await collectOtlpRequests(async (endpoint) => {
      const orchestrator = spawnTraceFixture(
        'orchestrator',
        endpoint,
        namespace,
        secret,
        orchestratorPort
      )
      let worker: ChildProcess | undefined
      try {
        const started = await nextFixtureMessage(orchestrator)
        expect(started.type).toBe('started')
        expect(started.executionId).toEqual(expect.any(String))

        worker = spawnTraceFixture('worker', endpoint, namespace, secret, workerPort)
        expect((await nextFixtureMessage(worker)).type).toBe('ready')

        const completed = await nextFixtureMessage(orchestrator)
        expect(completed).toMatchObject({ type: 'complete', result: `processed:${secret}` })
        worker.send?.({ type: 'stop' })
        expect((await nextFixtureMessage(worker)).type).toBe('stopped')
        await Promise.all([waitForFixtureExit(orchestrator), waitForFixtureExit(worker)])
      } finally {
        if (orchestrator.exitCode === null && orchestrator.signalCode === null)
          orchestrator.kill('SIGKILL')
        if (worker && worker.exitCode === null && worker.signalCode === null) worker.kill('SIGKILL')
      }
    })

    const spans = exportedOtlpSpans(requests)
    const dispatch = spans.find((span) => span.name === 'better-workflows.activity.dispatch')
    const execute = spans.find((span) => span.name === 'better-workflows.activity.execute')
    expect(dispatch).toBeDefined()
    expect(execute).toBeDefined()
    // SAFETY: these assertions verify that the producer and worker spans were exported.
    expect(execute!.traceId).toBe(dispatch!.traceId)
    expect(execute!.parentSpanId).toBe(dispatch!.spanId)
    expect(requests.map((request) => request.body).join('\n')).not.toContain(secret)
  },
  30_000
)
