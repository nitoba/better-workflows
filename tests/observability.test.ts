import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { expect, test } from 'bun:test'
import { Effect, ManagedRuntime } from 'effect'
import { otlp } from '../src/observability'
import { Activity, Activities, Workflow, defineQueue } from '../src'
import type { WorkflowContext, WorkflowsOptions } from '../src'
import { WorkflowError } from '../src/errors'
import { otlpLayer, otlpResource } from '../src/internal/otlp'
import { TelemetryService } from '../src/internal/telemetry'
import { testApp } from './helpers'
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
  expect(resource.attributes['better_workflows.version']).toBe('0.1.0-alpha.6')
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
