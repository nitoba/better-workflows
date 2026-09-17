import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { expect, test } from 'bun:test'
import { Effect, ManagedRuntime } from 'effect'
import { otlp } from '../src/observability'
import type { WorkflowsOptions } from '../src'
import { WorkflowError } from '../src/errors'
import { otlpLayer, otlpResource } from '../src/internal/otlp'
import { TelemetryService } from '../src/internal/telemetry'

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
