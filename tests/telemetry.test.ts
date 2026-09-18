import { expect, test } from 'bun:test'
import { Effect, ManagedRuntime, Tracer } from 'effect'
import {
  Activity,
  ActivityError,
  Activities,
  ActivitiesContract,
  Workflow,
  defineQueue
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { z } from 'zod'
import {
  Telemetry,
  TelemetryAttributeKey,
  TelemetryMetricName,
  TelemetryPackage,
  TelemetrySpanName,
  logAnnotations,
  metricAttributes,
  spanAttributes,
  telemetryLayer,
  TelemetryService
} from '../src/internal/telemetry'
import { WorkflowsRuntime } from '../src/internal/runtime'
import { ExecutionNotifier } from '../src/internal/notifier'
import { WorkflowInterpreter } from '../src/internal/interpreter'
import type { Journal, RunRow } from '../src/internal/journal'
import type { Registry, RegisteredWorkflow } from '../src/internal/registry'
import type { ActivityTransport } from '../src/internal/activity-transport'
import type { Failure } from '../src/errors'
import { eventually, testApp } from './helpers'

const MetricsQueue = defineQueue('metrics')
const MetricValue = z.union([z.string(), z.number()])
const MetricsSignal = {
  name: 'metrics.approval',
  schema: z.string()
} as const

@Activities()
class MetricsActivity {
  @Activity({
    name: 'metrics.echo',
    version: 1,
    queue: MetricsQueue,
    input: MetricValue,
    output: MetricValue
  })
  async echo(input: z.infer<typeof MetricValue>): Promise<z.infer<typeof MetricValue>> {
    return input
  }
}

@Workflow({
  name: 'metrics.workflow',
  version: 1,
  input: MetricValue,
  output: MetricValue,
  signals: [MetricsSignal],
  idempotencyKey: () => 'metrics-key'
})
class MetricsWorkflow {
  async run(
    input: z.infer<typeof MetricValue>,
    context: WorkflowContext
  ): Promise<z.infer<typeof MetricValue>> {
    const value = await context.activities(MetricsActivity).echo(input, { stepId: 'echo' })
    await context.sleep('brief-delay', '1ms')
    await context.waitForSignal('approval', MetricsSignal, { timeout: '5s' })
    return value
  }
}

@Workflow({ name: 'metrics.continued', version: 1, input: MetricValue, output: MetricValue })
class MetricsContinuedWorkflow {
  async run(
    input: z.infer<typeof MetricValue>,
    context: WorkflowContext
  ): Promise<z.infer<typeof MetricValue>> {
    if (input === 0) return context.continueAsNew(1)
    return input
  }
}

@Activities()
class MetricsRetryActivity {
  attempts = 0

  @Activity({
    name: 'metrics.retry',
    version: 1,
    queue: MetricsQueue,
    input: z.string(),
    output: z.string(),
    retry: { maxAttempts: 2, initialDelay: '1ms', maxDelay: '1ms' }
  })
  async execute(input: string, context: ActivityContext): Promise<string> {
    this.attempts++
    if (context.attempt === 1)
      throw new ActivityError({
        code: 'TEMPORARY',
        message: 'transient business failure',
        retryable: true
      })
    return input
  }
}

@Workflow({ name: 'metrics.retry-workflow', version: 1, input: z.string(), output: z.string() })
class MetricsRetryWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(MetricsRetryActivity).execute(input, { stepId: 'retry' })
  }
}

@Workflow({ name: 'metrics.failed-workflow', version: 1, input: z.string(), output: z.string() })
class MetricsFailedWorkflow {
  async run(_input: string, _context: WorkflowContext): Promise<string> {
    throw new Error('business failure')
  }
}

@Workflow({ name: 'metrics.cancelled-workflow', version: 1, input: z.string(), output: z.string() })
class MetricsCancelledWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    await context.sleep('hold', '10s')
    return input
  }
}

const AdvancedMetricsQueue = defineQueue('advanced-metrics')

@ActivitiesContract({ queue: AdvancedMetricsQueue })
class AdvancedMetricsContract {
  @Activity({ name: 'metrics.advanced', version: 3, input: z.string(), output: z.string() })
  execute(_input: string, _context: ActivityContext): Promise<string> {
    throw new Error('contract-only')
  }
}

@Activities(AdvancedMetricsContract)
class AdvancedMetricsHandler implements AdvancedMetricsContract {
  async execute(input: string, _context: ActivityContext): Promise<string> {
    return `advanced:${input}`
  }
}

@Workflow({ name: 'metrics.advanced-workflow', version: 1, input: z.string(), output: z.string() })
class AdvancedMetricsWorkflow {
  async run(input: string, context: WorkflowContext): Promise<string> {
    return context.activities(AdvancedMetricsContract).execute(input, { stepId: 'execute' })
  }
}

test('central telemetry vocabulary uses stable package prefixes', () => {
  expect(
    Object.values(TelemetryMetricName).every((name) => name.startsWith('better_workflows.'))
  ).toBe(true)
  expect(
    Object.values(TelemetrySpanName).every((name) => name.startsWith('better-workflows.'))
  ).toBe(true)
  expect(Telemetry.package).toBe(TelemetryPackage)
  expect(Telemetry.metric.workflowStarted).toBe('better_workflows.workflow.started')
  expect(Telemetry.span.workflowRound).toBe('better-workflows.workflow.round')
})

test('workflow round spans end with correlation attributes', async () => {
  const row: RunRow = {
    execution_id: 'execution-1',
    namespace: 'trace-test',
    workflow_name: 'trace.workflow',
    version: 3,
    dedupe_key: 'dedupe-key',
    input_json: 'input',
    created_at: 0,
    updated_at: 0,
    state: 'running',
    control: 'run',
    control_revision: 0,
    applied_revision: 0,
    dispatched: 1,
    event_sequence: 0,
    wait_type: null,
    wait_step: null,
    result_json: null,
    failure_json: null,
    chain_id: 'chain-1',
    generation: 2,
    continued_from: null,
    continued_to: null
  }
  // SAFETY: this focused round has no durable commands, so only these Journal methods are evaluated.
  const journal = Object.assign(Object.create(null), {
    namespace: row.namespace,
    get: () => Effect.succeed(row),
    assertEnd: () => Effect.succeed(undefined)
  }) as Journal
  // SAFETY: the immediate workflow does not resolve activities, children or sagas.
  const registry = Object.create(null) as Registry
  // SAFETY: the round only reads the contract identity from this registered workflow.
  const workflow = {
    options: { name: row.workflow_name, version: row.version }
  } as RegisteredWorkflow
  // SAFETY: the immediate workflow never uses the activity transport.
  const transport = Object.create(null) as ActivityTransport
  const interpreter = new WorkflowInterpreter(
    journal,
    registry,
    workflow,
    row.execution_id,
    () => Effect.succeed(undefined),
    transport
  )
  const spans: Array<Tracer.NativeSpan> = []
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options)
      spans.push(span)
      return span
    }
  })

  const tracedRound = interpreter
    .run(async () => 'done')
    .pipe(Effect.provideService(Tracer.Tracer, tracer))
  // SAFETY: the immediate round does not evaluate any WorkflowServices dependency.
  await Effect.runPromise(tracedRound as Effect.Effect<string, Failure, never>)

  const round = spans.find((span) => span.name === TelemetrySpanName.workflowRound)
  expect(round?.status._tag).toBe('Ended')
  expect(round?.attributes.get(TelemetryAttributeKey.workflowName)).toBe(row.workflow_name)
  expect(round?.attributes.get(TelemetryAttributeKey.workflowVersion)).toBe(row.version)
  expect(round?.attributes.get(TelemetryAttributeKey.executionId)).toBe(row.execution_id)
  expect(round?.attributes.get(TelemetryAttributeKey.executionChainId)).toBe(row.chain_id)
  expect(round?.attributes.get(TelemetryAttributeKey.executionGeneration)).toBe(row.generation)
  expect([...spans].every((span) => span.status._tag === 'Ended')).toBe(true)
})

test('metric attributes exclude high-cardinality diagnostic identity', () => {
  const attributes = {
    [TelemetryAttributeKey.namespace]: 'reports',
    [TelemetryAttributeKey.workflowName]: 'reports.generate',
    [TelemetryAttributeKey.workflowVersion]: 2,
    [TelemetryAttributeKey.executionId]: 'execution-123',
    [TelemetryAttributeKey.executionChainId]: 'chain-123',
    [TelemetryAttributeKey.executionGeneration]: 4,
    [TelemetryAttributeKey.stepId]: 'step-1',
    [TelemetryAttributeKey.activityName]: 'reports.write',
    [TelemetryAttributeKey.activityVersion]: 1,
    [TelemetryAttributeKey.queueName]: 'emails',
    [TelemetryAttributeKey.activityBusinessAttempt]: 2,
    [TelemetryAttributeKey.activityDeliveryAttempt]: 7,
    [TelemetryAttributeKey.deadLetterId]: 'dead-letter-123',
    [TelemetryAttributeKey.deadLetterReason]: 'DELIVERY_ATTEMPTS_EXHAUSTED',
    [TelemetryAttributeKey.failureCode]: 'PROVIDER_BUSY',
    [TelemetryAttributeKey.signalName]: 'approval.received'
  }

  expect(metricAttributes(attributes)).toEqual({
    [TelemetryAttributeKey.workflowName]: 'reports.generate',
    [TelemetryAttributeKey.workflowVersion]: '2',
    [TelemetryAttributeKey.activityName]: 'reports.write',
    [TelemetryAttributeKey.activityVersion]: '1',
    [TelemetryAttributeKey.queueName]: 'emails',
    [TelemetryAttributeKey.deadLetterReason]: 'DELIVERY_ATTEMPTS_EXHAUSTED',
    [TelemetryAttributeKey.signalName]: 'approval.received'
  })

  for (const forbidden of [
    TelemetryAttributeKey.namespace,
    TelemetryAttributeKey.executionId,
    TelemetryAttributeKey.executionChainId,
    TelemetryAttributeKey.executionGeneration,
    TelemetryAttributeKey.stepId,
    TelemetryAttributeKey.activityBusinessAttempt,
    TelemetryAttributeKey.activityDeliveryAttempt,
    TelemetryAttributeKey.deadLetterId,
    TelemetryAttributeKey.failureCode
  ]) {
    expect(forbidden in metricAttributes(attributes)).toBe(false)
  }

  expect(
    metricAttributes({
      [TelemetryAttributeKey.deadLetterReason]: 'reason message from an application'
    })
  ).toEqual({})
})

test('spans and logs retain correlation attributes without payload fields', () => {
  const attributes = {
    [TelemetryAttributeKey.namespace]: 'reports',
    [TelemetryAttributeKey.workflowName]: 'reports.generate',
    [TelemetryAttributeKey.workflowVersion]: 2,
    [TelemetryAttributeKey.executionId]: 'execution-123',
    [TelemetryAttributeKey.executionChainId]: 'chain-123',
    [TelemetryAttributeKey.executionGeneration]: 4,
    [TelemetryAttributeKey.stepId]: 'step-1',
    [TelemetryAttributeKey.activityName]: 'reports.write',
    [TelemetryAttributeKey.activityVersion]: 1,
    [TelemetryAttributeKey.queueName]: 'emails',
    [TelemetryAttributeKey.activityBusinessAttempt]: 2,
    [TelemetryAttributeKey.activityDeliveryAttempt]: 7,
    [TelemetryAttributeKey.executionStatus]: 'running',
    [TelemetryAttributeKey.failureCode]: 'PROVIDER_BUSY',
    [TelemetryAttributeKey.deadLetterId]: 'dead-letter-123',
    [TelemetryAttributeKey.deadLetterReason]: 'DELIVERY_ATTEMPTS_EXHAUSTED',
    [TelemetryAttributeKey.signalName]: 'approval.received'
  }

  expect(spanAttributes(attributes)).toEqual(attributes)
  expect(logAnnotations('activity-worker', attributes)).toEqual({
    package: TelemetryPackage,
    component: 'activity-worker',
    ...attributes
  })
  expect('input' in logAnnotations('workflow', attributes)).toBe(false)
  expect('output' in logAnnotations('workflow', attributes)).toBe(false)
  expect('payload' in logAnnotations('workflow', attributes)).toBe(false)
})

test('telemetry metrics use isolated registries and ignore invalid observations', async () => {
  const first = ManagedRuntime.make(telemetryLayer)
  const second = ManagedRuntime.make(telemetryLayer)
  try {
    const telemetry = await first.runPromise(TelemetryService)
    telemetry.count('workflowStarted', {
      [TelemetryAttributeKey.workflowName]: 'reports.generate',
      [TelemetryAttributeKey.workflowVersion]: 1,
      [TelemetryAttributeKey.executionId]: 'execution-123'
    })
    telemetry.observe('workflowDuration', 125, {
      [TelemetryAttributeKey.workflowName]: 'reports.generate',
      [TelemetryAttributeKey.workflowVersion]: 1
    })
    telemetry.observe('activityRetryLag', -1)
    telemetry.setGauge('notifierWaiters', 3)

    const snapshots = (await first.runPromise(TelemetryService)).snapshot()
    const started = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.workflowStarted
    )
    const duration = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.workflowDuration
    )
    const waiters = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.notifierWaiters
    )
    const invalidLag = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.activityRetryLag
    )

    expect(started?.type).toBe('Counter')
    if (started?.type === 'Counter') {
      expect(started.state.count).toBe(1)
      expect(started.attributes).toEqual({
        [TelemetryAttributeKey.workflowName]: 'reports.generate',
        [TelemetryAttributeKey.workflowVersion]: '1'
      })
    }
    expect(duration?.type).toBe('Histogram')
    if (duration?.type === 'Histogram') expect(duration.state.count).toBe(1)
    expect(waiters?.type).toBe('Gauge')
    if (waiters?.type === 'Gauge') expect(waiters.state.value).toBe(3)
    expect(invalidLag).toBeUndefined()

    expect((await second.runPromise(TelemetryService)).snapshot()).toEqual([])
  } finally {
    await Promise.all([first.dispose(), second.dispose()])
  }
})

test('runtime emits committed workflow and activity metrics without execution labels', async () => {
  const app = await testApp(MetricsWorkflow, {
    providers: [MetricsActivity],
    queues: [{ queue: MetricsQueue, concurrency: 1 }]
  })
  try {
    const handle = await app.client.start('SUPER_SECRET_TEST_VALUE')
    await handle.signal(MetricsSignal, 'approved', { idempotencyKey: 'approval-1' })
    expect(await handle.result({ timeout: '5s' })).toBe('SUPER_SECRET_TEST_VALUE')
    expect((await app.client.start('SUPER_SECRET_TEST_VALUE')).created).toBe(false)

    const runtime = app.module.get(WorkflowsRuntime)
    const telemetry = await runtime.run(TelemetryService)
    const snapshots = telemetry.snapshot()
    const count = (name: string) => {
      const metric = snapshots.find(
        (snapshot) => snapshot.id === name && snapshot.type === 'Counter'
      )
      return metric?.type === 'Counter' ? metric.state.count : 0
    }
    const observations = (name: string) => {
      const metric = snapshots.find(
        (snapshot) => snapshot.id === name && snapshot.type === 'Histogram'
      )
      return metric?.type === 'Histogram' ? metric.state.count : 0
    }

    expect(count(TelemetryMetricName.workflowStarted)).toBe(1)
    expect(count(TelemetryMetricName.workflowCompleted)).toBe(1)
    expect(count(TelemetryMetricName.activityDispatched)).toBe(1)
    expect(count(TelemetryMetricName.activityStarted)).toBe(1)
    expect(count(TelemetryMetricName.activityCompleted)).toBe(1)
    expect(count(TelemetryMetricName.timerDelivered)).toBe(1)
    expect(count(TelemetryMetricName.signalAccepted)).toBe(1)
    expect(count(TelemetryMetricName.signalConsumed)).toBe(1)
    expect(observations(TelemetryMetricName.workflowDuration)).toBe(1)
    expect(observations(TelemetryMetricName.workflowChainDuration)).toBe(1)
    expect(observations(TelemetryMetricName.activityDuration)).toBe(1)
    expect(observations(TelemetryMetricName.activityQueueWait)).toBe(1)
    expect(observations(TelemetryMetricName.timerLag)).toBe(1)
    expect(observations(TelemetryMetricName.signalWaitDuration)).toBe(1)

    for (const snapshot of snapshots)
      expect(snapshot.attributes?.[TelemetryAttributeKey.executionId]).toBeUndefined()
  } finally {
    await app.close()
  }
})

test('advanced activity metrics use contract identity rather than handler identity', async () => {
  const app = await testApp(AdvancedMetricsWorkflow, {
    providers: [AdvancedMetricsHandler],
    queues: [{ queue: AdvancedMetricsQueue, concurrency: 1 }]
  })
  try {
    const handle = await app.client.start('value')
    expect(await handle.result({ timeout: '5s' })).toBe('advanced:value')

    const snapshots = (await app.module.get(WorkflowsRuntime).run(TelemetryService)).snapshot()
    const activity = snapshots.find(
      (snapshot) =>
        snapshot.id === TelemetryMetricName.activityCompleted && snapshot.type === 'Counter'
    )
    expect(activity?.type).toBe('Counter')
    if (activity?.type === 'Counter')
      expect(activity.attributes).toMatchObject({
        [TelemetryAttributeKey.activityName]: 'metrics.advanced',
        [TelemetryAttributeKey.activityVersion]: '3',
        [TelemetryAttributeKey.queueName]: AdvancedMetricsQueue.name
      })
    expect(JSON.stringify(snapshots)).not.toContain('AdvancedMetricsHandler')
  } finally {
    await app.close()
  }
})

test('continuation metrics count generations but emit one chain duration', async () => {
  const app = await testApp(MetricsContinuedWorkflow)
  try {
    const handle = await app.client.start(0)
    expect(await handle.result({ timeout: '5s' })).toBe(1)

    const telemetry = await app.module.get(WorkflowsRuntime).run(TelemetryService)
    const snapshots = telemetry.snapshot()
    const counter = (name: string) =>
      snapshots.find((snapshot) => snapshot.id === name && snapshot.type === 'Counter')
    const histogram = (name: string) =>
      snapshots.find((snapshot) => snapshot.id === name && snapshot.type === 'Histogram')
    expect(counter(TelemetryMetricName.workflowStarted)?.state).toMatchObject({ count: 2 })
    expect(counter(TelemetryMetricName.workflowContinued)?.state).toMatchObject({ count: 1 })
    expect(counter(TelemetryMetricName.workflowCompleted)?.state).toMatchObject({ count: 1 })
    expect(histogram(TelemetryMetricName.workflowDuration)?.state).toMatchObject({ count: 2 })
    expect(histogram(TelemetryMetricName.workflowChainDuration)?.state).toMatchObject({ count: 1 })
  } finally {
    await app.close()
  }
})

test('business retries emit retry metrics without delivery redelivery metrics', async () => {
  const app = await testApp(MetricsRetryWorkflow, {
    providers: [MetricsRetryActivity],
    queues: [{ queue: MetricsQueue, concurrency: 1 }]
  })
  try {
    const handle = await app.client.start('retry-value')
    expect(await handle.result({ timeout: '5s' })).toBe('retry-value')
    expect(app.module.get(MetricsRetryActivity).attempts).toBe(2)

    const telemetry = await app.module.get(WorkflowsRuntime).run(TelemetryService)
    const snapshots = telemetry.snapshot()
    const count = (name: string) => {
      const metric = snapshots.find(
        (snapshot) => snapshot.id === name && snapshot.type === 'Counter'
      )
      return metric?.type === 'Counter' ? metric.state.count : 0
    }
    const observations = (name: string) => {
      const metric = snapshots.find(
        (snapshot) => snapshot.id === name && snapshot.type === 'Histogram'
      )
      return metric?.type === 'Histogram' ? metric.state.count : 0
    }

    expect(count(TelemetryMetricName.activityFailed)).toBe(1)
    expect(count(TelemetryMetricName.activityCompleted)).toBe(1)
    expect(count(TelemetryMetricName.activityRetryScheduled)).toBe(1)
    expect(count(TelemetryMetricName.activityDeliveryRetried)).toBe(0)
    expect(observations(TelemetryMetricName.activityRetryDelay)).toBe(1)
    expect(observations(TelemetryMetricName.activityRetryLag)).toBe(1)
    expect(observations(TelemetryMetricName.activityDuration)).toBe(2)
  } finally {
    await app.close()
  }
})

test('terminal workflow outcomes emit distinct failed and cancelled metrics', async () => {
  const failedApp = await testApp(MetricsFailedWorkflow)
  try {
    const handle = await failedApp.client.start('failed')
    await expect(handle.result({ timeout: '5s' })).rejects.toBeDefined()
    const telemetry = await failedApp.module.get(WorkflowsRuntime).run(TelemetryService)
    const failed = telemetry
      .snapshot()
      .find((snapshot) => snapshot.id === TelemetryMetricName.workflowFailed)
    expect(failed?.type).toBe('Counter')
    if (failed?.type === 'Counter') expect(failed.state.count).toBe(1)
  } finally {
    await failedApp.close()
  }

  const cancelledApp = await testApp(MetricsCancelledWorkflow)
  try {
    const handle = await cancelledApp.client.start('cancelled')
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'waiting'
    )
    await handle.cancel({ reason: 'test cancellation' })
    await eventually(
      () => handle.describe(),
      (snapshot) => snapshot.status === 'cancelled'
    )
    const telemetry = await cancelledApp.module.get(WorkflowsRuntime).run(TelemetryService)
    const cancelled = telemetry
      .snapshot()
      .find((snapshot) => snapshot.id === TelemetryMetricName.workflowCancelled)
    expect(cancelled?.type).toBe('Counter')
    if (cancelled?.type === 'Counter') expect(cancelled.state.count).toBe(1)
  } finally {
    await cancelledApp.close()
  }
})

test('result notifier records fallback waits and local waiter gauge', async () => {
  const runtime = ManagedRuntime.make(telemetryLayer)
  try {
    const telemetry = await runtime.runPromise(TelemetryService)
    const notifier = new ExecutionNotifier('telemetry-notifier', async () => 0, 5, telemetry)
    await notifier.wait('execution', 0)

    const snapshots = telemetry.snapshot()
    const fallback = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.resultFallbackPoll
    )
    const duration = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.resultWaitDuration
    )
    const waiters = snapshots.find(
      (snapshot) => snapshot.id === TelemetryMetricName.notifierWaiters
    )
    expect(fallback?.type).toBe('Counter')
    if (fallback?.type === 'Counter') expect(fallback.state.count).toBe(1)
    expect(duration?.type).toBe('Histogram')
    if (duration?.type === 'Histogram') expect(duration.state.count).toBe(1)
    expect(waiters?.type).toBe('Gauge')
    if (waiters?.type === 'Gauge') expect(waiters.state.value).toBe(0)
  } finally {
    await runtime.dispose()
  }
})
