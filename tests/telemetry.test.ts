import { expect, test } from 'bun:test'
import { ManagedRuntime, Metric } from 'effect'
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

    const snapshots = await first.runPromise(Metric.snapshot)
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

    expect(await second.runPromise(Metric.snapshot)).toEqual([])
  } finally {
    await Promise.all([first.dispose(), second.dispose()])
  }
})
