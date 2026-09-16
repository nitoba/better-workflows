/**
 * Internal observability vocabulary.
 *
 * Keep telemetry names and attributes here so instrumentation does not create
 * incompatible conventions at each storage or runtime boundary. This module
 * deliberately contains no exporter or public Effect-facing API.
 */

import { Context, Effect, Layer, Metric } from 'effect'

export const TelemetryMetricName = {
  workflowStarted: 'better_workflows.workflow.started',
  workflowCompleted: 'better_workflows.workflow.completed',
  workflowFailed: 'better_workflows.workflow.failed',
  workflowCancelled: 'better_workflows.workflow.cancelled',
  workflowContinued: 'better_workflows.workflow.continued',
  workflowDuration: 'better_workflows.workflow.duration',
  workflowChainDuration: 'better_workflows.workflow.chain.duration',

  activityDispatched: 'better_workflows.activity.dispatched',
  activityStarted: 'better_workflows.activity.started',
  activityCompleted: 'better_workflows.activity.completed',
  activityFailed: 'better_workflows.activity.failed',
  activityRetryScheduled: 'better_workflows.activity.retry_scheduled',
  activityDeliveryRetried: 'better_workflows.activity.delivery_retried',
  activityDeadLettered: 'better_workflows.activity.dead_lettered',
  activityLeaseLost: 'better_workflows.activity.lease_lost',
  activityDuration: 'better_workflows.activity.duration',
  activityQueueWait: 'better_workflows.activity.queue_wait',
  activityPermitWait: 'better_workflows.activity.permit_wait',
  activityRetryDelay: 'better_workflows.activity.retry_delay',
  activityRetryLag: 'better_workflows.activity.retry_lag',

  timerDelivered: 'better_workflows.timer.delivered',
  timerLag: 'better_workflows.timer.lag',

  signalAccepted: 'better_workflows.signal.accepted',
  signalConsumed: 'better_workflows.signal.consumed',
  signalTimeout: 'better_workflows.signal.timeout',
  signalWaitDuration: 'better_workflows.signal.wait_duration',

  deadLetterCreated: 'better_workflows.dead_letter.created',
  deadLetterRequeued: 'better_workflows.dead_letter.requeued',
  deadLetterResolved: 'better_workflows.dead_letter.resolved',
  deadLetterDiscarded: 'better_workflows.dead_letter.discarded',

  resultWaitDuration: 'better_workflows.result.wait_duration',
  resultFallbackPoll: 'better_workflows.result.fallback_poll',
  notifierReconnect: 'better_workflows.notifier.reconnect',
  notifierWaiters: 'better_workflows.notifier.waiters',

  dispatcherIterationDuration: 'better_workflows.dispatcher.iteration_duration',
  dispatcherFailure: 'better_workflows.dispatcher.failure',
  dispatcherRecovery: 'better_workflows.dispatcher.recovery'
} as const

export type TelemetryMetricName = (typeof TelemetryMetricName)[keyof typeof TelemetryMetricName]

export const TelemetryCounterName = {
  workflowStarted: TelemetryMetricName.workflowStarted,
  workflowCompleted: TelemetryMetricName.workflowCompleted,
  workflowFailed: TelemetryMetricName.workflowFailed,
  workflowCancelled: TelemetryMetricName.workflowCancelled,
  workflowContinued: TelemetryMetricName.workflowContinued,
  activityDispatched: TelemetryMetricName.activityDispatched,
  activityStarted: TelemetryMetricName.activityStarted,
  activityCompleted: TelemetryMetricName.activityCompleted,
  activityFailed: TelemetryMetricName.activityFailed,
  activityRetryScheduled: TelemetryMetricName.activityRetryScheduled,
  activityDeliveryRetried: TelemetryMetricName.activityDeliveryRetried,
  activityDeadLettered: TelemetryMetricName.activityDeadLettered,
  activityLeaseLost: TelemetryMetricName.activityLeaseLost,
  timerDelivered: TelemetryMetricName.timerDelivered,
  signalAccepted: TelemetryMetricName.signalAccepted,
  signalConsumed: TelemetryMetricName.signalConsumed,
  signalTimeout: TelemetryMetricName.signalTimeout,
  deadLetterCreated: TelemetryMetricName.deadLetterCreated,
  deadLetterRequeued: TelemetryMetricName.deadLetterRequeued,
  deadLetterResolved: TelemetryMetricName.deadLetterResolved,
  deadLetterDiscarded: TelemetryMetricName.deadLetterDiscarded,
  resultFallbackPoll: TelemetryMetricName.resultFallbackPoll,
  notifierReconnect: TelemetryMetricName.notifierReconnect,
  dispatcherFailure: TelemetryMetricName.dispatcherFailure,
  dispatcherRecovery: TelemetryMetricName.dispatcherRecovery
} as const

export type TelemetryCounterEvent = keyof typeof TelemetryCounterName

export const TelemetryHistogramName = {
  workflowDuration: TelemetryMetricName.workflowDuration,
  workflowChainDuration: TelemetryMetricName.workflowChainDuration,
  activityDuration: TelemetryMetricName.activityDuration,
  activityQueueWait: TelemetryMetricName.activityQueueWait,
  activityPermitWait: TelemetryMetricName.activityPermitWait,
  activityRetryDelay: TelemetryMetricName.activityRetryDelay,
  activityRetryLag: TelemetryMetricName.activityRetryLag,
  timerLag: TelemetryMetricName.timerLag,
  signalWaitDuration: TelemetryMetricName.signalWaitDuration,
  resultWaitDuration: TelemetryMetricName.resultWaitDuration,
  dispatcherIterationDuration: TelemetryMetricName.dispatcherIterationDuration
} as const

export type TelemetryHistogramEvent = keyof typeof TelemetryHistogramName

export const TelemetryGaugeName = {
  notifierWaiters: TelemetryMetricName.notifierWaiters
} as const

export type TelemetryGaugeEvent = keyof typeof TelemetryGaugeName

export const TelemetrySpanName = {
  workflowRound: 'better-workflows.workflow.round',
  workflowStart: 'better-workflows.workflow.start',
  activityDispatch: 'better-workflows.activity.dispatch',
  activityExecute: 'better-workflows.activity.execute',
  activityRedelivery: 'better-workflows.activity.redelivery',
  deadLetterCreate: 'better-workflows.dead_letter.create',
  deadLetterRequeue: 'better-workflows.dead_letter.requeue',
  deadLetterDiscard: 'better-workflows.dead_letter.discard',
  workflowContinueAsNew: 'better-workflows.workflow.continue_as_new',
  workflowChildStart: 'better-workflows.workflow.child.start',
  workflowChildWait: 'better-workflows.workflow.child.wait',
  signalAccept: 'better-workflows.signal.accept',
  signalConsume: 'better-workflows.signal.consume',
  timerDeliver: 'better-workflows.timer.deliver',
  retryDeliver: 'better-workflows.retry.deliver'
} as const

export type TelemetrySpanName = (typeof TelemetrySpanName)[keyof typeof TelemetrySpanName]

export const TelemetryAttributeKey = {
  namespace: 'better_workflows.namespace',
  workflowName: 'better_workflows.workflow.name',
  workflowVersion: 'better_workflows.workflow.version',
  workflowCreated: 'better_workflows.workflow.created',
  executionId: 'better_workflows.execution.id',
  executionChainId: 'better_workflows.execution.chain_id',
  executionGeneration: 'better_workflows.execution.generation',
  nextExecutionId: 'better_workflows.next_execution.id',
  nextGeneration: 'better_workflows.next_generation',
  parentExecutionId: 'better_workflows.parent.execution.id',
  childExecutionId: 'better_workflows.child.execution.id',
  parentClosePolicy: 'better_workflows.parent_close_policy',
  stepId: 'better_workflows.step.id',
  activityName: 'better_workflows.activity.name',
  activityVersion: 'better_workflows.activity.version',
  queueName: 'better_workflows.queue.name',
  activityBusinessAttempt: 'better_workflows.activity.business_attempt',
  activityDeliveryAttempt: 'better_workflows.activity.delivery_attempt',
  executionStatus: 'better_workflows.execution.status',
  failureCode: 'better_workflows.failure.code',
  deadLetterId: 'better_workflows.dead_letter.id',
  deadLetterReason: 'better_workflows.dead_letter.reason',
  signalName: 'better_workflows.signal.name'
} as const

export type TelemetryAttributeKey =
  (typeof TelemetryAttributeKey)[keyof typeof TelemetryAttributeKey]

export type TelemetryAttributeValue = string | number | boolean

/** Reason codes emitted by the library when an activity delivery enters DLQ. */
export const TelemetryDeadLetterReasonCode = {
  payloadDecodeFailed: 'PAYLOAD_DECODE_FAILED',
  invalidActivityEnvelope: 'INVALID_ACTIVITY_ENVELOPE',
  unknownActivity: 'UNKNOWN_ACTIVITY',
  unknownActivityVersion: 'UNKNOWN_ACTIVITY_VERSION',
  deliveryAttemptsExhausted: 'DELIVERY_ATTEMPTS_EXHAUSTED'
} as const

export type TelemetryDeadLetterReasonCode =
  (typeof TelemetryDeadLetterReasonCode)[keyof typeof TelemetryDeadLetterReasonCode]

/** Attributes accepted by spans and structured logs. */
export type TelemetryAttributes = Partial<Record<TelemetryAttributeKey, TelemetryAttributeValue>>

/**
 * Metric dimensions are intentionally narrower than span/log attributes.
 * IDs, attempts, payload-related values and other per-execution data must not
 * become metric series.
 */
export type TelemetryMetricAttributeKey =
  | (typeof TelemetryAttributeKey)['workflowName']
  | (typeof TelemetryAttributeKey)['workflowVersion']
  | (typeof TelemetryAttributeKey)['activityName']
  | (typeof TelemetryAttributeKey)['activityVersion']
  | (typeof TelemetryAttributeKey)['queueName']
  | (typeof TelemetryAttributeKey)['executionStatus']
  | (typeof TelemetryAttributeKey)['deadLetterReason']
  | (typeof TelemetryAttributeKey)['signalName']

export type TelemetryMetricAttributes = Partial<Record<TelemetryMetricAttributeKey, string>>

export const TelemetryLogComponent = {
  runtime: 'runtime',
  dispatcher: 'dispatcher',
  workflow: 'workflow',
  activityWorker: 'activity-worker',
  activityTransport: 'activity-transport',
  notifier: 'notifier',
  deadLetter: 'dead-letter',
  admin: 'admin'
} as const

export type TelemetryLogComponent =
  (typeof TelemetryLogComponent)[keyof typeof TelemetryLogComponent]

export const TelemetryPackage = 'better-workflows' as const

export type TelemetryLogAnnotations = Readonly<
  {
    readonly package: typeof TelemetryPackage
    readonly component: TelemetryLogComponent
  } & TelemetryAttributes
>

const ALL_ATTRIBUTE_KEYS = Object.values(TelemetryAttributeKey)
const METRIC_ATTRIBUTE_KEYS: readonly TelemetryMetricAttributeKey[] = [
  TelemetryAttributeKey.workflowName,
  TelemetryAttributeKey.workflowVersion,
  TelemetryAttributeKey.activityName,
  TelemetryAttributeKey.activityVersion,
  TelemetryAttributeKey.queueName,
  TelemetryAttributeKey.executionStatus,
  TelemetryAttributeKey.deadLetterReason,
  TelemetryAttributeKey.signalName
]
const DEAD_LETTER_REASON_CODES: ReadonlySet<string> = new Set(
  Object.values(TelemetryDeadLetterReasonCode)
)

function copyAttributes(
  attributes: TelemetryAttributes,
  keys: readonly TelemetryAttributeKey[]
): TelemetryAttributes {
  const result: TelemetryAttributes = {}
  for (const key of keys) {
    const value = attributes[key]
    if (value !== undefined) result[key] = value
  }
  return result
}

/** Select the complete semantic attribute set for a span or structured log. */
export function spanAttributes(attributes: TelemetryAttributes): Readonly<TelemetryAttributes> {
  return Object.freeze(copyAttributes(attributes, ALL_ATTRIBUTE_KEYS))
}

/**
 * Project semantic attributes onto the safe metric dimension allow-list.
 * Numeric and boolean values are normalized because Effect metric attributes
 * are string-valued in the pinned runtime.
 */
export function metricAttributes(
  attributes: TelemetryAttributes
): Readonly<Record<string, string>> {
  const result: Record<string, string> = {}
  for (const key of METRIC_ATTRIBUTE_KEYS) {
    const value = attributes[key]
    if (
      value !== undefined &&
      (key !== TelemetryAttributeKey.deadLetterReason ||
        DEAD_LETTER_REASON_CODES.has(String(value)))
    )
      result[key] = String(value)
  }
  return Object.freeze(result)
}

/** Add the standard package/component fields to a sanitized log annotation set. */
export function logAnnotations(
  component: TelemetryLogComponent,
  attributes: TelemetryAttributes = {}
): TelemetryLogAnnotations {
  return Object.freeze({
    package: TelemetryPackage,
    component,
    ...spanAttributes(attributes)
  })
}

export interface TelemetryApi {
  readonly count: (event: TelemetryCounterEvent, attributes?: TelemetryAttributes) => void
  readonly observe: (
    event: TelemetryHistogramEvent,
    value: number,
    attributes?: TelemetryAttributes
  ) => void
  readonly setGauge: (
    event: TelemetryGaugeEvent,
    value: number,
    attributes?: TelemetryAttributes
  ) => void
}

/** Runtime-private service used by Effect programs to record best-effort metrics. */
export class TelemetryService extends Context.Service<TelemetryService, TelemetryApi>()(
  'better-workflows/internal/telemetry'
) {}

const DURATION_BOUNDARIES = [
  0, 1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 300_000, 900_000,
  3_600_000, 86_400_000
] as const

const makeCounter = (name: TelemetryMetricName, description: string) =>
  Metric.counter(name, { description, incremental: true })

const COUNTERS: Record<TelemetryCounterEvent, Metric.Counter<number>> = {
  workflowStarted: makeCounter(TelemetryMetricName.workflowStarted, 'Accepted workflow executions'),
  workflowCompleted: makeCounter(
    TelemetryMetricName.workflowCompleted,
    'Completed workflow generations'
  ),
  workflowFailed: makeCounter(TelemetryMetricName.workflowFailed, 'Failed workflow generations'),
  workflowCancelled: makeCounter(
    TelemetryMetricName.workflowCancelled,
    'Cancelled workflow generations'
  ),
  workflowContinued: makeCounter(
    TelemetryMetricName.workflowContinued,
    'Workflow generations continued as new'
  ),
  activityDispatched: makeCounter(
    TelemetryMetricName.activityDispatched,
    'Persisted activity deliveries'
  ),
  activityStarted: makeCounter(TelemetryMetricName.activityStarted, 'Started activity attempts'),
  activityCompleted: makeCounter(
    TelemetryMetricName.activityCompleted,
    'Completed activity attempts'
  ),
  activityFailed: makeCounter(TelemetryMetricName.activityFailed, 'Failed activity attempts'),
  activityRetryScheduled: makeCounter(
    TelemetryMetricName.activityRetryScheduled,
    'Scheduled activity business retries'
  ),
  activityDeliveryRetried: makeCounter(
    TelemetryMetricName.activityDeliveryRetried,
    'Retried activity deliveries'
  ),
  activityDeadLettered: makeCounter(
    TelemetryMetricName.activityDeadLettered,
    'Activity deliveries moved to dead letters'
  ),
  activityLeaseLost: makeCounter(
    TelemetryMetricName.activityLeaseLost,
    'Activity deliveries that lost their lease'
  ),
  timerDelivered: makeCounter(TelemetryMetricName.timerDelivered, 'Delivered durable timers'),
  signalAccepted: makeCounter(TelemetryMetricName.signalAccepted, 'Accepted durable signals'),
  signalConsumed: makeCounter(TelemetryMetricName.signalConsumed, 'Consumed durable signals'),
  signalTimeout: makeCounter(TelemetryMetricName.signalTimeout, 'Timed out durable signals'),
  deadLetterCreated: makeCounter(
    TelemetryMetricName.deadLetterCreated,
    'Created activity dead letters'
  ),
  deadLetterRequeued: makeCounter(
    TelemetryMetricName.deadLetterRequeued,
    'Requeued activity dead letters'
  ),
  deadLetterResolved: makeCounter(
    TelemetryMetricName.deadLetterResolved,
    'Resolved activity dead letters'
  ),
  deadLetterDiscarded: makeCounter(
    TelemetryMetricName.deadLetterDiscarded,
    'Discarded activity dead letters'
  ),
  resultFallbackPoll: makeCounter(
    TelemetryMetricName.resultFallbackPoll,
    'Result waits that used fallback polling'
  ),
  notifierReconnect: makeCounter(
    TelemetryMetricName.notifierReconnect,
    'PostgreSQL notifier reconnects'
  ),
  dispatcherFailure: makeCounter(
    TelemetryMetricName.dispatcherFailure,
    'Dispatcher iteration failures'
  ),
  dispatcherRecovery: makeCounter(
    TelemetryMetricName.dispatcherRecovery,
    'Dispatcher recoveries after failure'
  )
}

const makeHistogram = (name: TelemetryMetricName, description: string) =>
  Metric.histogram(name, { description, boundaries: DURATION_BOUNDARIES })

const HISTOGRAMS: Record<TelemetryHistogramEvent, Metric.Histogram<number>> = {
  workflowDuration: makeHistogram(
    TelemetryMetricName.workflowDuration,
    'Duration of one workflow generation in milliseconds'
  ),
  workflowChainDuration: makeHistogram(
    TelemetryMetricName.workflowChainDuration,
    'Duration of a continuation chain in milliseconds'
  ),
  activityDuration: makeHistogram(
    TelemetryMetricName.activityDuration,
    'Activity handler duration in milliseconds'
  ),
  activityQueueWait: makeHistogram(
    TelemetryMetricName.activityQueueWait,
    'Activity queue wait in milliseconds'
  ),
  activityPermitWait: makeHistogram(
    TelemetryMetricName.activityPermitWait,
    'Activity permit wait in milliseconds'
  ),
  activityRetryDelay: makeHistogram(
    TelemetryMetricName.activityRetryDelay,
    'Planned activity retry delay in milliseconds'
  ),
  activityRetryLag: makeHistogram(
    TelemetryMetricName.activityRetryLag,
    'Activity retry wake-up lag in milliseconds'
  ),
  timerLag: makeHistogram(
    TelemetryMetricName.timerLag,
    'Durable timer delivery lag in milliseconds'
  ),
  signalWaitDuration: makeHistogram(
    TelemetryMetricName.signalWaitDuration,
    'Signal wait duration in milliseconds'
  ),
  resultWaitDuration: makeHistogram(
    TelemetryMetricName.resultWaitDuration,
    'Workflow result wait duration in milliseconds'
  ),
  dispatcherIterationDuration: makeHistogram(
    TelemetryMetricName.dispatcherIterationDuration,
    'Dispatcher iteration duration in milliseconds'
  )
}

const GAUGES: Record<TelemetryGaugeEvent, Metric.Gauge<number>> = {
  notifierWaiters: Metric.gauge(TelemetryMetricName.notifierWaiters, {
    description: 'Local result waiters in this runtime process'
  })
}

function makeTelemetry(context: Context.Context<never>): TelemetryApi {
  return {
    count(event, attributes = {}) {
      const metric = Metric.withAttributes(COUNTERS[event], metricAttributes(attributes))
      metric.updateUnsafe(1, context)
    },
    observe(event, value, attributes = {}) {
      if (!Number.isFinite(value) || value < 0) return
      const metric = Metric.withAttributes(HISTOGRAMS[event], metricAttributes(attributes))
      metric.updateUnsafe(value, context)
    },
    setGauge(event, value, attributes = {}) {
      if (!Number.isFinite(value) || value < 0) return
      const metric = Metric.withAttributes(GAUGES[event], metricAttributes(attributes))
      metric.updateUnsafe(value, context)
    }
  }
}

const metricRegistryLayer = Layer.effect(
  Metric.MetricRegistry,
  Effect.sync(() => new Map())
)

/** Supply a fresh metric registry and telemetry service for one managed runtime. */
export const telemetryLayer = Layer.effect(
  TelemetryService,
  Effect.gen(function* () {
    const registry = yield* Metric.MetricRegistry
    return TelemetryService.of(makeTelemetry(Context.make(Metric.MetricRegistry, registry)))
  })
).pipe(Layer.provideMerge(metricRegistryLayer))

/** Central vocabulary consumed by later metrics, tracing and logging adapters. */
export const Telemetry = {
  metric: TelemetryMetricName,
  span: TelemetrySpanName,
  attribute: TelemetryAttributeKey,
  deadLetterReasonCode: TelemetryDeadLetterReasonCode,
  counter: TelemetryCounterName,
  histogram: TelemetryHistogramName,
  gauge: TelemetryGaugeName,
  logComponent: TelemetryLogComponent,
  package: TelemetryPackage,
  metricAttributes,
  spanAttributes,
  logAnnotations
} as const
