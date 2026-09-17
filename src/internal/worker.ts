import { createHash, randomUUID } from 'node:crypto'
import { Cause, Clock, Effect, Exit, Result, Schema, Semaphore, Tracer } from 'effect'
import { DurableDeferred } from 'effect/unstable/workflow'
import { ActivityError, WorkflowError, toFailure } from '../errors'
import { SqlError } from 'effect/unstable/sql/SqlError'
import type { Failure } from '../errors'
import type { WorkflowsOptions } from '../types'
import { decode, encode, milliseconds, validate } from './values'
import { Permits } from './permits'
import { effectClock } from './clock'
import { durable, promised } from './effects'
import type { Journal } from './journal'
import type { RegisteredActivity } from './registry'
import { ActivityTransport, activityMetadata, type ActivityDelivery } from './activity-transport'
import { ActivityEnvelopeSchema, activityDeferred } from './wire'
import type { ActivityEnvelope } from './wire'
import {
  logAnnotations,
  TelemetryAttributeKey,
  TelemetryLogComponent,
  TelemetrySpanName
} from './telemetry'
import type { TelemetryAttributes } from './telemetry'

class LeaseLost extends Error {
  constructor() {
    super('Activity delivery lost ownership')
    this.name = 'LeaseLost'
  }
}

class ActivityPermitBlocked extends Error {
  constructor() {
    super('Activity delivery is waiting for a concurrency permit')
    this.name = 'ActivityPermitBlocked'
  }
}

class ActivityDeliveryError extends Error {
  constructor(
    readonly reasonCode: string,
    message: string,
    readonly metadata: ReturnType<typeof activityMetadata>
  ) {
    super(message)
    this.name = 'ActivityDeliveryError'
  }
}

const cancelled: Failure = {
  code: 'WORKFLOW_CANCELLED',
  message: 'Workflow cancellation was requested',
  retryable: false
}

function decodeEnvelope(payload: string): ActivityEnvelope {
  let value: unknown
  try {
    value = JSON.parse(payload)
  } catch {
    throw new ActivityDeliveryError(
      'PAYLOAD_DECODE_FAILED',
      'The persisted activity payload is not valid JSON',
      activityMetadata(payload)
    )
  }
  try {
    return Schema.decodeUnknownSync(ActivityEnvelopeSchema)(value)
  } catch {
    throw new ActivityDeliveryError(
      'INVALID_ACTIVITY_ENVELOPE',
      'The persisted activity envelope does not match the supported protocol',
      activityMetadata(payload)
    )
  }
}

const activityMetricAttributes = (activity: RegisteredActivity) => ({
  [TelemetryAttributeKey.activityName]: activity.options.name,
  [TelemetryAttributeKey.activityVersion]: activity.options.version,
  [TelemetryAttributeKey.queueName]: activity.options.queue
})

const activitySpanAttributes = (
  activity: RegisteredActivity,
  payload: Pick<ActivityEnvelope, 'executionId' | 'stepId' | 'attempt'>,
  deliveryAttempt: number
) => ({
  ...activityMetricAttributes(activity),
  [TelemetryAttributeKey.executionId]: payload.executionId,
  [TelemetryAttributeKey.stepId]: payload.stepId,
  [TelemetryAttributeKey.activityBusinessAttempt]: payload.attempt,
  [TelemetryAttributeKey.activityDeliveryAttempt]: deliveryAttempt
})

const workerLogAnnotations = (
  journal: Journal,
  queue: string,
  attributes: TelemetryAttributes = {}
) =>
  logAnnotations(TelemetryLogComponent.activityWorker, {
    [TelemetryAttributeKey.namespace]: journal.namespace,
    [TelemetryAttributeKey.queueName]: queue,
    ...attributes
  })

const leaseLostLog = (journal: Journal, queue: string, attributes: TelemetryAttributes) =>
  Effect.annotateLogs(
    Effect.logWarning('Activity lease lost'),
    workerLogAnnotations(journal, queue, {
      ...attributes,
      [TelemetryAttributeKey.failureCode]: 'LEASE_LOST'
    })
  )

const activityMetadataLogAttributes = (
  metadata: ReturnType<typeof activityMetadata>
): TelemetryAttributes => {
  const attributes: TelemetryAttributes = {}
  if (metadata.executionId !== null)
    attributes[TelemetryAttributeKey.executionId] = metadata.executionId
  if (metadata.stepId !== null) attributes[TelemetryAttributeKey.stepId] = metadata.stepId
  if (metadata.activityName !== null)
    attributes[TelemetryAttributeKey.activityName] = metadata.activityName
  if (metadata.activityVersion !== null)
    attributes[TelemetryAttributeKey.activityVersion] = metadata.activityVersion
  return attributes
}

export function activityWorker(
  queue: string,
  activities: readonly RegisteredActivity[],
  journal: Journal,
  semaphore: Semaphore.Semaphore,
  options: WorkflowsOptions,
  concurrency: number,
  transport: ActivityTransport
) {
  const lease = milliseconds(options.lease?.duration ?? '30s')
  const refresh = milliseconds(options.lease?.refreshInterval ?? '10s')
  const poll = Math.min(milliseconds(options.pollInterval ?? '100ms'), refresh)
  const maxDeliveryAttempts = options.deadLetter?.maxDeliveryAttempts ?? 10
  const permits = new Permits(journal)
  const activitiesByIdentity = new Map(
    activities.map((activity) => [
      JSON.stringify([activity.options.name, activity.options.version]),
      activity
    ])
  )
  const activityNames = new Set(activities.map((activity) => activity.options.name))

  const execute = (
    activity: RegisteredActivity,
    payload: ActivityEnvelope,
    delivery: ActivityDelivery
  ) =>
    Effect.gen(function* () {
      const attributes = activityMetricAttributes(activity)
      const run = yield* durable(journal.get(payload.executionId))
      if (
        run.control === 'cancel' ||
        ['continued', 'completed', 'failed', 'cancelled'].includes(run.state)
      )
        return Exit.fail(cancelled)
      const owner = randomUUID()
      const claim = yield* durable(
        permits.claim(activity.options.queue, payload, delivery.deliveryAttempt, owner, lease)
      )
      // A blocked delivery is released without changing its transport attempt.
      if (claim === 'blocked') return yield* Effect.fail(new ActivityPermitBlocked())
      if (claim === 'closed') return Exit.fail(cancelled)
      if (claim === 'stale') {
        journal.telemetry?.count('activityLeaseLost', attributes)
        yield* leaseLostLog(
          journal,
          queue,
          activitySpanAttributes(activity, payload, delivery.deliveryAttempt)
        )
        return yield* Effect.fail(new LeaseLost())
      }
      const owned = claim
      if (owned.state === 'completed') {
        return owned.failure_json
          ? Exit.fail(decode<Failure>(owned.failure_json))
          : Exit.succeed(owned.result_json!)
      }
      if (owned.owner_token !== owner) {
        journal.telemetry?.count('activityLeaseLost', attributes)
        yield* leaseLostLog(
          journal,
          queue,
          activitySpanAttributes(activity, payload, delivery.deliveryAttempt)
        )
        return yield* Effect.fail(new LeaseLost())
      }

      journal.telemetry?.count('activityStarted', attributes)
      yield* Effect.annotateLogs(
        Effect.logDebug('Activity claimed'),
        workerLogAnnotations(
          journal,
          queue,
          activitySpanAttributes(activity, payload, delivery.deliveryAttempt)
        )
      )
      if (permits.usesPermits(owned) && delivery.acquiredAt !== null) {
        const permitAt = yield* durable(journal.databaseNow())
        journal.telemetry?.observe(
          'activityPermitWait',
          Math.max(0, permitAt - delivery.acquiredAt),
          attributes
        )
      }

      yield* Effect.addFinalizer(() => permits.release(owned).pipe(Effect.orDie))
      const rawWork = promised(async (signal) => {
        const input = await validate(
          activity.options.input,
          decode(payload.input),
          `${activity.options.name} input`
        )
        const value = await activity.invoke(input, {
          executionId: payload.executionId,
          stepId: payload.stepId,
          attempt: payload.attempt,
          idempotencyKey: createHash('sha256')
            .update(JSON.stringify([payload.executionId, payload.stepId]))
            .digest('hex'),
          signal,
          async heartbeat(details = null) {
            if (signal.aborted)
              throw new WorkflowError('LEASE_LOST', 'This invocation has been aborted')
            const exit = await Effect.runPromiseExit(journal.heartbeat(owned, details))
            if (Exit.isFailure(exit)) {
              const error = Cause.squash(exit.cause)
              if (error instanceof SqlError) throw error
              throw new ActivityError(toFailure(error))
            }
          }
        })
        if (signal.aborted)
          throw new WorkflowError('LEASE_LOST', 'This invocation has been aborted')
        return encode(
          await validate(activity.options.output, value, `${activity.options.name} output`)
        )
      }).pipe(
        Effect.timeoutOrElse({
          duration: payload.timeoutMs,
          orElse: () =>
            Effect.fail<Failure>({
              code: 'ACTIVITY_TIMEOUT',
              message: `${activity.options.name} exceeded its timeout`,
              retryable: true
            })
        }),
        (effect) =>
          journal.clock
            ? Effect.provideService(effect, Clock.Clock, effectClock(journal.clock))
            : effect
      )
      const work = Effect.useSpan(
        TelemetrySpanName.activityExecute,
        {
          attributes: activitySpanAttributes(activity, payload, delivery.deliveryAttempt),
          parent: Tracer.externalSpan({
            traceId: payload.traceId,
            spanId: payload.spanId,
            sampled: payload.sampled
          }),
          kind: 'consumer'
        },
        (span) =>
          rawWork.pipe(
            Effect.tapError((error) =>
              Effect.sync(() => span.attribute(TelemetryAttributeKey.failureCode, error.code))
            )
          )
      ).pipe(Effect.result)

      const monitor = Effect.gen(function* () {
        let elapsed = 0
        while (true) {
          yield* Effect.sleep(poll)
          elapsed += poll
          const current = yield* durable(journal.get(payload.executionId))
          if (current.control === 'cancel' || current.state === 'cancelled')
            return Result.fail(cancelled)
          if (elapsed >= refresh) {
            elapsed = 0
            if (!(yield* durable(permits.renew(owned, lease)))) {
              journal.telemetry?.count('activityLeaseLost', attributes)
              yield* leaseLostLog(
                journal,
                queue,
                activitySpanAttributes(activity, payload, delivery.deliveryAttempt)
              )
              return yield* Effect.fail(new LeaseLost())
            }
          }
        }
      })

      let handlerFinished = false
      const handlerStartedAt = Date.now()
      const measuredWork = work.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            handlerFinished = true
          })
        )
      )
      return yield* Effect.gen(function* () {
        const outcome = yield* Effect.raceFirst(measuredWork, monitor)
        const failure = Result.isFailure(outcome) ? outcome.failure : null
        const result = Result.isSuccess(outcome) ? outcome.success : null
        const retryDelay =
          failure && payload.attempt < payload.maxAttempts ? payload.retryDelayMs : undefined
        if (!(yield* durable(permits.finish(owned, result, failure, retryDelay)))) {
          journal.telemetry?.count('activityLeaseLost', attributes)
          yield* leaseLostLog(
            journal,
            queue,
            activitySpanAttributes(activity, payload, delivery.deliveryAttempt)
          )
          return yield* Effect.fail(new LeaseLost())
        }
        if (handlerFinished)
          journal.telemetry?.observe(
            'activityDuration',
            Math.max(0, Date.now() - handlerStartedAt),
            attributes
          )
        if (failure) journal.telemetry?.count('activityFailed', attributes)
        else journal.telemetry?.count('activityCompleted', attributes)
        if (retryDelay !== undefined) {
          journal.telemetry?.count('activityRetryScheduled', attributes)
          journal.telemetry?.observe('activityRetryDelay', retryDelay, attributes)
        }
        const logAttributes: TelemetryAttributes = activitySpanAttributes(
          activity,
          payload,
          delivery.deliveryAttempt
        )
        if (failure) logAttributes[TelemetryAttributeKey.failureCode] = failure.code
        yield* Effect.annotateLogs(
          Effect.logDebug(failure ? 'Activity failed' : 'Activity completed'),
          workerLogAnnotations(journal, queue, logAttributes)
        )
        return failure ? Exit.fail(failure) : Exit.succeed(result!)
      }).pipe(semaphore.withPermits(1))
    }).pipe(Effect.scoped)

  const process = Effect.gen(function* () {
    const delivery = yield* durable(transport.take(queue, maxDeliveryAttempts, lease))
    if (!delivery) {
      yield* durable(transport.exhausted(queue, maxDeliveryAttempts, lease))
      yield* Effect.sleep(poll)
      return
    }
    const parsed = yield* Effect.result(
      Effect.try({
        try: () => decodeEnvelope(delivery.payload),
        // SAFETY: the decoder intentionally returns its operational error object through Effect.
        catch: (error) => error
      })
    )
    if (Result.isFailure(parsed)) {
      const error = parsed.failure
      if (error instanceof ActivityDeliveryError)
        yield* durable(
          transport.deadLetter(delivery, error.metadata, error.reasonCode, error.message)
        )
      else {
        yield* durable(transport.retry(delivery, 'Envelope decode failed', poll))
        const metadata = activityMetadata(delivery.payload)
        const attributes = activityMetadataLogAttributes(metadata)
        attributes[TelemetryAttributeKey.failureCode] = toFailure(error).code
        yield* Effect.annotateLogs(
          Effect.logWarning('Activity delivery will be retried'),
          workerLogAnnotations(journal, queue, attributes)
        )
      }
      return
    }
    const envelope = parsed.success
    const activity = activitiesByIdentity.get(
      JSON.stringify([envelope.activityName, envelope.activityVersion])
    )
    if (!activity) {
      const reasonCode = activityNames.has(envelope.activityName)
        ? 'UNKNOWN_ACTIVITY_VERSION'
        : 'UNKNOWN_ACTIVITY'
      yield* durable(
        transport.deadLetter(
          delivery,
          activityMetadata(delivery.payload),
          reasonCode,
          `${envelope.activityName}@${envelope.activityVersion} is not registered by this worker`
        )
      )
      return
    }
    const tracedExecution =
      delivery.deliveryAttempt > 1
        ? execute(activity, envelope, delivery).pipe(
            Effect.tapError((error) =>
              Effect.annotateCurrentSpan(TelemetryAttributeKey.failureCode, toFailure(error).code)
            ),
            Effect.withSpan(TelemetrySpanName.activityRedelivery, {
              attributes: activitySpanAttributes(activity, envelope, delivery.deliveryAttempt),
              parent: Tracer.externalSpan({
                traceId: envelope.traceId,
                spanId: envelope.spanId,
                sampled: envelope.sampled
              }),
              kind: 'consumer'
            })
          )
        : execute(activity, envelope, delivery)
    const outcome = yield* Effect.result(tracedExecution)
    if (Result.isFailure(outcome)) {
      const error = outcome.failure
      if (error instanceof ActivityPermitBlocked) yield* durable(transport.release(delivery, poll))
      else {
        yield* durable(
          transport.retry(
            delivery,
            error instanceof Error ? error.message : 'Activity delivery failed',
            poll
          )
        )
        yield* Effect.annotateLogs(
          Effect.logWarning('Activity delivery will be retried'),
          workerLogAnnotations(journal, queue, {
            ...activitySpanAttributes(activity, envelope, delivery.deliveryAttempt),
            [TelemetryAttributeKey.failureCode]: toFailure(error).code
          })
        )
      }
      return
    }
    yield* DurableDeferred.done(activityDeferred(envelope.stepId, envelope.attempt), {
      token: envelope.token,
      exit: outcome.success
    })
    if (!(yield* durable(transport.complete(delivery)))) {
      journal.telemetry?.count('activityLeaseLost', activityMetricAttributes(activity))
      yield* Effect.fail(new LeaseLost())
    }
  })

  const worker = Effect.gen(function* () {
    while (true) {
      yield* process.pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.annotateLogs(
                Effect.logWarning('Activity delivery will be retried'),
                workerLogAnnotations(journal, queue, {
                  [TelemetryAttributeKey.failureCode]: toFailure(Cause.squash(cause)).code
                })
              ).pipe(Effect.andThen(Effect.sleep(poll)))
        )
      )
    }
  })
  return Effect.replicateEffect(worker, concurrency, { concurrency, discard: true })
}
