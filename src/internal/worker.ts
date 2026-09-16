import { createHash, randomUUID } from 'node:crypto'
import { Cause, Effect, Exit, Result, Schedule, Clock, Semaphore, Tracer } from 'effect'
import { DurableDeferred } from 'effect/unstable/workflow'
import { PersistedQueue } from 'effect/unstable/persistence'
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
import { ActivityEnvelopeSchema, activityDeferred } from './wire'
import type { ActivityEnvelope, EngineQueue } from './wire'

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

const cancelled: Failure = {
  code: 'WORKFLOW_CANCELLED',
  message: 'Workflow cancellation was requested',
  retryable: false
}

export function activityWorker(
  queueDefinition: EngineQueue,
  activities: readonly RegisteredActivity[],
  journal: Journal,
  semaphore: Semaphore.Semaphore,
  options: WorkflowsOptions,
  concurrency: number
) {
  const lease = milliseconds(options.lease?.duration ?? '30s')
  const refresh = milliseconds(options.lease?.refreshInterval ?? '10s')
  const poll = Math.min(milliseconds(options.pollInterval ?? '100ms'), refresh)
  const permits = new Permits(journal)
  const activitiesByIdentity = new Map(
    activities.map((activity) => [
      JSON.stringify([activity.options.name, activity.options.version]),
      activity
    ])
  )

  const execute = (activity: RegisteredActivity, payload: ActivityEnvelope, delivery: number) =>
    Effect.gen(function* () {
      const run = yield* durable(journal.get(payload.executionId))
      if (run.control === 'cancel' || ['completed', 'failed', 'cancelled'].includes(run.state))
        return Exit.fail(cancelled)
      const owner = randomUUID()
      const claim = yield* durable(
        permits.claim(activity.options.queue, payload, delivery, owner, lease)
      )
      // A blocked delivery must leave the queue callback so another item can
      // be admitted. PersistedQueue retries this transport failure without
      // turning it into the activity's business result.
      if (claim === 'blocked') return yield* Effect.fail(new ActivityPermitBlocked())
      if (claim === 'closed') return Exit.fail(cancelled)
      if (claim === 'stale') return yield* Effect.fail(new LeaseLost())
      const owned = claim
      if (owned.state === 'completed') {
        return owned.failure_json
          ? Exit.fail(decode<Failure>(owned.failure_json))
          : Exit.succeed(owned.result_json!)
      }
      if (owned.owner_token !== owner) return yield* Effect.fail(new LeaseLost())

      yield* Effect.addFinalizer(() => permits.release(owned).pipe(Effect.orDie))
      const work = promised(async (signal) => {
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
              // Preserve infrastructure identity: the worker must NACK this delivery.
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
        Effect.result,
        (effect) =>
          journal.clock
            ? Effect.provideService(effect, Clock.Clock, effectClock(journal.clock))
            : effect
      )

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
            if (!(yield* durable(permits.renew(owned, lease))))
              return yield* Effect.fail(new LeaseLost())
          }
        }
      })

      return yield* Effect.gen(function* () {
        const outcome = yield* Effect.raceFirst(work, monitor)
        const failure = Result.isFailure(outcome) ? outcome.failure : null
        const result = Result.isSuccess(outcome) ? outcome.success : null
        if (
          !(yield* durable(
            permits.finish(
              owned,
              result,
              failure,
              payload.attempt < payload.maxAttempts ? payload.retryDelayMs : undefined
            )
          ))
        )
          return yield* Effect.fail(new LeaseLost())
        return failure ? Exit.fail(failure) : Exit.succeed(result!)
      }).pipe(semaphore.withPermits(1))
    }).pipe(Effect.scoped)

  return Effect.gen(function* () {
    const queue = yield* PersistedQueue.make({
      name: queueDefinition.name,
      schema: ActivityEnvelopeSchema,
      // Business retries have their own durable attempt identity. Infrastructure
      // redelivery is not silently dead-lettered after an arbitrary 10 crashes.
      maxAttempts: 2_147_483_647,
      // A process may own only part of a shared queue in distributed mode. An
      // envelope for another owner must become visible to that owner promptly.
      retrySchedule: Schedule.spaced(poll)
    })
    const worker = queue
      .take((item, metadata) => {
        const activity = activitiesByIdentity.get(
          JSON.stringify([item.activityName, item.activityVersion])
        )
        if (!activity)
          return Effect.fail(
            new WorkflowError(
              'ACTIVITY_NOT_AVAILABLE',
              `${item.activityName}@${item.activityVersion} is not registered by this worker`
            )
          )
        return execute(activity, item, metadata.attempts).pipe(
          Effect.flatMap((exit) =>
            DurableDeferred.done(activityDeferred(item.stepId, item.attempt), {
              token: item.token,
              exit
            })
          ),
          Effect.withSpan(
            `better-workflows/activity/${activity.options.name}@${activity.options.version}`,
            {
              parent: Tracer.externalSpan({
                traceId: item.traceId,
                spanId: item.spanId,
                sampled: item.sampled
              })
            }
          )
        )
      })
      .pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
          const error = Cause.findError(cause)
          if (Result.isSuccess(error) && error.success instanceof ActivityPermitBlocked)
            return Effect.sleep(poll)
          return Effect.logWarning('Activity delivery will be retried', Cause.pretty(cause)).pipe(
            Effect.andThen(Effect.sleep(poll))
          )
        }),
        Effect.forever
      )
    yield* Effect.replicateEffect(worker, concurrency, { concurrency, discard: true })
  })
}
