import { createHash, randomUUID } from 'node:crypto'
import { Cause, Effect, Exit, Result, Schema, Semaphore, Tracer } from 'effect'
import { DurableDeferred } from 'effect/unstable/workflow'
import { PersistedQueue } from 'effect/unstable/persistence'
import { ActivityError, WorkflowError, toFailure } from '../errors'
import { SqlError } from 'effect/unstable/sql/SqlError'
import type { Failure } from '../errors'
import type { WorkflowsOptions } from '../types'
import { decode, encode, milliseconds, validate } from './values'
import { durable, promised } from './effects'
import type { Journal } from './journal'
import type { RegisteredActivity } from './registry'
import type { ActivityEnvelope, EngineQueue } from './wire'

class LeaseLost extends Error {
  constructor() {
    super('Activity delivery lost ownership')
    this.name = 'LeaseLost'
  }
}

const cancelled: Failure = {
  code: 'WORKFLOW_CANCELLED',
  message: 'Workflow cancellation was requested',
  retryable: false
}

/**
 * Pinned DurableQueue wire protocol, consumed through Effect's PersistedQueue.
 * Unlike the stock worker, capture typed business failures only: interruption
 * and infrastructure defects must NACK, not complete the workflow's deferred.
 */
export function activityWorker(
  queueDefinition: EngineQueue,
  activity: RegisteredActivity,
  journal: Journal,
  semaphore: Semaphore.Semaphore,
  options: WorkflowsOptions
) {
  const lease = milliseconds(options.lease?.duration ?? '30s')
  const refresh = milliseconds(options.lease?.refreshInterval ?? '10s')
  const poll = Math.min(milliseconds(options.pollInterval ?? '100ms'), refresh)
  const concurrency = options.queues[activity.options.queue]!.concurrency

  const execute = (payload: ActivityEnvelope, delivery: number) =>
    Effect.gen(function* () {
      const run = yield* durable(journal.get(payload.executionId))
      if (run.control === 'cancel' || ['completed', 'failed', 'cancelled'].includes(run.state))
        return Exit.fail(cancelled)
      const owner = randomUUID()
      const claim = yield* durable(
        journal.claim(payload.executionId, payload.stepId, payload.attempt, delivery, owner, lease)
      )
      if (claim.state === 'completed') {
        return claim.failure_json
          ? Exit.fail(decode<Failure>(claim.failure_json))
          : Exit.succeed(claim.result_json!)
      }
      if (claim.owner_token !== owner) return yield* Effect.fail(new LeaseLost())

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
            const exit = await Effect.runPromiseExit(journal.heartbeat(claim, details))
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
        Effect.result
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
            if (!(yield* durable(journal.renewClaim(claim, lease))))
              return yield* Effect.fail(new LeaseLost())
          }
        }
      })

      const outcome = yield* Effect.raceFirst(work, monitor)
      const failure = Result.isFailure(outcome) ? outcome.failure : null
      const result = Result.isSuccess(outcome) ? outcome.success : null
      if (
        !(yield* durable(
          journal.finishClaim(
            claim,
            result,
            failure,
            payload.attempt < payload.maxAttempts ? payload.retryDelayMs : undefined
          )
        ))
      )
        return yield* Effect.fail(new LeaseLost())
      return failure ? Exit.fail(failure) : Exit.succeed(result!)
    }).pipe(semaphore.withPermits(1))

  return Effect.gen(function* () {
    const queue = yield* PersistedQueue.make({
      name: `DurableQueue/${queueDefinition.name}`,
      schema: Schema.Struct({
        token: DurableDeferred.Token,
        payload: queueDefinition.payloadSchema,
        traceId: Schema.String,
        spanId: Schema.String,
        sampled: Schema.Boolean
      }),
      // Business retries have their own durable attempt identity. Infrastructure
      // redelivery is not silently dead-lettered after an arbitrary 10 crashes.
      maxAttempts: 2_147_483_647
    })
    const worker = queue
      .take((item, metadata) =>
        execute(item.payload, metadata.attempts).pipe(
          Effect.flatMap((exit) =>
            DurableDeferred.done(queueDefinition.deferred, { token: item.token, exit })
          ),
          Effect.withSpan(`better-workflows/activity/${activity.options.name}`, {
            parent: Tracer.externalSpan({
              traceId: item.traceId,
              spanId: item.spanId,
              sampled: item.sampled
            })
          })
        )
      )
      .pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
          return Effect.logWarning('Activity delivery will be retried', Cause.pretty(cause)).pipe(
            Effect.andThen(Effect.sleep(poll))
          )
        }),
        Effect.forever
      )
    yield* Effect.replicateEffect(worker, concurrency, { concurrency, discard: true })
  })
}
