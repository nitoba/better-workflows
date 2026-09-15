import { createHash } from 'node:crypto'
import type { Type } from '@nestjs/common'
import { Cause, Effect, Exit, Result, Fiber } from 'effect'
import { SqlClient } from 'effect/unstable/sql'
import { PersistedQueue } from 'effect/unstable/persistence'
import {
  DurableClock,
  DurableDeferred,
  DurableQueue,
  Workflow,
  WorkflowEngine
} from 'effect/unstable/workflow'
import { WorkflowError } from '../errors'
import type { Failure } from '../errors'
import type {
  ActivityClient,
  ChildOptions,
  ParallelTasks,
  SagaContext,
  StepOptions,
  WorkflowClass,
  WorkflowContext
} from '../types'
import { AdvancedJournal } from './advanced-journal'
import type { Journal } from './journal'
import type { Registry, RegisteredWorkflow, ActivityContract } from './registry'
import { durable, promised } from './effects'
import { interpretAsync } from './bridge'
import type { Dispatcher } from './bridge'
import { decode, encode, identifier, milliseconds, positiveInteger, validate } from './values'
import { childDeferred, retryDeferred, signalDeferred, timerDeferred } from './wire'
import type { EngineQueue } from './wire'

export type WorkflowServices =
  | WorkflowEngine.WorkflowEngine
  | WorkflowEngine.WorkflowInstance
  | PersistedQueue.PersistedQueueFactory
  | SqlClient.SqlClient
const failure = (code: string, message: string): Failure => ({ code, message, retryable: false })
type WorkflowPath = readonly (readonly string[])[]
const scoped = (path: WorkflowPath, step: string) =>
  path.length === 0 ? step : `@bw/${JSON.stringify([...path, step])}`

/** Each structured branch has its own async interpreter and persisted ordinal stream. */
export class WorkflowInterpreter {
  readonly advanced: AdvancedJournal
  constructor(
    readonly journal: Journal,
    readonly registry: Registry,
    readonly workflow: RegisteredWorkflow,
    readonly executionId: string,
    readonly gate: () => Effect.Effect<void, Failure, WorkflowServices>,
    readonly queue: (activity: ActivityContract) => EngineQueue
  ) {
    this.advanced = new AdvancedJournal(journal)
  }

  run<A>(
    execute: (context: WorkflowContext, dispatch: Dispatcher<WorkflowServices>) => Promise<A>,
    path: WorkflowPath = []
  ): Effect.Effect<A, Failure, WorkflowServices> {
    const self = this
    return Effect.gen(function* () {
      let ordinal = 0
      const used = new Set<string>()
      const scope = path.length ? JSON.stringify(path) : ''
      const result = yield* interpretAsync<A, WorkflowServices>(async (dispatch) => {
        const command = <T>(
          step: string,
          kind: string,
          signature: string,
          operation: (id: string) => Effect.Effect<T, Failure, WorkflowServices>,
          signatureKey?: Effect.Effect<string, Failure>
        ): Promise<T> => {
          identifier(step, 'stepId')
          if (!path.length && step.startsWith('@bw/'))
            throw new WorkflowError('RESERVED_STEP_ID', 'Root step IDs cannot start with @bw/')
          const id = scoped(path, step)
          const sequence = ordinal++
          return dispatch(
            Effect.gen(function* () {
              if (used.has(step))
                return yield* Effect.fail(
                  failure('NON_DETERMINISTIC_WORKFLOW', `Duplicate stepId: ${id}`)
                )
              used.add(step)
              yield* self.gate()
              const recordedSignature = signatureKey
                ? encode([signature, yield* signatureKey])
                : signature
              yield* durable(
                self.journal.beginCommand(
                  self.executionId,
                  id,
                  sequence,
                  recordedSignature,
                  kind,
                  scope
                )
              )
              return yield* operation(id).pipe(
                Effect.matchEffect({
                  onSuccess: (value) =>
                    durable(self.journal.finishCommand(self.executionId, id, true)).pipe(
                      Effect.as(value)
                    ),
                  onFailure: (error) =>
                    durable(self.journal.finishCommand(self.executionId, id, false)).pipe(
                      Effect.andThen(Effect.fail(error))
                    )
                })
              )
            })
          )
        }
        const child = (
          step: string,
          provider: WorkflowClass,
          input: any,
          options: ChildOptions | undefined,
          wait: boolean
        ): Promise<any> => {
          const contract = self.registry.childContract(self.workflow, provider)
          const encoded = encode(input)
          const policy = options?.parentClosePolicy ?? 'request-cancel'
          if (policy !== 'abandon' && policy !== 'request-cancel')
            throw new WorkflowError('INVALID_CONFIGURATION', 'Unknown parentClosePolicy')
          return command(
            step,
            'child',
            encode({
              kind: 'child',
              workflow: contract.options.name,
              version: contract.options.version,
              input: encoded,
              policy,
              wait
            }),
            (id) =>
              Effect.gen(function* () {
                yield* promised(() =>
                  validate(
                    contract.options.input,
                    decode(encoded),
                    `${contract.options.name} input`
                  )
                )
                const key = createHash('sha256')
                  .update(JSON.stringify(['child', self.executionId, id]))
                  .digest('hex')
                const childId = yield* contract.definition.executionId({ key, input: encoded })
                yield* durable(
                  self.advanced.linkChild(
                    self.executionId,
                    id,
                    childId,
                    contract.options.name,
                    contract.options.version,
                    key,
                    encoded,
                    policy
                  )
                )
                if (!wait) return { executionId: childId }
                const value = yield* DurableDeferred.await(childDeferred(id))
                return decode(value)
              })
          )
        }
        const context: WorkflowContext = {
          executionId: self.executionId,
          activities<T>(provider: Type<T>): ActivityClient<T> {
            const entries = self.registry.activitiesFor(self.workflow, provider).map((activity) => [
              activity.method,
              (input: any, options: StepOptions) => {
                const encoded = encode(input)
                const policy = activity.options.retry
                const retry = {
                  maxAttempts: policy?.maxAttempts ?? 1,
                  backoff: policy?.backoff ?? 'exponential',
                  initialDelay: milliseconds(policy?.initialDelay ?? '1s'),
                  maxDelay: milliseconds(policy?.maxDelay ?? '1m')
                }
                const timeoutMs = milliseconds(activity.options.timeout ?? '5m')
                // Preserve the v1 command signature for unkeyed activities.
                let concurrencyKey: string | undefined
                const signatureKey = activity.options.key
                  ? promised(async () => {
                      const parsed = await validate(
                        activity.options.input,
                        decode(encoded),
                        `${activity.options.name} input`
                      )
                      concurrencyKey = activity.options.key!(parsed)
                      identifier(concurrencyKey, 'Activity concurrency key')
                      return concurrencyKey
                    })
                  : undefined
                const signature = encode({
                  kind: 'activity',
                  name: activity.options.name,
                  version: activity.options.version,
                  queue: activity.options.queue,
                  input: encoded,
                  retry,
                  timeoutMs
                })
                return command(
                  options.stepId,
                  'activity',
                  signature,
                  (id) =>
                    Effect.gen(function* () {
                      if (!signatureKey)
                        yield* promised(() =>
                          validate(
                            activity.options.input,
                            decode(encoded),
                            `${activity.options.name} input`
                          )
                        )
                      for (let attempt = 1; attempt <= retry.maxAttempts; attempt++) {
                        yield* self.gate()
                        const result = yield* Effect.result(
                          DurableQueue.process(self.queue(activity), {
                            executionId: self.executionId,
                            stepId: id,
                            name: activity.options.name,
                            version: activity.options.version,
                            input: encoded,
                            attempt,
                            timeoutMs,
                            maxAttempts: retry.maxAttempts,
                            retryDelayMs: Math.min(
                              retry.maxDelay,
                              retry.initialDelay *
                                (retry.backoff === 'exponential' ? 2 ** (attempt - 1) : 1)
                            ),
                            concurrencyKey
                          })
                        )
                        if (Result.isSuccess(result)) return decode(result.success)
                        if (!result.failure.retryable || attempt === retry.maxAttempts)
                          return yield* Effect.fail(result.failure)
                        yield* DurableDeferred.await(retryDeferred(id, attempt))
                      }
                      return yield* Effect.die('Invalid retry policy')
                    }),
                  signatureKey
                )
              }
            ])
            // Safety: discovered decorators define these methods; inputs and results are schema validated.
            return Object.fromEntries(entries) as ActivityClient<T>
          },
          sleep(step, duration) {
            const delay = milliseconds(duration)
            return command(step, 'timer', encode({ kind: 'timer', delay }), (id) =>
              Effect.gen(function* () {
                const protocol = yield* durable(self.advanced.timer(self.executionId, id, delay))
                if (protocol === 'legacy')
                  return yield* DurableClock.sleep({
                    name: `sleep/${encodeURIComponent(id)}`,
                    duration: delay,
                    inMemoryThreshold: 0
                  })
                return yield* DurableDeferred.await(timerDeferred(id))
              })
            )
          },
          waitForSignal(step, signal, options) {
            const allowed = self.workflow.options.signals?.find(
              (candidate) => candidate.name === signal.name
            )
            if (!allowed) throw new WorkflowError('UNKNOWN_SIGNAL', signal.name)
            const timeout =
              options?.timeout === undefined ? undefined : milliseconds(options.timeout)
            return command(
              step,
              'signal',
              encode({ kind: 'signal', name: signal.name, timeout: timeout ?? null }),
              (id) =>
                Effect.gen(function* () {
                  yield* durable(self.journal.wait(self.executionId, id, allowed.name, timeout))
                  return decode(yield* DurableDeferred.await(signalDeferred(id)))
                })
            )
          },
          map(step, items, options, execute) {
            positiveInteger(options.concurrency, 'Map concurrency')
            const keys = items.map((item, index) => options.key(item, index))
            for (const key of keys) identifier(key, 'Branch key')
            if (new Set(keys).size !== keys.length)
              throw new WorkflowError('DUPLICATE_BRANCH_KEY', 'Map keys must be unique')
            return command(
              step,
              'map',
              encode({ kind: 'map', items, keys, concurrency: options.concurrency }),
              (id) =>
                self.group(id, keys, options.concurrency, (key) => {
                  const index = keys.indexOf(key)
                  return self.run(
                    (ctx) => execute(items[index]!, ctx, index),
                    [...path, ['branch', step, key]]
                  )
                })
            )
          },
          parallel<T extends ParallelTasks>(
            step: string,
            tasks: T,
            options?: { readonly concurrency?: number }
          ) {
            const keys = Object.keys(tasks).sort()
            const concurrency = options?.concurrency ?? Math.max(1, keys.length)
            positiveInteger(concurrency, 'Parallel concurrency')
            for (const key of keys) identifier(key, 'Branch key')
            return command(
              step,
              'parallel',
              encode({ kind: 'parallel', keys, concurrency }),
              (id) =>
                self
                  .group(id, keys, concurrency, (key) =>
                    self.run(tasks[key]!, [...path, ['branch', step, key]])
                  )
                  .pipe(
                    Effect.map((values) => {
                      // Safety: keys and results use the same stable ordered branch list.
                      return Object.fromEntries(keys.map((key, i) => [key, values[i]])) as {
                        [K in keyof T]: Awaited<ReturnType<T[K]>>
                      }
                    })
                  )
            )
          },
          child: (step, provider, input, options) => child(step, provider, input, options, true),
          startChild: (step, provider, input, options) =>
            child(step, provider, input, options, false),
          saga<A>(step: string, execute: (saga: SagaContext) => Promise<A>): Promise<A> {
            return command(step, 'saga', encode({ kind: 'saga' }), (id) =>
              self.saga(id, [...path, ['saga', step]], execute)
            )
          }
        }
        return execute(context, dispatch)
      })
      yield* self.gate()
      yield* durable(self.journal.assertEnd(self.executionId, ordinal, scope))
      return result
    })
  }

  private group<A>(
    id: string,
    keys: readonly string[],
    concurrency: number,
    execute: (key: string) => Effect.Effect<A, Failure, WorkflowServices>
  ): Effect.Effect<A[], Failure, WorkflowServices> {
    const self = this
    return Effect.gen(function* () {
      yield* durable(self.advanced.initializeBranches(self.executionId, id, keys))
      const attempted = new Set<string>()
      while (true) {
        const active = yield* durable(
          self.advanced.admitBranches(self.executionId, id, concurrency)
        )
        const todo = active.filter((row) => !attempted.has(row.branch_key))
        const fibers = yield* Effect.forEach(todo, (row) =>
          execute(row.branch_key).pipe(
            Effect.tap((value) => promised(async () => encode(value))),
            Effect.forkChild({ startImmediately: true })
          )
        )
        yield* Effect.forEach(
          todo,
          (row, index) =>
            Effect.gen(function* () {
              attempted.add(row.branch_key)
              const exit = yield* Fiber.await(fibers[index]!)
              if (Exit.isSuccess(exit)) {
                yield* durable(
                  self.advanced.finishBranch(
                    self.executionId,
                    id,
                    row.branch_key,
                    encode(exit.value),
                    null
                  )
                )
              } else {
                const errors = Cause.findError(exit.cause)
                if (Result.isSuccess(errors)) {
                  if (errors.success.code === 'NON_DETERMINISTIC_WORKFLOW')
                    return yield* Effect.fail(errors.success)
                  yield* durable(
                    self.advanced.finishBranch(
                      self.executionId,
                      id,
                      row.branch_key,
                      null,
                      errors.success
                    )
                  )
                } else if (!Cause.hasInterruptsOnly(exit.cause))
                  return yield* Effect.failCause(exit.cause)
              }
            }),
          { concurrency: 'unbounded', discard: true }
        )
        const rows = yield* durable(self.advanced.branches(self.executionId, id))
        if (rows.every((row) => row.state === 'completed' || row.state === 'failed')) {
          const failed = rows.find((row) => row.state === 'failed')
          if (failed) return yield* Effect.fail(decode<Failure>(failed.failure_json!))
          return rows.map((row) => decode<A>(row.result_json!))
        }
        if (
          !rows.some((row) => row.state === 'pending') ||
          rows.filter((row) => row.state === 'running').length >= concurrency
        ) {
          return yield* Workflow.suspend(yield* WorkflowEngine.WorkflowInstance)
        }
      }
    })
  }

  private saga<A>(
    id: string,
    path: WorkflowPath,
    execute: (saga: SagaContext) => Promise<A>
  ): Effect.Effect<A, Failure, WorkflowServices> {
    const self = this
    return Effect.gen(function* () {
      const record = yield* durable(self.advanced.saga(self.executionId, id))
      if (record.state === 'completed') return decode<A>(record.result_json!)
      if (record.state === 'compensated' || record.state === 'compensation-failed')
        return yield* Effect.fail(decode<Failure>(record.failure_json!))
      const callbacks = new Map<string, (value: any, ctx: WorkflowContext) => Promise<void>>()
      const forward = yield* Effect.result(
        self
          .run(async (context, dispatch) => {
            let ordinal = 0
            const saga: SagaContext = {
              ...context,
              step<T>(
                step: string,
                run: (ctx: WorkflowContext) => Promise<T>,
                compensate: (value: T, ctx: WorkflowContext) => Promise<void>
              ): Promise<T> {
                identifier(step, 'Saga step ID')
                if (callbacks.has(step))
                  throw new WorkflowError(
                    'NON_DETERMINISTIC_WORKFLOW',
                    `Duplicate saga step: ${step}`
                  )
                callbacks.set(step, compensate)
                const sequence = ordinal++
                return dispatch(
                  Effect.gen(function* () {
                    const cached = (yield* durable(
                      self.advanced.compensations(self.executionId, id)
                    )).find((row) => row.step_id === step)
                    if (cached) {
                      if (cached.ordinal !== sequence)
                        return yield* Effect.fail(
                          failure('NON_DETERMINISTIC_WORKFLOW', `Saga step order changed: ${step}`)
                        )
                      return decode<T>(cached.result_json)
                    }
                    if (record.state !== 'running')
                      return yield* Effect.fail(decode<Failure>(record.failure_json!))
                    const result = yield* self
                      .run(run, [...path, ['forward', step]])
                      .pipe(Effect.tap((value) => promised(async () => encode(value))))
                    yield* durable(
                      self.advanced.registerCompensation(
                        self.executionId,
                        id,
                        step,
                        sequence,
                        encode(result)
                      )
                    )
                    return result
                  })
                )
              }
            }
            return execute(saga)
          }, path)
          .pipe(Effect.tap((value) => promised(async () => encode(value))))
      )
      if (Result.isSuccess(forward) && record.state === 'running') {
        yield* durable(
          self.advanced.sagaState(self.executionId, id, 'completed', encode(forward.success), null)
        )
        return forward.success
      }
      const original = record.failure_json
        ? decode<Failure>(record.failure_json)
        : Result.isFailure(forward)
          ? forward.failure
          : failure('SAGA_FAILED', 'Saga failed')
      if (original.code === 'NON_DETERMINISTIC_WORKFLOW') return yield* Effect.fail(original)
      yield* durable(self.advanced.sagaState(self.executionId, id, 'compensating', null, original))
      const compensations = yield* durable(self.advanced.compensations(self.executionId, id))
      for (const row of compensations) {
        const callback = callbacks.get(row.step_id)
        if (!callback)
          return yield* Effect.fail(
            failure('NON_DETERMINISTIC_WORKFLOW', `Missing compensation: ${row.step_id}`)
          )
        if (row.state !== 'registered') continue
        const result = yield* Effect.result(
          self.run(
            (ctx) => callback(decode(row.result_json), ctx),
            [...path, ['compensate', row.step_id]]
          )
        )
        if (Result.isFailure(result) && result.failure.code === 'NON_DETERMINISTIC_WORKFLOW')
          return yield* Effect.fail(result.failure)
        yield* durable(
          self.advanced.finishCompensation(
            self.executionId,
            id,
            row.step_id,
            Result.isFailure(result) ? result.failure : null
          )
        )
      }
      const failed = (yield* durable(self.advanced.compensations(self.executionId, id))).filter(
        (row) => row.state === 'failed'
      )
      const final = failed.length
        ? failure(
            'COMPENSATION_FAILED',
            `${original.code}: ${original.message}; failed compensations: ${failed.map((row) => row.step_id).join(', ')}`
          )
        : original
      yield* durable(
        self.advanced.sagaState(
          self.executionId,
          id,
          failed.length ? 'compensation-failed' : 'compensated',
          null,
          final
        )
      )
      return yield* Effect.fail(final)
    })
  }
}
