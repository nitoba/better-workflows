import { randomUUID } from 'node:crypto'
import { Inject, Injectable, Logger, Optional } from '@nestjs/common'
import type { OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown } from '@nestjs/common'
import { DiscoveryService } from '@nestjs/core'
import { Cause, Effect, Exit, Option, Scope, Semaphore } from 'effect'
import { SqlClient } from 'effect/unstable/sql'
import { DurableDeferred, Workflow, WorkflowEngine } from 'effect/unstable/workflow'
import { PersistedQueue } from 'effect/unstable/persistence'
import { WorkflowError, toFailure } from '../errors'
import type { Failure } from '../errors'
import type {
  ExecutionSnapshot,
  SignalDefinition,
  WorkflowClass,
  WorkflowInput,
  WorkflowsOptions
} from '../types'
import { Registry } from './registry'
import type { ActivityContract, RegisteredWorkflow } from './registry'
import { Journal } from './journal'
import type { RunRow } from './journal'
import { decode, encode, identifier, milliseconds, validate } from './values'
import { makeInfrastructure, validateOptions } from './infrastructure'
import type { Infrastructure } from './infrastructure'
import { durable, promised } from './effects'
import { activityQueue, retryDeferred, signalDeferred, workflowDefinition } from './wire'
import type { EngineQueue } from './wire'
import type { AdminBackend } from '../admin-types'
import { SqlAdministration } from './administration'
import { migrationStatus, validateMigrations } from './schema-admin'
import { Permits } from './permits'
import { WorkflowInterpreter } from './interpreter'
import { AdvancedJournal } from './advanced-journal'
import { WORKFLOWS_TEST_CLOCK } from './clock'
import type { BusinessClock } from './clock'
import { childDeferred, timerDeferred } from './wire'
import { activityWorker } from './worker'

export const WORKFLOWS_OPTIONS = Symbol.for('better-workflows/options')
type Services =
  | WorkflowEngine.WorkflowEngine
  | PersistedQueue.PersistedQueueFactory
  | SqlClient.SqlClient
const terminal = (row: RunRow) => ['completed', 'failed', 'cancelled'].includes(row.state)

@Injectable()
export class WorkflowsRuntime
  implements OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown
{
  private readonly logger = new Logger('better-workflows')
  readonly registry: Registry
  private infrastructure: Infrastructure | undefined
  private journal: Journal | undefined
  private readonly queues = new Map<string, EngineQueue>()
  private ready = false
  private stopping = false
  private stopPromise?: Promise<void>
  private dispatchCursor = ''
  private waitCursor = ''
  private readonly dispatchLock = Semaphore.makeUnsafe(1)
  private lastDispatchError: string | undefined

  constructor(
    @Inject(WORKFLOWS_OPTIONS) readonly options: WorkflowsOptions,
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Optional() @Inject(WORKFLOWS_TEST_CLOCK) private readonly clock?: BusinessClock
  ) {
    validateOptions(options)
    this.registry = new Registry(options)
  }

  registerContract(workflow: WorkflowClass): void {
    this.registry.contract(workflow)
  }

  async onApplicationBootstrap(): Promise<void> {
    if (this.ready) return
    const roots = new Set(
      this.discovery
        .getProviders()
        .flatMap((wrapper) => (wrapper.token === WorkflowsRuntime ? [wrapper.instance] : []))
    )
    if (roots.size > 1)
      throw new WorkflowError(
        'DUPLICATE_ROOT',
        'Register WorkflowsModule.forRoot only once per Nest application'
      )
    this.registry.discover(this.discovery)
    for (const activity of this.registry.activities.values()) this.queue(activity)
    const infrastructure = await makeInfrastructure(this.options)
    this.infrastructure = infrastructure
    try {
      const sql = await infrastructure.runPromise(SqlClient.SqlClient)
      this.journal = new Journal(sql, this.options.namespace, this.clock)
      for (const [name, options] of this.registry.queues.entries())
        await this.run(new Permits(this.journal).register(name, options))
      const workflowSlots = Semaphore.makeUnsafe(
        this.options.execution?.workflows?.concurrency ?? 20
      )
      const featureSlots = new Map<symbol, Semaphore.Semaphore>()
      if (this.options.execution?.workflows?.enabled !== false) {
        for (const workflow of this.registry.workflows.values()) {
          if (!workflow.handler || !workflow.enabled) continue
          const feature = workflow.owner!.registration.id
          if (workflow.concurrency !== undefined && !featureSlots.has(feature))
            featureSlots.set(feature, Semaphore.makeUnsafe(workflow.concurrency))
          const featureLimit = featureSlots.get(feature)
          const engine = await infrastructure.runPromise(WorkflowEngine.WorkflowEngine)
          await infrastructure.runPromise(
            Scope.provide(
              engine.register(workflow.definition, (payload, id) =>
                this.execute(workflow, payload.input, id).pipe(
                  workflowSlots.withPermits(1),
                  (effect) => (featureLimit ? featureLimit.withPermits(1)(effect) : effect)
                )
              ),
              infrastructure.scope
            )
          )
        }
      }
      if (this.options.execution?.activities?.enabled !== false) {
        const slots = new Map(
          [...this.registry.queues].map(([name, queue]) => [
            name,
            Semaphore.makeUnsafe(queue.concurrency)
          ])
        )
        for (const activity of this.registry.activities.values()) {
          if (!activity.enabled) continue
          infrastructure.runFork(
            activityWorker(
              this.queue(activity),
              activity,
              this.journal,
              slots.get(activity.options.queue)!,
              this.options,
              this.registry.queues.get(activity.options.queue)!.concurrency
            )
          )
        }
      }
      this.ready = true
      const self = this
      infrastructure.runFork(
        Effect.gen(function* () {
          while (true) {
            yield* self.dispatch().pipe(
              Effect.catchCause((cause) => {
                if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
                return Effect.sync(() => {
                  self.lastDispatchError = Cause.pretty(cause)
                  self.logger.error(self.lastDispatchError)
                })
              })
            )
            yield* Effect.sleep(milliseconds(self.options.pollInterval ?? '100ms'))
          }
        })
      )
    } catch (error) {
      await infrastructure.dispose()
      this.infrastructure = undefined
      this.journal = undefined
      throw error
    }
  }

  async stop(): Promise<void> {
    if (!this.stopPromise) {
      this.stopping = true
      this.ready = false
      this.stopPromise = this.infrastructure?.dispose() ?? Promise.resolve()
    }
    await this.stopPromise
  }

  onModuleDestroy(): Promise<void> {
    return this.stop()
  }
  onApplicationShutdown(): Promise<void> {
    return this.stop()
  }

  health() {
    return { ready: this.ready && !this.stopping, lastDispatchError: this.lastDispatchError }
  }

  private store(): Journal {
    if (!this.journal || this.stopping)
      throw new WorkflowError(
        'RUNTIME_NOT_READY',
        'Initialize the Nest application before using workflow clients'
      )
    return this.journal
  }

  async run<A, E>(effect: Effect.Effect<A, E, Services>): Promise<A> {
    if (!this.infrastructure || this.stopping)
      throw new WorkflowError('RUNTIME_NOT_READY', 'The workflow runtime is not running')
    const exit = await this.infrastructure.runPromiseExit(effect)
    if (Exit.isFailure(exit)) {
      const failure = toFailure(Cause.squash(exit.cause))
      throw new WorkflowError(failure.code, failure.message)
    }
    return exit.value
  }

  async start<W extends WorkflowClass>(workflow: W, input: WorkflowInput<W>, keyOverride?: string) {
    if (!this.ready)
      throw new WorkflowError(
        'RUNTIME_NOT_READY',
        'Initialize the Nest application before starting workflows'
      )
    const entry = this.registry.contract(workflow)
    const encoded = encode(input)
    const parsed = await validate(
      entry.options.input,
      decode(encoded),
      `${entry.options.name} input`
    )
    const key = keyOverride ?? entry.options.idempotencyKey?.(parsed) ?? randomUUID()
    identifier(key, 'Idempotency key')
    const executionId = await this.run(entry.definition.executionId({ key, input: encoded }))
    const accepted = await this.run(
      this.store().accept(executionId, entry.options.name, entry.options.version, key, encoded)
    )
    return { executionId: accepted.row.execution_id, created: accepted.created }
  }

  async row(workflow: WorkflowClass, id: string): Promise<RunRow> {
    const entry = this.registry.contract(workflow)
    return this.run(this.store().get(id, entry.options.name))
  }

  resultVersion(workflow: WorkflowClass): number {
    return this.registry.contract(workflow).options.version
  }

  async describe(workflow: WorkflowClass, id: string): Promise<ExecutionSnapshot> {
    let row = await this.row(workflow, id)
    if (!terminal(row) && row.dispatched) {
      await this.run(this.reconcile(row))
      row = await this.row(workflow, id)
    }
    const snapshot: ExecutionSnapshot = {
      executionId: row.execution_id,
      workflow: row.workflow_name,
      version: row.version,
      status: terminal(row)
        ? row.state
        : row.control === 'pause'
          ? 'paused'
          : row.control === 'cancel'
            ? 'cancelling'
            : row.state,
      createdAt: new Date(row.created_at).toISOString(),
      updatedAt: new Date(row.updated_at).toISOString()
    }
    const withWait =
      row.wait_type && row.wait_step
        ? { ...snapshot, waitingOn: { type: row.wait_type, stepId: row.wait_step } }
        : snapshot
    return row.failure_json ? { ...withWait, failure: decode<Failure>(row.failure_json) } : withWait
  }

  async history(workflow: WorkflowClass, id: string, after?: number, limit?: number) {
    await this.row(workflow, id)
    return this.run(this.store().history(id, after, limit))
  }

  async control(
    workflow: WorkflowClass,
    id: string,
    action: 'run' | 'pause' | 'cancel',
    reason: string
  ): Promise<void> {
    await this.describe(workflow, id)
    if (reason.length > 4096)
      throw new WorkflowError('INVALID_REASON', 'Control reasons are limited to 4096 characters')
    await this.run(this.store().control(id, action, reason))
  }

  async signal<I, O>(
    workflow: WorkflowClass,
    id: string,
    signal: SignalDefinition<I, O>,
    input: I,
    key: string
  ) {
    identifier(key, 'Signal idempotency key')
    const run = await this.row(workflow, id)
    const contract = this.registry.workflow(run.workflow_name, run.version)
    const allowed = contract.options.signals?.find((candidate) => candidate.name === signal.name)
    if (!allowed)
      throw new WorkflowError(
        'UNKNOWN_SIGNAL',
        `${signal.name} is not declared by ${run.workflow_name}@${run.version}`
      )
    const payload = await validate(allowed.schema, input, `${signal.name} signal`)
    return this.run(this.store().signal(id, signal.name, key, encode(payload)))
  }

  private queue(activity: ActivityContract): EngineQueue {
    const { name, version, queue } = activity.options
    if (!this.registry.queues.get(queue))
      throw new WorkflowError('UNKNOWN_QUEUE', `Configure queue ${queue} used by ${name}`)
    if (this.registry.queues.get(queue)?.perKeyConcurrency !== undefined && !activity.options.key)
      throw new WorkflowError(
        'ACTIVITY_KEY_REQUIRED',
        `${name} must declare key for queue ${queue}`
      )
    const key = JSON.stringify([name, version, queue])
    let definition = this.queues.get(key)
    if (!definition) {
      definition = activityQueue(this.options.namespace, queue, name, version)
      this.queues.set(key, definition)
    }
    return definition
  }

  private gate(executionId: string) {
    const self = this
    return Effect.gen(function* () {
      const run = yield* durable(self.store().get(executionId))
      const instance = yield* WorkflowEngine.WorkflowInstance
      if (run.control === 'cancel' || terminal(run)) return yield* Effect.interrupt
      if (run.control === 'pause') return yield* Workflow.suspend(instance)
    })
  }

  private execute(workflow: RegisteredWorkflow, payload: string, executionId: string) {
    const self = this
    return Effect.gen(function* () {
      yield* self.gate(executionId)
      yield* durable(self.store().running(executionId))
      const input = yield* promised(() =>
        validate(workflow.options.input, decode(payload), `${workflow.options.name} input`)
      )
      const interpreter = new WorkflowInterpreter(
        self.store(),
        self.registry,
        workflow,
        executionId,
        () => self.gate(executionId),
        (activity) => self.queue(activity)
      )
      const value = yield* interpreter.run((ctx) => workflow.handler!(input, ctx))
      return yield* promised(async () =>
        encode(await validate(workflow.options.output, value, `${workflow.options.name} output`))
      )
    })
  }

  private reconcile(row: RunRow) {
    const self = this
    return Effect.gen(function* () {
      const definition = workflowDefinition(self.options.namespace, row.workflow_name, row.version)
      const polled = yield* definition.poll(row.execution_id)
      if (Option.isNone(polled) || polled.value._tag !== 'Complete') return
      const exit = polled.value.exit
      if (Exit.isSuccess(exit)) yield* self.store().complete(row.execution_id, exit.value, null)
      else
        yield* self
          .store()
          .complete(
            row.execution_id,
            null,
            toFailure(Cause.squash(exit.cause)),
            row.control === 'cancel'
          )
    })
  }

  adminBackend(): AdminBackend {
    return {
      migrationStatus: () => this.run(migrationStatus(this.store().sql)),
      validateMigrations: () => this.run(validateMigrations(this.store().sql)),
      migrate: () =>
        Promise.reject(
          new WorkflowError(
            'MIGRATION_RUNTIME_ACTIVE',
            'Use createWorkflowsAdmin before starting application workers'
          )
        ),
      previewRetention: (options) => this.run(new SqlAdministration(this.store()).preview(options)),
      pruneRetention: (plan, confirm) =>
        this.run(new SqlAdministration(this.store()).prune(plan, confirm)),
      setQueueLimits: async (queue, options) => {
        await this.run(new SqlAdministration(this.store()).setQueueLimits(queue, options))
      }
    }
  }

  async flush(): Promise<void> {
    await this.run(this.dispatch())
  }

  async testingSnapshot(): Promise<{ revision: string; nextDeadline: number | null }> {
    const store = this.store()
    const snapshot = await this.run(
      Effect.gen(function* () {
        const rows = yield* store.sql<{
          execution_id: string
          event_sequence: number
          state: string
          dispatched: number
          applied_revision: number
        }>`SELECT execution_id, event_sequence, state, dispatched, applied_revision FROM better_workflows_runs WHERE namespace = ${store.namespace} ORDER BY execution_id`
        const [timer] = yield* store.sql<{
          deadline: number | null
        }>`SELECT MIN(deadline) AS deadline FROM (
        SELECT t.deadline FROM better_workflows_timers t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.delivered=0 AND r.control<>'cancel' AND r.state NOT IN ('completed','failed','cancelled')
        UNION ALL SELECT t.deadline FROM better_workflows_retries t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.delivered=0 AND r.control<>'cancel' AND r.state NOT IN ('completed','failed','cancelled')
        UNION ALL SELECT t.deadline FROM better_workflows_waits t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.state='pending' AND r.control<>'cancel' AND r.state NOT IN ('completed','failed','cancelled')
      ) deadlines`
        return { revision: JSON.stringify(rows), nextDeadline: timer?.deadline ?? null }
      })
    )
    if (this.lastDispatchError)
      throw new WorkflowError('TEST_DISPATCH_ERROR', this.lastDispatchError)
    return snapshot
  }

  private dispatch() {
    const self = this
    return Effect.gen(function* () {
      const journal = self.store()
      const advanced = new AdvancedJournal(journal)
      yield* advanced.closeChildren()
      for (const timer of yield* advanced.dueTimers()) {
        const run = yield* journal.get(timer.execution_id)
        const definition = workflowDefinition(
          self.options.namespace,
          run.workflow_name,
          run.version
        )
        const deferred = timerDeferred(timer.step_id)
        yield* DurableDeferred.done(deferred, {
          token: DurableDeferred.tokenFromExecutionId(deferred, {
            workflow: definition,
            executionId: timer.execution_id
          }),
          exit: Exit.void
        })
        yield* advanced.timerDelivered(timer)
      }
      for (const child of yield* advanced.readyChildren()) {
        const parent = yield* journal.get(child.parent_id)
        const run = yield* journal.get(child.child_id)
        const definition = workflowDefinition(
          self.options.namespace,
          parent.workflow_name,
          parent.version
        )
        const deferred = childDeferred(child.step_id)
        const error =
          run.state === 'cancelled'
            ? {
                code: 'CHILD_WORKFLOW_CANCELLED',
                message: `Child ${child.child_id} was cancelled`,
                retryable: false
              }
            : run.failure_json
              ? decode<Failure>(run.failure_json)
              : null
        yield* DurableDeferred.done(deferred, {
          token: DurableDeferred.tokenFromExecutionId(deferred, {
            workflow: definition,
            executionId: child.parent_id
          }),
          exit: error ? Exit.fail(error) : Exit.succeed(run.result_json!)
        })
        yield* advanced.childDelivered(child)
      }
      for (const row of yield* journal.pendingDispatch()) {
        const definition = workflowDefinition(
          self.options.namespace,
          row.workflow_name,
          row.version
        )
        if (row.control === 'cancel') {
          if (row.dispatched) yield* definition.interrupt(row.execution_id)
          yield* journal.complete(
            row.execution_id,
            null,
            {
              code: 'WORKFLOW_CANCELLED',
              message: 'Workflow cancellation was requested',
              retryable: false
            },
            true
          )
          continue
        }
        if (!row.dispatched)
          yield* definition.execute(
            { key: row.dedupe_key, input: row.input_json },
            { discard: true }
          )
        else if (row.control_revision > row.applied_revision)
          yield* definition.resume(row.execution_id)
        yield* journal.dispatched(row)
      }
      for (const retry of yield* journal.pendingRetries()) {
        const run = yield* journal.get(retry.execution_id)
        const definition = workflowDefinition(
          self.options.namespace,
          run.workflow_name,
          run.version
        )
        const deferred = retryDeferred(retry.step_id, retry.attempt)
        yield* DurableDeferred.done(deferred, {
          token: DurableDeferred.tokenFromExecutionId(deferred, {
            workflow: definition,
            executionId: retry.execution_id
          }),
          exit: Exit.void
        })
        yield* journal.retryDelivered(retry)
      }
      const waits = yield* journal.pendingWaits(self.waitCursor)
      for (const candidate of waits) {
        const wait = yield* journal.resolveWait(candidate)
        if (!wait || wait.state === 'pending') continue
        const run = yield* journal.get(wait.execution_id)
        const definition = workflowDefinition(
          self.options.namespace,
          run.workflow_name,
          run.version
        )
        const deferred = signalDeferred(wait.step_id)
        yield* DurableDeferred.done(deferred, {
          token: DurableDeferred.tokenFromExecutionId(deferred, {
            workflow: definition,
            executionId: wait.execution_id
          }),
          exit:
            wait.state === 'success'
              ? Exit.succeed(wait.result_json!)
              : Exit.fail({
                  code: 'SIGNAL_TIMEOUT',
                  message: `Signal ${wait.signal_name} did not arrive before its deadline`,
                  retryable: false
                })
        })
        yield* journal.delivered(wait)
      }
      self.waitCursor = waits.length === 100 ? waits.at(-1)!.execution_id : ''
      const active = yield* journal.activeAfter(self.dispatchCursor)
      for (const row of active) yield* self.reconcile(row)
      self.dispatchCursor = active.length === 100 ? active.at(-1)!.execution_id : ''
      self.lastDispatchError = undefined
    }).pipe(this.dispatchLock.withPermits(1))
  }
}
