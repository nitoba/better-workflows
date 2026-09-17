import { randomUUID } from 'node:crypto'
import { Inject, Injectable, Optional } from '@nestjs/common'
import type { OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown } from '@nestjs/common'
import { DiscoveryService } from '@nestjs/core'
import { Cause, Effect, Exit, Option, Queue, Scope, Semaphore } from 'effect'
import * as PgClient from '@effect/sql-pg/PgClient'
import { SqlClient } from 'effect/unstable/sql'
import * as Reactivity from 'effect/unstable/reactivity/Reactivity'
import { DurableDeferred, Workflow, WorkflowEngine } from 'effect/unstable/workflow'
import { PersistedQueue } from 'effect/unstable/persistence'
import { WorkflowError, toFailure } from '../errors'
import type { Failure } from '../errors'
import type {
  ExecutionSnapshot,
  SignalDefinition,
  WorkflowContractClass,
  WorkflowInput,
  WorkflowsLiveness,
  WorkflowsReadiness,
  WorkflowsOptions
} from '../types'
import { Registry } from './registry'
import type { RegisteredWorkflow } from './registry'
import { Journal } from './journal'
import type { RunRow } from './journal'
import { decode, encode, identifier, milliseconds, validate } from './values'
import { makeInfrastructure, validateOptions } from './infrastructure'
import type { Infrastructure } from './infrastructure'
import { durable, promised } from './effects'
import { retryDeferred, signalDeferred, workflowDefinition } from './wire'
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
import { ActivityTransport } from './activity-transport'
import {
  logAnnotations,
  TelemetryAttributeKey,
  TelemetryLogComponent,
  TelemetryService,
  TelemetrySpanName
} from './telemetry'
import type { TelemetryApi, TelemetryAttributes } from './telemetry'
import {
  executionNotificationChannel,
  executionNotificationPayload,
  executionNotifierKey,
  ExecutionNotifier
} from './notifier'

export const WORKFLOWS_OPTIONS = Symbol.for('better-workflows/options')
type Services =
  | WorkflowEngine.WorkflowEngine
  | PersistedQueue.PersistedQueueFactory
  | SqlClient.SqlClient
  | PgClient.PgClient
  | TelemetryService
const terminal = (row: RunRow) =>
  ['continued', 'completed', 'failed', 'cancelled'].includes(row.state)
const safetySweepInterval = 60_000

interface DispatcherDetails {
  staleAfterMs: number
  lastSuccessfulAt?: string
  lastFailureAt?: string
}

@Injectable()
export class WorkflowsRuntime
  implements OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown
{
  readonly registry: Registry
  private infrastructure: Infrastructure | undefined
  private journal: Journal | undefined
  private activityTransport: ActivityTransport | undefined
  private notifier: ExecutionNotifier | undefined
  private telemetry: TelemetryApi | undefined
  private ready = false
  private stopping = false
  private stopPromise?: Promise<void>
  private safetySweepCursor = ''
  private safetySweepAt = Date.now() + safetySweepInterval
  private readonly dispatchLock = Semaphore.makeUnsafe(1)
  private lastDispatchError: string | undefined
  private dispatcherFailed = false
  private dispatcherStartedAt: number | undefined
  private lastSuccessfulDispatchAt: number | undefined
  private lastDispatchFailureAt: number | undefined
  private dispatcherRunning = false
  private readonly activityWorkerRunning: boolean[] = []
  private configuredWorkflowCount = 0
  private workflowRuntimeRegistered = false
  private configuredActivityWorkerCount = 0
  private notifierConnected = false

  constructor(
    @Inject(WORKFLOWS_OPTIONS) readonly options: WorkflowsOptions,
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Optional() @Inject(WORKFLOWS_TEST_CLOCK) private readonly clock?: BusinessClock
  ) {
    validateOptions(options)
    this.registry = new Registry(options)
  }

  registerContract(workflow: WorkflowContractClass): void {
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
    const infrastructure = await makeInfrastructure(this.options)
    this.infrastructure = infrastructure
    try {
      const sql = await infrastructure.runPromise(SqlClient.SqlClient)
      const telemetry = await infrastructure.runPromise(TelemetryService)
      this.telemetry = telemetry
      const notifierKey = executionNotifierKey(this.options.storage, this.options.namespace)
      this.journal = new Journal(sql, this.options.namespace, this.clock, notifierKey, telemetry)
      const journal = this.journal
      this.notifier = new ExecutionNotifier(
        notifierKey,
        (executionId) => this.run(journal.revision(executionId)),
        undefined,
        telemetry
      )
      this.notifierConnected = this.options.storage.driver !== 'postgres'
      if (this.options.storage.driver === 'postgres') {
        const pg = await infrastructure.runPromise(PgClient.PgClient)
        this.notifierConnected = false
        this.startPostgresNotifier(pg.config, this.notifier)
      }
      this.activityTransport = new ActivityTransport(this.journal)
      for (const [name, options] of this.registry.queues.entries())
        await this.run(new Permits(this.journal).register(name, options))
      const workflowSlots = Semaphore.makeUnsafe(
        this.options.execution?.workflows?.concurrency ?? 20
      )
      const featureSlots = new Map<symbol, Semaphore.Semaphore>()
      this.configuredWorkflowCount = [...this.registry.workflows.values()].filter(
        (workflow) => workflow.invoke && workflow.enabled
      ).length
      this.workflowRuntimeRegistered = false
      if (this.options.execution?.workflows?.enabled !== false) {
        for (const workflow of this.registry.workflows.values()) {
          if (!workflow.invoke || !workflow.enabled) continue
          const feature = workflow.owner!.registration.id
          if (workflow.concurrency !== undefined && !featureSlots.has(feature))
            featureSlots.set(feature, Semaphore.makeUnsafe(workflow.concurrency))
          const featureLimit = featureSlots.get(feature)
          const engine = await infrastructure.runPromise(WorkflowEngine.WorkflowEngine)
          await infrastructure.runPromise(
            Scope.provide(
              engine.register(workflow.definition, (payload, id) =>
                this.executeAndEnqueue(workflow, payload.input, id).pipe(
                  workflowSlots.withPermits(1),
                  (effect) => (featureLimit ? featureLimit.withPermits(1)(effect) : effect)
                )
              ),
              infrastructure.scope
            )
          )
          this.workflowRuntimeRegistered = true
        }
      }
      this.configuredActivityWorkerCount = 0
      this.activityWorkerRunning.length = 0
      if (this.options.execution?.activities?.enabled !== false) {
        const slots = new Map(
          [...this.registry.queues].map(([name, queue]) => [
            name,
            Semaphore.makeUnsafe(queue.concurrency)
          ])
        )
        for (const [name, queue] of this.registry.queues) {
          const activities = [...this.registry.activities.values()].filter(
            (activity) => activity.enabled && activity.options.queue === name
          )
          if (activities.length === 0) continue
          this.configuredActivityWorkerCount += 1
          const workerIndex = this.activityWorkerRunning.push(true) - 1
          const workerFiber = infrastructure.runFork(
            activityWorker(
              name,
              activities,
              this.journal,
              slots.get(name)!,
              this.options,
              queue.concurrency,
              this.activityTransport!
            )
          )
          workerFiber.addObserver(() => {
            this.activityWorkerRunning[workerIndex] = false
          })
        }
      }
      this.ready = true
      this.dispatcherStartedAt = Date.now()
      const self = this
      this.dispatcherRunning = true
      const dispatcherFiber = infrastructure.runFork(
        Effect.gen(function* () {
          yield* Effect.annotateLogs(
            Effect.logInfo('Runtime started'),
            logAnnotations(TelemetryLogComponent.runtime, {
              [TelemetryAttributeKey.namespace]: self.options.namespace
            })
          )
          while (true) {
            yield* self.dispatch().pipe(
              Effect.catchCause((cause) => {
                if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
                return Effect.sync(() => {
                  self.lastDispatchError = Cause.pretty(cause)
                  self.lastDispatchFailureAt = Date.now()
                }).pipe(
                  Effect.andThen(
                    Effect.annotateLogs(
                      Effect.logError('Dispatcher iteration failed'),
                      logAnnotations(TelemetryLogComponent.dispatcher, {
                        [TelemetryAttributeKey.namespace]: self.options.namespace,
                        [TelemetryAttributeKey.failureCode]: toFailure(Cause.squash(cause)).code
                      })
                    )
                  )
                )
              })
            )
            yield* Effect.sleep(milliseconds(self.options.pollInterval ?? '100ms'))
          }
        })
      )
      dispatcherFiber.addObserver(() => {
        self.dispatcherRunning = false
      })
    } catch (error) {
      await infrastructure
        .runPromise(
          Effect.annotateLogs(
            Effect.logError('Runtime bootstrap failed'),
            logAnnotations(TelemetryLogComponent.runtime, {
              [TelemetryAttributeKey.namespace]: this.options.namespace,
              [TelemetryAttributeKey.failureCode]: toFailure(error).code
            })
          )
        )
        .catch(() => undefined)
      await infrastructure.dispose()
      this.infrastructure = undefined
      this.journal = undefined
      this.activityTransport = undefined
      this.telemetry = undefined
      this.dispatcherRunning = false
      this.dispatcherStartedAt = undefined
      this.lastSuccessfulDispatchAt = undefined
      this.activityWorkerRunning.length = 0
      this.notifier?.shutdown()
      this.notifier = undefined
      throw error
    }
  }

  async stop(): Promise<void> {
    if (!this.stopPromise) {
      this.stopping = true
      this.ready = false
      this.notifier?.shutdown()
      const infrastructure = this.infrastructure
      this.stopPromise = infrastructure
        ? infrastructure
            .runPromise(
              Effect.annotateLogs(
                Effect.logInfo('Runtime stopped'),
                logAnnotations(TelemetryLogComponent.runtime, {
                  [TelemetryAttributeKey.namespace]: this.options.namespace
                })
              )
            )
            .catch(() => undefined)
            .then(() => infrastructure.dispose())
        : Promise.resolve()
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

  liveness(): WorkflowsLiveness {
    const running = this.ready && !this.stopping
    return {
      status: running ? 'up' : 'down',
      runtime: { running, stopping: this.stopping }
    }
  }

  async readiness(): Promise<WorkflowsReadiness> {
    const checkedAt = new Date().toISOString()
    const runtimeUp = this.ready && !this.stopping && this.infrastructure !== undefined
    if (!runtimeUp)
      return {
        status: 'down',
        ready: false,
        checkedAt,
        checks: {
          runtime: 'down',
          storage: 'down',
          schema: 'down',
          dispatcher: 'down',
          notifier: 'down',
          workflows: 'down',
          workers: 'down'
        },
        dispatcher: { staleAfterMs: this.dispatcherStalenessThreshold() }
      }

    const infrastructure = this.infrastructure!
    const storageExit = await infrastructure
      .runPromiseExit(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          yield* sql`SELECT 1`
        })
      )
      .catch(() => undefined)
    const storage = storageExit && Exit.isSuccess(storageExit) ? 'up' : 'down'
    let schema: 'up' | 'down' = 'down'
    if (storage === 'up') {
      const schemaExit = await infrastructure
        .runPromiseExit(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient
            return yield* migrationStatus(sql)
          })
        )
        .catch(() => undefined)
      if (schemaExit && Exit.isSuccess(schemaExit) && schemaExit.value.valid) schema = 'up'
    }

    const staleAfterMs = this.dispatcherStalenessThreshold()
    const lastSuccessfulAt = this.lastSuccessfulDispatchAt
    const freshnessAt = lastSuccessfulAt ?? this.dispatcherStartedAt
    const stale = freshnessAt === undefined || Date.now() - freshnessAt > staleAfterMs
    const dispatcher =
      !this.dispatcherRunning || stale ? 'down' : this.dispatcherFailed ? 'degraded' : 'up'
    const notifier =
      this.options.storage.driver === 'postgres'
        ? this.notifierConnected
          ? 'up'
          : 'degraded'
        : 'up'
    const workflows =
      this.options.execution?.workflows?.enabled === false || this.configuredWorkflowCount === 0
        ? 'disabled'
        : this.workflowRuntimeRegistered
          ? 'up'
          : 'down'
    const workers =
      this.options.execution?.activities?.enabled === false ||
      this.configuredActivityWorkerCount === 0
        ? 'disabled'
        : this.activityWorkerRunning.length === this.configuredActivityWorkerCount &&
            this.activityWorkerRunning.every((running) => running)
          ? 'up'
          : 'down'
    const checks = {
      runtime: 'up' as const,
      storage,
      schema,
      dispatcher,
      notifier,
      workflows,
      workers
    } satisfies WorkflowsReadiness['checks']
    const hardFailure = Object.values(checks).some((check) => check === 'down')
    const degraded = Object.values(checks).some((check) => check === 'degraded')
    const dispatcherDetails: DispatcherDetails = { staleAfterMs }
    if (lastSuccessfulAt !== undefined)
      dispatcherDetails.lastSuccessfulAt = new Date(lastSuccessfulAt).toISOString()
    if (this.lastDispatchFailureAt !== undefined)
      dispatcherDetails.lastFailureAt = new Date(this.lastDispatchFailureAt).toISOString()
    return {
      status: hardFailure ? 'down' : degraded ? 'degraded' : 'up',
      ready: !hardFailure,
      checkedAt,
      checks,
      dispatcher: dispatcherDetails
    }
  }

  private dispatcherStalenessThreshold(): number {
    return Math.max(milliseconds(this.options.pollInterval ?? '100ms') * 3, 1_000)
  }

  private store(): Journal {
    if (!this.journal || this.stopping)
      throw new WorkflowError(
        'RUNTIME_NOT_READY',
        'Initialize the Nest application before using workflow clients'
      )
    return this.journal
  }

  private startPostgresNotifier(
    config: PgClient.PgClientConfig,
    notifier: ExecutionNotifier
  ): void {
    const self = this
    const channel = executionNotificationChannel(this.options.namespace)
    const telemetry = this.journal?.telemetry
    this.infrastructure!.runFork(
      Effect.gen(function* () {
        let everConnected = false
        while (true) {
          const listening = yield* Effect.exit(
            Effect.provide(
              Effect.scoped(
                Effect.gen(function* () {
                  const client = yield* PgClient.makeClient(config)
                  const queue = yield* client.listen(channel)
                  if (everConnected) {
                    telemetry?.count('notifierReconnect')
                    yield* Effect.annotateLogs(
                      Effect.logWarning('Notifier reconnected'),
                      logAnnotations(TelemetryLogComponent.notifier, {
                        [TelemetryAttributeKey.namespace]: self.options.namespace
                      })
                    )
                  } else
                    yield* Effect.annotateLogs(
                      Effect.logInfo('Notifier connected'),
                      logAnnotations(TelemetryLogComponent.notifier, {
                        [TelemetryAttributeKey.namespace]: self.options.namespace
                      })
                    )
                  everConnected = true
                  self.notifierConnected = true
                  notifier.reconnected()
                  while (true) {
                    const notification = yield* Queue.take(queue)
                    const payload = executionNotificationPayload(notification.payload)
                    if (payload) {
                      yield* Effect.annotateLogs(
                        Effect.logDebug('Notification received'),
                        logAnnotations(TelemetryLogComponent.notifier, {
                          [TelemetryAttributeKey.namespace]: self.options.namespace,
                          [TelemetryAttributeKey.executionId]: payload.executionId
                        })
                      )
                      notifier.publish(payload.executionId, payload.revision)
                    }
                  }
                })
              ),
              Reactivity.layer
            )
          )
          if (Exit.isFailure(listening) && Cause.hasInterruptsOnly(listening.cause))
            return yield* Effect.failCause(listening.cause)
          self.notifierConnected = false
          if (Exit.isFailure(listening))
            yield* Effect.annotateLogs(
              Effect.logWarning('Notifier connection lost'),
              logAnnotations(TelemetryLogComponent.notifier, {
                [TelemetryAttributeKey.namespace]: self.options.namespace,
                [TelemetryAttributeKey.failureCode]: toFailure(Cause.squash(listening.cause)).code
              })
            )
          yield* Effect.sleep(1_000)
        }
      })
    )
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

  async start<W extends WorkflowContractClass>(
    workflow: W,
    input: WorkflowInput<W>,
    keyOverride?: string
  ) {
    if (!this.ready)
      throw new WorkflowError(
        'RUNTIME_NOT_READY',
        'Initialize the Nest application before starting workflows'
      )
    const entry = this.registry.contract(workflow)
    const encoded = encode(input)
    const self = this
    const accepted = await this.run(
      Effect.useSpan(
        TelemetrySpanName.workflowStart,
        {
          attributes: {
            [TelemetryAttributeKey.workflowName]: entry.options.name,
            [TelemetryAttributeKey.workflowVersion]: entry.options.version
          },
          kind: 'producer'
        },
        (span) =>
          Effect.gen(function* () {
            const parsed = yield* promised(() =>
              validate(entry.options.input, decode(encoded), `${entry.options.name} input`)
            )
            const key = yield* Effect.try({
              try: () => {
                const value = keyOverride ?? entry.options.idempotencyKey?.(parsed) ?? randomUUID()
                identifier(value, 'Idempotency key')
                return value
              },
              catch: toFailure
            })
            const executionId = yield* entry.definition.executionId({ key, input: encoded })
            const accepted = yield* self
              .store()
              .accept(executionId, entry.options.name, entry.options.version, key, encoded)
            span.attribute(TelemetryAttributeKey.executionId, executionId)
            span.attribute(TelemetryAttributeKey.workflowCreated, accepted.created)
            yield* Effect.annotateLogs(
              Effect.logDebug('Workflow accepted'),
              logAnnotations(TelemetryLogComponent.workflow, {
                [TelemetryAttributeKey.namespace]: self.options.namespace,
                [TelemetryAttributeKey.workflowName]: entry.options.name,
                [TelemetryAttributeKey.workflowVersion]: entry.options.version,
                [TelemetryAttributeKey.executionId]: executionId,
                [TelemetryAttributeKey.workflowCreated]: accepted.created
              })
            )
            return accepted
          })
      )
    )
    if (accepted.created)
      this.telemetry?.count('workflowStarted', {
        [TelemetryAttributeKey.workflowName]: accepted.row.workflow_name,
        [TelemetryAttributeKey.workflowVersion]: accepted.row.version
      })
    return { executionId: accepted.row.execution_id, created: accepted.created }
  }

  async row(workflow: WorkflowContractClass, id: string): Promise<RunRow> {
    const entry = this.registry.contract(workflow)
    return this.run(this.store().get(id, entry.options.name))
  }

  async wait(
    _workflow: WorkflowContractClass,
    executionId: string,
    afterRevision: number,
    options: { readonly signal?: AbortSignal | undefined; readonly timeout?: number | undefined }
  ): Promise<void> {
    if (!this.notifier)
      throw new WorkflowError('RUNTIME_NOT_READY', 'The workflow runtime is not running')
    await this.notifier.wait(executionId, afterRevision, options)
  }

  resultVersion(workflow: WorkflowContractClass): number {
    return this.registry.contract(workflow).options.version
  }

  async describe(workflow: WorkflowContractClass, id: string): Promise<ExecutionSnapshot> {
    const row = await this.row(workflow, id)
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
    let withContinuation = snapshot
    if (row.state === 'continued') {
      if (!row.continued_to)
        throw new WorkflowError(
          'STORAGE_INTEGRITY',
          `Execution ${row.execution_id} is continued without a next generation`
        )
      const next = await this.run(this.store().get(row.continued_to))
      if (
        next.workflow_name !== row.workflow_name ||
        next.version !== row.version ||
        next.chain_id !== row.chain_id ||
        next.generation !== row.generation + 1 ||
        next.continued_from !== row.execution_id
      )
        throw new WorkflowError(
          'STORAGE_INTEGRITY',
          `Continuation ${row.continued_to} does not match execution ${row.execution_id}`
        )
      withContinuation = {
        ...snapshot,
        continuation: {
          executionId: next.execution_id,
          generation: next.generation
        }
      }
    }
    const withWait =
      row.wait_type && row.wait_step
        ? { ...withContinuation, waitingOn: { type: row.wait_type, stepId: row.wait_step } }
        : withContinuation
    const blocked =
      row.state === 'blocked'
        ? await this.run(
            new ActivityTransport(this.store()).deadLetters.blockedOn(row.execution_id)
          )
        : null
    const withBlocked = blocked ? { ...withWait, blockedOn: blocked } : withWait
    return row.failure_json
      ? { ...withBlocked, failure: decode<Failure>(row.failure_json) }
      : withBlocked
  }

  async history(workflow: WorkflowContractClass, id: string, after?: number, limit?: number) {
    await this.row(workflow, id)
    return this.run(this.store().history(id, after, limit))
  }

  async control(
    workflow: WorkflowContractClass,
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
    workflow: WorkflowContractClass,
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
    return this.run(
      Effect.useSpan(
        TelemetrySpanName.signalAccept,
        {
          attributes: {
            [TelemetryAttributeKey.executionId]: id,
            [TelemetryAttributeKey.workflowName]: contract.options.name,
            [TelemetryAttributeKey.workflowVersion]: contract.options.version,
            [TelemetryAttributeKey.signalName]: signal.name
          },
          kind: 'producer'
        },
        () => this.store().signal(id, signal.name, key, encode(payload))
      )
    )
  }

  private queue(queue: string): string {
    if (!this.registry.queues.get(queue))
      throw new WorkflowError('UNKNOWN_QUEUE', `Configure queue ${queue}`)
    return queue
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
        self.activityTransport!
      )
      const value = yield* interpreter.run((ctx) => workflow.invoke!(input, ctx))
      return yield* promised(async () =>
        encode(await validate(workflow.options.output, value, `${workflow.options.name} output`))
      )
    })
  }

  private executeAndEnqueue(workflow: RegisteredWorkflow, payload: string, executionId: string) {
    const self = this
    return self.execute(workflow, payload, executionId).pipe(
      Effect.matchCauseEffect({
        onSuccess: (result) =>
          durable(self.store().enqueueReconciliation(executionId, result, null)).pipe(
            Effect.as(result)
          ),
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
          return durable(
            self.store().enqueueReconciliation(executionId, null, toFailure(Cause.squash(cause)))
          ).pipe(Effect.andThen(Effect.failCause(cause)))
        }
      })
    )
  }

  private reconcileSweep(row: RunRow) {
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
      stats: () => this.run(new SqlAdministration(this.store()).stats()),
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
      },
      listDeadLetters: (options) =>
        this.run(new ActivityTransport(this.store()).deadLetters.list(options)),
      getDeadLetter: (id, options) =>
        this.run(
          new ActivityTransport(this.store()).deadLetters.get(id, options?.includePayload === true)
        ),
      requeueDeadLetter: (id) => {
        const transport = new ActivityTransport(this.store())
        return this.run(
          transport.deadLetters.requeue(
            id,
            (queue, payload, deliveryId, initialAttempt, deadLetterId) =>
              transport.offerInTransaction(queue, payload, deliveryId, initialAttempt, deadLetterId)
          )
        )
      },
      discardDeadLetter: (id, options) =>
        this.run(new ActivityTransport(this.store()).deadLetters.discard(id, options))
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
        SELECT t.deadline FROM better_workflows_timers t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.delivered=0 AND r.control<>'cancel' AND r.state NOT IN ('continued','completed','failed','cancelled')
        UNION ALL SELECT t.deadline FROM better_workflows_retries t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.delivered=0 AND r.control<>'cancel' AND r.state NOT IN ('continued','completed','failed','cancelled')
        UNION ALL SELECT t.deadline FROM better_workflows_waits t JOIN better_workflows_runs r ON r.execution_id=t.execution_id WHERE r.namespace=${store.namespace} AND t.state='pending' AND r.control<>'cancel' AND r.state NOT IN ('continued','completed','failed','cancelled')
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
    const startedAt = Date.now()
    const operation = Effect.gen(function* () {
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
        yield* Effect.useSpan(
          TelemetrySpanName.timerDeliver,
          {
            attributes: {
              [TelemetryAttributeKey.workflowName]: run.workflow_name,
              [TelemetryAttributeKey.workflowVersion]: run.version,
              [TelemetryAttributeKey.executionId]: timer.execution_id,
              [TelemetryAttributeKey.stepId]: timer.step_id
            },
            kind: 'consumer'
          },
          () =>
            Effect.gen(function* () {
              yield* DurableDeferred.done(deferred, {
                token: DurableDeferred.tokenFromExecutionId(deferred, {
                  workflow: definition,
                  executionId: timer.execution_id
                }),
                exit: Exit.void
              })
              const deliveredAt = yield* journal.now()
              yield* Effect.annotateLogs(
                Effect.logDebug('Timer wakeup'),
                logAnnotations(TelemetryLogComponent.runtime, {
                  [TelemetryAttributeKey.namespace]: self.options.namespace,
                  [TelemetryAttributeKey.workflowName]: run.workflow_name,
                  [TelemetryAttributeKey.workflowVersion]: run.version,
                  [TelemetryAttributeKey.executionId]: timer.execution_id,
                  [TelemetryAttributeKey.stepId]: timer.step_id
                })
              )
              if (yield* advanced.timerDelivered(timer)) {
                const attributes = {
                  [TelemetryAttributeKey.workflowName]: run.workflow_name,
                  [TelemetryAttributeKey.workflowVersion]: run.version
                }
                self.telemetry?.count('timerDelivered', attributes)
                self.telemetry?.observe(
                  'timerLag',
                  Math.max(0, Number(deliveredAt) - Number(timer.deadline)),
                  attributes
                )
              }
            })
        )
      }
      for (const child of yield* advanced.readyChildren()) {
        const parent = yield* journal.get(child.parent_id)
        const run = yield* journal.followContinuation(child.child_id)
        if (!['completed', 'failed', 'cancelled'].includes(run.state)) continue
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
        if (row.state === 'failed' && row.control === 'cancel') {
          if (row.dispatched) yield* definition.interrupt(row.execution_id)
          yield* journal.dispatched(row)
          continue
        }
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
      for (const reconciliation of yield* journal.pendingReconciliations()) {
        yield* journal.complete(
          reconciliation.execution_id,
          reconciliation.result_json,
          reconciliation.failure_json ? decode<Failure>(reconciliation.failure_json) : null
        )
        yield* journal.reconciliationDelivered(reconciliation)
      }
      for (const retry of yield* journal.pendingRetries()) {
        const run = yield* journal.get(retry.execution_id)
        const definition = workflowDefinition(
          self.options.namespace,
          run.workflow_name,
          run.version
        )
        const deferred = retryDeferred(retry.step_id, retry.attempt)
        const identity = yield* journal.activityMetricIdentity(retry.execution_id, retry.step_id)
        const retryAttributes: TelemetryAttributes = {
          [TelemetryAttributeKey.executionId]: retry.execution_id,
          [TelemetryAttributeKey.stepId]: retry.step_id,
          [TelemetryAttributeKey.activityBusinessAttempt]: retry.attempt
        }
        if (identity) {
          retryAttributes[TelemetryAttributeKey.activityName] = identity.activityName
          retryAttributes[TelemetryAttributeKey.activityVersion] = identity.activityVersion
          retryAttributes[TelemetryAttributeKey.queueName] = identity.queueName
        }
        yield* Effect.useSpan(
          TelemetrySpanName.retryDeliver,
          { attributes: retryAttributes, kind: 'consumer' },
          () =>
            Effect.gen(function* () {
              yield* DurableDeferred.done(deferred, {
                token: DurableDeferred.tokenFromExecutionId(deferred, {
                  workflow: definition,
                  executionId: retry.execution_id
                }),
                exit: Exit.void
              })
              const deliveredAt = yield* journal.now()
              if (yield* journal.retryDelivered(retry))
                self.telemetry?.observe(
                  'activityRetryLag',
                  Math.max(0, Number(deliveredAt) - Number(retry.deadline)),
                  identity
                    ? {
                        [TelemetryAttributeKey.activityName]: identity.activityName,
                        [TelemetryAttributeKey.activityVersion]: identity.activityVersion,
                        [TelemetryAttributeKey.queueName]: identity.queueName
                      }
                    : {}
                )
            })
        )
      }
      const waits = yield* journal.pendingWaits()
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
        const deliver = Effect.gen(function* () {
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
          yield* Effect.annotateLogs(
            Effect.logDebug('Signal wakeup'),
            logAnnotations(TelemetryLogComponent.runtime, {
              [TelemetryAttributeKey.namespace]: self.options.namespace,
              [TelemetryAttributeKey.workflowName]: run.workflow_name,
              [TelemetryAttributeKey.workflowVersion]: run.version,
              [TelemetryAttributeKey.executionId]: wait.execution_id,
              [TelemetryAttributeKey.stepId]: wait.step_id,
              [TelemetryAttributeKey.signalName]: wait.signal_name
            })
          )
        })
        if (wait.state === 'success')
          yield* Effect.useSpan(
            TelemetrySpanName.signalConsume,
            {
              attributes: {
                [TelemetryAttributeKey.executionId]: wait.execution_id,
                [TelemetryAttributeKey.workflowName]: run.workflow_name,
                [TelemetryAttributeKey.workflowVersion]: run.version,
                [TelemetryAttributeKey.stepId]: wait.step_id,
                [TelemetryAttributeKey.signalName]: wait.signal_name
              },
              kind: 'consumer'
            },
            () => deliver
          )
        else yield* deliver
      }
      if (Date.now() >= self.safetySweepAt) {
        const active = yield* journal.activeAfter(self.safetySweepCursor)
        for (const row of active) yield* self.reconcileSweep(row)
        self.safetySweepCursor = active.length === 100 ? active.at(-1)!.execution_id : ''
        self.safetySweepAt = Date.now() + safetySweepInterval
      }
    })
    const measured = operation.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          self.telemetry?.observe(
            'dispatcherIterationDuration',
            Math.max(0, Date.now() - startedAt)
          )
        })
      )
    )
    return measured.pipe(
      Effect.tap(() =>
        Effect.gen(function* () {
          if (self.dispatcherFailed) {
            self.telemetry?.count('dispatcherRecovery')
            yield* Effect.annotateLogs(
              Effect.logWarning('Dispatcher recovered after error'),
              logAnnotations(TelemetryLogComponent.dispatcher, {
                [TelemetryAttributeKey.namespace]: self.options.namespace
              })
            )
          }
          self.dispatcherFailed = false
          self.lastDispatchError = undefined
          self.lastSuccessfulDispatchAt = Date.now()
        })
      ),
      Effect.tapCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : Effect.sync(() => {
              self.lastDispatchError = Cause.pretty(cause)
              self.dispatcherFailed = true
              self.lastDispatchFailureAt = Date.now()
              self.telemetry?.count('dispatcherFailure')
            })
      ),
      this.dispatchLock.withPermits(1)
    )
  }
}
