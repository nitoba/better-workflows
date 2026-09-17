import type { Type, ModuleMetadata, InjectionToken } from '@nestjs/common'
import type { QueueReference } from './queues'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Failure } from './errors'

/**
 * A nonnegative duration, expressed as milliseconds or a unit-suffixed string.
 * Numeric values and converted strings must resolve to safe integer milliseconds.
 * Units are `ms`, `s`, `m`, `h` and `d` (24 hours); fractions such as `"1.5s"`
 * are allowed when the conversion is integral. Individual options may reject zero.
 * @example
 * ```ts
 * import type { Duration } from 'better-workflows'
 * const timeout: Duration = '2m'
 * const interval: Duration = 250 // milliseconds
 * ```
 */
export type Duration = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`
/**
 * Values that can cross a durable serialization boundary without losing information.
 * Use finite numbers, strings, booleans, null, dense arrays and plain objects.
 * Dates, class instances, BigInt, accessors, cycles and nested undefined are rejected.
 * A top-level `undefined` is supported separately for void results, not as JsonValue.
 * Encoded transport envelopes are limited to 1 MiB; store large files externally
 * and pass their identifiers instead.
 */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue }

/**
 * Persisted business-retry policy for an activity.
 * Only failures marked retryable consume another business attempt. Worker crashes
 * and infrastructure redelivery are separate from these attempts. A more-specific
 * retry policy replaces the entire inherited object; it is not deep-merged.
 * @example
 * ```ts
 * import type { RetryOptions } from 'better-workflows'
 * const retry: RetryOptions = {
 *   maxAttempts: 3, backoff: 'exponential', initialDelay: '1s', maxDelay: '30s'
 * }
 * ```
 */
export interface RetryOptions {
  /**
   * Total business attempts, including the initial call. Must be a positive integer.
   * @defaultValue 1 when no retry policy is supplied
   */
  readonly maxAttempts: number
  /**
   * Delay progression between business attempts.
   * @defaultValue "exponential"
   */
  readonly backoff?: 'fixed' | 'exponential'
  /**
   * Delay after the first retryable failure; subsequent exponential delays double.
   * @defaultValue "1s"
   */
  readonly initialDelay?: Duration
  /**
   * Upper bound applied to each retry delay; the deadline is persisted with the failure.
   * @defaultValue "1m"
   */
  readonly maxDelay?: Duration
}

/**
 * Identity, Standard Schema contracts and accepted signals for {@link Workflow} and
 * {@link WorkflowContract}.
 * Keep a compatible handler registered for every version with unfinished executions.
 * Schemas validate durable data; do not transform values into Dates or class instances.
 * @typeParam I - Workflow input accepted by the schema and the handler.
 * @typeParam O - Successful workflow output.
 */
export interface WorkflowOptions<I = unknown, O = unknown> {
  /**
   * Stable workflow identity within the application namespace (1–256 characters, no ASCII controls).
   */
  readonly name: string
  /**
   * Positive contract/handler version. Retain old versions while their executions need replay.
   */
  readonly version: number
  /**
   * Standard Schema validating input before acceptance and again when the handler runs.
   */
  readonly input: StandardSchemaV1<I, I>
  /**
   * Standard Schema validating the successful handler result before persistence.
   */
  readonly output: StandardSchemaV1<O, O>
  /**
   * Pure key resolver receiving validated input. StartOptions.idempotencyKey takes precedence.
   * The deduplication scope is namespace + workflow name + key, not version.
   * Omitting both resolvers creates a new random key for each start.
   * @param input - Validated workflow input.
   * @returns Stable deduplication key for the logical request.
   */
  readonly idempotencyKey?: (input: I) => string
  /**
   * Signals allowed by this workflow version. Undeclared signals are rejected.
   * @defaultValue An empty signal list
   */
  readonly signals?: readonly SignalDefinition[]
}

/**
 * Contract and optional policy overrides for an {@link Activity} method.
 * Queue, retry and timeout inherit from root defaults, owner-feature defaults and
 * {@link Activities}, in that order, before these method-specific values apply.
 * @typeParam I - Validated activity input.
 * @typeParam O - Validated activity output.
 */
export interface ActivityOptions<I = unknown, O = unknown> {
  /**
   * Stable activity identity; name and version must be unique in the application catalog.
   */
  readonly name: string
  /**
   * Positive activity contract version. Keep compatible contracts available for queued work.
   */
  readonly version: number
  /**
   * Destination queue, which must be owned/imported by the activity feature after inheritance.
   */
  readonly queue?: QueueReference
  /**
   * Standard Schema applied to the payload before dispatch and when the worker invokes it.
   */
  readonly input: StandardSchemaV1<I, I>
  /**
   * Standard Schema applied before an activity result is committed.
   */
  readonly output: StandardSchemaV1<O, O>
  /**
   * Method-level replacement of the inherited business retry policy.
   */
  readonly retry?: RetryOptions
  /**
   * Positive per-attempt execution timeout; waiting in a queue is not counted.
   * Expiry aborts the signal and yields a retryable ACTIVITY_TIMEOUT failure.
   * @defaultValue "5m" if no level supplies a timeout
   */
  readonly timeout?: Duration
  /**
   * Pure concurrency-key resolver evaluated on validated input and recorded in the command.
   * Required for every activity targeting a queue with perKeyConcurrency; keys are
   * shared by activity types in that queue. This is not an idempotency key.
   * @param input - Validated activity input.
   * @returns Stable concurrency-group key within the logical queue.
   */
  readonly key?: (input: I) => string
}

/**
 * Named, schema-validated event accepted by a workflow's durable signal inbox.
 * Create it with {@link defineSignal}; declare it in WorkflowOptions.signals.
 * @typeParam I - Payload accepted from the signal producer.
 * @typeParam O - Validated (possibly transformed) value returned to the workflow.
 */
export interface SignalDefinition<I = unknown, O = I> {
  /**
   * Stable event name within an execution (1–256 characters, no ASCII controls).
   */
  readonly name: string
  /**
   * Standard Schema validating producer input into the durable value consumed by the workflow.
   */
  readonly schema: StandardSchemaV1<I, O>
}

/**
 * Options for a durable signal wait inside WorkflowContext.waitForSignal.
 * This deadline belongs to the workflow, unlike a caller-local result timeout.
 */
export interface SignalWaitOptions {
  /**
   * Deadline duration measured from the first recorded wait, preserved across replay.
   * Expiry raises a catchable SIGNAL_TIMEOUT business failure.
   * @defaultValue No deadline
   */
  readonly timeout?: Duration
}
/**
 * Caller-local controls for WorkflowHandle.result; neither option cancels the workflow.
 */
export interface ResultWaitOptions {
  /**
   * Maximum local wait, measured with real time. Expiry raises WAIT_TIMEOUT.
   * It bounds notification/fallback waiting, not an in-flight storage request.
   * @defaultValue No deadline
   */
  readonly timeout?: Duration
  /**
   * Abort only this wait with WAIT_ABORTED; execution continues in the background.
   */
  readonly signal?: AbortSignal
}
/**
 * Options for durably accepting an execution through WorkflowClient.start.
 */
export interface StartOptions {
  /**
   * Explicit key overriding the workflow resolver; identical payloads return the existing run.
   * Different payloads for the same key fail with IDEMPOTENCY_CONFLICT.
   * A pruned execution reserves its key and fails with EXECUTION_PRUNED.
   */
  readonly idempotencyKey?: string
}
/**
 * Pagination for one execution's journal, ordered by increasing sequence number.
 */
export interface HistoryOptions {
  /**
   * Exclusive sequence cursor; use the previous page's nextCursor.
   * @defaultValue 0
   */
  readonly after?: number
  /**
   * Number of events per page, an integer from 1 through 1000.
   * @defaultValue 100
   */
  readonly limit?: number
}
/**
 * Metadata for a cooperative, durable cancellation request; not a rollback policy.
 */
export interface CancelOptions {
  /**
   * Journaled explanation, up to 4096 characters. Avoid secrets and personal data.
   * @defaultValue An empty string
   */
  readonly reason?: string
}
/**
 * Deduplication options for one signal delivery to one execution.
 */
export interface SignalDeliveryOptions {
  /**
   * Event key scoped to execution + signal name. Reusing the key with the same
   * validated payload returns accepted: false; a changed payload is a conflict.
   */
  readonly idempotencyKey: string
}

/**
 * Stable command identity for a call made through an activity client.
 */
export interface StepOptions {
  /**
   * Unique command ID in this scope, stable across replay (1–256 characters).
   * Repeated activity calls need distinct IDs. Branches supply separate scopes.
   * The @bw/ prefix is reserved at the root; do not use random IDs.
   */
  readonly stepId: string
}

/**
 * Per-invocation worker context supplied as the activity handler's second argument.
 * Do not save this context on a singleton provider. External operations must be
 * idempotent: an effect may succeed before its result is persisted, causing redelivery.
 */
export interface ActivityContext {
  /**
   * Workflow execution owning this invocation.
   */
  readonly executionId: string
  /**
   * Durable step path; nested branch/saga prefixes are assigned by the interpreter.
   */
  readonly stepId: string
  /**
   * One-based business attempt. Infrastructure redelivery may repeat the same attempt.
   */
  readonly attempt: number
  /**
   * Stable key across business retries and infrastructure redeliveries of this step.
   * Forward it to external providers that support idempotency; the library cannot
   * guarantee exactly-once side effects in another system.
   */
  readonly idempotencyKey: string
  /**
   * Cooperative abort signal for timeout, cancellation, shutdown or lost ownership.
   * Pass it to abort-aware I/O. It cannot forcibly stop arbitrary user code.
   */
  readonly signal: AbortSignal
  /**
   * Record application progress while this invocation still owns a live lease.
   * The worker renews leases automatically; this method is not required for renewal
   * and does not extend the activity timeout. Details are appended to history.
   * @param details - Serializable progress data; defaults to null.
   * @returns Resolves after the heartbeat has been recorded.
   * @throws WorkflowError with LEASE_LOST if aborted; ownership/storage failures also reject.
   * @example
   * ```ts
   * import type { ActivityContext } from 'better-workflows'
   * async function reportProgress(ctx: ActivityContext) {
   *   await ctx.heartbeat({ processed: 50, total: 100 })
   * }
   * ```
   */
  heartbeat(details?: JsonValue): Promise<void>
}

/**
 * Typed proxy returned by WorkflowContext.activities, not the original Nest service.
 * Async methods take their input plus StepOptions; the worker receives ActivityContext
 * instead. Only decorated methods are dispatched at runtime, so keep unrelated public
 * async helpers off an activities contract. Await every durable call.
 * @typeParam T - Instance type of the activities provider.
 */
export type ActivityClient<T> = {
  [
    K in keyof T as T[K] extends (input: infer _I, ...args: never[]) => Promise<infer _O>
      ? K
      : never
  ]: T[K] extends (input: infer I, ...args: never[]) => Promise<infer O>
    ? (input: I, options: StepOptions) => Promise<O>
    : never
}

/**
 * Stable item identities and persisted branch-admission limit for WorkflowContext.map.
 * @typeParam I - Input item type.
 */
export interface MapOptions<I> {
  /**
   * Deterministic, unique key for each item. Keys are persisted with input order.
   * Do not depend on clocks, random values or mutable service state.
   * @param input - Item being mapped.
   * @param index - Original zero-based item index.
   * @returns Stable item key, unique within this map.
   */
  readonly key: (input: I, index: number) => string
  /**
   * Positive number of admitted branches, persisted across restart. Suspended
   * branches retain admission until settled; this is not the activity worker limit.
   */
  readonly concurrency: number
}

/**
 * Policy recorded with a parent-child link when a child workflow is started.
 */
export interface ChildOptions {
  /**
   * Action when the parent becomes terminal, including normal completion.
   * request-cancel requests cancellation of unfinished children; abandon lets them
   * continue independently. Parent close does not wait for cancellation to finish.
   * @defaultValue "request-cancel"
   */
  readonly parentClosePolicy?: 'request-cancel' | 'abandon'
}

/**
 * Identifier returned after a child start has been durably linked to its parent.
 * This is not a WorkflowHandle; use the child's typed client to observe it externally.
 */
export interface ChildExecution {
  /**
   * Stable child ID derived from the parent execution and scoped child step.
   */
  readonly executionId: string
}

/**
 * Workflow context with compensatable steps, available only inside WorkflowContext.saga.
 * Use the callback-provided contexts for forward and undo commands; capturing an
 * outer context for nested durable operations is rejected instead of deadlocking.
 * `continueAsNew` is rejected on this context because it is not the root context.
 */
export interface SagaContext extends WorkflowContext {
  /**
   * Owning workflow execution; a saga does not create a separate workflow execution.
   */
  readonly executionId: string
  /**
   * Run a forward step and durably register its compensation after success.
   * Completed forward results are reused during replay. On a later business failure
   * in this saga, registered compensations execute in reverse registration order.
   * A failed forward step is not registered: ambiguous external side effects still
   * need application/provider idempotency and reconciliation.
   * @typeParam A - Serializable forward result passed to the compensation.
   * @param stepId - Stable, unique step name within this saga.
   * @param execute - Forward work using its supplied context.
   * @param compensate - Undo work using the saved result and its supplied context.
   * @returns The forward result, including a previously recorded result on replay.
   * @throws A business failure from forward work; compensation failures are reported by saga.
   * @example
   * ```ts
   * import type { SagaContext } from 'better-workflows'
   * declare const saga: SagaContext
   * await saga.step('reservation-window', async forward => {
   *   await forward.sleep('hold', '1s')
   *   return { reservation: 'example' }
   * }, async (_reservation, undo) => {
   *   await undo.sleep('cooldown', '1s')
   * })
   * ```
   */
  step<A>(
    stepId: string,
    execute: (context: WorkflowContext) => Promise<A>,
    compensate: (value: A, context: WorkflowContext) => Promise<void>
  ): Promise<A>
}

/**
 * Named branch callbacks for WorkflowContext.parallel.
 * Keys are sorted lexicographically for deterministic scheduling and failure selection.
 * Each branch must use its supplied context and return a serializable value.
 */
export type ParallelTasks = Readonly<Record<string, (context: WorkflowContext) => Promise<any>>>
/**
 * Result object preserving the keys and awaited result type of each parallel branch.
 * @typeParam T - Record of named parallel callbacks.
 */
export type ParallelResults<T extends ParallelTasks> = {
  readonly [K in keyof T]: Awaited<ReturnType<T[K]>>
}

/**
 * Fully resolved policy for a logical activity queue.
 * Local worker slots and distributed permits are distinct. All handlers sharing the
 * queue share its limits; workflow interpreter slots and map admission are separate.
 */
export interface QueueOptions {
  /**
   * Positive local slot count shared by all handlers on this logical queue in one process.
   */
  readonly concurrency: number
  /**
   * Optional live-permit limit shared across processes in the namespace and queue.
   * Processes must agree on this value; omission means no global limit.
   */
  readonly globalConcurrency?: number
  /**
   * Optional shared permit limit for each ActivityOptions.key value within the queue.
   * External work ignoring cancellation may outlive its lease; permits cannot stop it.
   */
  readonly perKeyConcurrency?: number
}

/**
 * Inheritable activity policy; declaring defaults creates no queues or handlers.
 * Resolution is root, owner feature, provider decorator, then method decorator.
 * The caller's feature never replaces the activity owner's policy.
 */
export interface ActivityDefaults {
  /**
   * Default destination; does not register or export that queue.
   */
  readonly queue?: QueueReference
  /**
   * Whole-policy replacement at this level; omitted fields use library retry defaults.
   */
  readonly retry?: RetryOptions
  /**
   * Default positive per-attempt timeout, overridden by more-specific levels.
   */
  readonly timeout?: Duration
}

/**
 * Partial queue policy used by defaults, queue registrations and root overrides.
 * Omission inherits; null explicitly removes a shared limit. Local concurrency cannot
 * be null. These are per-queue policies, not an aggregate budget across queues.
 */
export interface QueueSettings {
  /**
   * Positive local slot count, inherited when omitted. Library default is 4.
   */
  readonly concurrency?: number
  /**
   * Shared limit; undefined inherits and null removes an inherited limit.
   */
  readonly globalConcurrency?: number | null
  /**
   * Per-key shared limit; undefined inherits and null removes an inherited limit.
   */
  readonly perKeyConcurrency?: number | null
}

/**
 * Explicit policy ownership for a queue, or one final root deployment override.
 * In queues, a logical identity has exactly one owner. In queueOverrides, the queue
 * must already be registered and cannot occur twice; overrides do not create queues.
 */
export interface QueueRegistration extends QueueSettings {
  /**
   * Reference returned by defineQueue; durable identity is its name, not object identity.
   */
  readonly queue: QueueReference
}

/**
 * Defaults applied before explicit provider, method and queue configuration.
 */
export interface WorkflowDefaults {
  /**
   * Per-queue defaults. Equal defaults across two queues do not combine their capacity.
   */
  readonly queues?: QueueSettings
  /**
   * Defaults for the activities owned by this scope, not those called from it.
   */
  readonly activities?: ActivityDefaults
}

/**
 * Process or feature restrictions on which handlers run and with what local capacity.
 * Root and feature switches/selectors are intersected: a feature cannot enable work
 * forbidden by the root. Disabling execution does not avoid instantiating registered
 * implementation providers; use clients/activityContracts for contract-only roles.
 */
export interface ExecutionOptions {
  /**
   * Workflow handler activation and local interpreter capacity, not the number of unfinished runs.
   */
  readonly workflows?: {
    /** Enable workflow implementations at this level; root false cannot be overridden. @defaultValue true */
    readonly enabled?: boolean
    /** Positive interpreter-round budget. Root default is 20; feature omission adds no extra cap. */
    readonly concurrency?: number
  }
  /**
   * Activity worker activation and an optional allowlist of queue references.
   */
  readonly activities?: {
    /** Enable activity workers at this level; root false cannot be overridden. @defaultValue true */
    readonly enabled?: boolean
    /** Allowlist intersected with the root selector; omission permits all visible queues, [] permits none. */
    readonly queues?: readonly QueueReference[]
  }
}

/** Log severities accepted by the OTLP logging exporter configuration. */
export type OtlpLogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal' | 'none'

/** Common enablement and batching settings for one OTLP signal. */
export interface OtlpSignalOptions {
  /** Whether this signal should be exported. @defaultValue true when configured */
  readonly enabled?: boolean
  /** Positive interval between export attempts. */
  readonly exportInterval?: Duration
}

/** OTLP metrics signal settings. */
export interface OtlpMetricsOptions extends OtlpSignalOptions {
  /** Aggregation mode used by the metrics exporter. @defaultValue "cumulative" */
  readonly temporality?: 'cumulative' | 'delta'
}

/** OTLP logs signal settings. */
export interface OtlpLogsOptions extends OtlpSignalOptions {
  /** Minimum log severity exported by the runtime. @defaultValue "info" */
  readonly level?: OtlpLogLevel
}

/** Input used by {@link otlp} to configure OTLP/HTTP export. */
export interface OtlpOptions {
  /** Service identity sent as the standard `service.name` resource attribute. */
  readonly serviceName: string
  /** Optional application release sent as `service.version`. */
  readonly serviceVersion?: string
  /** OTLP/HTTP collector base URL; `/v1/*` paths are added by the exporter. */
  readonly endpoint: string
  /** Trace export settings; `true` enables traces with Effect's default interval. */
  readonly traces?: boolean | OtlpSignalOptions
  /** Metrics export settings; omitted metrics are disabled. */
  readonly metrics?: boolean | OtlpMetricsOptions
  /** Logs export settings; omitted logs are disabled. */
  readonly logs?: boolean | OtlpLogsOptions
  /** Fallback interval for configured signals without their own interval. */
  readonly exportInterval?: Duration
  /** Maximum records in one OTLP request. @defaultValue 1000 */
  readonly maxBatchSize?: number
  /** Maximum time allowed for exporter final flush. @defaultValue "3s" */
  readonly shutdownTimeout?: Duration
  /** Metrics aggregation mode when not specified on `metrics`. */
  readonly metricsTemporality?: 'cumulative' | 'delta'
  /** Additional low-cardinality resource attributes. */
  readonly attributes?: Readonly<Record<string, string | number | boolean>>
  /** Headers sent to the collector; values are never included in library logs. */
  readonly headers?: Readonly<Record<string, string>>
}

/** Validated OTLP configuration returned by {@link otlp}. */
export interface OtlpObservabilityOptions extends OtlpOptions {
  /** Identifies the built-in OTLP exporter configuration. */
  readonly kind: 'otlp'
}

/** Public observability configuration accepted by the workflow root module. */
export type ObservabilityOptions = OtlpObservabilityOptions

/**
 * Register a Nest implementation class or reuse an already-exported instance.
 * A class is instantiated in the feature; `{ provide, useExisting }` identifies the
 * decorated contract and resolves an existing Nest injection token through imports.
 * Singleton dependency trees are required; request/transient scopes are rejected.
 * @typeParam T - Decorated implementation or contract constructor.
 */
export type HandlerRegistration<T extends Type = Type> =
  | T
  | {
      /** Decorated workflow/activity contract that the existing instance implements. */
      readonly provide: T
      /** Nest token exported by an imported module; resolved without constructing a duplicate. */
      readonly useExisting: InjectionToken<InstanceType<T>>
    }

/**
 * Feature-owned values, resolved synchronously or by forFeatureAsync's factory.
 * These settings cannot add providers after Nest has built the dependency graph.
 */
export interface FeatureConfiguration {
  /**
   * Policies owned by this feature; private unless explicitly exported.
   */
  readonly queues?: readonly QueueRegistration[]
  /**
   * Defaults for owned queues and activities; imports retain their own defaults.
   */
  readonly defaults?: WorkflowDefaults
  /**
   * Additional restrictions intersected with the root process configuration.
   */
  readonly execution?: ExecutionOptions
}

/**
 * Queue and activity capabilities exposed through normal Nest module exports.
 * Exporting an activity alone lets consumers call it without knowing its private
 * queue. Export the queue too only when consumers may put their own handlers on it.
 */
export interface FeatureExports {
  /**
   * Owned or imported queues that downstream features may use for their own handlers.
   */
  readonly queues?: readonly QueueReference[]
  /**
   * Owned or imported activity contracts callable by downstream workflows.
   * Does not export the raw Nest service or require exporting its private queue.
   */
  readonly activities?: readonly Type[]
}

/**
 * Static Nest dependency graph and handler contracts for one feature.
 * Imports/providers are ordinary Nest metadata. Dependencies of feature handlers
 * must be declared here or exported by these imports, not merely by an outer module.
 */
export interface FeatureStructure extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  /**
   * Unique owner name required for handlers, activityContracts or any configuration.
   * Client-only registrations may omit it. It never changes durable routing names.
   */
  readonly name?: string
  /**
   * Concrete implementations to instantiate (or reuse), and their typed workflow
   * clients. In advanced mode, the client is keyed by the contract passed to
   * `@Workflow`, not by this implementation class. Do not also register handlers in
   * the outer module's providers.
   */
  readonly workflows?: readonly HandlerRegistration<WorkflowImplementationClass>[]
  /**
   * Activity implementations to instantiate or reuse through Nest dependency injection.
   */
  readonly activities?: readonly HandlerRegistration[]
  /**
   * Owner contracts/defaults without constructing activity services, for orchestrators.
   * Do not list the same contract in activities and activityContracts.
   */
  readonly activityContracts?: readonly Type[]
  /**
   * Typed workflow contracts only. No workflow implementation is instantiated.
   * Also declares contracts that this feature may start as children.
   */
  readonly clients?: readonly WorkflowContractClass[]
  /**
   * Explicit activity/queue exports; workflow clients are exported automatically.
   * An outer Nest module must reexport WorkflowsModule to forward these capabilities.
   */
  readonly exports?: FeatureExports
}

/**
 * Synchronous domain registration accepted by WorkflowsModule.forFeature.
 * Combines static provider structure with queue/default/execution settings.
 */
export interface WorkflowsFeatureOptions extends FeatureStructure, FeatureConfiguration {}

/**
 * Static feature registration with DI-resolved configuration values.
 * Only queues, defaults and execution are returned asynchronously. The module's
 * imports, providers, handlers and export capabilities remain static.
 */
export interface WorkflowsFeatureAsyncOptions extends FeatureStructure {
  /**
   * Ordered Nest dependency tokens passed to useFactory.
   */
  readonly inject?: readonly InjectionToken[]
  // Only values are asynchronous: Nest's provider graph is declared statically above.
  /**
   * Resolves queue/default/execution values before catalog validation and worker start.
   * @param dependencies - Values resolved in the order of inject.
   * @returns Synchronous settings or a promise of settings; not new handler registrations.
   */
  readonly useFactory: (
    ...dependencies: any[]
  ) => FeatureConfiguration | Promise<FeatureConfiguration>
}

/**
 * Replay-aware commands passed to WorkflowHandler.run.
 * The handler may run again to reconstruct its state. Keep I/O, time-dependent reads
 * and randomness in activities; await every durable command and use stable step IDs.
 * Internal suspension never enters application catch/finally blocks. Use map or
 * parallel for durable concurrency, not Promise.all on one context.
 */
export interface WorkflowContext {
  /**
   * Stable identifier for this workflow execution; shared by its branch and saga contexts.
   */
  readonly executionId: string
  /**
   * Finish this execution and start a clean next generation of the same
   * workflow contract. Only the root workflow context may invoke this control
   * operation; it never resolves because the current execution is replaced.
   * @param input - New input validated by the current workflow contract.
   * @returns A promise that never resolves when the transition is accepted.
   * @throws WorkflowError with CONTINUE_AS_NEW_NOT_ROOT from a branch or saga context.
   */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The current contract schema validates this public boundary at runtime.
  continueAsNew(input: unknown): Promise<never>
  /**
   * Create a typed durable-call proxy for an owned or imported activities contract.
   * Calling a proxy method dispatches work to its resolved queue and records its result;
   * calling the original Nest service directly is not a durable activity invocation.
   * @typeParam T - Instance type of the activities provider.
   * @param provider - Class decorated with Activities and registered/exported to this feature.
   * @returns Proxy of async activity methods accepting input and StepOptions.
   * @throws WorkflowError if the contract is missing, unregistered or not visible.
   * @example
   * ```ts
   * import { Activities, Activity, defineQueue } from 'better-workflows'
   * import type { WorkflowContext } from 'better-workflows'
   * import { z } from 'zod'
   * const Reports = defineQueue('reports')
   * @Activities({ queue: Reports })
   * class ReportActivities {
   *   @Activity({ name: 'reports.total', version: 1, input: z.array(z.number()), output: z.number() })
   *   async total(values: number[]) { return values.reduce((sum, value) => sum + value, 0) }
   * }
   * // Register Reports and ReportActivities in the workflow's feature before use.
   * declare const ctx: WorkflowContext
   * const total = await ctx.activities(ReportActivities).total([10, 20], { stepId: 'total' })
   * ```
   */
  activities<T>(provider: Type<T>): ActivityClient<T>
  /**
   * Map items through durably admitted branches and return results in input order.
   * Completed branches are reused. Suspended branches retain their admission slot.
   * The group settles every branch before reporting the first failure in input order;
   * it is not fail-fast. Empty input returns an empty array. Use each branch's context.
   * @typeParam I - Serializable input item.
   * @typeParam O - Serializable branch result.
   * @param stepId - Stable group command name within this scope.
   * @param items - Ordered items; changing them during replay is a command mismatch.
   * @param options - Stable unique item keys and positive admission concurrency.
   * @param execute - Callback receiving the item, branch context and original index.
   * @returns Results in the original item order, regardless of completion order.
   * @throws WorkflowError on duplicate keys or invalid configuration; branch business failures reject.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare const ctx: WorkflowContext
   * const reports = await ctx.map('reports', ['report-1', 'report-2'],
   *   { key: id => id, concurrency: 2 }, async (id, branch) => {
   *     await branch.sleep('delay', '1s')
   *     return { id, ready: true }
   *   })
   * ```
   */
  map<I, O>(
    stepId: string,
    items: readonly I[],
    options: MapOptions<I>,
    execute: (item: I, context: WorkflowContext, index: number) => Promise<O>
  ): Promise<O[]>
  /**
   * Run named callbacks as a durable group with separate branch contexts.
   * All branches settle before failure is reported; selection uses sorted branch names,
   * not completion timing. Unlike Promise.all, branch admission/results survive replay.
   * @typeParam T - Named callbacks whose return types determine the result object.
   * @param stepId - Stable group name in this scope.
   * @param tasks - Deterministically named callbacks; use their supplied contexts.
   * @param options - Optional positive concurrency; defaults to all branches (at least 1).
   * @returns An object with each task key and its awaited value; {} for no tasks.
   * @throws Business failure of the first failed branch in sorted name order.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare const ctx: WorkflowContext
   * const result = await ctx.parallel('deadlines', {
   *   email: async branch => { await branch.sleep('wait', '1h'); return 'email-ready' },
   *   report: async branch => { await branch.sleep('wait', '2h'); return { ready: true } }
   * }, { concurrency: 2 })
   * ```
   */
  parallel<T extends ParallelTasks>(
    stepId: string,
    tasks: T,
    options?: {
      /** Positive branch-admission limit; defaults to all named branches (at least 1). */
      readonly concurrency?: number
    }
  ): Promise<ParallelResults<T>>
  /**
   * Start a durably linked child workflow and wait for its terminal result.
   * Identity derives from this execution and scoped step, so replay reuses the child.
   * The child's contract must be owned/declared as a client/imported by this feature;
   * its implementation must run in an orchestrator. It retains its own owner defaults.
   * @typeParam W - Child workflow constructor.
   * @param stepId - Unique stable child occurrence in this scope.
   * @param workflow - Registered/visible child workflow class.
   * @param input - Input matching the child's schema and handler.
   * @param options - Parent-close policy, defaulting to request-cancel.
   * @returns The child's successful output; waiting suspends durably.
   * @throws The child's terminal failure, or WorkflowError for an invalid/invisible contract.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare class AnalyzeDocument { run(input: { documentId: string }): Promise<{ summary: string }> }
   * declare const ctx: WorkflowContext
   * const analysis = await ctx.child('analyze', AnalyzeDocument, { documentId: 'doc-1' })
   * ```
   */
  child<W extends WorkflowContractClass>(
    stepId: string,
    workflow: W,
    input: WorkflowInput<W>,
    options?: ChildOptions
  ): Promise<WorkflowOutput<W>>
  /**
   * Start and link a child without waiting for its completion.
   * The default request-cancel policy still applies on normal parent completion.
   * Explicitly choose abandon when the child should outlive a completed parent.
   * @typeParam W - Child workflow constructor.
   * @param stepId - Unique stable child occurrence in this scope.
   * @param workflow - Visible child contract with an implementation in an orchestrator.
   * @param input - Serializable input matching the child's schema.
   * @param options - Parent-close policy; not an activity retry policy.
   * @returns The durably linked child identifier, not proof that it has started or finished.
   * @throws WorkflowError for invalid input, changed replay commands or invisible contracts.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare class FollowUp { run(input: { orderId: string }): Promise<void> }
   * declare const ctx: WorkflowContext
   * const child = await ctx.startChild('follow-up', FollowUp, { orderId: 'order-1' },
   *   { parentClosePolicy: 'abandon' })
   * ```
   */
  startChild<W extends WorkflowContractClass>(
    stepId: string,
    workflow: W,
    input: WorkflowInput<W>,
    options?: ChildOptions
  ): Promise<ChildExecution>
  /**
   * Execute a scope that compensates successful registered steps on business failure.
   * Compensations run in reverse order, resume after restart, and can use retrying
   * activities. A permanently failed compensation does not skip the remaining ones.
   * Cancellation, shutdown and suspension do not trigger rollback. A successful saga
   * is closed: a later failure outside it does not compensate it.
   * @typeParam A - Serializable result of the successful saga body.
   * @param stepId - Stable saga command name within this scope.
   * @param execute - Body using the supplied SagaContext and its step callbacks.
   * @returns The saga result when its forward body succeeds.
   * @throws Original business failure after rollback, or COMPENSATION_FAILED when undo fails.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare const ctx: WorkflowContext
   * await ctx.saga('checkout', async saga => {
   *   return saga.step('hold', async forward => {
   *     await forward.sleep('reservation', '1s')
   *     return { reservationId: 'reservation-1' }
   *   }, async (_reservation, undo) => { await undo.sleep('release-window', '1s') })
   * })
   * ```
   */
  saga<A>(stepId: string, execute: (saga: SagaContext) => Promise<A>): Promise<A>
  /**
   * Wait on a persisted business timer without holding a worker for the duration.
   * The deadline is established once; replay does not add another full delay.
   * @param stepId - Stable timer command name within this scope.
   * @param duration - Nonnegative wait (milliseconds or a supported suffix).
   * @returns Resolves after the durable deadline; cancellation is not a normal rejection.
   * @throws WorkflowError for an invalid duration or incompatible replay command.
   * @example
   * ```ts
   * import type { WorkflowContext } from 'better-workflows'
   * declare const ctx: WorkflowContext
   * await ctx.sleep('remind-tomorrow', '24h')
   * ```
   */
  sleep(stepId: string, duration: Duration): Promise<void>
  /**
   * Consume one signal from the durable inbox, suspending when none is available.
   * Early events are buffered; consumption is FIFO per signal name and survives replay.
   * An event accepted at or before the deadline wins over timeout even if polling is late.
   * @typeParam I - Producer payload type.
   * @typeParam O - Schema-validated value returned to the workflow.
   * @param stepId - Stable name of this particular wait; repeated waits need new IDs.
   * @param signal - Definition declared in this workflow version's signals list.
   * @param options - Optional durable timeout; omission waits indefinitely.
   * @returns The registered schema's output, not the unvalidated producer payload.
   * @throws Catchable SIGNAL_TIMEOUT on expiry; UNKNOWN_SIGNAL for an undeclared event.
   * @example
   * ```ts
   * import { defineSignal } from 'better-workflows'
   * import type { WorkflowContext } from 'better-workflows'
   * import { z } from 'zod'
   * const Approval = defineSignal('approval', z.object({ approved: z.boolean() }))
   * // Also declare signals: [Approval] in the @Workflow options.
   * declare const ctx: WorkflowContext
   * const approval = await ctx.waitForSignal('review', Approval, { timeout: '7d' })
   * ```
   */
  waitForSignal<I, O>(
    stepId: string,
    signal: SignalDefinition<I, O>,
    options?: SignalWaitOptions
  ): Promise<O>
}

/**
 * Application-owned orchestration implementation registered as a Nest singleton.
 * The run method must be replay-safe; mutable provider fields are not durable state.
 * @typeParam I - Input consumed by run.
 * @typeParam O - Successful output persisted for the execution.
 */
export interface WorkflowHandler<I = never, O = unknown> {
  /**
   * Orchestrate deterministic logic and awaited durable commands.
   * @param input - Input validated by the workflow's registered schema.
   * @param context - Per-round context; do not retain it outside this invocation.
   * @returns A serializable value matching the workflow's output schema.
   * @throws Business errors fail the execution; use activities for external effects.
   */
  run(input: I, context: WorkflowContext): Promise<O>
}

/**
 * Constructor of a workflow contract. Contracts may be abstract because they are
 * metadata and type declarations, not Nest providers. Pass `typeof MyWorkflow` to
 * WorkflowClient, not its instance type.
 */
export type WorkflowContractClass = abstract new (...args: any[]) => WorkflowHandler<any, any>

/**
 * Constructor of a concrete workflow implementation. Unlike a contract, an
 * implementation is instantiated by Nest when registered in a feature.
 * @typeParam C - Contract implemented by the handler.
 */
export type WorkflowImplementationClass<C extends WorkflowContractClass = WorkflowContractClass> =
  new (...args: any[]) => {
    run(input: WorkflowInput<C>, context: WorkflowContext): Promise<WorkflowOutput<C>>
  }

/**
 * Backward-compatible concrete workflow-handler constructor alias.
 * Use {@link WorkflowContractClass} for APIs that also accept abstract contracts.
 */
export type WorkflowClass = Type<WorkflowHandler>
/**
 * Input parameter inferred from a workflow class's run method.
 * @typeParam W - Workflow constructor, such as `typeof MyWorkflow`.
 */
export type WorkflowInput<W extends WorkflowContractClass> = Parameters<InstanceType<W>['run']>[0]
/**
 * Awaited return type inferred from a workflow class's run method.
 * @typeParam W - Workflow constructor, such as `typeof MyWorkflow`.
 */
export type WorkflowOutput<W extends WorkflowContractClass> = Awaited<
  ReturnType<InstanceType<W>['run']>
>

/**
 * Immutable SQLite storage description returned by the sqlite adapter.
 * Use a persistent local file for restart recovery; :memory: loses state on disposal.
 * SQLite is for the single-node topology, not a multi-host shared database.
 */
export interface SqliteStorage {
  /**
   * Adapter discriminator for SQLite.
   */
  readonly driver: 'sqlite'
  /**
   * Local database path, or :memory: for process-local disposable state.
   */
  readonly filename: string
  /**
   * Driver implementation selection; auto detects Bun, otherwise uses Node SQLite.
   */
  readonly runtime: 'auto' | 'bun' | 'node'
}

/**
 * PostgreSQL connection description returned by the postgres adapter.
 * A connection string alone does not configure a distributed workflow cluster.
 */
export interface PostgresStorage {
  /**
   * Adapter discriminator for PostgreSQL.
   */
  readonly driver: 'postgres'
  /**
   * PostgreSQL URL; load from protected configuration rather than source control.
   */
  readonly connectionString: string
  /**
   * Positive maximum number of connections in the runtime's PostgreSQL pool.
   */
  readonly maxConnections: number
}

/**
 * Root infrastructure, application defaults and explicit deployment overrides.
 * Register one root per Nest application. Domain queues are optional here: features
 * can own them. This configuration does not authenticate your application's APIs.
 */
export interface WorkflowsOptions {
  /**
   * Stable data/identity namespace shared by participating processes (1–256 characters).
   */
  readonly namespace: string
  /**
   * Storage description from the sqlite or postgres subpath; connections open at bootstrap.
   */
  readonly storage: SqliteStorage | PostgresStorage
  /** Optional best-effort OTLP/HTTP tracing, metrics and logging export. */
  readonly observability?: ObservabilityOptions
  /**
   * Whether infrastructure providers are global; domain handlers/clients are never made global.
   * With false, explicitly import/reexport the same root module into features.
   * @defaultValue true
   */
  readonly isGlobal?: boolean
  /**
   * single-node uses an embedded runner; distributed requires PostgreSQL and socket routing.
   * @defaultValue "single-node"
   */
  readonly topology?: 'single-node' | 'distributed'
  /**
   * Distributed runner addresses; required when distributed workflow execution is enabled.
   * Keep runner sockets on a trusted private network; this option adds no authentication.
   */
  readonly cluster?: {
    /** Address advertised to other distributed runners; must be reachable by them. */
    readonly address: {
      /** Private-network hostname or IP advertised to peers. */
      readonly host: string
      /** Advertised TCP port, from 1 through 65535. */
      readonly port: number
    }
    /** Optional bind address differing from the advertised address. */
    readonly listenAddress?: {
      /** Local interface to bind; do not expose unauthenticated runner traffic publicly. */
      readonly host: string
      /** Local TCP listening port. */
      readonly port: number
    }
  }
  /**
   * Process-wide handler switches, queue selectors and interpreter capacity.
   */
  readonly execution?: ExecutionOptions
  /**
   * Optional root-owned queue policies, visible to every registered feature.
   */
  readonly queues?: readonly QueueRegistration[]
  /**
   * Application defaults, overridden by more-specific owner configuration.
   */
  readonly defaults?: WorkflowDefaults
  /**
   * Final deployment overrides for registered queues. Unknown/repeated targets are errors.
   */
  readonly queueOverrides?: readonly QueueRegistration[]
  /**
   * run applies additive schemas at startup; validate checks existing schemas first.
   * @defaultValue "run"
   */
  readonly migrations?: 'run' | 'validate'
  /**
   * Positive real-time polling interval for dispatch, storage and cluster coordination.
   * @defaultValue "100ms"
   */
  readonly pollInterval?: Duration
  /**
   * Ownership lease and renewal periods. Refresh must be positive and at most one
   * third of duration. Leases use real database time, even under the testing clock.
   */
  readonly lease?: {
    /** Positive lease lifetime. @defaultValue "30s" */
    readonly duration: Duration
    /** Positive renewal interval, no greater than duration / 3. @defaultValue "10s" */
    readonly refreshInterval: Duration
  }
  /** Delivery failures that are not business failures are retained for administration. */
  readonly deadLetter?: {
    /** Maximum transport deliveries before an unhandled delivery is dead-lettered. @defaultValue 10 */
    readonly maxDeliveryAttempts?: number
  }
}

/**
 * Options for WorkflowsModule.forRootAsync with Nest-injected dependencies.
 * Set isGlobal here, not in the factory result: module visibility is static metadata.
 */
export interface WorkflowsAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  /**
   * Static infrastructure visibility; do not return this option from useFactory.
   * @defaultValue true
   */
  readonly isGlobal?: boolean
  /**
   * Ordered tokens resolved from the root module's imports.
   */
  readonly inject?: readonly (string | symbol | Type)[]
  // Nest's factory dependency tuple is heterogeneous; consumers retain their own argument types.
  /**
   * Factory producing root settings before bootstrap.
   * @param dependencies - Nest instances in inject order.
   * @returns Infrastructure settings, excluding static isGlobal, or a promise of them.
   */
  readonly useFactory: (
    ...dependencies: any[]
  ) => Omit<WorkflowsOptions, 'isGlobal'> | Promise<Omit<WorkflowsOptions, 'isGlobal'>>
}

/**
 * Observable lifecycle state, not a worker-delivery or progress-percentage value.
 * accepted: persisted, awaiting dispatch; running: interpreter advancing; waiting:
 * one or more durable commands pending; paused: cooperative pause requested;
 * blocked: an operational activity dependency needs an administrator;
 * cancelling: cancellation being reconciled; continued: this execution was
 * replaced by a next generation; completed/failed/cancelled: terminal.
 */
export type ExecutionStatus =
  | 'accepted'
  | 'running'
  | 'waiting'
  | 'paused'
  | 'blocked'
  | 'cancelling'
  | 'continued'
  | 'completed'
  | 'failed'
  | 'cancelled'

/**
 * Point-in-time view returned by WorkflowHandle.describe.
 * Control requests may precede physical worker completion. waitingOn identifies one
 * observed pending command, not an exhaustive list of concurrently waiting branches.
 */
export interface ExecutionSnapshot {
  /**
   * Stable execution identifier used by WorkflowClient.getHandle.
   */
  readonly executionId: string
  /**
   * Registered workflow name; not the JavaScript class name.
   */
  readonly workflow: string
  /**
   * Version that accepted this execution, which may differ from a newer client version.
   */
  readonly version: number
  /**
   * Observed lifecycle/control state. Continued, completed, failed and cancelled are terminal for this execution.
   */
  readonly status: ExecutionStatus
  /**
   * Next generation when this execution ended through continueAsNew.
   */
  readonly continuation?: {
    /**
     * Execution identifier of the next generation.
     */
    readonly executionId: string
    /**
     * Zero-based generation number of the next execution.
     */
    readonly generation: number
  }
  /**
   * Acceptance timestamp as a UTC ISO 8601 string.
   */
  readonly createdAt: string
  /**
   * Last journal-event update as a UTC ISO 8601 string; not a worker heartbeat SLA.
   */
  readonly updatedAt: string
  /**
   * One observed pending command. Parallel executions can have other waits.
   */
  readonly waitingOn?: {
    /** Command category, such as activity, signal, timer, map or child. */
    readonly type: string
    /** Scoped ID of the observed command. */
    readonly stepId: string
  }
  /** Operational activity dependency preventing progress; not a terminal failure. */
  readonly blockedOn?: {
    /** Dependency category; currently always `activity`. */
    readonly type: 'activity'
    /** Dead-letter record requiring operator action. */
    readonly deadLetterId: string
    /** Durable activity command step waiting for recovery. */
    readonly stepId: string
    /** Registered activity contract name, when it could be decoded. */
    readonly activity: string | null
    /** Registered activity contract version, when it could be decoded. */
    readonly version: number | null
    /** Logical queue containing the blocked delivery. */
    readonly queue: string
  }
  /**
   * Serializable failure when one has been recorded; absent on success.
   */
  readonly failure?: Failure
}

/**
 * One journal event in an execution's ordered, paginated history.
 * Event detail shapes depend on type and are not a substitute for the typed result.
 */
export interface HistoryEvent {
  /**
   * Increasing execution-local sequence used as an exclusive pagination cursor.
   */
  readonly sequence: number
  /**
   * Event timestamp as a UTC ISO 8601 string.
   */
  readonly at: string
  /**
   * Event kind, such as command.scheduled or activity.heartbeat.
   */
  readonly type: string
  /**
   * Scoped command path for step-specific events; absent for execution-level events.
   */
  readonly stepId?: string
  /**
   * Persisted JSON details; avoid storing secrets in heartbeat/control payloads.
   */
  readonly details: JsonValue
}

/**
 * One ordered history page, with a cursor only when more events were available.
 */
export interface HistoryPage {
  /**
   * Events in ascending sequence order; can be empty.
   */
  readonly events: readonly HistoryEvent[]
  /**
   * Exclusive cursor for the next page; absent when no further events were observed.
   */
  readonly nextCursor?: number
}
