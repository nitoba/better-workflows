import type { Type, ModuleMetadata } from '@nestjs/common'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Failure } from './errors'

export type Duration = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue }

export interface RetryOptions {
  readonly maxAttempts: number
  readonly backoff?: 'fixed' | 'exponential'
  readonly initialDelay?: Duration
  readonly maxDelay?: Duration
}

export interface WorkflowOptions<I = unknown, O = unknown> {
  readonly name: string
  readonly version: number
  readonly input: StandardSchemaV1<I, I>
  readonly output: StandardSchemaV1<O, O>
  readonly idempotencyKey?: (input: I) => string
  readonly signals?: readonly SignalDefinition[]
}

export interface ActivityOptions<I = unknown, O = unknown> {
  readonly name: string
  readonly version: number
  readonly queue: string
  readonly input: StandardSchemaV1<I, I>
  readonly output: StandardSchemaV1<O, O>
  readonly retry?: RetryOptions
  readonly timeout?: Duration
  /** Stable key within a logical queue, evaluated on validated input. */
  readonly key?: (input: I) => string
}

export interface SignalDefinition<I = unknown, O = I> {
  readonly name: string
  readonly schema: StandardSchemaV1<I, O>
}

export interface SignalWaitOptions {
  readonly timeout?: Duration
}
export interface ResultWaitOptions {
  readonly timeout?: Duration
  readonly signal?: AbortSignal
}
export interface StartOptions {
  readonly idempotencyKey?: string
}
export interface HistoryOptions {
  readonly after?: number
  readonly limit?: number
}
export interface CancelOptions {
  readonly reason?: string
}
export interface SignalDeliveryOptions {
  readonly idempotencyKey: string
}

export interface StepOptions {
  readonly stepId: string
}

export interface ActivityContext {
  readonly executionId: string
  readonly stepId: string
  readonly attempt: number
  /** Stable across business retries AND infrastructure redeliveries. */
  readonly idempotencyKey: string
  readonly signal: AbortSignal
  heartbeat(details?: JsonValue): Promise<void>
}

export type ActivityClient<T> = {
  [
    K in keyof T as T[K] extends (input: infer _I, ...args: never[]) => Promise<infer _O>
      ? K
      : never
  ]: T[K] extends (input: infer I, ...args: never[]) => Promise<infer O>
    ? (input: I, options: StepOptions) => Promise<O>
    : never
}

export interface MapOptions<I> {
  readonly key: (input: I, index: number) => string
  readonly concurrency: number
}

export interface ChildOptions {
  /** Applied durably when the parent terminates, including cancellation. */
  readonly parentClosePolicy?: 'request-cancel' | 'abandon'
}

export interface ChildExecution {
  readonly executionId: string
}

export interface SagaContext extends WorkflowContext {
  readonly executionId: string
  step<A>(
    stepId: string,
    execute: (context: WorkflowContext) => Promise<A>,
    compensate: (value: A, context: WorkflowContext) => Promise<void>
  ): Promise<A>
}

export type ParallelTasks = Readonly<Record<string, (context: WorkflowContext) => Promise<any>>>
export type ParallelResults<T extends ParallelTasks> = {
  readonly [K in keyof T]: Awaited<ReturnType<T[K]>>
}

export interface QueueOptions {
  /** Local worker slots per process. */
  readonly concurrency: number
  /** Shared across processes in the same namespace and queue. */
  readonly globalConcurrency?: number
  /** Shared across processes for each Activity.key in the queue. */
  readonly perKeyConcurrency?: number
}

export interface WorkflowContext {
  readonly executionId: string
  activities<T>(provider: Type<T>): ActivityClient<T>
  map<I, O>(
    stepId: string,
    items: readonly I[],
    options: MapOptions<I>,
    execute: (item: I, context: WorkflowContext, index: number) => Promise<O>
  ): Promise<O[]>
  parallel<T extends ParallelTasks>(
    stepId: string,
    tasks: T,
    options?: { readonly concurrency?: number }
  ): Promise<ParallelResults<T>>
  child<W extends WorkflowClass>(
    stepId: string,
    workflow: W,
    input: WorkflowInput<W>,
    options?: ChildOptions
  ): Promise<WorkflowOutput<W>>
  startChild<W extends WorkflowClass>(
    stepId: string,
    workflow: W,
    input: WorkflowInput<W>,
    options?: ChildOptions
  ): Promise<ChildExecution>
  saga<A>(stepId: string, execute: (saga: SagaContext) => Promise<A>): Promise<A>
  sleep(stepId: string, duration: Duration): Promise<void>
  waitForSignal<I, O>(
    stepId: string,
    signal: SignalDefinition<I, O>,
    options?: SignalWaitOptions
  ): Promise<O>
}

export interface WorkflowHandler<I = never, O = unknown> {
  run(input: I, context: WorkflowContext): Promise<O>
}

export type WorkflowClass = Type<WorkflowHandler>
export type WorkflowInput<W extends WorkflowClass> = Parameters<InstanceType<W>['run']>[0]
export type WorkflowOutput<W extends WorkflowClass> = Awaited<ReturnType<InstanceType<W>['run']>>

export interface SqliteStorage {
  readonly driver: 'sqlite'
  readonly filename: string
  readonly runtime: 'auto' | 'bun' | 'node'
}

export interface PostgresStorage {
  readonly driver: 'postgres'
  readonly connectionString: string
  readonly maxConnections: number
}

export interface WorkflowsOptions {
  readonly namespace: string
  readonly storage: SqliteStorage | PostgresStorage
  /** One root runtime per Nest application. Infrastructure providers are global. */
  readonly isGlobal?: true
  readonly topology?: 'single-node' | 'distributed'
  readonly cluster?: {
    readonly address: { readonly host: string; readonly port: number }
    readonly listenAddress?: { readonly host: string; readonly port: number }
  }
  readonly execution?: {
    readonly workflows?: { readonly enabled?: boolean; readonly concurrency?: number }
    readonly activities?: { readonly enabled?: boolean; readonly queues?: readonly string[] }
  }
  readonly queues: Readonly<Record<string, QueueOptions>>
  readonly migrations?: 'run' | 'validate'
  readonly pollInterval?: Duration
  readonly lease?: { readonly duration: Duration; readonly refreshInterval: Duration }
}

export interface WorkflowsAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  readonly inject?: readonly (string | symbol | Type)[]
  // Nest's factory dependency tuple is heterogeneous; consumers retain their own argument types.
  readonly useFactory: (...dependencies: any[]) => WorkflowsOptions | Promise<WorkflowsOptions>
}

export type ExecutionStatus =
  | 'accepted'
  | 'running'
  | 'waiting'
  | 'paused'
  | 'cancelling'
  | 'completed'
  | 'failed'
  | 'cancelled'

export interface ExecutionSnapshot {
  readonly executionId: string
  readonly workflow: string
  readonly version: number
  readonly status: ExecutionStatus
  readonly createdAt: string
  readonly updatedAt: string
  readonly waitingOn?: { readonly type: string; readonly stepId: string }
  readonly failure?: Failure
}

export interface HistoryEvent {
  readonly sequence: number
  readonly at: string
  readonly type: string
  readonly stepId?: string
  readonly details: JsonValue
}

export interface HistoryPage {
  readonly events: readonly HistoryEvent[]
  readonly nextCursor?: number
}
