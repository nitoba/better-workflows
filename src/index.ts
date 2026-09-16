/**
 * NestJS-native durable workflows, queued activities, signals and structured orchestration.
 * Register infrastructure once with WorkflowsModule and compose domain-owned features.
 * Database adapters, standalone administration and test utilities have dedicated
 * better-workflows/sqlite, /postgres, /admin and /testing entry points.
 * @packageDocumentation
 */
export { defineQueue } from './queues'
export type { QueueReference } from './queues'
export {
  Activities,
  Activity,
  Workflow,
  InjectWorkflow,
  defineSignal,
  getWorkflowToken
} from './decorators'
export { WorkflowsModule } from './module'
export { WorkflowClient, WorkflowHandle } from './client'
export { ActivityError, WorkflowError, WorkflowExecutionError } from './errors'
export type { Failure } from './errors'
export type {
  ActivityClient,
  ActivityContext,
  ActivityOptions,
  Duration,
  ExecutionSnapshot,
  ExecutionStatus,
  HistoryEvent,
  HistoryPage,
  JsonValue,
  RetryOptions,
  SignalDefinition,
  SignalWaitOptions,
  ResultWaitOptions,
  StartOptions,
  HistoryOptions,
  CancelOptions,
  SignalDeliveryOptions,
  StepOptions,
  MapOptions,
  ChildOptions,
  ChildExecution,
  SagaContext,
  ParallelTasks,
  ParallelResults,
  QueueOptions,
  QueueRegistration,
  QueueSettings,
  ActivityDefaults,
  WorkflowDefaults,
  ExecutionOptions,
  HandlerRegistration,
  FeatureConfiguration,
  FeatureStructure,
  FeatureExports,
  WorkflowsFeatureOptions,
  WorkflowsFeatureAsyncOptions,
  WorkflowClass,
  WorkflowContext,
  WorkflowHandler,
  WorkflowInput,
  WorkflowOptions,
  WorkflowOutput,
  WorkflowsAsyncOptions,
  WorkflowsOptions
} from './types'

export { WorkflowsAdmin } from './admin'
