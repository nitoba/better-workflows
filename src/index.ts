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
  WorkflowContract,
  Cron,
  Interval,
  InjectWorkflow,
  defineSignal,
  getWorkflowToken
} from './decorators'
export { WorkflowsModule } from './module'
export { WorkflowsHealth } from './health'
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
  WorkflowContractClass,
  WorkflowImplementationClass,
  WorkflowContext,
  WorkflowHandler,
  WorkflowInput,
  WorkflowOptions,
  CronOptions,
  IntervalOptions,
  ScheduleCommonOptions,
  ScheduleInput,
  ScheduleMisfirePolicy,
  ScheduleOverlapPolicy,
  ScheduleOccurrence,
  ScheduleOccurrenceSnapshot,
  ScheduleOccurrenceStatus,
  ScheduleSnapshot,
  ScheduleStatus,
  WorkflowOutput,
  WorkflowsAsyncOptions,
  WorkflowsOptions,
  OtlpLogLevel,
  OtlpLogsOptions,
  OtlpMetricsOptions,
  OtlpObservabilityOptions,
  OtlpOptions,
  OtlpSignalOptions,
  ObservabilityOptions,
  HealthCheckStatus,
  HealthStatus,
  WorkflowsLiveness,
  WorkflowsReadiness
} from './types'

export { WorkflowsAdmin } from './admin'
export type {
  DeadLetter,
  DeadLetterListOptions,
  DeadLetterPage,
  DeadLetterState,
  DeadlineStats,
  DeadLetterStats,
  DiscardDeadLetterOptions,
  QueueStats,
  ScheduleDefinitionUpdate,
  ScheduleDefinitionUpdateOptions,
  ScheduleListOptions,
  ScheduleOccurrenceListOptions,
  ScheduleOccurrencePage,
  ScheduleRetentionCandidate,
  ScheduleRetentionOptions,
  ScheduleRetentionPlan,
  ScheduleRetentionResult,
  ScheduleRemoveOptions,
  ScheduleStats,
  ScheduleTriggerOptions,
  ScheduleTriggerResult,
  WorkflowExecutionStats,
  WorkflowsStats
} from './admin-types'
