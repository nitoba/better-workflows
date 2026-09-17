import type {
  WorkflowsOptions,
  QueueOptions,
  ScheduleOccurrence,
  ScheduleOccurrenceSnapshot,
  ScheduleOccurrenceStatus,
  ScheduleMisfirePolicy,
  ScheduleOverlapPolicy,
  ScheduleSnapshot,
  ScheduleStatus,
  JsonValue
} from './types'

/**
 * Connection settings for createWorkflowsAdmin. Schema inspection concerns the shared
 * database; execution retention and queue limits are scoped to namespace.
 */
export interface AdminOptions {
  /**
   * Namespace used by the workflows being administered.
   */
  readonly namespace: string
  /**
   * Same database configuration as the workflow runtime, from sqlite/postgres.
   */
  readonly storage: WorkflowsOptions['storage']
}
/**
 * Read-only inspection of the package-pinned engine and database schema ledgers.
 * Inspection checks known required tables/columns, not arbitrary corruption or every index.
 */
export interface MigrationStatus {
  /**
   * Engine version pinned by this package.
   */
  readonly engine: string
  /**
   * Applied and pending journal migration numbers.
   */
  readonly journal: {
    /** Ordered migration versions already recorded in this ledger. */
    readonly applied: readonly number[]
    /** Known migration versions not yet applied. */
    readonly pending: readonly number[]
  }
  /**
   * Applied and pending engine-cluster migration numbers.
   */
  readonly cluster: {
    /** Ordered migration versions already recorded in this ledger. */
    readonly applied: readonly number[]
    /** Known migration versions not yet applied. */
    readonly pending: readonly number[]
  }
  /**
   * Applied and pending persisted-queue migration numbers.
   */
  readonly queue: {
    /** Ordered migration versions already recorded in this ledger. */
    readonly applied: readonly number[]
    /** Known migration versions not yet applied. */
    readonly pending: readonly number[]
  }
  /**
   * Required table or table.column names not found by inspection.
   */
  readonly missing: readonly string[]
  /**
   * True only when known migration ledgers and required schema elements are present.
   */
  readonly valid: boolean
}
/**
 * Eligibility cutoff and scan size for a read-only retention preview.
 */
export interface RetentionOptions {
  /**
   * UTC ISO timestamp; only terminal executions (including continued generations) last updated strictly before it are scanned.
   * Future or invalid cutoffs are rejected.
   */
  readonly before: string
  /**
   * Maximum number of terminal rows inspected, including blocked rows; 1–1000.
   * @defaultValue 100
   */
  readonly limit?: number
}
/**
 * Terminal execution eligible at preview time; continuation chains are selected as a unit and revalidated during prune.
 */
export interface RetentionCandidate {
  /**
   * Execution selected for deletion if it remains eligible.
   */
  readonly executionId: string
  /**
   * Journal update timestamp in epoch milliseconds used to detect stale plans.
   */
  readonly updatedAt: number
}
/**
 * Read-only preview to inspect before a destructive retention operation.
 * Pass the intact plan to prune with confirm: true; do not edit IDs or timestamps.
 * The token detects accidental changes, not malicious modification or unauthorized access.
 */
export interface RetentionPlan {
  /**
   * Namespace that created the plan; applying it elsewhere is rejected.
   */
  readonly namespace: string
  /**
   * Normalized UTC cutoff used for preview and revalidation.
   */
  readonly before: string
  /**
   * Eligible executions and update timestamps; not permission to skip safety checks.
   */
  readonly candidates: readonly RetentionCandidate[]
  /**
   * Scanned executions excluded because of live claims, messages or active relations.
   */
  readonly blocked: readonly {
    /** Execution excluded from this preview. */
    readonly executionId: string
    /** Blocking condition, such as live-activity-claim or active-parent-or-child. */
    readonly reason: string
  }[]
  /**
   * Integrity checksum of the preview, not a secret or an authorization token.
   */
  readonly token: string
}
/**
 * Summary of one transactional prune; includes only the executions actually deleted.
 */
export interface RetentionResult {
  /**
   * Number of executions deleted in this transaction.
   */
  readonly deleted: number
  /**
   * Identifiers whose journal/engine/queue history was removed.
   */
  readonly executionIds: readonly string[]
  /**
   * Deduplication tombstones retained by this prune, not the database-wide total.
   */
  readonly tombstonesRetained: number
}

/** Counts of active executions in the administered namespace by observable status. */
export interface WorkflowExecutionStats {
  /** Executions persisted but not yet dispatched. */
  readonly accepted: number
  /** Executions whose interpreter is advancing. */
  readonly running: number
  /** Executions with one or more durable commands pending. */
  readonly waiting: number
  /** Executions blocked on an operational activity dependency. */
  readonly blocked: number
  /** Executions with a cooperative pause requested. */
  readonly paused: number
  /** Executions with cancellation being reconciled. */
  readonly cancelling: number
}

/** Namespace-wide activity delivery backlog grouped by logical queue. */
export interface QueueStats {
  /** Logical activity queue name. */
  readonly name: string
  /** Deliveries waiting to become visible or be acquired. */
  readonly pending: number
  /** Deliveries currently owned by a worker. */
  readonly processing: number
  /** Age of the oldest pending delivery, or zero when none are pending. */
  readonly oldestPendingAgeMs: number
}

/** Namespace-wide dead-letter backlog counts. */
export interface DeadLetterStats {
  /** Dead letters awaiting operator action. */
  readonly open: number
  /** Dead letters that have been requeued and await a replacement outcome. */
  readonly requeued: number
  /** Age of the oldest open dead letter, or zero when none are open. */
  readonly oldestOpenAgeMs: number
}

/** Namespace-wide overdue durable deadline counts. */
export interface DeadlineStats {
  /** Timers whose deadline has passed and have not been delivered. */
  readonly dueTimers: number
  /** Activity retries whose deadline has passed and have not been delivered. */
  readonly overdueRetries: number
  /** Greatest overdue age across due timers and retries, or zero when none are due. */
  readonly oldestLagMs: number
}

/** Bounded schedule-definition listing options. */
export interface ScheduleListOptions {
  /** Restrict the result to one persisted schedule state. */
  readonly status?: ScheduleStatus
  /** Return names after this stable schedule name. */
  readonly cursor?: string
  /** Maximum records to return, from 1 through 1000. @defaultValue 1000 */
  readonly limit?: number
}

/** Bounded occurrence-history listing options for one schedule. */
export interface ScheduleOccurrenceListOptions {
  /** Restrict results to one materialization outcome. */
  readonly state?: ScheduleOccurrenceStatus
  /** Exclusive occurrence sequence cursor from the previous page. */
  readonly after?: number
  /** Maximum records to return, from 1 through 1000. @defaultValue 100 */
  readonly limit?: number
}

/** One bounded page of schedule occurrence metadata. */
export interface ScheduleOccurrencePage {
  /** Occurrences in ascending sequence order; inputs are intentionally omitted. */
  readonly occurrences: readonly ScheduleOccurrenceSnapshot[]
  /** Exclusive cursor for the next page, when more records exist. */
  readonly nextCursor?: number
}

/** Bounded preview options for explicitly pruning old schedule occurrence metadata. */
export interface ScheduleRetentionOptions {
  /** UTC cutoff; occurrences created strictly before it may be selected. */
  readonly before: string
  /** Restrict pruning to one schedule name. */
  readonly schedule?: string
  /** Maximum occurrences inspected, from 1 through 1000. @defaultValue 100 */
  readonly limit?: number
}

/** One schedule occurrence selected by a retention preview. */
export interface ScheduleRetentionCandidate {
  /** Schedule identity within the administered namespace. */
  readonly scheduleName: string
  /** Persisted occurrence timestamp in UTC ISO form. */
  readonly scheduledAt: string
  /** Stable occurrence sequence used to detect a changed preview. */
  readonly sequence: number
  /** Materialization outcome retained by the occurrence row. */
  readonly state: ScheduleOccurrenceStatus
  /** Storage creation timestamp used to detect a changed preview. */
  readonly createdAt: number
}

/** Read-only schedule-retention preview requiring explicit confirmation to apply. */
export interface ScheduleRetentionPlan {
  /** Namespace that created the plan. */
  readonly namespace: string
  /** Normalized UTC cutoff used by preview and prune. */
  readonly before: string
  /** Occurrences eligible for deletion after safety checks. */
  readonly candidates: readonly ScheduleRetentionCandidate[]
  /** Integrity checksum of the preview. */
  readonly token: string
}

/** Result of explicitly pruning schedule occurrence metadata. */
export interface ScheduleRetentionResult {
  /** Number of occurrence rows deleted. */
  readonly deleted: number
  /** Identities of rows deleted from occurrence history. */
  readonly occurrences: readonly Pick<ScheduleRetentionCandidate, 'scheduleName' | 'scheduledAt'>[]
}

/** Optional stable identity for an operator-initiated schedule trigger. */
export interface ScheduleTriggerOptions {
  /** Repeating this key returns the original manual trigger instead of creating another one. */
  readonly idempotencyKey?: string
}

/** Result of one manual schedule trigger. */
export interface ScheduleTriggerResult {
  /** Occurrence recorded independently from the normal schedule cursor. */
  readonly occurrence: ScheduleOccurrence
  /** Accepted workflow execution identifier. */
  readonly executionId: string
  /** False when an idempotency key returned an already accepted trigger. */
  readonly created: boolean
}

/** Static schedule definition supplied to an explicit definition reconciliation. */
export interface ScheduleDefinitionUpdate {
  /** Workflow contract identity used by future occurrences. */
  readonly workflow: string
  /** Workflow contract version used by future occurrences. */
  readonly workflowVersion: number
  /** Recurrence kind. */
  readonly type: 'cron' | 'interval'
  /** Cron expression; required for cron definitions. */
  readonly expression?: string
  /** IANA timezone for cron definitions. */
  readonly timezone?: string
  /** Positive interval in milliseconds; required for interval definitions. */
  readonly intervalMs?: number
  /** Misfire policy for the reconciled definition. */
  readonly misfire: ScheduleMisfirePolicy
  /** Overlap policy for the reconciled definition. */
  readonly overlap: ScheduleOverlapPolicy
  /** Maximum catch-up batch. */
  readonly maxCatchUp: number
  /** Static input retained for standalone/manual administration. */
  readonly input?: JsonValue
  /** Use the application resolver instead of persisted static input. */
  readonly inputMode?: 'none' | 'static' | 'resolver'
}

/** Explicit confirmation for changing a persisted schedule timeline. */
export interface ScheduleDefinitionUpdateOptions {
  /** Only `now` is supported in this first reconciliation flow. */
  readonly from: 'now'
  /** Required acknowledgement that the recurring cursor will be reset from now. */
  readonly confirm: true
  /** Replacement definition from the deployment being reconciled. */
  readonly definition: ScheduleDefinitionUpdate
}

/** Explicit confirmation required before removing a persisted schedule definition. */
export interface ScheduleRemoveOptions {
  /** Acknowledges that the definition is removed while its occurrence history remains. */
  readonly confirm: true
}

/** Namespace-wide schedule backlog counts. */
export interface ScheduleStats {
  /** Persisted schedules currently generating occurrences. */
  readonly active: number
  /** Persisted schedules paused by an operator. */
  readonly paused: number
  /** Active schedules whose next occurrence is due. */
  readonly overdue: number
  /** Oldest active schedule lag, or zero when none are overdue. */
  readonly oldestLagMs: number
}

/** Read-only operational snapshot of the durable namespace; payloads are excluded. */
export interface WorkflowsStats {
  /** Database time at which this global snapshot was generated. */
  readonly generatedAt: string
  /** Active execution counts; terminal and continued history is intentionally omitted. */
  readonly executions: WorkflowExecutionStats
  /** Activity delivery backlog grouped by queue. */
  readonly queues: readonly QueueStats[]
  /** Operational dead-letter backlog. */
  readonly deadLetters: DeadLetterStats
  /** Overdue timer and retry backlog. */
  readonly deadlines: DeadlineStats
  /** Aggregate schedule state; definitions are returned by listSchedules. */
  readonly schedules: ScheduleStats
}

/** Operational state of one persisted activity delivery dead letter. */
export type DeadLetterState = 'open' | 'requeued' | 'resolved' | 'discarded'

/** Metadata retained for an operationally blocked activity delivery. */
export interface DeadLetter {
  /** Stable dead-letter identifier. */
  readonly id: string
  /** Namespace that owns the delivery. */
  readonly namespace: string
  /** Logical activity queue. */
  readonly queue: string
  /** Owning workflow execution, when the delivery belongs to one. */
  readonly executionId: string | null
  /** Owning workflow step, when the delivery belongs to one. */
  readonly stepId: string | null
  /** Activity contract name, when the envelope metadata was available. */
  readonly activityName: string | null
  /** Activity contract version, when the envelope metadata was available. */
  readonly activityVersion: number | null
  /** Business retry attempt from the persisted envelope. */
  readonly businessAttempt: number | null
  /** Transport delivery attempt that failed operationally. */
  readonly deliveryAttempt: number
  /** Stable classification of the operational failure. */
  readonly reasonCode: string
  /** Bounded operator-facing explanation of the failure. */
  readonly reasonMessage: string
  /** Time of the first failure as a UTC ISO 8601 string. */
  readonly firstFailedAt: string
  /** Time of the latest administrative state change as a UTC ISO 8601 string. */
  readonly updatedAt: string
  /** Number of operator requeues already issued. */
  readonly requeueCount: number
  /** Current operational lifecycle state. */
  readonly state: DeadLetterState
  /** Returned only when getDeadLetter is called with includePayload: true. */
  readonly payload?: string
}

/** Filters and cursor for dead-letter administration. */
export interface DeadLetterListOptions {
  /** Restrict results to one logical queue. */
  readonly queue?: string
  /** Restrict results to one owning execution. */
  readonly executionId?: string
  /** Restrict results to one activity contract name. */
  readonly activity?: string
  /** Restrict results to one dead-letter state. */
  readonly state?: DeadLetterState
  /** Return records after this stable identifier. */
  readonly cursor?: string
  /** Maximum records to return, from 1 through 1000. */
  readonly limit?: number
}

/** One bounded page of dead-letter metadata. */
export interface DeadLetterPage {
  /** Metadata-only dead-letter records in stable identifier order. */
  readonly deadLetters: readonly DeadLetter[]
  /** Cursor for the next bounded page, when more records exist. */
  readonly nextCursor?: string
}

/** Explicit operator decision used to terminate a dead-letter owner. */
export interface DiscardDeadLetterOptions {
  /** Required operator explanation for terminating the owner. */
  readonly reason: string
}

export interface AdminBackend {
  migrationStatus(): Promise<MigrationStatus>
  migrate(): Promise<MigrationStatus>
  validateMigrations(): Promise<MigrationStatus>
  stats(): Promise<WorkflowsStats>
  listSchedules(options?: ScheduleListOptions): Promise<readonly ScheduleSnapshot[]>
  listScheduleOccurrences(
    name: string,
    options?: ScheduleOccurrenceListOptions
  ): Promise<ScheduleOccurrencePage>
  getSchedule(name: string): Promise<ScheduleSnapshot>
  pauseSchedule(name: string): Promise<ScheduleSnapshot>
  resumeSchedule(name: string): Promise<ScheduleSnapshot>
  triggerSchedule(name: string, options?: ScheduleTriggerOptions): Promise<ScheduleTriggerResult>
  removeSchedule(name: string, options: ScheduleRemoveOptions): Promise<void>
  updateScheduleDefinition(
    name: string,
    options: ScheduleDefinitionUpdateOptions
  ): Promise<ScheduleSnapshot>
  previewRetention(options: RetentionOptions): Promise<RetentionPlan>
  pruneRetention(plan: RetentionPlan, confirm: boolean): Promise<RetentionResult>
  previewScheduleRetention(options: ScheduleRetentionOptions): Promise<ScheduleRetentionPlan>
  pruneScheduleRetention(
    plan: ScheduleRetentionPlan,
    confirm: boolean
  ): Promise<ScheduleRetentionResult>
  setQueueLimits(
    queue: string,
    options: Pick<QueueOptions, 'globalConcurrency' | 'perKeyConcurrency'>
  ): Promise<void>
  listDeadLetters(options?: DeadLetterListOptions): Promise<DeadLetterPage>
  getDeadLetter(id: string, options?: { readonly includePayload?: boolean }): Promise<DeadLetter>
  requeueDeadLetter(id: string): Promise<DeadLetter>
  discardDeadLetter(id: string, options: DiscardDeadLetterOptions): Promise<DeadLetter>
}
