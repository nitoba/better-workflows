import type { WorkflowsOptions, QueueOptions } from './types'

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
export interface AdminBackend {
  migrationStatus(): Promise<MigrationStatus>
  migrate(): Promise<MigrationStatus>
  validateMigrations(): Promise<MigrationStatus>
  previewRetention(options: RetentionOptions): Promise<RetentionPlan>
  pruneRetention(plan: RetentionPlan, confirm: boolean): Promise<RetentionResult>
  setQueueLimits(
    queue: string,
    options: Pick<QueueOptions, 'globalConcurrency' | 'perKeyConcurrency'>
  ): Promise<void>
}
