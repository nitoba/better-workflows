import type { WorkflowsOptions, QueueOptions } from './types'

export interface AdminOptions {
  readonly namespace: string
  readonly storage: WorkflowsOptions['storage']
}
export interface MigrationStatus {
  readonly engine: string
  readonly journal: { readonly applied: readonly number[]; readonly pending: readonly number[] }
  readonly cluster: { readonly applied: readonly number[]; readonly pending: readonly number[] }
  readonly queue: { readonly applied: readonly number[]; readonly pending: readonly number[] }
  readonly missing: readonly string[]
  readonly valid: boolean
}
export interface RetentionOptions {
  /** Terminal runs last updated strictly before this UTC instant. */
  readonly before: string
  readonly limit?: number
}
export interface RetentionCandidate {
  readonly executionId: string
  readonly updatedAt: number
}
export interface RetentionPlan {
  readonly namespace: string
  readonly before: string
  readonly candidates: readonly RetentionCandidate[]
  readonly blocked: readonly { readonly executionId: string; readonly reason: string }[]
  /** Integrity check for an exported preview, not an authentication token. */
  readonly token: string
}
export interface RetentionResult {
  readonly deleted: number
  readonly executionIds: readonly string[]
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
