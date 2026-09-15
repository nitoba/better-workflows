import { Inject, Injectable } from '@nestjs/common'
import { queueName } from './queues'
import type { QueueReference } from './queues'
import { Cause, Effect, Exit, Layer, ManagedRuntime, Crypto } from 'effect'
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { SqlClient } from 'effect/unstable/sql'
import { makeDatabase } from './internal/infrastructure'
import { Journal } from './internal/journal'
import { SqlAdministration } from './internal/administration'
import { migrateAll, migrationStatus, validateMigrations } from './internal/schema-admin'
import { identifier } from './internal/values'
import { WorkflowError, toFailure } from './errors'
import type { AdminBackend, AdminOptions, RetentionPlan, RetentionOptions } from './admin-types'

export const WORKFLOWS_ADMIN_BACKEND = Symbol.for('better-workflows/admin/backend')

@Injectable()
export class WorkflowsAdmin {
  constructor(@Inject(WORKFLOWS_ADMIN_BACKEND) private readonly backend: AdminBackend) {}
  readonly migrations = {
    status: () => this.backend.migrationStatus(),
    run: () => this.backend.migrate(),
    validate: () => this.backend.validateMigrations()
  }
  readonly retention = {
    preview: (options: RetentionOptions) => this.backend.previewRetention(options),
    prune: (plan: RetentionPlan, options: { readonly confirm: true }) =>
      this.backend.pruneRetention(plan, options?.confirm === true)
  }
  readonly queues = {
    setLimits: (queue: QueueReference, options: Parameters<AdminBackend['setQueueLimits']>[1]) =>
      this.backend.setQueueLimits(queueName(queue), options)
  }
}
export class StandaloneWorkflowsAdmin extends WorkflowsAdmin {
  constructor(
    backend: AdminBackend,
    readonly close: () => Promise<void>
  ) {
    super(backend)
  }
}

/** Standalone administrative connection; no workflow runner or activity worker is started. */
export async function createWorkflowsAdmin(
  options: AdminOptions
): Promise<StandaloneWorkflowsAdmin> {
  identifier(options.namespace, 'Namespace')
  const runtime = ManagedRuntime.make(
    Layer.mergeAll(await makeDatabase(options.storage), NodeCrypto.layer)
  )
  try {
    const sql = await runtime.runPromise(SqlClient.SqlClient)
    const journal = new Journal(sql, options.namespace)
    const admin = new SqlAdministration(journal)
    const run = async <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient | Crypto.Crypto>) => {
      const exit = await runtime.runPromiseExit(effect)
      if (Exit.isFailure(exit)) {
        const error = toFailure(Cause.squash(exit.cause))
        throw new WorkflowError(error.code, error.message)
      }
      return exit.value
    }
    const backend: AdminBackend = {
      migrationStatus: () => run(migrationStatus(sql)),
      migrate: () => run(migrateAll(options.namespace)),
      validateMigrations: () => run(validateMigrations(sql)),
      previewRetention: async (settings) => {
        await run(validateMigrations(sql))
        return run(admin.preview(settings))
      },
      pruneRetention: async (plan, confirm) => {
        await run(validateMigrations(sql))
        return run(admin.prune(plan, confirm))
      },
      setQueueLimits: async (queue, settings) => {
        await run(validateMigrations(sql))
        await run(admin.setQueueLimits(queue, settings))
      }
    }
    return new StandaloneWorkflowsAdmin(backend, () => runtime.dispose())
  } catch (error) {
    await runtime.dispose()
    throw error
  }
}

export type {
  AdminOptions,
  MigrationStatus,
  RetentionOptions,
  RetentionPlan,
  RetentionResult
} from './admin-types'
