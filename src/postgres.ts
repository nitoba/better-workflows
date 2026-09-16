import type { PostgresStorage } from './types'
import { positiveInteger } from './internal/values'

/**
 * PostgreSQL connection settings used by postgres; no connection opens during configuration.
 */
export interface PostgresOptions {
  /**
   * PostgreSQL URL including database/authentication; read it from protected configuration.
   */
  readonly connectionString: string
  /**
   * Positive maximum pool size for this runtime/administrative connection.
   * @defaultValue 10
   */
  readonly maxConnections?: number
}

/**
 * Describe PostgreSQL storage without opening a pool.
 * Distributed workflow runners also require topology and advertised socket addresses
 * in WorkflowsOptions; changing the storage adapter alone does not create a cluster.
 * @param options - Connection URL and optional positive pool-size limit.
 * @returns Frozen PostgreSQL storage description.
 * @throws WorkflowError with INVALID_CONFIGURATION for an invalid pool limit.
 * @example
 * ```ts
 * import { postgres } from 'better-workflows/postgres'
 * const connectionString = process.env.WORKFLOWS_DATABASE_URL
 * if (!connectionString) throw new Error('WORKFLOWS_DATABASE_URL is required')
 * const storage = postgres({ connectionString, maxConnections: 10 })
 * ```
 */
export function postgres(options: PostgresOptions): PostgresStorage {
  positiveInteger(options.maxConnections ?? 10, 'maxConnections')
  return Object.freeze({
    driver: 'postgres',
    connectionString: options.connectionString,
    maxConnections: options.maxConnections ?? 10
  })
}
