import type { SqliteStorage } from './types'
import { WorkflowError } from './errors'

/**
 * Options for sqlite; importing the adapter does not open a connection.
 */
export interface SqliteOptions {
  /**
   * Local database filename. Use :memory: only for disposable tests; no restart recovery remains.
   */
  readonly filename: string
  /**
   * Choose the native Bun/Node driver or detect Bun automatically.
   * @defaultValue "auto"
   */
  readonly runtime?: 'auto' | 'bun' | 'node'
}

/**
 * Describe SQLite storage for a single-node runtime or standalone administrator.
 * The file is opened by the consumer at bootstrap/connection time. Distributed
 * multi-host execution requires PostgreSQL; SQLite must not be shared over a network.
 * @param options - Required filename and optional driver implementation selection.
 * @returns Frozen SQLite storage description; no connection is opened here.
 * @throws WorkflowError with INVALID_CONFIGURATION when filename is empty.
 * @example
 * ```ts
 * import { sqlite } from 'better-workflows/sqlite'
 * const persistent = sqlite({ filename: './data/workflows.sqlite' })
 * const disposable = sqlite({ filename: ':memory:', runtime: 'bun' })
 * ```
 */
export function sqlite(options: SqliteOptions): SqliteStorage {
  if (!options.filename)
    throw new WorkflowError('INVALID_CONFIGURATION', 'SQLite filename is required')
  return Object.freeze({
    driver: 'sqlite',
    filename: options.filename,
    runtime: options.runtime ?? 'auto'
  })
}
