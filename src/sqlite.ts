import type { SqliteStorage } from './types'
import { WorkflowError } from './errors'

export interface SqliteOptions {
  readonly filename: string
  readonly runtime?: 'auto' | 'bun' | 'node'
}

export function sqlite(options: SqliteOptions): SqliteStorage {
  if (!options.filename)
    throw new WorkflowError('INVALID_CONFIGURATION', 'SQLite filename is required')
  return Object.freeze({
    driver: 'sqlite',
    filename: options.filename,
    runtime: options.runtime ?? 'auto'
  })
}
