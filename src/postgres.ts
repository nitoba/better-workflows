import type { PostgresStorage } from './types'
import { positiveInteger } from './internal/values'

export interface PostgresOptions {
  readonly connectionString: string
  readonly maxConnections?: number
}

export function postgres(options: PostgresOptions): PostgresStorage {
  positiveInteger(options.maxConnections ?? 10, 'maxConnections')
  return Object.freeze({
    driver: 'postgres',
    connectionString: options.connectionString,
    maxConnections: options.maxConnections ?? 10
  })
}
