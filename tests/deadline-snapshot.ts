import { Database } from 'bun:sqlite'

export interface DeadlineSnapshot {
  readonly kind: string
  readonly step_id: string
  readonly attempt: number
  readonly deadline: number
  readonly delivered: number
}

/** Read persisted business deadlines, not the host-dependent time to bootstrap a process. */
export function readDeadlines(filename: string, executionId: string): DeadlineSnapshot[] {
  const db = new Database(filename, { readonly: true })
  try {
    return db
      .query<DeadlineSnapshot, [string, string]>(
        `SELECT 'retry' AS kind, step_id, attempt, deadline, delivered
         FROM better_workflows_retries WHERE execution_id = ?
         UNION ALL
         SELECT 'timer' AS kind, step_id, 0 AS attempt, deadline, delivered
         FROM better_workflows_timers WHERE execution_id = ?
         ORDER BY kind, step_id, attempt`
      )
      .all(executionId, executionId)
  } finally {
    db.close()
  }
}

export async function expireDeadlines(snapshots: readonly DeadlineSnapshot[]): Promise<void> {
  const due = Math.max(...snapshots.map((snapshot) => snapshot.deadline))
  const delay = Math.max(0, due - Date.now()) + 50
  await new Promise((resolve) => setTimeout(resolve, delay))
}
