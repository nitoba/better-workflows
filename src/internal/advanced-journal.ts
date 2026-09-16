import { Effect } from 'effect'
import type { Failure } from '../errors'
import type { ChildOptions } from '../types'
import type { Journal, CommandRow } from './journal'
import { encode } from './values'

export interface BranchRow {
  readonly branch_key: string
  readonly ordinal: number
  readonly state: 'pending' | 'running' | 'completed' | 'failed'
  readonly result_json: string | null
  readonly failure_json: string | null
}
export interface ChildRow {
  readonly parent_id: string
  readonly step_id: string
  readonly child_id: string
  readonly close_policy: 'request-cancel' | 'abandon'
  readonly delivered: number
  readonly close_applied: number
}
export interface SagaRow {
  readonly state: 'running' | 'completed' | 'compensating' | 'compensated' | 'compensation-failed'
  readonly result_json: string | null
  readonly failure_json: string | null
}
export interface CompensationRow {
  readonly step_id: string
  readonly ordinal: number
  readonly state: 'registered' | 'completed' | 'failed'
  readonly result_json: string
  readonly failure_json: string | null
}
export interface TimerRow {
  readonly execution_id: string
  readonly step_id: string
  readonly deadline: number
}
const fail = (message: string) =>
  Effect.fail<Failure>({
    code: 'NON_DETERMINISTIC_WORKFLOW',
    message,
    retryable: false
  })

/** SQL state for structured scopes. No callback or closure is serialized. */
export class AdvancedJournal {
  constructor(readonly journal: Journal) {}

  private lock(id: string) {
    return this.journal.sql`UPDATE better_workflows_runs SET event_sequence = event_sequence
      WHERE execution_id = ${id} AND namespace = ${this.journal.namespace}`
  }

  branches(id: string, group: string) {
    return this.journal.sql<BranchRow>`SELECT * FROM better_workflows_branches
      WHERE execution_id = ${id} AND group_id = ${group} ORDER BY ordinal`
  }

  initializeBranches(id: string, group: string, keys: readonly string[]) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        for (const [ordinal, key] of keys.entries()) {
          yield* self.journal
            .sql`INSERT INTO better_workflows_branches(execution_id, group_id, branch_key, ordinal)
          VALUES (${id}, ${group}, ${key}, ${ordinal}) ON CONFLICT DO NOTHING`
        }
        const rows = yield* self.branches(id, group)
        if (
          rows.length !== keys.length ||
          rows.some((row, index) => row.branch_key !== keys[index])
        ) {
          return yield* fail(`Branch keys changed in ${group}`)
        }
      })
    )
  }

  admitBranches(id: string, group: string, concurrency: number) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        const rows = yield* self.branches(id, group)
        const active = rows.filter((row) => row.state === 'running')
        const pending = rows
          .filter((row) => row.state === 'pending')
          .slice(0, concurrency - active.length)
        for (const row of pending) {
          yield* self.journal.sql`UPDATE better_workflows_branches SET state = 'running'
          WHERE execution_id = ${id} AND group_id = ${group} AND branch_key = ${row.branch_key}`
          yield* self.journal.event(id, 'branch.started', { group, key: row.branch_key })
        }
        return [...active, ...pending]
      })
    )
  }

  finishBranch(
    id: string,
    group: string,
    key: string,
    result: string | null,
    failure: Failure | null
  ) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        const rows = yield* self.journal.sql`UPDATE better_workflows_branches
        SET state = ${failure ? 'failed' : 'completed'}, result_json = ${result}, failure_json = ${failure ? encode(failure) : null}
        WHERE execution_id = ${id} AND group_id = ${group} AND branch_key = ${key} AND state = 'running'
        RETURNING branch_key`
        if (rows.length)
          yield* self.journal.event(id, failure ? 'branch.failed' : 'branch.completed', {
            group,
            key
          })
      })
    )
  }

  linkChild(
    parent: string,
    step: string,
    child: string,
    name: string,
    version: number,
    key: string,
    input: string,
    policy: ChildOptions['parentClosePolicy']
  ) {
    const self = this
    const closePolicy = policy ?? 'request-cancel'
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(parent)
        yield* self.journal.accept(child, name, version, key, input)
        const inserted = yield* self.journal
          .sql`INSERT INTO better_workflows_children(parent_id, step_id, child_id, close_policy)
        VALUES (${parent}, ${step}, ${child}, ${closePolicy}) ON CONFLICT DO NOTHING RETURNING child_id`
        const [row] = yield* self.journal.sql<ChildRow>`SELECT * FROM better_workflows_children
        WHERE parent_id = ${parent} AND step_id = ${step}`
        if (!row || row.child_id !== child || row.close_policy !== closePolicy)
          return yield* fail(`Child ${step} changed`)
        if (inserted.length)
          yield* self.journal.event(
            parent,
            'child.started',
            { executionId: child, workflow: name, version, closePolicy },
            step
          )
        return row
      })
    )
  }

  readyChildren() {
    return this.journal.sql<ChildRow>`SELECT c.* FROM better_workflows_children c
      JOIN better_workflows_runs p ON p.execution_id = c.parent_id
      JOIN better_workflows_runs r ON r.execution_id = c.child_id
      WHERE p.namespace = ${this.journal.namespace} AND c.delivered = 0
      AND r.state IN ('continued', 'completed', 'failed', 'cancelled')
      AND p.control <> 'cancel' AND p.state NOT IN ('continued', 'completed', 'failed', 'cancelled') ORDER BY c.parent_id, c.step_id LIMIT 100`
  }

  childDelivered(child: ChildRow) {
    return this.journal.sql`UPDATE better_workflows_children SET delivered = 1
      WHERE parent_id = ${child.parent_id} AND step_id = ${child.step_id}`
  }

  closeChildren() {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const children = yield* self.journal
          .sql<ChildRow>`SELECT c.* FROM better_workflows_children c
        JOIN better_workflows_runs p ON p.execution_id = c.parent_id
        WHERE p.namespace = ${self.journal.namespace} AND c.close_applied = 0
        AND (p.control = 'cancel' OR p.state IN ('continued', 'completed', 'failed', 'cancelled'))
        ORDER BY c.parent_id, c.step_id LIMIT 100`
        for (const child of children) {
          if (child.close_policy === 'request-cancel') {
            const owner = yield* self.journal.followContinuation(child.child_id)
            yield* self.journal.control(
              owner.execution_id,
              'cancel',
              `Parent ${child.parent_id} closed`
            )
          }
          yield* self.journal.sql`UPDATE better_workflows_children SET close_applied = 1
          WHERE parent_id = ${child.parent_id} AND step_id = ${child.step_id}`
        }
      })
    )
  }

  saga(id: string, saga: string) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.journal.sql`INSERT INTO better_workflows_sagas(execution_id, saga_id)
        VALUES (${id}, ${saga}) ON CONFLICT DO NOTHING`
        const [row] = yield* self.journal.sql<SagaRow>`SELECT * FROM better_workflows_sagas
        WHERE execution_id = ${id} AND saga_id = ${saga}`
        return row!
      })
    )
  }

  sagaState(
    id: string,
    saga: string,
    state: SagaRow['state'],
    result: string | null,
    failure: Failure | null
  ) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        const rows = yield* self.journal.sql`UPDATE better_workflows_sagas
        SET state = ${state}, result_json = ${result}, failure_json = ${failure ? encode(failure) : null}
        WHERE execution_id = ${id} AND saga_id = ${saga} AND state <> ${state} RETURNING saga_id`
        if (rows.length)
          yield* self.journal.event(
            id,
            `saga.${state}`,
            failure ? { code: failure.code } : null,
            saga
          )
      })
    )
  }

  compensations(id: string, saga: string) {
    return this.journal.sql<CompensationRow>`SELECT * FROM better_workflows_compensations
      WHERE execution_id = ${id} AND saga_id = ${saga} ORDER BY ordinal DESC`
  }

  registerCompensation(id: string, saga: string, step: string, ordinal: number, result: string) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        const rows = yield* self.journal
          .sql`INSERT INTO better_workflows_compensations(execution_id, saga_id, step_id, ordinal, result_json)
        VALUES (${id}, ${saga}, ${step}, ${ordinal}, ${result}) ON CONFLICT DO NOTHING RETURNING step_id`
        const [row] = yield* self.journal
          .sql<CompensationRow>`SELECT * FROM better_workflows_compensations
        WHERE execution_id = ${id} AND saga_id = ${saga} AND step_id = ${step}`
        if (!row || row.ordinal !== ordinal || row.result_json !== result)
          return yield* fail(`Compensation ${step} changed`)
        if (rows.length)
          yield* self.journal.event(id, 'compensation.registered', { saga, ordinal }, step)
      })
    )
  }

  finishCompensation(id: string, saga: string, step: string, failure: Failure | null) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lock(id)
        const rows = yield* self.journal.sql`UPDATE better_workflows_compensations
        SET state = ${failure ? 'failed' : 'completed'}, failure_json = ${failure ? encode(failure) : null}
        WHERE execution_id = ${id} AND saga_id = ${saga} AND step_id = ${step} AND state = 'registered' RETURNING step_id`
        if (rows.length)
          yield* self.journal.event(
            id,
            failure ? 'compensation.failed' : 'compensation.completed',
            { saga },
            step
          )
      })
    )
  }

  timer(id: string, step: string, duration: number) {
    const self = this
    return self.journal.sql.withTransaction(
      Effect.gen(function* () {
        const [command] = yield* self.journal
          .sql<CommandRow>`SELECT * FROM better_workflows_commands WHERE execution_id = ${id} AND step_id = ${step}`
        if (command?.protocol === 1) return 'legacy' as const
        const now = yield* self.journal.now()
        yield* self.journal.sql`INSERT INTO better_workflows_timers(execution_id, step_id, deadline)
        VALUES (${id}, ${step}, ${now + duration}) ON CONFLICT DO NOTHING`
        return 'journal' as const
      })
    )
  }

  dueTimers() {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.journal.now()
      return yield* self.journal.sql<TimerRow>`SELECT t.* FROM better_workflows_timers t
        JOIN better_workflows_runs r ON r.execution_id = t.execution_id
        WHERE r.namespace = ${self.journal.namespace} AND t.delivered = 0 AND t.deadline <= ${now}
        AND r.control <> 'cancel' AND r.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
        ORDER BY t.deadline, t.execution_id, t.step_id LIMIT 100`
    })
  }

  timerDelivered(timer: TimerRow) {
    return this.journal.sql`UPDATE better_workflows_timers SET delivered = 1
      WHERE execution_id = ${timer.execution_id} AND step_id = ${timer.step_id}`
  }
}
