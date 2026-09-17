import { randomUUID } from 'node:crypto'
import { Cause, Effect, Exit } from 'effect'
import type { RegisteredSchedule } from './schedule'
import { ScheduleStore } from './schedule-store'
import {
  logAnnotations,
  TelemetryAttributeKey,
  TelemetryLogComponent,
  TelemetrySpanName
} from './telemetry'
import { toFailure } from '../errors'

/** Result of one bounded scheduler polling pass. */
export interface SchedulerPassResult {
  readonly schedules: number
  readonly occurrences: number
}

/**
 * Distributed schedule dispatcher.
 *
 * It deliberately has no per-schedule timer. Each pass asks storage for a bounded
 * due batch; ScheduleStore performs the fenced claim and transactional materialization.
 */
export class Scheduler {
  private readonly owner = randomUUID()

  constructor(
    readonly store: ScheduleStore,
    readonly schedules: ReadonlyMap<string, RegisteredSchedule>,
    readonly leaseMs: number,
    readonly batchSize = 100,
    /** Polling lateness that still counts as the current deadline for misfire=skip. */
    readonly misfireGraceMs = 0,
    readonly refreshMs = Math.max(1, Math.floor(leaseMs / 3))
  ) {}

  pass() {
    const self = this
    const operation = Effect.gen(function* () {
      const due = yield* self.store.due(self.schedules.keys(), self.batchSize)
      let materializedSchedules = 0
      let occurrences = 0
      let firstFailure: ReturnType<typeof toFailure> | undefined
      for (const row of due) {
        const schedule = self.schedules.get(row.schedule_name)
        // Orphaned rows are excluded by the due query. Keep this guard so a malformed
        // registry cannot accidentally execute a definition it does not own.
        if (!schedule) continue
        const attempt = yield* Effect.exit(
          self.store.materialize(
            schedule,
            self.owner,
            self.leaseMs,
            self.misfireGraceMs,
            self.refreshMs
          )
        )
        if (Exit.isFailure(attempt)) {
          if (Cause.hasInterruptsOnly(attempt.cause)) return yield* Effect.failCause(attempt.cause)
          firstFailure ??= toFailure(Cause.squash(attempt.cause))
          continue
        }
        const result = attempt.value
        if (result.processed > 0) {
          materializedSchedules++
          occurrences += result.processed
        }
      }
      const result: SchedulerPassResult = {
        schedules: materializedSchedules,
        occurrences
      }
      if (firstFailure) return yield* Effect.fail(firstFailure)
      return result
    })
    return Effect.useSpan(
      TelemetrySpanName.scheduleTick,
      {
        attributes: { [TelemetryAttributeKey.namespace]: self.store.journal.namespace },
        kind: 'consumer'
      },
      (tickSpan) => Effect.withParentSpan(operation, tickSpan)
    ).pipe(
      Effect.tapError((error) =>
        Effect.sync(() => {
          const failure = toFailure(error)
          self.store.journal.telemetry?.count('scheduleFailure')
          return failure
        }).pipe(
          Effect.andThen(
            toFailure(error).code === 'SCHEDULE_LEASE_LOST'
              ? Effect.annotateLogs(
                  Effect.logWarning('Scheduler lease lost'),
                  logAnnotations(TelemetryLogComponent.scheduler, {
                    [TelemetryAttributeKey.namespace]: self.store.journal.namespace,
                    [TelemetryAttributeKey.failureCode]: toFailure(error).code
                  })
                )
              : Effect.annotateLogs(
                  Effect.logError('Persistent scheduler failure'),
                  logAnnotations(TelemetryLogComponent.scheduler, {
                    [TelemetryAttributeKey.namespace]: self.store.journal.namespace,
                    [TelemetryAttributeKey.failureCode]: toFailure(error).code
                  })
                )
          )
        )
      )
    )
  }
}
