import { createHash } from 'node:crypto'
import { Predicate } from 'effect'
import { WorkflowError } from '../errors'
import type { PostgresStorage, SqliteStorage } from '../types'
import type { TelemetryApi } from './telemetry'

export const RESULT_WAIT_FALLBACK_INTERVAL = 5_000

type Storage = PostgresStorage | SqliteStorage
type LocalSubscriber = (executionId: string, revision: number) => void

const localSubscribers = new Map<string, Set<LocalSubscriber>>()

/** Publish a best-effort in-process wake-up. The database remains authoritative. */
export function publishLocalExecutionChange(
  key: string,
  executionId: string,
  revision: number
): void {
  for (const subscriber of localSubscribers.get(key) ?? []) subscriber(executionId, revision)
}

/** Keep local notifier identity independent from database credentials and long filenames. */
export function executionNotifierKey(storage: Storage, namespace: string): string {
  const identity = storage.driver === 'postgres' ? storage.connectionString : storage.filename
  return createHash('sha256')
    .update(JSON.stringify([storage.driver, identity, namespace]))
    .digest('hex')
}

/** PostgreSQL channel names are limited to NAMEDATALEN - 1 bytes. */
export function executionNotificationChannel(namespace: string): string {
  return `better_workflows_${createHash('sha256').update(namespace).digest('hex').slice(0, 42)}`
}

interface WaitOptions {
  readonly signal?: AbortSignal | undefined
  readonly timeout?: number | undefined
}

interface Waiter {
  readonly executionId: string
  readonly afterRevision: number
  readonly signal: AbortSignal | undefined
  readonly resolve: () => void
  readonly reject: (error: Error) => void
  readonly startedAt: number
  timer?: ReturnType<typeof setTimeout>
  abort?: () => void
  settled: boolean
}

/**
 * Shared local wait registry used by result() and by the PostgreSQL listener.
 * Notifications are hints only: every wake-up is followed by a revision read.
 */
export class ExecutionNotifier {
  private readonly waiters = new Map<string, Set<Waiter>>()
  private readonly unsubscribe: () => void
  private closed = false

  constructor(
    readonly key: string,
    private readonly readRevision: (executionId: string) => Promise<number>,
    private readonly fallbackInterval = RESULT_WAIT_FALLBACK_INTERVAL,
    private readonly telemetry?: TelemetryApi
  ) {
    let subscribers = localSubscribers.get(key)
    if (!subscribers) {
      subscribers = new Set()
      localSubscribers.set(key, subscribers)
    }
    const subscriber: LocalSubscriber = (executionId, revision) =>
      this.publish(executionId, revision)
    subscribers.add(subscriber)
    this.unsubscribe = () => {
      subscribers!.delete(subscriber)
      if (subscribers!.size === 0) localSubscribers.delete(key)
    }
  }

  /** Wake waiters that observed an older revision. */
  publish(executionId: string, revision: number): void {
    if (this.closed) return
    for (const waiter of this.waiters.get(executionId) ?? [])
      if (revision > waiter.afterRevision) this.resolve(waiter)
  }

  /** Recheck all executions after listener recovery or a connection transition. */
  reconnected(): void {
    if (this.closed) return
    for (const waiters of this.waiters.values()) for (const waiter of waiters) this.resolve(waiter)
  }

  /**
   * Register before reading again, closing the read/register race. A fallback
   * wake-up bounds notification loss without becoming a polling loop.
   */
  wait(executionId: string, afterRevision: number, options: WaitOptions = {}): Promise<void> {
    if (this.closed)
      return Promise.reject(
        new WorkflowError('RUNTIME_SHUTDOWN', 'The workflow runtime is shutting down')
      )
    if (options.signal?.aborted)
      return Promise.reject(
        new WorkflowError('WAIT_ABORTED', 'Result wait was aborted; workflow continues')
      )

    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = {
        executionId,
        afterRevision,
        signal: options.signal,
        resolve,
        reject,
        startedAt: Date.now(),
        settled: false
      }
      let executionWaiters = this.waiters.get(executionId)
      if (!executionWaiters) {
        executionWaiters = new Set()
        this.waiters.set(executionId, executionWaiters)
      }
      executionWaiters.add(waiter)
      this.updateGauge()

      waiter.abort = () =>
        this.reject(
          waiter,
          new WorkflowError('WAIT_ABORTED', 'Result wait was aborted; workflow continues')
        )
      options.signal?.addEventListener('abort', waiter.abort, { once: true })

      const timeout = options.timeout ?? Infinity
      if (timeout <= 0) {
        this.reject(
          waiter,
          new WorkflowError('WAIT_TIMEOUT', 'Result wait timed out; workflow continues')
        )
        return
      }
      waiter.timer = setTimeout(
        () =>
          timeout <= this.fallbackInterval
            ? this.reject(
                waiter,
                new WorkflowError('WAIT_TIMEOUT', 'Result wait timed out; workflow continues')
              )
            : this.fallback(waiter),
        Math.min(timeout, this.fallbackInterval)
      )

      void this.readRevision(executionId).then(
        (revision) => {
          if (revision > afterRevision) this.resolve(waiter)
        },
        (error: Error) => this.reject(waiter, error)
      )
    })
  }

  /** Reject and remove every waiter when the owning runtime closes. */
  shutdown(): void {
    if (this.closed) return
    this.closed = true
    this.unsubscribe()
    const error = new WorkflowError('RUNTIME_SHUTDOWN', 'The workflow runtime has shut down')
    for (const waiters of this.waiters.values())
      for (const waiter of waiters) this.reject(waiter, error)
  }

  /** Exposed only for focused lifecycle tests and diagnostics inside the package. */
  get waiterCount(): number {
    let count = 0
    for (const waiters of this.waiters.values()) count += waiters.size
    return count
  }

  private resolve(waiter: Waiter): void {
    if (waiter.settled) return
    this.finish(waiter)
    waiter.resolve()
  }

  private reject(waiter: Waiter, error: Error): void {
    if (waiter.settled) return
    this.finish(waiter)
    waiter.reject(error)
  }

  private fallback(waiter: Waiter): void {
    if (waiter.settled) return
    this.telemetry?.count('resultFallbackPoll')
    this.resolve(waiter)
  }

  private finish(waiter: Waiter): void {
    waiter.settled = true
    if (waiter.timer !== undefined) clearTimeout(waiter.timer)
    if (waiter.abort) waiter.signal?.removeEventListener('abort', waiter.abort)
    const waiters = this.waiters.get(waiter.executionId)
    waiters?.delete(waiter)
    if (waiters?.size === 0) this.waiters.delete(waiter.executionId)
    this.telemetry?.observe('resultWaitDuration', Math.max(0, Date.now() - waiter.startedAt))
    this.updateGauge()
  }

  private updateGauge(): void {
    this.telemetry?.setGauge('notifierWaiters', this.waiterCount)
  }
}

/** Parse untrusted LISTEN payloads without allowing malformed notifications to affect waits. */
export function executionNotificationPayload(
  payload: string
): { readonly executionId: string; readonly revision: number } | undefined {
  try {
    const value: unknown = JSON.parse(payload)
    if (
      !Predicate.isObject(value) ||
      !Predicate.isString(value.executionId) ||
      !Predicate.isNumber(value.revision) ||
      !Number.isSafeInteger(value.revision) ||
      value.revision < 0
    )
      return undefined
    return { executionId: value.executionId, revision: value.revision }
  } catch {
    return undefined
  }
}
