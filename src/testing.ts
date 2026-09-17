import { Inject, Injectable, Module } from '@nestjs/common'
import type { DynamicModule } from '@nestjs/common'
import { randomUUID } from 'node:crypto'
import { WorkflowsModule } from './module'
import { WorkflowError } from './errors'
import type { Duration, WorkflowsOptions } from './types'
import { milliseconds } from './internal/values'
import { WORKFLOWS_TEST_CLOCK } from './internal/clock'
import type { BusinessClock } from './internal/clock'
import { WorkflowsRuntime } from './internal/runtime'

/**
 * Settings for a real workflow runtime with isolated manual business time.
 * Root defaults, overrides and features use the same production resolver. The default
 * storage is a fresh in-memory SQLite database; topology is always single-node.
 */
export interface TestingOptions extends Omit<
  WorkflowsOptions,
  'namespace' | 'storage' | 'queues' | 'topology' | 'cluster'
> {
  /**
   * Manual business clock selection; omitting it also uses manual time.
   * No automatic wall-clock or fake-timer mode is provided.
   * @defaultValue "manual"
   */
  readonly clock?: 'manual'
  /**
   * Nonnegative safe integer epoch milliseconds at which business time starts.
   * @defaultValue Date.now() when the testing module is created
   */
  readonly initialTime?: number
  /**
   * Optional namespace for repeatable/file-backed tests.
   * @defaultValue A unique test-prefixed UUID namespace
   */
  readonly namespace?: string
  /**
   * Optional real adapter. Use a file-backed SQLite database only when testing restart recovery.
   * @defaultValue A fresh in-memory SQLite database
   */
  readonly storage?: WorkflowsOptions['storage']
  /**
   * Optional root-owned queues; ordinary domain features may own all queues instead.
   */
  readonly queues?: WorkflowsOptions['queues']
}
interface TestingBackend {
  flush(): Promise<void>
  testingSnapshot(): Promise<{ revision: string; nextDeadline: number | null }>
}

/**
 * Test-only business clock used by durable waits, retry deadlines and activity timeouts.
 * Does not replace Date.now, arbitrary user timers, SQL leases or cluster polling.
 * Prefer WorkflowsTestHarness.advanceTime so persisted and in-memory deadlines are
 * visited together; direct clock changes do not flush engine transitions.
 */
export class WorkflowsTestClock {
  private time: number
  private sequence = 0
  private readonly sleepers = new Map<number, { at: number; resume: () => void }>()
  /**
   * Create an isolated manual clock.
   * @param initialTime - Nonnegative safe integer epoch milliseconds; defaults to Date.now().
   * @throws WorkflowError with INVALID_CLOCK for invalid input.
   */
  constructor(initialTime = Date.now()) {
    if (!Number.isSafeInteger(initialTime) || initialTime < 0)
      throw new WorkflowError(
        'INVALID_CLOCK',
        'initialTime must be a nonnegative epoch millisecond value'
      )
    this.time = initialTime
  }
  /**
   * Read current virtual business time.
   * @returns Epoch milliseconds; reading does not advance time.
   */
  now(): number {
    return this.time
  }
  /**
   * Schedule an in-memory callback on this manual clock, not a durable workflow timer.
   * Intended for test-clock integration. Use ctx.sleep for persisted business waits.
   * @param delay - Nonnegative delay in milliseconds supplied by the clock adapter.
   * @param resume - Callback run when advanceTo reaches the deadline.
   * @returns Cancellation function removing this scheduled callback.
   */
  schedule(delay: number, resume: () => void): () => void {
    const id = this.sequence++
    this.sleepers.set(id, { at: this.time + delay, resume })
    return () => {
      this.sleepers.delete(id)
    }
  }
  /**
   * Inspect the earliest scheduled in-memory callback.
   * @returns Epoch deadline or null if none; does not include SQL-only durable deadlines.
   */
  nextDeadline(): number | null {
    const values = [...this.sleepers.values()].map((timer) => timer.at)
    return values.length ? Math.min(...values) : null
  }
  /**
   * Set manual time and invoke due callbacks in deadline/insertion order.
   * Does not visit intermediate persisted deadlines or flush the engine; application
   * tests should normally use WorkflowsTestHarness.advanceTime instead.
   * @param epochMilliseconds - Safe integer target no earlier than current clock time.
   * @returns Nothing; invokes due callbacks synchronously.
   * @throws WorkflowError with INVALID_CLOCK for backwards/overflowing time; callback exceptions propagate.
   */
  advanceTo(epochMilliseconds: number): void {
    if (!Number.isSafeInteger(epochMilliseconds) || epochMilliseconds < this.time)
      throw new WorkflowError('INVALID_CLOCK', 'Virtual time cannot move backwards or overflow')
    this.time = epochMilliseconds
    const due = [...this.sleepers.entries()]
      .filter(([, timer]) => timer.at <= this.time)
      .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])
    for (const [id, timer] of due) {
      if (this.sleepers.delete(id)) timer.resume()
    }
  }
}

/**
 * Nest-injected harness for deterministic business-time tests using the real engine.
 * Import from better-workflows/testing. Network calls and arbitrary user timers remain
 * real; replace application integrations explicitly when testing without external I/O.
 */
@Injectable()
export class WorkflowsTestHarness {
  /**
   * Constructed by WorkflowsTestingModule through Nest injection.
   * @param runtime - Test-facing runtime operations.
   * @param clock - Module-owned manual business clock.
   * @internal
   */
  constructor(
    @Inject(WorkflowsRuntime) private readonly runtime: TestingBackend,
    /**
     * Manual business clock used by this harness. Read now(); prefer advanceTime for advancing it.
     */
    @Inject(WORKFLOWS_TEST_CLOCK) readonly clock: WorkflowsTestClock
  ) {}

  /**
   * Flush outboxes and wait for a stable observed journal/clock snapshot.
   * This is a bounded heuristic for engine transitions, not proof that arbitrary
   * external I/O has completed. Use waitFor with a domain predicate when needed.
   * It neither advances business time nor requires all workflows to be terminal.
   * @param options - Real-time safety timeout, default 5s; not a virtual deadline.
   * @returns Resolves after repeated snapshots stabilize.
   * @throws WorkflowError with TEST_NOT_IDLE on timeout, or backend errors.
   */
  async runUntilIdle(
    options: {
      /** Real-time safety timeout for polling; not affected by advanceTime. @defaultValue "5s" */
      readonly timeout?: Duration
    } = {}
  ): Promise<void> {
    const deadline = Date.now() + milliseconds(options.timeout ?? '5s')
    let previous = ''
    let stable = 0
    while (Date.now() <= deadline) {
      await this.runtime.flush()
      const snapshot = await this.runtime.testingSnapshot()
      const signature = `${snapshot.revision}/${snapshot.nextDeadline}/${this.clock.nextDeadline()}`
      stable = signature === previous ? stable + 1 : 0
      if (stable >= 5) return
      previous = signature
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    throw new WorkflowError(
      'TEST_NOT_IDLE',
      'The workflow system did not become idle before the real-time safety deadline'
    )
  }

  /**
   * Flush one or more scheduler, workflow and activity transitions to quiescence.
   * Schedule deadlines use the same virtual business clock as durable timers, so no
   * real-time wait is required.
   * @returns Resolves after the runtime and schedule dispatcher are observed stable.
   */
  async flush(): Promise<void> {
    await this.runUntilIdle()
  }

  /**
   * Advance business time while visiting pending deadlines in chronological order.
   * Flushes before/after each instant so retries and follow-up timers do not restart
   * at the final target. Activity timeout clocks are included; network time is not.
   * @param duration - Nonnegative delta from current manual time, in milliseconds or suffix form.
   * @returns Resolves when the target instant and its observed transitions have been flushed.
   * @throws WorkflowError for invalid time, TEST_NOT_IDLE or TEST_CLOCK_LIVELOCK
   * when more than 100000 deadline iterations occur in one advance.
   * @example
   * ```ts
   * import type { WorkflowsTestHarness } from 'better-workflows/testing'
   * declare const harness: WorkflowsTestHarness
   * await harness.runUntilIdle()
   * await harness.advanceTime('7d')
   * ```
   */
  async advanceTime(duration: Duration): Promise<void> {
    await this.runUntilIdle()
    const target = this.clock.now() + milliseconds(duration)
    if (!Number.isSafeInteger(target))
      throw new WorkflowError('INVALID_CLOCK', 'Virtual time overflow')
    let iterations = 0
    while (true) {
      if (++iterations > 100_000)
        throw new WorkflowError('TEST_CLOCK_LIVELOCK', 'More than 100000 deadlines in one advance')
      const snapshot = await this.runtime.testingSnapshot()
      const deadlines = [snapshot.nextDeadline, this.clock.nextDeadline()].filter(
        (value): value is number => value !== null
      )
      const next = deadlines.length ? Math.max(this.clock.now(), Math.min(...deadlines)) : target
      if (next > target) break
      this.clock.advanceTo(next)
      await this.runUntilIdle()
      if (next === target) {
        const after = await this.runtime.testingSnapshot()
        if (
          (after.nextDeadline === null || after.nextDeadline > target) &&
          (this.clock.nextDeadline() === null || this.clock.nextDeadline()! > target)
        )
          return
      }
    }
    this.clock.advanceTo(target)
    await this.runUntilIdle()
  }

  /**
   * Poll an application observation until its predicate becomes true, flushing outboxes.
   * Does not advance the manual clock. A read callback or predicate that throws rejects
   * immediately; a slow external callback is not forcibly cancelled by the safety timeout.
   * @typeParam T - Observed value type.
   * @param read - Async observation, such as handle.describe.
   * @param predicate - Condition accepting an observed value.
   * @param options - Real-time polling safety timeout, default 5s.
   * @returns First observed value satisfying the predicate.
   * @throws WorkflowError with TEST_WAIT_TIMEOUT if the condition remains false, or callback/backend errors.
   * @example
   * ```ts
   * import type { WorkflowHandle } from 'better-workflows'
   * import type { WorkflowsTestHarness } from 'better-workflows/testing'
   * declare class Report { run(id: string): Promise<string> }
   * declare const handle: WorkflowHandle<typeof Report>
   * declare const harness: WorkflowsTestHarness
   * await harness.waitFor(() => handle.describe(), state => state.status === 'waiting')
   * ```
   */
  async waitFor<T>(
    read: () => Promise<T>,
    predicate: (value: T) => boolean,
    options: {
      /** Real-time safety timeout for polling; not affected by advanceTime. @defaultValue "5s" */
      readonly timeout?: Duration
    } = {}
  ): Promise<T> {
    const deadline = Date.now() + milliseconds(options.timeout ?? '5s')
    while (true) {
      await this.runtime.flush()
      const value = await read()
      if (predicate(value)) return value
      if (Date.now() >= deadline)
        throw new WorkflowError('TEST_WAIT_TIMEOUT', 'Test predicate did not become true')
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
}

/**
 * Testing root module providing real persistence/engine integration and a manual clock.
 * Import domain features alongside it, not a second WorkflowsModule.forRoot.
 */
@Module({})
export class WorkflowsTestingModule {
  /**
   * Configure a test runtime and export its harness, clock and root infrastructure.
   * Defaults to a unique namespace, in-memory SQLite, single-node topology and 5ms
   * polling. It uses the production feature resolver, visibility checks and worker code.
   * @param options - Manual-clock settings plus optional root configuration overrides.
   * @returns Global dynamic testing module; close the Nest application after each test.
   * @throws WorkflowError for unsupported clock mode or invalid root/feature configuration.
   * @example
   * ```ts
   * import { Test } from '@nestjs/testing'
   * import { WorkflowsModule, Workflow, getWorkflowToken } from 'better-workflows'
   * import type { WorkflowContext, WorkflowClient } from 'better-workflows'
   * import { WorkflowsTestingModule, WorkflowsTestHarness } from 'better-workflows/testing'
   * import { z } from 'zod'
   * @Workflow({ name: 'delayed', version: 1, input: z.string(), output: z.string() })
   * class Delayed {
   *   async run(id: string, ctx: WorkflowContext) { await ctx.sleep('wait', '1d'); return id }
   * }
   * const app = await Test.createTestingModule({ imports: [
   *   WorkflowsTestingModule.forRoot({ initialTime: Date.UTC(2026, 0, 1) }),
   *   WorkflowsModule.forFeature({ name: 'test', workflows: [Delayed] })
   * ] }).compile()
   * await app.init()
   * try {
   *   const client = app.get<WorkflowClient<typeof Delayed>>(getWorkflowToken(Delayed))
   *   const handle = await client.start('report-1')
   *   await app.get(WorkflowsTestHarness).advanceTime('1d')
   *   console.log(await handle.result({ timeout: '1s' }))
   * } finally { await app.close() }
   * ```
   */
  static forRoot(options: TestingOptions = {}): DynamicModule {
    if (options.clock !== undefined && options.clock !== 'manual')
      throw new WorkflowError('INVALID_CLOCK', 'Only manual business time is supported')
    const { clock: _clock, initialTime, ...root } = options
    const clock = new WorkflowsTestClock(initialTime) satisfies BusinessClock
    const settings: WorkflowsOptions = {
      ...root,
      namespace: root.namespace ?? `test-${randomUUID()}`,
      storage: root.storage ?? { driver: 'sqlite', filename: ':memory:', runtime: 'auto' },
      queues: root.queues ?? [],
      pollInterval: root.pollInterval ?? '5ms',
      topology: 'single-node'
    }
    return {
      module: WorkflowsTestingModule,
      global: true,
      imports: [WorkflowsModule.forRoot(settings)],
      providers: [
        { provide: WORKFLOWS_TEST_CLOCK, useValue: clock },
        { provide: WorkflowsTestClock, useValue: clock },
        WorkflowsTestHarness
      ],
      exports: [WORKFLOWS_TEST_CLOCK, WorkflowsTestClock, WorkflowsTestHarness, WorkflowsModule]
    }
  }
}
