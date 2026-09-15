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

export interface TestingOptions extends Omit<
  WorkflowsOptions,
  'namespace' | 'storage' | 'queues' | 'topology' | 'cluster'
> {
  readonly clock?: 'manual'
  readonly initialTime?: number
  readonly namespace?: string
  readonly storage?: WorkflowsOptions['storage']
  readonly queues?: WorkflowsOptions['queues']
}
interface TestingBackend {
  flush(): Promise<void>
  testingSnapshot(): Promise<{ revision: string; nextDeadline: number | null }>
}

/** Test-only business time. SQL leases, cluster polling and Date.now are never monkey-patched. */
export class WorkflowsTestClock {
  private time: number
  private sequence = 0
  private readonly sleepers = new Map<number, { at: number; resume: () => void }>()
  constructor(initialTime = Date.now()) {
    if (!Number.isSafeInteger(initialTime) || initialTime < 0)
      throw new WorkflowError(
        'INVALID_CLOCK',
        'initialTime must be a nonnegative epoch millisecond value'
      )
    this.time = initialTime
  }
  now(): number {
    return this.time
  }
  schedule(delay: number, resume: () => void): () => void {
    const id = this.sequence++
    this.sleepers.set(id, { at: this.time + delay, resume })
    return () => {
      this.sleepers.delete(id)
    }
  }
  nextDeadline(): number | null {
    const values = [...this.sleepers.values()].map((timer) => timer.at)
    return values.length ? Math.min(...values) : null
  }
  /** Called by the harness after the engine has flushed the preceding instant. */
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

@Injectable()
export class WorkflowsTestHarness {
  constructor(
    @Inject(WorkflowsRuntime) private readonly runtime: TestingBackend,
    @Inject(WORKFLOWS_TEST_CLOCK) readonly clock: WorkflowsTestClock
  ) {}

  /** Flush outboxes and asynchronous engine transitions, with a bounded real-time safety timeout. */
  async runUntilIdle(options: { readonly timeout?: Duration } = {}): Promise<void> {
    const deadline = Date.now() + milliseconds(options.timeout ?? '5s')
    let previous = ''
    let stable = 0
    while (Date.now() <= deadline) {
      await this.runtime.flush()
      const snapshot = await this.runtime.testingSnapshot()
      const signature = `${snapshot.revision}/${this.clock.nextDeadline()}`
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

  /** Visits every pending business deadline in order, rather than resetting retries at the target time. */
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

  async waitFor<T>(
    read: () => Promise<T>,
    predicate: (value: T) => boolean,
    options: { readonly timeout?: Duration } = {}
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

@Module({})
export class WorkflowsTestingModule {
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
