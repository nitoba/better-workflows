import { WorkflowError, WorkflowExecutionError } from './errors'
import type { Failure } from './errors'
import type {
  ResultWaitOptions,
  StartOptions,
  HistoryOptions,
  CancelOptions,
  SignalDeliveryOptions,
  SignalDefinition,
  WorkflowClass,
  WorkflowInput,
  WorkflowOutput
} from './types'
import type { ClientBackend } from './internal/client-backend'
import { decode, milliseconds } from './internal/values'

/**
 * Typed entry point for accepting and observing one workflow contract.
 * Obtain it through InjectWorkflow or getWorkflowToken after forFeature registration.
 * Use `WorkflowClient<typeof MyWorkflow>`; no Effect types are needed in application code.
 * @typeParam W - Decorated workflow constructor.
 */
export class WorkflowClient<W extends WorkflowClass> {
  /**
   * Constructed by WorkflowsModule.forFeature; application code should use Nest injection.
   * @param runtime - Library-owned client backend.
   * @param workflow - Workflow contract associated with this client.
   * @internal
   */
  constructor(
    private readonly runtime: ClientBackend,
    private readonly workflow: W
  ) {}

  /**
   * Durably accept a workflow execution without waiting for its business result.
   * Resolves after acceptance and pending dispatch are recorded, not after a worker starts.
   * Same key + same payload reuses the execution, even across workflow versions; a changed
   * payload conflicts. Acceptance does not make external effects exactly-once.
   * @param input - Serializable payload matching the registered input schema.
   * @param options - Optional idempotency key overriding the workflow's key resolver.
   * @returns Handle with executionId and created indicating whether this call inserted the run.
   * @throws WorkflowError for invalid input, unavailable runtime, IDEMPOTENCY_CONFLICT,
   * EXECUTION_PRUNED (retained key), or a storage failure.
   * @example
   * ```ts
   * import type { WorkflowClient } from 'better-workflows'
   * declare class GenerateReport { run(input: { reportId: string }): Promise<{ objectKey: string }> }
   * declare const reports: WorkflowClient<typeof GenerateReport>
   * const handle = await reports.start({ reportId: 'report-1' }, { idempotencyKey: 'report-1' })
   * const result = await handle.result({ timeout: '30s' })
   * ```
   */
  async start(input: WorkflowInput<W>, options?: StartOptions): Promise<WorkflowHandle<W>> {
    const accepted = await this.runtime.start(this.workflow, input, options?.idempotencyKey)
    return new WorkflowHandle(this.runtime, this.workflow, accepted.executionId, accepted.created)
  }

  /**
   * Create a lightweight reference to an existing execution without querying storage.
   * The workflow name and namespace are checked when an operation is performed. This
   * method does not start or verify the execution; the returned handle has created: false.
   * @param executionId - Previously returned durable execution identifier.
   * @returns A handle using this client's workflow contract and output type.
   * @example
   * ```ts
   * import type { WorkflowClient } from 'better-workflows'
   * declare class GenerateReport { run(input: { reportId: string }): Promise<{ objectKey: string }> }
   * declare const reports: WorkflowClient<typeof GenerateReport>
   * const snapshot = await reports.getHandle('saved-execution-id').describe()
   * ```
   */
  getHandle(executionId: string): WorkflowHandle<W> {
    return new WorkflowHandle(this.runtime, this.workflow, executionId, false)
  }
}

/**
 * Reference for observing, signalling and controlling one durable execution.
 * Obtain it through WorkflowClient.start/getHandle. A handle is not a running worker
 * and does not need to remain alive for the workflow to progress.
 * @typeParam W - Workflow constructor determining the successful result type.
 */
export class WorkflowHandle<W extends WorkflowClass> {
  /**
   * Constructed by WorkflowClient; prefer start/getHandle in application code.
   * @param runtime - Library-owned client backend.
   * @param workflow - Contract used for name checks and typed result decoding.
   * @param executionId - Existing durable execution identifier.
   * @param created - Whether the creating start call accepted a new execution.
   * @internal
   */
  constructor(
    private readonly runtime: ClientBackend,
    private readonly workflow: W,
    /**
     * Stable identifier of this execution; persist it to query the workflow later.
     */
    readonly executionId: string,
    /**
     * True only on the handle returned by the first successful acceptance.
     * getHandle and deduplicated starts return false, regardless of execution status.
     */
    readonly created: boolean
  ) {}

  /**
   * Read the current observable execution and control state.
   * May reconcile a completed engine result into the journal before returning. A paused
   * state reflects a cooperative request, not proof that in-flight I/O has stopped.
   * @returns Snapshot containing name/version, status, timestamps and optional wait/failure.
   * @throws WorkflowError with EXECUTION_NOT_FOUND for a missing or differently named run.
   */
  describe() {
    return this.runtime.describe(this.workflow, this.executionId)
  }

  /**
   * Read one page of ordered journal events for this execution.
   * @param options - Exclusive after cursor and limit (default 100, maximum 1000).
   * @returns Events and, only when another page was observed, its nextCursor.
   * @throws WorkflowError for a missing execution or invalid pagination.
   * @example
   * ```ts
   * import type { WorkflowHandle } from 'better-workflows'
   * declare class GenerateReport { run(input: { reportId: string }): Promise<void> }
   * declare const handle: WorkflowHandle<typeof GenerateReport>
   * let after = 0
   * for (;;) {
   *   const page = await handle.history({ after, limit: 100 })
   *   for (const event of page.events) console.log(event.sequence, event.type)
   *   if (page.nextCursor === undefined) break
   *   after = page.nextCursor
   * }
   * ```
   */
  history(options?: HistoryOptions) {
    return this.runtime.history(this.workflow, this.executionId, options?.after, options?.limit)
  }

  /**
   * Persist a cooperative pause request at durable command boundaries.
   * Already-started activities may finish; arbitrary I/O is not forcibly frozen.
   * This does not cascade a pause to independent child workflows or undo side effects.
   * Repeated requests are harmless; a terminal execution is not reopened.
   * @returns Resolves after the control request is persisted, not after all workers stop.
   * @throws WorkflowError for a missing execution or backend failure.
   */
  pause(): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'pause', '')
  }
  /**
   * Clear a persisted pause so the engine can continue from recorded commands.
   * Restart recovery itself is automatic and needs no manual resume. This method does
   * not clear results, restart failed/cancelled workflows or resume independent children.
   * @returns Resolves when the control request has been recorded.
   * @throws WorkflowError with TERMINAL_EXECUTION for failure/cancellation or a pending cancel.
   */
  resume(): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'run', '')
  }
  /**
   * Persist a cooperative cancellation request; not a business compensation.
   * Workers receive an abort signal and child close policies are reconciled. An external
   * effect may already have completed; the library cannot undo it. Cancelled executions
   * cannot be resumed. Use a saga for explicitly modelled business rollback.
   * @param options - Optional journaled reason, up to 4096 characters.
   * @returns Resolves after the request is durable; describe may temporarily show cancelling.
   * @throws WorkflowError for invalid reason, a missing execution or backend failure.
   * @example
   * ```ts
   * import type { WorkflowHandle } from 'better-workflows'
   * declare class GenerateReport { run(input: { reportId: string }): Promise<void> }
   * declare const handle: WorkflowHandle<typeof GenerateReport>
   * await handle.pause()
   * await handle.resume()
   * await handle.cancel({ reason: 'Request withdrawn by the user' })
   * ```
   */
  cancel(options?: CancelOptions): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'cancel', options?.reason ?? '')
  }

  /**
   * Persist a signal delivery for the execution's registered workflow version.
   * The registered schema is authoritative, not an arbitrary caller-supplied schema.
   * Signals can arrive before the workflow begins waiting. Identical duplicates are
   * recognized even after termination; new events to a terminal/cancelling run fail.
   * @typeParam I - Producer payload accepted by the signal schema.
   * @typeParam O - Schema output consumed by the workflow.
   * @param signal - Definition declared in this workflow version's signals list.
   * @param payload - Data validated and serialized before inbox insertion.
   * @param options - Required event idempotency key scoped to execution + signal name.
   * @returns `{ accepted: true }` for a new event, `{ accepted: false }` for an identical duplicate.
   * @throws WorkflowError for UNKNOWN_SIGNAL, IDEMPOTENCY_CONFLICT, TERMINAL_EXECUTION or validation.
   * @example
   * ```ts
   * import { defineSignal } from 'better-workflows'
   * import type { WorkflowHandle } from 'better-workflows'
   * import { z } from 'zod'
   * const Approved = defineSignal('approved', z.object({ reviewerId: z.string() }))
   * declare class ApprovalWorkflow { run(input: { documentId: string }): Promise<void> }
   * declare const handle: WorkflowHandle<typeof ApprovalWorkflow>
   * await handle.signal(Approved, { reviewerId: 'reviewer-1' }, { idempotencyKey: 'approval-event-1' })
   * ```
   */
  signal<I, O>(signal: SignalDefinition<I, O>, payload: I, options: SignalDeliveryOptions) {
    return this.runtime.signal(
      this.workflow,
      this.executionId,
      signal,
      payload,
      options.idempotencyKey
    )
  }

  /**
   * Poll until this execution completes, then return its version-correct output.
   * This wait is local to the caller: timeout/abort never cancels the workflow and does
   * not use the test business clock. Without timeout or abort, it can wait indefinitely.
   * A result is not reinterpreted using a newer workflow version's output type.
   * @param options - Optional real-time wait timeout and caller AbortSignal.
   * @returns Successful result using this client's workflow output type.
   * @throws WorkflowExecutionError when execution failed or was cancelled.
   * @throws WorkflowError for WAIT_TIMEOUT, WAIT_ABORTED, RESULT_VERSION_MISMATCH,
   * a missing execution, or a storage error.
   * @example
   * ```ts
   * import { WorkflowError, WorkflowExecutionError } from 'better-workflows'
   * import type { WorkflowHandle } from 'better-workflows'
   * declare class GenerateReport { run(input: { reportId: string }): Promise<{ objectKey: string }> }
   * declare const handle: WorkflowHandle<typeof GenerateReport>
   * try {
   *   const report = await handle.result({ timeout: '30s' })
   *   console.log(report.objectKey)
   * } catch (error) {
   *   if (error instanceof WorkflowExecutionError) console.error(error.failure)
   *   else if (error instanceof WorkflowError && error.code === 'WAIT_TIMEOUT') {
   *     console.log('Still running:', handle.executionId)
   *   } else throw error
   * }
   * ```
   */
  async result(options?: ResultWaitOptions): Promise<WorkflowOutput<W>> {
    const deadline =
      options?.timeout === undefined ? Infinity : Date.now() + milliseconds(options.timeout)
    while (true) {
      if (options?.signal?.aborted)
        throw new WorkflowError('WAIT_ABORTED', 'Result wait was aborted; workflow continues')
      const snapshot = await this.describe()
      if (snapshot.status === 'completed') {
        const row = await this.runtime.row(this.workflow, this.executionId)
        if (row.version !== this.runtime.resultVersion(this.workflow)) {
          throw new WorkflowError(
            'RESULT_VERSION_MISMATCH',
            `Execution belongs to version ${row.version}; use that version's typed client to read its result`
          )
        }
        return decode<WorkflowOutput<W>>(row.result_json!)
      }
      if (snapshot.status === 'failed' || snapshot.status === 'cancelled') {
        const failure: Failure = snapshot.failure ?? {
          code: 'WORKFLOW_CANCELLED',
          message: 'Workflow was cancelled',
          retryable: false
        }
        throw new WorkflowExecutionError(this.executionId, failure)
      }
      if (Date.now() >= deadline)
        throw new WorkflowError('WAIT_TIMEOUT', 'Result wait timed out; workflow continues')
      await new Promise<void>((resolve, reject) => {
        const signal = options?.signal
        const abort = () => {
          clearTimeout(timer)
          signal?.removeEventListener('abort', abort)
          reject(new WorkflowError('WAIT_ABORTED', 'Result wait was aborted; workflow continues'))
        }
        const timer = setTimeout(
          () => {
            signal?.removeEventListener('abort', abort)
            resolve()
          },
          Math.min(100, deadline - Date.now())
        )
        signal?.addEventListener('abort', abort, { once: true })
        if (signal?.aborted) abort()
      })
    }
  }
}
