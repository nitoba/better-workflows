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

export class WorkflowClient<W extends WorkflowClass> {
  /** Created by WorkflowsModule.forFeature; use @InjectWorkflow in application providers. */
  constructor(
    private readonly runtime: ClientBackend,
    private readonly workflow: W
  ) {}

  async start(input: WorkflowInput<W>, options?: StartOptions): Promise<WorkflowHandle<W>> {
    const accepted = await this.runtime.start(this.workflow, input, options?.idempotencyKey)
    return new WorkflowHandle(this.runtime, this.workflow, accepted.executionId, accepted.created)
  }

  getHandle(executionId: string): WorkflowHandle<W> {
    return new WorkflowHandle(this.runtime, this.workflow, executionId, false)
  }
}

export class WorkflowHandle<W extends WorkflowClass> {
  constructor(
    private readonly runtime: ClientBackend,
    private readonly workflow: W,
    readonly executionId: string,
    /** True only on the handle returned by the first successful acceptance. */
    readonly created: boolean
  ) {}

  describe() {
    return this.runtime.describe(this.workflow, this.executionId)
  }

  history(options?: HistoryOptions) {
    return this.runtime.history(this.workflow, this.executionId, options?.after, options?.limit)
  }

  pause(): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'pause', '')
  }
  resume(): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'run', '')
  }
  cancel(options?: CancelOptions): Promise<void> {
    return this.runtime.control(this.workflow, this.executionId, 'cancel', options?.reason ?? '')
  }

  signal<I, O>(signal: SignalDefinition<I, O>, payload: I, options: SignalDeliveryOptions) {
    return this.runtime.signal(
      this.workflow,
      this.executionId,
      signal,
      payload,
      options.idempotencyKey
    )
  }

  /** Waiting is local to this caller. Its timeout/abort never cancels the workflow. */
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
