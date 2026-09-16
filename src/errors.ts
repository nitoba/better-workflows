/**
 * Serializable failure propagated through activities and workflow history.
 * Only explicitly retryable activity failures are retried, subject to maxAttempts.
 * Do not store secrets in code/message: they may appear in persisted diagnostics.
 */
export interface Failure {
  /**
   * Stable machine-readable category used for application handling and diagnostics.
   */
  readonly code: string
  /**
   * Human-readable explanation; not a stack trace or an authorization decision.
   */
  readonly message: string
  /**
   * Whether the activity retry policy may schedule another business attempt.
   */
  readonly retryable: boolean
}

/**
 * Explicit business error to throw from an activity implementation.
 * A retryable error uses the resolved retry policy; a nonretryable error is delivered
 * to the workflow. Infrastructure failure and runtime suspension are not business retries.
 * @example
 * ```ts
 * import { ActivityError } from 'better-workflows'
 * throw new ActivityError({ code: 'PROVIDER_BUSY', message: 'Try the provider later', retryable: true })
 * ```
 */
export class ActivityError extends Error implements Failure {
  /**
   * Machine-readable business failure category.
   */
  readonly code: string
  /**
   * Whether another business attempt is allowed by policy.
   */
  readonly retryable: boolean

  /**
   * Create an explicit activity failure.
   * @param failure - Serializable code/message/retryable fields.
   */
  constructor(failure: Failure) {
    super(failure.message)
    this.name = 'ActivityError'
    this.code = failure.code
    this.retryable = failure.retryable
  }
}

/**
 * Error for invalid configuration, control requests and workflow-library operations.
 * Use the stable code for handling instead of matching message strings.
 * A WorkflowExecutionError represents a terminal execution failure specifically.
 */
export class WorkflowError extends Error {
  /**
   * Create a library-operation error.
   * @param code - Stable error category.
   * @param message - Human-readable explanation suitable for diagnostics.
   */
  constructor(
    /**
     * Stable category, such as WAIT_TIMEOUT, IDEMPOTENCY_CONFLICT or TERMINAL_EXECUTION.
     */
    readonly code: string,
    message: string
  ) {
    super(message)
    this.name = 'WorkflowError'
  }
}

/**
 * Terminal workflow failure surfaced by WorkflowHandle.result.
 * Includes the owning execution ID and its serializable failure. This is distinct
 * from WAIT_TIMEOUT/WAIT_ABORTED, which stop only the caller's local wait.
 */
export class WorkflowExecutionError extends WorkflowError {
  /**
   * Wrap a terminal execution failure.
   * @param executionId - Failed or cancelled execution identifier.
   * @param failure - Persisted business/cancellation failure details.
   */
  constructor(
    /**
     * Execution that failed or was cancelled.
     */
    readonly executionId: string,
    /**
     * Original serializable failure; retryable does not restart a terminal execution.
     */
    readonly failure: Failure
  ) {
    super(failure.code, failure.message)
    this.name = 'WorkflowExecutionError'
  }
}

/** An untrusted exception crosses the public handler/engine boundary here. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- User code may throw any JavaScript value.
export function toFailure(error: unknown): Failure {
  /* oxlint-disable anti-slop/no-runtime-typeof -- Deserialized Effect errors have no class prototype. */
  if (
    error !== null &&
    typeof error === 'object' &&
    'code' in error &&
    'message' in error &&
    'retryable' in error &&
    typeof error.code === 'string' &&
    typeof error.message === 'string' &&
    typeof error.retryable === 'boolean'
  ) {
    return { code: error.code, message: error.message, retryable: error.retryable }
  }
  /* oxlint-enable anti-slop/no-runtime-typeof */
  if (error instanceof WorkflowError) {
    return { code: error.code, message: error.message, retryable: false }
  }
  return {
    code: 'UNEXPECTED_ERROR',
    message: error instanceof Error ? error.message : String(error),
    retryable: false
  }
}
