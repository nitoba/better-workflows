/** Serializable business failure. Only explicitly retryable failures are retried. */
export interface Failure {
  readonly code: string
  readonly message: string
  readonly retryable: boolean
}

export class ActivityError extends Error implements Failure {
  readonly code: string
  readonly retryable: boolean

  constructor(failure: Failure) {
    super(failure.message)
    this.name = 'ActivityError'
    this.code = failure.code
    this.retryable = failure.retryable
  }
}

export class WorkflowError extends Error {
  constructor(
    readonly code: string,
    message: string
  ) {
    super(message)
    this.name = 'WorkflowError'
  }
}

export class WorkflowExecutionError extends WorkflowError {
  constructor(
    readonly executionId: string,
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
