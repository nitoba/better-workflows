import { ActivityError } from 'better-workflows';
import type { ActivityContext } from 'better-workflows';

export class IntegrationError extends Error {
  constructor(readonly code: string, message: string, readonly retryable = false) {
    super(message); this.name = 'IntegrationError';
  }
}

export async function executeActivity<T>(ctx: ActivityContext, work: () => Promise<T>): Promise<T> {
  try {
    ctx.signal.throwIfAborted();
    const result = await work();
    ctx.signal.throwIfAborted();
    return result;
  } catch (error) {
    if (ctx.signal.aborted || error instanceof ActivityError) throw error;
    if (error instanceof IntegrationError) {
      throw new ActivityError({ code: error.code, message: error.message, retryable: error.retryable });
    }
    // No request payloads, API keys or upstream response bodies go into the journal.
    throw new ActivityError({ code: 'INTEGRATION_FAILURE', message: 'An integration failed; inspect the local application logs.', retryable: false });
  }
}
