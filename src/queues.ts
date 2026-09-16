import { WorkflowError } from './errors'
import { identifier } from './internal/values'

const QUEUE_REFERENCE: unique symbol = Symbol.for('better-workflows/queue-reference')

/**
 * Immutable, branded logical queue identity returned by defineQueue.
 * Importing a reference does not register capacity or start workers. Identity is the
 * explicit name within a namespace, not object identity, module name or file path.
 * @typeParam Name - Literal queue name retained for tooling and type inference.
 */
export interface QueueReference<Name extends string = string> {
  /**
   * Stable routing name. Two references with the same name identify the same logical queue.
   */
  readonly name: Name
  /**
   * Internal brand; create references through defineQueue rather than hand-written objects.
   * @internal
   */
  readonly [QUEUE_REFERENCE]: true
}

/**
 * Create a reusable queue identity without side effects.
 * Register its policy once in root/feature queues. A feature-private queue must be
 * explicitly exported and imported before another feature can place handlers on it.
 * Different references with the same name do not permit duplicate policy ownership.
 * @typeParam Name - String literal naming the queue.
 * @param name - Explicit stable name (1–256 characters, no ASCII controls).
 * @returns Frozen QueueReference preserving the literal name.
 * @throws WorkflowError with INVALID_IDENTIFIER for an invalid name.
 * @example
 * ```ts
 * import { defineQueue, WorkflowsModule } from 'better-workflows'
 * const RenderQueue = defineQueue('reports.render')
 * const reports = WorkflowsModule.forFeature({
 *   name: 'report-queues', queues: [{ queue: RenderQueue, concurrency: 2, globalConcurrency: 6 }],
 *   exports: { queues: [RenderQueue] }
 * })
 * ```
 */
export function defineQueue<const Name extends string>(name: Name): QueueReference<Name> {
  identifier(name, 'Queue name')
  return Object.freeze({ name, [QUEUE_REFERENCE]: true as const })
}

export function queueName(queue: QueueReference): string {
  if (!queue || queue[QUEUE_REFERENCE] !== true) {
    throw new WorkflowError(
      'INVALID_QUEUE_REFERENCE',
      'Use defineQueue(name), not a string or an unregistered object'
    )
  }
  identifier(queue.name, 'Queue name')
  return queue.name
}

export function queueToken(queue: QueueReference): symbol {
  return Symbol.for(`better-workflows/queue/${encodeURIComponent(queueName(queue))}`)
}
