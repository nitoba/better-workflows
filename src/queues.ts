import { WorkflowError } from './errors'
import { identifier } from './internal/values'

const QUEUE_REFERENCE: unique symbol = Symbol.for('better-workflows/queue-reference')

/** Stable logical identity; importing a reference does not register a queue or start workers. */
export interface QueueReference<Name extends string = string> {
  readonly name: Name
  readonly [QUEUE_REFERENCE]: true
}

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
