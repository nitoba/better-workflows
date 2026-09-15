import 'reflect-metadata'
import { Inject, Injectable } from '@nestjs/common'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  ActivityDefaults,
  ActivityOptions,
  SignalDefinition,
  WorkflowClass,
  WorkflowOptions
} from './types'
import { identifier, milliseconds, positiveInteger } from './internal/values'
import { WorkflowError } from './errors'
import { queueName } from './queues'

export const WORKFLOW_METADATA = Symbol.for('better-workflows/workflow')
export const ACTIVITIES_METADATA = Symbol.for('better-workflows/activities')
export const ACTIVITY_METADATA = Symbol.for('better-workflows/activity')
const CLIENT_TOKENS = new WeakMap<WorkflowClass, symbol>()

export function Workflow<I, O>(options: WorkflowOptions<I, O>): ClassDecorator {
  identifier(options.name, 'Workflow name')
  positiveInteger(options.version, 'Workflow version')
  const names = new Set<string>()
  for (const signal of options.signals ?? []) {
    identifier(signal.name, 'Signal name')
    if (names.has(signal.name)) throw new WorkflowError('DUPLICATE_SIGNAL', signal.name)
    names.add(signal.name)
  }
  return (target) => {
    Injectable()(target)
    Reflect.defineMetadata(WORKFLOW_METADATA, Object.freeze({ ...options }), target)
  }
}

export function Activities(defaults: ActivityDefaults = {}): ClassDecorator {
  validateActivityDefaults(defaults)
  const settings = freezeActivityDefaults(defaults)
  return (target) => {
    Injectable()(target)
    Reflect.defineMetadata(ACTIVITIES_METADATA, settings, target)
  }
}

export function Activity<I, O>(options: ActivityOptions<I, O>): MethodDecorator {
  identifier(options.name, 'Activity name')
  validateActivityDefaults(options)
  positiveInteger(options.version, 'Activity version')
  return (target, property, descriptor) => {
    // oxlint-disable anti-slop/no-runtime-typeof -- Decorator boundary must reject static methods and non-method descriptors.
    if (
      typeof target === 'function' ||
      typeof descriptor.value !== 'function' ||
      typeof property !== 'string'
    ) {
      throw new WorkflowError('INVALID_ACTIVITY', 'Activities must be named instance methods')
    }
    // oxlint-enable anti-slop/no-runtime-typeof
    Reflect.defineMetadata(
      ACTIVITY_METADATA,
      Object.freeze({ ...options, ...freezeActivityDefaults(options) }),
      target,
      property
    )
  }
}

export function defineSignal<I, O>(
  name: string,
  schema: StandardSchemaV1<I, O>
): SignalDefinition<I, O> {
  identifier(name, 'Signal name')
  return Object.freeze({ name, schema })
}

export function getWorkflowToken(workflow: WorkflowClass): symbol {
  let token = CLIENT_TOKENS.get(workflow)
  if (!token) {
    token = Symbol(`better-workflows/client/${workflow.name}`)
    CLIENT_TOKENS.set(workflow, token)
  }
  return token
}

export function InjectWorkflow(workflow: WorkflowClass): ParameterDecorator & PropertyDecorator {
  return Inject(getWorkflowToken(workflow))
}

export function validateActivityDefaults(options: ActivityDefaults): void {
  if (options.queue !== undefined) queueName(options.queue)
  if (options.retry) {
    positiveInteger(options.retry.maxAttempts, 'maxAttempts')
    if (
      options.retry.backoff !== undefined &&
      !['fixed', 'exponential'].includes(options.retry.backoff)
    )
      throw new WorkflowError('INVALID_CONFIGURATION', 'Invalid retry backoff')
    milliseconds(options.retry.initialDelay ?? '1s')
    milliseconds(options.retry.maxDelay ?? '1m')
  }
  if (options.timeout !== undefined && milliseconds(options.timeout) === 0)
    throw new WorkflowError('INVALID_CONFIGURATION', 'Activity timeout must be greater than zero')
}

export function freezeActivityDefaults(options: ActivityDefaults): ActivityDefaults {
  const result = { ...options }
  if (options.retry) result.retry = Object.freeze({ ...options.retry })
  return Object.freeze(result)
}
