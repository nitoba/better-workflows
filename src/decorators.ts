import 'reflect-metadata'
import { Inject, Injectable } from '@nestjs/common'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { ActivityOptions, SignalDefinition, WorkflowClass, WorkflowOptions } from './types'
import { identifier, milliseconds, positiveInteger } from './internal/values'
import { WorkflowError } from './errors'

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

export function Activities(): ClassDecorator {
  return (target) => {
    Injectable()(target)
    Reflect.defineMetadata(ACTIVITIES_METADATA, true, target)
  }
}

export function Activity<I, O>(options: ActivityOptions<I, O>): MethodDecorator {
  identifier(options.name, 'Activity name')
  identifier(options.queue, 'Queue name')
  positiveInteger(options.version, 'Activity version')
  if (options.retry) {
    positiveInteger(options.retry.maxAttempts, 'maxAttempts')
    milliseconds(options.retry.initialDelay ?? '1s')
    milliseconds(options.retry.maxDelay ?? '1m')
  }
  if (options.timeout !== undefined && milliseconds(options.timeout) === 0) {
    throw new WorkflowError('INVALID_CONFIGURATION', 'Activity timeout must be greater than zero')
  }
  return (target, property, descriptor) => {
    // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Legacy decorators must reject static methods and non-method descriptors.
    if (
      typeof target === 'function' ||
      typeof descriptor.value !== 'function' ||
      typeof property !== 'string'
    ) {
      throw new WorkflowError('INVALID_ACTIVITY', 'Activities must be named instance methods')
    }
    Reflect.defineMetadata(ACTIVITY_METADATA, Object.freeze({ ...options }), target, property)
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
