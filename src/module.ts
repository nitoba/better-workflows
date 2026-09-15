import { Module } from '@nestjs/common'
import type { DynamicModule, Provider, Type } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'
import { WorkflowsAdmin, WORKFLOWS_ADMIN_BACKEND } from './admin'
import { WorkflowClient } from './client'
import { getWorkflowToken } from './decorators'
import { WorkflowError } from './errors'
import { queueToken } from './queues'
import type {
  FeatureConfiguration,
  FeatureStructure,
  WorkflowsFeatureOptions,
  WorkflowsFeatureAsyncOptions,
  WorkflowsAsyncOptions,
  WorkflowsOptions
} from './types'
import { WORKFLOWS_OPTIONS, WorkflowsRuntime } from './internal/runtime'
import {
  FEATURE_FACTORY,
  FeatureExport,
  FeatureRegistration,
  activityToken,
  handlerClass,
  handlerToken
} from './internal/feature'

const adminProviders = [
  {
    provide: WORKFLOWS_ADMIN_BACKEND,
    inject: [WorkflowsRuntime],
    useFactory: (runtime: WorkflowsRuntime) => runtime.adminBackend()
  },
  WorkflowsAdmin
]

@Module({})
export class WorkflowsModule {
  static forRoot(options: WorkflowsOptions): DynamicModule {
    return {
      module: WorkflowsModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [
        { provide: WORKFLOWS_OPTIONS, useValue: options },
        WorkflowsRuntime,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsAdmin]
    }
  }

  static forRootAsync(options: WorkflowsAsyncOptions): DynamicModule {
    return {
      module: WorkflowsModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        {
          provide: WORKFLOWS_OPTIONS,
          useFactory: options.useFactory,
          inject: [...(options.inject ?? [])]
        },
        WorkflowsRuntime,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsAdmin]
    }
  }

  static forFeature(options: WorkflowsFeatureOptions): DynamicModule {
    return createFeature(options, (token) => ({ provide: token, useValue: options }))
  }

  static forFeatureAsync(options: WorkflowsFeatureAsyncOptions): DynamicModule {
    return createFeature(options, (token) => ({
      provide: token,
      inject: [...(options.inject ?? [])],
      useFactory: options.useFactory
    }))
  }
}

function createFeature(
  options: FeatureStructure,
  configuration: (token: symbol) => Provider<FeatureConfiguration>
): DynamicModule {
  if (!options || Array.isArray(options))
    throw new WorkflowError('INVALID_FEATURE', 'forFeature expects an options object')
  const id = Symbol(`better-workflows/feature/${options.name ?? 'clients'}`)
  const configToken = Symbol('better-workflows/feature-options')
  const workflows = [...(options.workflows ?? [])]
  const activities = [...(options.activities ?? [])]
  const handlers = [...workflows, ...activities]
  const classes = handlers.map(handlerClass)
  if (new Set(classes).size !== classes.length)
    throw new WorkflowError('DUPLICATE_HANDLER', 'A feature must register each implementation once')
  const clients = [...new Set([...workflows.map(handlerClass), ...(options.clients ?? [])])]
  const structure: FeatureStructure = Object.freeze({
    ...options,
    workflows: Object.freeze(workflows),
    activities: Object.freeze(activities),
    clients: Object.freeze([...(options.clients ?? [])]),
    activityContracts: Object.freeze([...(options.activityContracts ?? [])])
  })
  const factory = (
    settings: FeatureConfiguration,
    _runtime: WorkflowsRuntime,
    ...instances: any[]
  ) =>
    new FeatureRegistration(
      id,
      structure,
      settings,
      new Map(classes.map((type, i) => [type, instances[i]]))
    )
  Reflect.defineMetadata(FEATURE_FACTORY, true, factory)
  const providers: Provider[] = [...(options.providers ?? []), configuration(configToken)]
  for (const entry of handlers) {
    // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Descriptor registrations resolve existing exported Nest instances.
    if (typeof entry === 'function') providers.push(entry)
  }
  providers.push({
    provide: id,
    useFactory: factory,
    inject: [configToken, WorkflowsRuntime, ...handlers.map(handlerToken)]
  })
  const exported: (symbol | Type)[] = []
  for (const workflow of clients) {
    const token = getWorkflowToken(workflow)
    providers.push({
      provide: token,
      inject: [WorkflowsRuntime],
      useFactory: (runtime: WorkflowsRuntime) => {
        runtime.registerContract(workflow)
        return new WorkflowClient(runtime, workflow)
      }
    })
    exported.push(token)
  }
  for (const queue of options.exports?.queues ?? []) {
    const token = queueToken(queue)
    providers.push({ provide: token, useValue: new FeatureExport(id) })
    exported.push(token)
  }
  for (const activity of options.exports?.activities ?? []) {
    const token = activityToken(activity)
    providers.push({ provide: token, useValue: new FeatureExport(id) })
    exported.push(token)
  }
  return {
    module: WorkflowsModule,
    imports: [...(options.imports ?? [])],
    providers,
    exports: exported
  }
}
