import { Module } from '@nestjs/common'
import type { DynamicModule, Provider, Type } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'
import { WorkflowsAdmin, WORKFLOWS_ADMIN_BACKEND } from './admin'
import { WorkflowClient } from './client'
import { getWorkflowToken, workflowContractClass } from './decorators'
import { WorkflowError } from './errors'
import { WorkflowsHealth } from './health'
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

/**
 * Nest integration for one shared durable runtime and domain-owned feature registrations.
 * Use forRoot/forRootAsync once, then forFeature/forFeatureAsync in each domain.
 * Registering features does not create additional database pools or workflow engines.
 */
@Module({})
export class WorkflowsModule {
  /**
   * Configure infrastructure and optional application defaults/queues synchronously.
   * Root queues are optional and visible to all features. Domain queues belong in
   * forFeature. Defaults are resolved before validation and before workers start.
   * @param options - Namespace, storage, topology and application policy.
   * @returns Dynamic Nest module exporting infrastructure and WorkflowsAdmin; global by default.
   * @throws Invalid settings or duplicate roots are rejected during Nest initialization.
   * @example
   * ```ts
   * import { Module } from '@nestjs/common'
   * import { WorkflowsModule } from 'better-workflows'
   * import { sqlite } from 'better-workflows/sqlite'
   * @Module({ imports: [WorkflowsModule.forRoot({
   *   namespace: 'reports-app', storage: sqlite({ filename: './data/workflows.sqlite' }),
   *   defaults: { queues: { concurrency: 4 } }
   * })] })
   * class AppModule {}
   * ```
   */
  static forRoot(options: WorkflowsOptions): DynamicModule {
    return {
      module: WorkflowsModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [
        { provide: WORKFLOWS_OPTIONS, useValue: options },
        WorkflowsRuntime,
        WorkflowsHealth,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsHealth, WorkflowsAdmin]
    }
  }

  /**
   * Configure the single root using a Nest dependency-injected factory.
   * Keep module visibility in the static isGlobal option; a factory cannot change it.
   * Imported modules must export the dependencies listed in inject.
   * @param options - Static imports/isGlobal plus inject and a settings factory.
   * @returns Dynamic root module whose settings resolve before bootstrap.
   * @throws Factory errors or invalid resolved settings abort initialization.
   * @example
   * ```ts
   * import { Injectable, Module } from '@nestjs/common'
   * import { WorkflowsModule } from 'better-workflows'
   * import { sqlite } from 'better-workflows/sqlite'
   * @Injectable()
   * class Settings { readonly file = './data/workflows.sqlite' }
   * @Module({ providers: [Settings], exports: [Settings] })
   * class SettingsModule {}
   * const root = WorkflowsModule.forRootAsync({
   *   imports: [SettingsModule], inject: [Settings],
   *   useFactory: (settings: Settings) => ({ namespace: 'reports', storage: sqlite({ filename: settings.file }) })
   * })
   * ```
   */
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
        WorkflowsHealth,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsHealth, WorkflowsAdmin]
    }
  }

  /**
   * Register a domain's handlers, contracts, clients and queue policies.
   * workflows registers implementations plus typed clients; activities registers worker
   * implementations. clients and activityContracts do not instantiate implementations.
   * Dependencies must be in this feature's imports/providers. Do not register the same
   * handler again in an outer module. Reexport WorkflowsModule to forward capabilities.
   * @param options - Static graph and synchronous domain configuration; not an array.
   * @returns Feature module exporting clients and explicitly selected queue/activity capabilities.
   * @throws Invalid ownership, scope, queue visibility or duplicate registrations fail initialization.
   * @example
   * ```ts
   * import { Activities, Activity, Workflow, WorkflowsModule, defineQueue } from 'better-workflows'
   * import type { WorkflowContext } from 'better-workflows'
   * import { z } from 'zod'
   * const Reports = defineQueue('reports')
   * @Activities({ queue: Reports })
   * class Totals {
   *   @Activity({ name: 'reports.total', version: 1, input: z.array(z.number()), output: z.number() })
   *   async total(values: number[]) { return values.reduce((a, b) => a + b, 0) }
   * }
   * @Workflow({ name: 'reports.generate', version: 1, input: z.array(z.number()), output: z.number() })
   * class GenerateReport {
   *   async run(values: number[], ctx: WorkflowContext) {
   *     return ctx.activities(Totals).total(values, { stepId: 'total' })
   *   }
   * }
   * const feature = WorkflowsModule.forFeature({
   *   name: 'reports', workflows: [GenerateReport], activities: [Totals],
   *   queues: [{ queue: Reports, concurrency: 2 }]
   * })
   * ```
   */
  static forFeature(options: WorkflowsFeatureOptions): DynamicModule {
    return createFeature(options, (token) => ({ provide: token, useValue: options }))
  }

  /**
   * Register static domain structure with DI-resolved queue/default/execution values.
   * The factory cannot return new imports, providers or handlers. All factories finish
   * before the catalog is validated; import order never decides conflicting policies.
   * @param options - Static feature graph, ordered dependency tokens and a configuration factory.
   * @returns Feature module whose clients and capabilities follow normal Nest visibility.
   * @throws Factory errors or invalid final configuration abort initialization before worker startup.
   * @example
   * ```ts
   * import { Injectable, Module } from '@nestjs/common'
   * import { WorkflowsModule, defineQueue } from 'better-workflows'
   * @Injectable()
   * class Settings { readonly concurrency = 2 }
   * @Module({ providers: [Settings], exports: [Settings] })
   * class SettingsModule {}
   * const Reports = defineQueue('reports')
   * const feature = WorkflowsModule.forFeatureAsync({
   *   name: 'reports', imports: [SettingsModule], inject: [Settings],
   *   useFactory: (settings: Settings) => ({ queues: [{ queue: Reports, concurrency: settings.concurrency }] }),
   *   exports: { queues: [Reports] }
   * })
   * ```
   */
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
  const clients = [
    ...new Set([
      ...workflows.map((entry) => workflowContractClass(handlerClass(entry))),
      ...(options.clients ?? [])
    ])
  ]
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
