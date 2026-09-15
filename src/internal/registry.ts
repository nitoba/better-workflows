import type { Type } from '@nestjs/common'
import { Scope } from '@nestjs/common'
import type { DiscoveryService } from '@nestjs/core'
import { ACTIVITY_METADATA, ACTIVITIES_METADATA, WORKFLOW_METADATA } from '../decorators'
import { WorkflowError } from '../errors'
import type {
  ActivityContext,
  ActivityOptions,
  WorkflowClass,
  WorkflowContext,
  WorkflowOptions
} from '../types'
import { workflowDefinition } from './wire'
import type { EngineWorkflow } from './wire'

/** Heterogeneous provider types are erased only inside the Nest dispatch boundary. */
export interface ActivityContract {
  readonly provider: Type
  readonly method: string
  readonly options: ActivityOptions<any, any>
}

export interface RegisteredActivity extends ActivityContract {
  readonly invoke: (input: any, context: ActivityContext) => Promise<any>
}

export interface RegisteredWorkflow {
  readonly provider: WorkflowClass
  readonly options: WorkflowOptions<any, any>
  readonly definition: EngineWorkflow
  handler?: (input: any, context: WorkflowContext) => Promise<any>
}

export class Registry {
  readonly workflows = new Map<string, RegisteredWorkflow>()
  readonly activities = new Map<string, RegisteredActivity>()
  readonly providers = new Map<Type, ActivityContract[]>()

  constructor(private readonly namespace: string) {}

  key(name: string, version: number): string {
    return JSON.stringify([name, version])
  }

  contract(provider: WorkflowClass): RegisteredWorkflow {
    // Safety: only @Workflow writes this metadata; it validates and freezes its options.
    const options = Reflect.getOwnMetadata(WORKFLOW_METADATA, provider) as
      | WorkflowOptions<any, any>
      | undefined
    if (!options)
      throw new WorkflowError('MISSING_DECORATOR', `${provider.name} has no @Workflow decorator`)
    const key = this.key(options.name, options.version)
    const existing = this.workflows.get(key)
    if (existing) {
      if (existing.provider !== provider) throw new WorkflowError('DUPLICATE_WORKFLOW', key)
      return existing
    }
    const contract: RegisteredWorkflow = {
      provider,
      options,
      definition: workflowDefinition(this.namespace, options.name, options.version)
    }
    this.workflows.set(key, contract)
    return contract
  }

  workflow(name: string, version: number): RegisteredWorkflow {
    const registered = this.workflows.get(this.key(name, version))
    if (!registered)
      throw new WorkflowError(
        'MISSING_WORKFLOW_VERSION',
        `${name}@${version} must remain registered while executions or clients still need it`
      )
    return registered
  }

  activityContracts(provider: Type): readonly ActivityContract[] {
    const cached = this.providers.get(provider)
    if (cached) return cached
    if (!Reflect.hasOwnMetadata(ACTIVITIES_METADATA, provider)) {
      throw new WorkflowError('MISSING_DECORATOR', `${provider.name} has no @Activities decorator`)
    }
    const contracts: ActivityContract[] = []
    const methods = new Set<string>()
    for (
      let prototype = provider.prototype;
      prototype && prototype !== Object.prototype;
      prototype = Object.getPrototypeOf(prototype)
    ) {
      for (const method of Object.getOwnPropertyNames(prototype)) {
        if (methods.has(method)) continue
        methods.add(method)
        // Safety: @Activity is the only writer of this validated metadata.
        const options = Reflect.getOwnMetadata(ACTIVITY_METADATA, prototype, method) as
          | ActivityOptions<any, any>
          | undefined
        if (options) contracts.push({ provider, method, options })
      }
    }
    this.providers.set(provider, contracts)
    return contracts
  }

  discover(discovery: DiscoveryService): void {
    const seen = new Set<object>()
    for (const wrapper of discovery.getProviders()) {
      const provider = wrapper.token
      // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Nest provider tokens can be strings, symbols or constructors.
      if (typeof provider !== 'function') continue
      const workflow = Reflect.hasOwnMetadata(WORKFLOW_METADATA, provider)
      const activities = Reflect.hasOwnMetadata(ACTIVITIES_METADATA, provider)
      if (!workflow && !activities) continue
      if (
        !wrapper.isDependencyTreeStatic() ||
        (wrapper.scope !== undefined && wrapper.scope !== Scope.DEFAULT)
      ) {
        throw new WorkflowError(
          'UNSUPPORTED_SCOPE',
          `${provider.name}: workflows and activities require singleton dependency trees`
        )
      }
      if (!wrapper.instance || seen.has(wrapper.instance)) continue
      seen.add(wrapper.instance)
      // Safety: a decorated Nest class token is a constructible provider, verified above.
      const type = provider as Type
      if (workflow) {
        const contract = this.contract(type)
        if (contract.handler)
          throw new WorkflowError(
            'DUPLICATE_WORKFLOW_PROVIDER',
            `${provider.name} is instantiated by more than one module; import/export the owning module instead`
          )
        const handler = wrapper.instance.run
        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Reject malformed provider overrides at the dispatch boundary.
        if (typeof handler !== 'function')
          throw new WorkflowError('MISSING_HANDLER', `${provider.name}.run is required`)
        contract.handler = (input, context) => handler.call(wrapper.instance, input, context)
      }
      if (!activities) continue
      const registered: RegisteredActivity[] = []
      const methods = new Set<string>()
      for (
        let prototype = provider.prototype;
        prototype && prototype !== Object.prototype;
        prototype = Object.getPrototypeOf(prototype)
      ) {
        for (const method of Object.getOwnPropertyNames(prototype)) {
          if (methods.has(method)) continue
          methods.add(method)
          // Safety: @Activity is the only writer of this metadata and checks its options.
          const options = Reflect.getOwnMetadata(ACTIVITY_METADATA, prototype, method) as
            | ActivityOptions<any, any>
            | undefined
          if (!options) continue
          const key = this.key(options.name, options.version)
          if (this.activities.has(key)) throw new WorkflowError('DUPLICATE_ACTIVITY', key)
          const handler = wrapper.instance[method]
          // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Provider overrides must implement registered activity methods.
          if (typeof handler !== 'function')
            throw new WorkflowError('MISSING_HANDLER', `${provider.name}.${method} is required`)
          const activity: RegisteredActivity = {
            provider: type,
            method,
            options,
            invoke: (input, context) => handler.call(wrapper.instance, input, context)
          }
          this.activities.set(key, activity)
          registered.push(activity)
        }
      }
      this.providers.set(type, registered)
    }
  }
}
