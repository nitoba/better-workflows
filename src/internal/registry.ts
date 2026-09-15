import type { Type } from '@nestjs/common'
import type { DiscoveryService } from '@nestjs/core'
import {
  ACTIVITY_METADATA,
  ACTIVITIES_METADATA,
  WORKFLOW_METADATA,
  getWorkflowToken,
  validateActivityDefaults
} from '../decorators'
import { WorkflowError } from '../errors'
import { queueName, queueToken } from '../queues'
import type { QueueReference } from '../queues'
import type {
  ActivityContext,
  ActivityDefaults,
  ActivityOptions,
  QueueOptions,
  QueueRegistration,
  QueueSettings,
  WorkflowClass,
  WorkflowContext,
  WorkflowOptions,
  WorkflowsOptions
} from '../types'
import {
  FEATURE_FACTORY,
  FeatureExport,
  FeatureRegistration,
  activityToken,
  handlerClass,
  handlerToken,
  requireSingleton,
  visibleProviders
} from './feature'
import type { FeatureHost } from './feature'
import { identifier, positiveInteger } from './values'
import { workflowDefinition } from './wire'
import type { EngineWorkflow } from './wire'

/** Erasure is confined to the validated Nest dispatch boundary. */
export type ResolvedActivityOptions = Omit<ActivityOptions<any, any>, 'queue'> & {
  readonly queue: string
}
export interface ActivityContract {
  readonly provider: Type
  readonly method: string
  readonly options: ResolvedActivityOptions
}
export interface RegisteredActivity extends ActivityContract {
  readonly enabled: boolean
  readonly invoke: (input: any, context: ActivityContext) => Promise<any>
}
interface Feature {
  readonly registration: FeatureRegistration
  readonly host: FeatureHost
}
export interface RegisteredWorkflow {
  readonly provider: WorkflowClass
  readonly options: WorkflowOptions<any, any>
  readonly definition: EngineWorkflow
  owner?: Feature
  enabled?: boolean
  concurrency?: number
  handler?: (input: any, context: WorkflowContext) => Promise<any>
}
interface OwnedQueue {
  readonly reference: QueueReference
  readonly owner: symbol | null
  readonly options: QueueOptions
}

export class Registry {
  readonly workflows = new Map<string, RegisteredWorkflow>()
  readonly activities = new Map<string, RegisteredActivity>()
  readonly providers = new Map<Type, readonly ActivityContract[]>()
  readonly queues = new Map<string, QueueOptions>()
  private readonly ownedQueues = new Map<string, OwnedQueue>()
  private readonly features = new Map<symbol, Feature>()
  private readonly activityOwners = new Map<Type, Feature>()
  private sealed = false

  constructor(private readonly options: WorkflowsOptions) {}

  key(name: string, version: number): string {
    return JSON.stringify([name, version])
  }

  contract(provider: WorkflowClass): RegisteredWorkflow {
    // SAFETY: @Workflow validates and owns this metadata.
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
    if (this.sealed)
      throw new WorkflowError(
        'UNREGISTERED_WORKFLOW',
        `${provider.name} must be registered in workflows or clients`
      )
    const contract: RegisteredWorkflow = {
      provider,
      options,
      definition: workflowDefinition(this.options.namespace, options.name, options.version)
    }
    this.workflows.set(key, contract)
    return contract
  }

  workflow(name: string, version: number): RegisteredWorkflow {
    const registered = this.workflows.get(this.key(name, version))
    if (!registered)
      throw new WorkflowError('MISSING_WORKFLOW_VERSION', `${name}@${version} is not registered`)
    return registered
  }

  activityContracts(provider: Type): readonly ActivityContract[] {
    const contracts = this.providers.get(provider)
    if (!contracts)
      throw new WorkflowError(
        'UNREGISTERED_ACTIVITY',
        `${provider.name} must be registered in activities or activityContracts`
      )
    return contracts
  }

  activitiesFor(workflow: RegisteredWorkflow, provider: Type): readonly ActivityContract[] {
    const owner = this.activityOwners.get(provider)
    if (!owner) return this.activityContracts(provider)
    if (
      !workflow.owner ||
      (owner !== workflow.owner &&
        !this.importsExport(workflow.owner, activityToken(provider), owner.registration.id))
    )
      throw new WorkflowError(
        'ACTIVITY_NOT_VISIBLE',
        `${provider.name} is not exported to feature ${workflow.owner?.registration.name ?? 'unknown'}`
      )
    return this.activityContracts(provider)
  }

  childContract(parent: RegisteredWorkflow, provider: WorkflowClass): RegisteredWorkflow {
    const feature = parent.owner
    const declared =
      feature &&
      [
        ...(feature.registration.structure.clients ?? []),
        ...(feature.registration.structure.workflows ?? []).map(handlerClass)
      ].includes(provider)
    if (
      !feature ||
      (!declared && !visibleProviders(feature.host, getWorkflowToken(provider)).length)
    )
      throw new WorkflowError(
        'WORKFLOW_NOT_VISIBLE',
        `${provider.name} must be imported or registered as a client in the parent feature`
      )
    return this.contract(provider)
  }

  /** Resolve every feature before opening storage or starting workers. */
  discover(discovery: DiscoveryService): void {
    if (this.sealed)
      throw new WorkflowError('REGISTRY_SEALED', 'The workflow catalog is already initialized')
    const names = new Set<string>()
    for (const wrapper of discovery.getProviders()) {
      if (!wrapper.metatype || !Reflect.hasOwnMetadata(FEATURE_FACTORY, wrapper.metatype)) continue
      if (!wrapper.isDependencyTreeStatic())
        throw new WorkflowError(
          'UNSUPPORTED_SCOPE',
          'Feature configuration and handlers require singleton dependencies'
        )
      const registration = wrapper.instance
      if (!(registration instanceof FeatureRegistration) || !wrapper.host)
        throw new WorkflowError(
          'INVALID_FEATURE',
          'Feature registration was not initialized by Nest'
        )
      if (this.features.has(registration.id)) continue
      const { structure, configuration } = registration
      if (!configuration || Array.isArray(configuration))
        throw new WorkflowError('INVALID_FEATURE', 'Feature factory must return configuration')
      const owns =
        (structure.workflows?.length ?? 0) +
          (structure.activities?.length ?? 0) +
          (structure.activityContracts?.length ?? 0) +
          (configuration.queues?.length ?? 0) >
          0 ||
        configuration.defaults !== undefined ||
        configuration.execution !== undefined
      if (owns && !structure.name)
        throw new WorkflowError(
          'FEATURE_NAME_REQUIRED',
          'A feature owning handlers, contracts or configuration needs a name'
        )
      if (structure.name) {
        identifier(structure.name, 'Feature name')
        if (names.has(structure.name))
          throw new WorkflowError(
            'DUPLICATE_FEATURE',
            `Feature ${structure.name} was independently registered more than once; reuse its module`
          )
        names.add(structure.name)
      }
      this.features.set(registration.id, { registration, host: wrapper.host })
    }
    this.validateDefaults(this.options.defaults)
    for (const registration of this.queueList(this.options.queues))
      this.registerQueue(registration, null)
    for (const feature of this.features.values()) {
      this.validateDefaults(feature.registration.configuration.defaults)
      for (const queue of this.queueList(feature.registration.configuration.queues))
        this.registerQueue(queue, feature)
    }
    const overrides = new Set<string>()
    for (const override of this.queueList(this.options.queueOverrides)) {
      const name = queueName(override.queue)
      const entry = this.ownedQueues.get(name)
      if (!entry) throw new WorkflowError('UNKNOWN_QUEUE_OVERRIDE', name)
      if (overrides.has(name)) throw new WorkflowError('DUPLICATE_QUEUE_OVERRIDE', name)
      overrides.add(name)
      const options = resolveQueue(entry.options, override)
      this.ownedQueues.set(name, { ...entry, options })
      this.queues.set(name, options)
    }
    for (const queue of this.options.execution?.activities?.queues ?? [])
      if (!this.queues.has(queueName(queue)))
        throw new WorkflowError('UNKNOWN_QUEUE', queueName(queue))
    for (const feature of this.features.values()) {
      const { structure, configuration } = feature.registration
      const execution = configuration.execution
      if (execution?.workflows?.concurrency !== undefined)
        positiveInteger(execution.workflows.concurrency, 'Feature workflow concurrency')
      for (const queue of execution?.activities?.queues ?? []) this.requireQueue(feature, queue)
      if (configuration.defaults?.activities?.queue)
        this.requireQueue(feature, configuration.defaults.activities.queue)
      for (const entry of structure.workflows ?? []) {
        requireSingleton(feature.host, handlerToken(entry))
        const provider = handlerClass(entry)
        const contract = this.contract(provider)
        if (contract.handler)
          throw new WorkflowError(
            'DUPLICATE_WORKFLOW_PROVIDER',
            `${provider.name} already belongs to another feature`
          )
        const instance = feature.registration.instances.get(provider)
        const handler = instance?.run
        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Validate Nest overrides at the execution boundary.
        if (typeof handler !== 'function')
          throw new WorkflowError('MISSING_HANDLER', `${provider.name}.run is required`)
        contract.owner = feature
        contract.enabled =
          this.options.execution?.workflows?.enabled !== false &&
          execution?.workflows?.enabled !== false
        if (execution?.workflows?.concurrency !== undefined)
          contract.concurrency = execution.workflows.concurrency
        contract.handler = (input, context) => handler.call(instance, input, context)
      }
      for (const provider of structure.clients ?? []) this.contract(provider)
      const implemented = new Set((structure.activities ?? []).map(handlerClass))
      const contracts = [...implemented, ...(structure.activityContracts ?? [])]
      if (new Set(contracts).size !== contracts.length)
        throw new WorkflowError(
          'DUPLICATE_ACTIVITY_PROVIDER',
          'Use either activities or activityContracts for an owning provider'
        )
      for (const provider of contracts) {
        if (this.activityOwners.has(provider))
          throw new WorkflowError(
            'DUPLICATE_ACTIVITY_PROVIDER',
            `${provider.name} already has an owning feature; import its exports instead`
          )
        this.activityOwners.set(provider, feature)
        const instance = feature.registration.instances.get(provider)
        const registration = structure.activities?.find((entry) => handlerClass(entry) === provider)
        if (registration) requireSingleton(feature.host, handlerToken(registration))
        const resolved = this.resolveActivities(provider, feature)
        this.providers.set(provider, Object.freeze(resolved))
        if (!implemented.has(provider)) continue
        for (const activity of resolved) {
          const key = this.key(activity.options.name, activity.options.version)
          if (this.activities.has(key)) throw new WorkflowError('DUPLICATE_ACTIVITY', key)
          const handler = instance?.[activity.method]
          // oxlint-disable-next-line anti-slop/no-runtime-typeof -- An overridden Nest provider must implement the declared contract.
          if (typeof handler !== 'function')
            throw new WorkflowError('MISSING_HANDLER', `${provider.name}.${activity.method}`)
          const enabled =
            this.options.execution?.activities?.enabled !== false &&
            execution?.activities?.enabled !== false &&
            selected(this.options.execution?.activities?.queues, activity.options.queue) &&
            selected(execution?.activities?.queues, activity.options.queue)
          this.activities.set(
            key,
            Object.freeze({
              ...activity,
              enabled,
              invoke: (input: any, context: ActivityContext) =>
                handler.call(instance, input, context)
            })
          )
        }
      }
    }
    // Even contract-only providers must not disagree about an activity identity.
    const identities = new Set<string>()
    for (const contracts of this.providers.values())
      for (const contract of contracts) {
        const key = this.key(contract.options.name, contract.options.version)
        if (identities.has(key)) throw new WorkflowError('DUPLICATE_ACTIVITY', key)
        identities.add(key)
      }
    for (const feature of this.features.values()) {
      for (const queue of feature.registration.structure.exports?.queues ?? [])
        this.requireQueue(feature, queue)
      for (const provider of feature.registration.structure.exports?.activities ?? []) {
        const owner = this.activityOwners.get(provider)
        if (
          !owner ||
          (owner !== feature &&
            !this.importsExport(feature, activityToken(provider), owner.registration.id))
        )
          throw new WorkflowError(
            'INVALID_FEATURE_EXPORT',
            `${provider.name} is neither owned nor imported by ${feature.registration.name}`
          )
        this.activityContracts(provider)
      }
    }
    this.sealed = true
    for (const workflow of this.workflows.values()) Object.freeze(workflow)
  }

  private importsExport(
    feature: Feature,
    token: symbol,
    owner: symbol,
    visited = new Set<symbol>()
  ): boolean {
    if (visited.has(feature.registration.id)) return false
    visited.add(feature.registration.id)
    // Reexports must ultimately resolve to the real owner. A cycle of export markers
    // cannot manufacture visibility to an unrelated feature's private queue/contract.
    for (const imported of feature.host.imports) {
      for (const marker of exportedMarkers(imported, token)) {
        if (marker.feature === owner) return true
        const next = this.features.get(marker.feature)
        if (next && this.importsExport(next, token, owner, visited)) return true
      }
    }
    return false
  }

  private requireQueue(feature: Feature, reference: QueueReference): OwnedQueue {
    const name = queueName(reference)
    const queue = this.ownedQueues.get(name)
    if (!queue)
      throw new WorkflowError(
        'UNKNOWN_QUEUE',
        `${name} referenced by ${feature.registration.name} is not registered`
      )
    if (
      queue.owner !== null &&
      queue.owner !== feature.registration.id &&
      !this.importsExport(feature, queueToken(reference), queue.owner)
    )
      throw new WorkflowError(
        'QUEUE_NOT_VISIBLE',
        `${name} must be exported by its owning module and imported by ${feature.registration.name}`
      )
    return queue
  }

  private registerQueue(registration: QueueRegistration, feature: Feature | null): void {
    const name = queueName(registration.queue)
    if (this.ownedQueues.has(name))
      throw new WorkflowError(
        'DUPLICATE_QUEUE_OWNER',
        `${name} has multiple configuration owners; use queueOverrides for deployment settings`
      )
    const options = resolveQueue(
      this.options.defaults?.queues,
      feature?.registration.configuration.defaults?.queues,
      registration
    )
    this.ownedQueues.set(name, {
      reference: registration.queue,
      owner: feature?.registration.id ?? null,
      options
    })
    this.queues.set(name, options)
  }

  private queueList(
    queues: readonly QueueRegistration[] | undefined
  ): readonly QueueRegistration[] {
    if (queues !== undefined && !Array.isArray(queues))
      throw new WorkflowError(
        'INVALID_QUEUE_CONFIGURATION',
        'queues must be an array of { queue: defineQueue(name), ...settings }'
      )
    return queues ?? []
  }

  private validateDefaults(defaults: WorkflowsOptions['defaults']): void {
    if (defaults?.queues) resolveQueue(defaults.queues)
    if (defaults?.activities) validateActivityDefaults(defaults.activities)
  }

  private resolveActivities(provider: Type, feature: Feature): ActivityContract[] {
    // SAFETY: @Activities writes and validates these defaults; getOwnMetadata avoids implicit class ownership inheritance.
    const classDefaults = Reflect.getOwnMetadata(ACTIVITIES_METADATA, provider) as
      | ActivityDefaults
      | undefined
    if (!classDefaults)
      throw new WorkflowError('MISSING_DECORATOR', `${provider.name} has no @Activities decorator`)
    const methods = new Set<string>()
    const contracts: ActivityContract[] = []
    for (
      let prototype = provider.prototype;
      prototype && prototype !== Object.prototype;
      prototype = Object.getPrototypeOf(prototype)
    ) {
      for (const method of Object.getOwnPropertyNames(prototype)) {
        if (methods.has(method)) continue
        methods.add(method)
        // SAFETY: @Activity is the only writer of validated method metadata.
        const declared = Reflect.getOwnMetadata(ACTIVITY_METADATA, prototype, method) as
          | ActivityOptions<any, any>
          | undefined
        if (!declared) continue
        const defaults = mergeActivityDefaults(
          this.options.defaults?.activities,
          feature.registration.configuration.defaults?.activities,
          classDefaults,
          declared
        )
        if (!defaults.queue)
          throw new WorkflowError(
            'ACTIVITY_QUEUE_REQUIRED',
            `${provider.name}.${method} needs an explicit or inherited queue`
          )
        const queue = this.requireQueue(feature, defaults.queue)
        if (queue.options.perKeyConcurrency !== undefined && !declared.key)
          throw new WorkflowError(
            'ACTIVITY_KEY_REQUIRED',
            `${declared.name} needs key for ${queue.reference.name}`
          )
        const options = Object.freeze({ ...declared, ...defaults, queue: queue.reference.name })
        contracts.push(Object.freeze({ provider, method, options }))
      }
    }
    return contracts
  }
}

function exportedMarkers(
  host: FeatureHost,
  token: symbol,
  seen = new Set<FeatureHost>()
): readonly FeatureExport[] {
  if (seen.has(host)) return []
  seen.add(host)
  const local = host.providers.get(token)
  if (host.exports.has(token) && local?.instance instanceof FeatureExport) return [local.instance]
  const markers: FeatureExport[] = []
  for (const child of host.imports)
    if (host.exports.has(child.metatype)) markers.push(...exportedMarkers(child, token, seen))
  return markers
}

function selected(queues: readonly QueueReference[] | undefined, name: string): boolean {
  return queues === undefined || queues.some((queue) => queueName(queue) === name)
}

function mergeActivityDefaults(
  ...layers: readonly (ActivityDefaults | undefined)[]
): ActivityDefaults {
  let result: ActivityDefaults = {}
  for (const layer of layers) {
    if (!layer) continue
    validateActivityDefaults(layer)
    if (layer.queue !== undefined) result = { ...result, queue: layer.queue }
    if (layer.retry !== undefined) result = { ...result, retry: Object.freeze({ ...layer.retry }) }
    if (layer.timeout !== undefined) result = { ...result, timeout: layer.timeout }
  }
  return Object.freeze(result)
}

function resolveQueue(...layers: readonly (QueueSettings | undefined)[]): QueueOptions {
  let concurrency = 4
  let globalConcurrency: number | null = null
  let perKeyConcurrency: number | null = null
  for (const layer of layers) {
    if (!layer) continue
    if (layer.concurrency !== undefined) {
      positiveInteger(layer.concurrency, 'Queue concurrency')
      concurrency = layer.concurrency
    }
    if (layer.globalConcurrency !== undefined) {
      if (layer.globalConcurrency !== null)
        positiveInteger(layer.globalConcurrency, 'Queue global concurrency')
      globalConcurrency = layer.globalConcurrency
    }
    if (layer.perKeyConcurrency !== undefined) {
      if (layer.perKeyConcurrency !== null)
        positiveInteger(layer.perKeyConcurrency, 'Queue per-key concurrency')
      perKeyConcurrency = layer.perKeyConcurrency
    }
  }
  let result: QueueOptions = { concurrency }
  if (globalConcurrency !== null) result = { ...result, globalConcurrency }
  if (perKeyConcurrency !== null) result = { ...result, perKeyConcurrency }
  return Object.freeze(result)
}
