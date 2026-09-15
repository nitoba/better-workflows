import { Scope } from '@nestjs/common'
import type { InjectionToken, Type } from '@nestjs/common'
import type { DiscoveryService } from '@nestjs/core'
import type { FeatureConfiguration, FeatureStructure, HandlerRegistration } from '../types'
import { WorkflowError } from '../errors'

export const FEATURE_FACTORY = Symbol.for('better-workflows/feature-factory')
const ACTIVITY_TOKENS = new WeakMap<Type, symbol>()

export function activityToken(provider: Type): symbol {
  let token = ACTIVITY_TOKENS.get(provider)
  if (!token) {
    token = Symbol(`better-workflows/activity-contract/${provider.name}`)
    ACTIVITY_TOKENS.set(provider, token)
  }
  return token
}

export function handlerClass<T extends Type>(entry: HandlerRegistration<T>): T {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Public Nest registration boundary: class or useExisting descriptor.
  return typeof entry === 'function' ? entry : entry.provide
}

export function handlerToken(entry: HandlerRegistration): InjectionToken {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Existing instances must be resolved by Nest, never constructed by the library.
  return typeof entry === 'function' ? entry : entry.useExisting
}

export class FeatureRegistration {
  constructor(
    readonly id: symbol,
    readonly structure: FeatureStructure,
    readonly configuration: FeatureConfiguration,
    readonly instances: ReadonlyMap<Type, any>
  ) {}

  get name(): string {
    return this.structure.name ?? 'clients'
  }
}

/** A capability exported using Nest's actual module imports/exports, not a global registry lookup. */
export class FeatureExport {
  constructor(readonly feature: symbol) {}
}

type Wrapper = ReturnType<DiscoveryService['getProviders']>[number]
export type FeatureHost = NonNullable<Wrapper['host']>

/** Mirror Nest export traversal; do not use ModuleRef.get({ strict: false }). */
export function visibleProviders(host: FeatureHost, token: InjectionToken): readonly Wrapper[] {
  const local = host.providers.get(token)
  if (local) return [local]
  const visited = new Set<FeatureHost>()
  const matches = new Set<Wrapper>()
  const visit = (module: FeatureHost): void => {
    if (visited.has(module)) return
    visited.add(module)
    const provider = module.providers.get(token)
    if (provider && module.exports.has(token)) {
      matches.add(provider)
      return
    }
    for (const imported of module.imports)
      if (module.exports.has(imported.metatype)) visit(imported)
  }
  for (const imported of host.imports) visit(imported)
  return [...matches]
}

export function requireSingleton(host: FeatureHost, token: InjectionToken): void {
  const candidates = visibleProviders(host, token)
  for (const wrapper of candidates) {
    if (
      !wrapper.isDependencyTreeStatic() ||
      (wrapper.scope !== undefined && wrapper.scope !== Scope.DEFAULT)
    )
      throw new WorkflowError(
        'UNSUPPORTED_SCOPE',
        'Workflow and activity implementations require singleton dependency trees'
      )
  }
  if (!candidates.length)
    throw new WorkflowError(
      'MISSING_HANDLER',
      `Handler token ${String(token)} is not visible in its feature`
    )
  if (new Set(candidates.map((wrapper) => wrapper.instance)).size > 1)
    throw new WorkflowError(
      'AMBIGUOUS_HANDLER',
      `Multiple imported providers export ${String(token)}`
    )
}
