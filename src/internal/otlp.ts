import { Layer, Predicate, References } from 'effect'
import * as FetchHttpClient from 'effect/unstable/http/FetchHttpClient'
import * as OtlpLogger from 'effect/unstable/observability/OtlpLogger'
import * as OtlpMetrics from 'effect/unstable/observability/OtlpMetrics'
import * as OtlpSerialization from 'effect/unstable/observability/OtlpSerialization'
import * as OtlpTracer from 'effect/unstable/observability/OtlpTracer'
import { WorkflowError } from '../errors'
import type {
  Duration,
  OtlpLogLevel,
  OtlpLogsOptions,
  OtlpMetricsOptions,
  OtlpObservabilityOptions,
  OtlpOptions,
  OtlpSignalOptions,
  WorkflowsOptions
} from '../types'
import { milliseconds, identifier, positiveInteger } from './values'
import { betterWorkflowsVersion } from './version'
import { metricRegistryLayer, telemetryLayerWithRegistry } from './telemetry'

const LOG_LEVELS: ReadonlySet<OtlpLogLevel> = new Set([
  'trace',
  'debug',
  'info',
  'warn',
  'error',
  'fatal',
  'none'
])

const effectLogLevel: Record<
  OtlpLogLevel,
  'Trace' | 'Debug' | 'Info' | 'Warn' | 'Error' | 'Fatal' | 'None'
> = {
  trace: 'Trace',
  debug: 'Debug',
  info: 'Info',
  warn: 'Warn',
  error: 'Error',
  fatal: 'Fatal',
  none: 'None'
}

function configurationError(message: string): never {
  throw new WorkflowError('INVALID_CONFIGURATION', message)
}

function validateSignal(signal: boolean | OtlpSignalOptions | undefined, label: string): void {
  if (signal === undefined || Predicate.isBoolean(signal)) return
  if (!Predicate.isObject(signal)) configurationError(`${label} must be a boolean or object`)
  // SAFETY: Predicate.isObject above excludes booleans, null and primitive signal values.
  const options = signal as OtlpSignalOptions
  if (options.enabled !== undefined && !Predicate.isBoolean(options.enabled))
    configurationError(`${label}.enabled must be a boolean`)
  if (options.exportInterval !== undefined)
    validateInterval(options.exportInterval, `${label}.exportInterval`)
}

function validateInterval(value: Duration, label: string): void {
  const interval = configurationMilliseconds(value, label)
  if (interval < 1) configurationError(`${label} must be positive`)
}

function configurationMilliseconds(value: Duration, label: string): number {
  try {
    return milliseconds(value)
  } catch {
    configurationError(`${label} must be a valid duration`)
  }
}

function validateAttributes(attributes: OtlpOptions['attributes']): void {
  if (attributes === undefined) return
  if (!Predicate.isObject(attributes))
    configurationError('observability.attributes must be an object')
  for (const [key, value] of Object.entries(attributes)) {
    // oxlint-disable-next-line eslint/no-control-regex -- Configuration keys reject ASCII control characters.
    if (!key || /[\u0000-\u001f]/.test(key))
      configurationError('Observability attribute names must be printable')
    if (!Predicate.isString(value) && !Predicate.isNumber(value) && !Predicate.isBoolean(value))
      configurationError(`Observability attribute ${key} must be a string, number or boolean`)
    if (Predicate.isNumber(value) && !Number.isFinite(value))
      configurationError(`Observability attribute ${key} must be finite`)
  }
}

function validateHeaders(headers: OtlpOptions['headers']): void {
  if (headers === undefined) return
  if (!Predicate.isObject(headers)) configurationError('observability.headers must be an object')
  for (const [key, value] of Object.entries(headers)) {
    // oxlint-disable-next-line eslint/no-control-regex -- Header names reject ASCII control characters.
    if (!key || /[\u0000-\u001f\u007f]/.test(key))
      configurationError('OTLP header names must be printable')
    if (!Predicate.isString(value) || /[\r\n]/.test(value))
      configurationError(`OTLP header ${key} must be a string without line breaks`)
  }
}

/** Validate OTLP settings before infrastructure layers are created. */
export function validateOtlpOptions(options: OtlpOptions | OtlpObservabilityOptions): void {
  if (!Predicate.isObject(options))
    configurationError('observability must be an OTLP configuration')
  if ('kind' in options && options.kind !== 'otlp')
    configurationError('observability.kind must be otlp')
  identifier(options.serviceName, 'OTLP serviceName')
  if (options.serviceVersion !== undefined)
    identifier(options.serviceVersion, 'OTLP serviceVersion')

  let endpoint: URL
  try {
    endpoint = new URL(options.endpoint)
  } catch {
    configurationError('OTLP endpoint must be a valid URL')
  }
  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:')
    configurationError('OTLP endpoint must use http or https')
  if (endpoint.username || endpoint.password)
    configurationError('OTLP endpoint must not contain credentials')
  if (endpoint.search || endpoint.hash)
    configurationError('OTLP endpoint must not contain a query or fragment')

  validateSignal(options.traces, 'observability.traces')
  validateSignal(options.metrics, 'observability.metrics')
  validateSignal(options.logs, 'observability.logs')
  if (options.exportInterval !== undefined)
    validateInterval(options.exportInterval, 'observability.exportInterval')
  if (options.maxBatchSize !== undefined) positiveInteger(options.maxBatchSize, 'OTLP maxBatchSize')
  if (options.shutdownTimeout !== undefined) {
    const shutdownTimeout = configurationMilliseconds(
      options.shutdownTimeout,
      'observability.shutdownTimeout'
    )
    if (shutdownTimeout < 0) configurationError('observability.shutdownTimeout must be nonnegative')
  }
  if (
    options.metricsTemporality !== undefined &&
    options.metricsTemporality !== 'cumulative' &&
    options.metricsTemporality !== 'delta'
  )
    configurationError('OTLP metricsTemporality must be cumulative or delta')

  if (Predicate.isObject(options.metrics) && !Predicate.isBoolean(options.metrics)) {
    // SAFETY: Predicate.isObject above excludes the boolean metrics shorthand.
    const metrics = options.metrics as OtlpMetricsOptions
    if (
      metrics.temporality !== undefined &&
      metrics.temporality !== 'cumulative' &&
      metrics.temporality !== 'delta'
    )
      configurationError('OTLP metrics.temporality must be cumulative or delta')
  }
  if (Predicate.isObject(options.logs) && !Predicate.isBoolean(options.logs)) {
    // SAFETY: Predicate.isObject above excludes the boolean logs shorthand.
    const logs = options.logs as OtlpLogsOptions
    if (logs.level !== undefined && !LOG_LEVELS.has(logs.level))
      configurationError('OTLP logs.level is invalid')
  }
  validateAttributes(options.attributes)
  validateHeaders(options.headers)
}

interface SignalSettings {
  readonly enabled: boolean
  readonly exportInterval: number | undefined
}

function signalSettings(
  signal: boolean | OtlpSignalOptions | undefined,
  fallback: boolean,
  commonInterval: Duration | undefined
): SignalSettings {
  if (signal === undefined)
    return {
      enabled: fallback,
      exportInterval: commonInterval === undefined ? undefined : milliseconds(commonInterval)
    }
  if (Predicate.isBoolean(signal))
    return {
      enabled: signal,
      exportInterval: commonInterval === undefined ? undefined : milliseconds(commonInterval)
    }
  return {
    enabled: signal.enabled ?? true,
    exportInterval:
      signal.exportInterval === undefined
        ? commonInterval === undefined
          ? undefined
          : milliseconds(commonInterval)
        : milliseconds(signal.exportInterval)
  }
}

function role(
  options: WorkflowsOptions
): 'producer' | 'orchestrator' | 'activity-worker' | 'mixed' {
  const workflows = options.execution?.workflows?.enabled !== false
  const activities = options.execution?.activities?.enabled !== false
  if (workflows && activities) return 'mixed'
  if (workflows) return 'orchestrator'
  if (activities) return 'activity-worker'
  return 'producer'
}

interface OtlpResource {
  readonly serviceName: string
  readonly serviceVersion?: string
  readonly attributes: Record<string, string | number | boolean>
}

/** Resource metadata shared by the trace, metric and log exporters. */
export function otlpResource(options: WorkflowsOptions): OtlpResource {
  const observability = options.observability
  if (!observability) configurationError('OTLP observability is not configured')
  const resource: OtlpResource = {
    serviceName: observability.serviceName,
    attributes: {
      ...observability.attributes,
      'better_workflows.namespace': options.namespace,
      'better_workflows.version': betterWorkflowsVersion,
      'better_workflows.topology': options.topology ?? 'single-node',
      'better_workflows.storage.driver': options.storage.driver,
      'better_workflows.role': role(options)
    }
  }
  if (observability.serviceVersion === undefined) return resource
  return { ...resource, serviceVersion: observability.serviceVersion }
}

function enabledSignal(
  signal: boolean | OtlpSignalOptions | undefined,
  fallback: boolean,
  commonInterval: Duration | undefined
) {
  return signalSettings(signal, fallback, commonInterval)
}

function mergeLayers(left: Layer.Any, right: Layer.Any): Layer.Any {
  // SAFETY: all layers are internal Effect layers and the casts only erase their
  // service unions while constructing the same merged layer.
  return Layer.merge(
    left as Layer.Layer<any, any, any>,
    right as Layer.Layer<any, any, any>
  ) as Layer.Any
}

/** Build the internal OTLP layer and share its metric registry with instrumentation. */
export function otlpLayer(options: WorkflowsOptions): Layer.Layer<any, never, never> {
  const observability = options.observability
  if (!observability) return telemetryLayerWithRegistry
  validateOtlpOptions(observability)

  const traces = enabledSignal(observability.traces, true, observability.exportInterval)
  const metrics = enabledSignal(observability.metrics, false, observability.exportInterval)
  const logs = enabledSignal(observability.logs, false, observability.exportInterval)
  const resource = otlpResource(options)
  const headers = observability.headers
  const shutdownTimeout =
    observability.shutdownTimeout === undefined
      ? undefined
      : milliseconds(observability.shutdownTimeout)
  const maxBatchSize = observability.maxBatchSize
  let signals: Layer.Any = Layer.empty

  if (traces.enabled)
    signals = mergeLayers(
      signals,
      OtlpTracer.layer({
        url: `${observability.endpoint.replace(/\/+$/, '')}/v1/traces`,
        resource,
        headers,
        exportInterval: traces.exportInterval,
        maxBatchSize,
        shutdownTimeout
      })
    )
  if (metrics.enabled) {
    // SAFETY: Predicate.isObject above excludes the boolean metrics shorthand.
    const configuredMetrics = Predicate.isObject(observability.metrics)
      ? (observability.metrics as OtlpMetricsOptions)
      : undefined
    signals = mergeLayers(
      signals,
      OtlpMetrics.layer({
        url: `${observability.endpoint.replace(/\/+$/, '')}/v1/metrics`,
        resource,
        headers,
        exportInterval: metrics.exportInterval,
        shutdownTimeout,
        temporality: configuredMetrics?.temporality ?? observability.metricsTemporality
      })
    )
  }
  if (logs.enabled) {
    // SAFETY: Predicate.isObject above excludes the boolean logs shorthand.
    const configuredLogs = Predicate.isObject(observability.logs)
      ? (observability.logs as OtlpLogsOptions)
      : undefined
    signals = mergeLayers(
      signals,
      OtlpLogger.layer({
        url: `${observability.endpoint.replace(/\/+$/, '')}/v1/logs`,
        resource,
        headers,
        exportInterval: logs.exportInterval,
        maxBatchSize,
        shutdownTimeout,
        mergeWithExisting: true
      })
    )
    signals = mergeLayers(
      signals,
      Layer.succeed(References.MinimumLogLevel, effectLogLevel[configuredLogs?.level ?? 'info'])
    )
  }
  if (!traces.enabled && !metrics.enabled && !logs.enabled) return telemetryLayerWithRegistry

  // SAFETY: every signal layer is supplied with serialization, HTTP, metric registry
  // and telemetry services before it is returned to the infrastructure runtime.
  const preparedSignals = (signals as Layer.Layer<any, never, any>).pipe(
    Layer.provide(OtlpSerialization.layerJson),
    Layer.provide(FetchHttpClient.layer),
    Layer.provide(metricRegistryLayer),
    Layer.provideMerge(telemetryLayerWithRegistry)
  )
  // SAFETY: the prepared layer has no remaining construction requirements after all providers above.
  return preparedSignals as Layer.Layer<any, never, never>
}
