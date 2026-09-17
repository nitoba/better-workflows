import type {
  OtlpLogLevel,
  OtlpLogsOptions,
  OtlpMetricsOptions,
  OtlpObservabilityOptions,
  OtlpOptions,
  OtlpSignalOptions,
  ObservabilityOptions
} from './types'
import { validateOtlpOptions } from './internal/otlp'

/**
 * Configure best-effort OTLP/HTTP export for a workflow runtime.
 *
 * The returned value is consumed by `WorkflowsModule.forRoot`; it does not expose
 * Effect layers, tracers, metrics or services. Traces default to enabled, while
 * metrics and logs are opt-in. Collector failures are isolated from workflows.
 *
 * @param options - Service identity, collector endpoint and optional signals.
 * @returns A validated observability configuration for `WorkflowsOptions.observability`.
 * @throws WorkflowError with INVALID_CONFIGURATION for invalid local settings.
 * @example
 * ```ts
 * import { WorkflowsModule } from 'better-workflows'
 * import { otlp } from 'better-workflows/observability'
 * import { sqlite } from 'better-workflows/sqlite'
 *
 * WorkflowsModule.forRoot({
 *   namespace: 'reports',
 *   storage: sqlite({ filename: './reports.sqlite' }),
 *   observability: otlp({
 *     serviceName: 'reports-worker',
 *     endpoint: 'http://otel-collector:4318',
 *     traces: true,
 *     metrics: { enabled: true, exportInterval: '10s' },
 *     logs: { enabled: true, level: 'info' }
 *   })
 * })
 * ```
 */
export function otlp(options: OtlpOptions): OtlpObservabilityOptions {
  validateOtlpOptions(options)
  const configured = { ...options, kind: 'otlp' as const }
  if (configured.attributes !== undefined)
    configured.attributes = Object.freeze({ ...configured.attributes })
  if (configured.headers !== undefined)
    configured.headers = Object.freeze({ ...configured.headers })
  return Object.freeze(configured)
}

export type {
  OtlpLogLevel,
  OtlpLogsOptions,
  OtlpMetricsOptions,
  OtlpObservabilityOptions,
  OtlpOptions,
  OtlpSignalOptions,
  ObservabilityOptions
}
