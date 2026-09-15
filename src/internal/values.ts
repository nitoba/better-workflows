import type { StandardSchemaV1 } from '@standard-schema/spec'
import { Schema, SchemaGetter } from 'effect'
import { WorkflowError } from '../errors'
import type { Duration, JsonValue } from '../types'

const UNITS = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }
const MAX_BYTES = 1_048_576

const JsonValueSchema = Schema.Unknown.pipe(
  Schema.decodeTo(Schema.Json, {
    decode: SchemaGetter.transform((input) => {
      const ancestors = new Set<object>()

      // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The schema transformation receives untrusted values by design.
      const visit = (value: unknown): JsonValue => {
        if (value === null) return null
        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Required to reject lossy JSON serialization at the persistence boundary.
        switch (typeof value) {
          case 'string':
          case 'boolean':
            return value
          case 'number':
            if (!Number.isFinite(value)) {
              throw new WorkflowError('INVALID_TRANSPORT', 'Non-finite number')
            }
            return value
          case 'object': {
            if (ancestors.has(value)) {
              throw new WorkflowError('INVALID_TRANSPORT', 'Circular value')
            }
            ancestors.add(value)
            try {
              if (Object.getOwnPropertySymbols(value).length > 0) {
                throw new WorkflowError('INVALID_TRANSPORT', 'Symbol properties are not supported')
              }
              if (Array.isArray(value)) {
                if (
                  Object.getPrototypeOf(value) !== Array.prototype ||
                  Object.getOwnPropertyNames(value).length !== value.length + 1
                ) {
                  throw new WorkflowError(
                    'INVALID_TRANSPORT',
                    'Sparse arrays, subclasses and custom array properties are not supported'
                  )
                }
                return Array.from({ length: value.length }, (_, index) => {
                  const descriptor = Object.getOwnPropertyDescriptor(value, String(index))
                  if (!descriptor || descriptor.get || descriptor.set || !descriptor.enumerable) {
                    throw new WorkflowError(
                      'INVALID_TRANSPORT',
                      'Sparse arrays and accessor elements are not supported'
                    )
                  }
                  return visit(descriptor.value)
                })
              }
              if (
                Object.getPrototypeOf(value) !== Object.prototype &&
                Object.getPrototypeOf(value) !== null
              ) {
                throw new WorkflowError(
                  'INVALID_TRANSPORT',
                  'Use plain JSON objects, not Date or class instances'
                )
              }
              const entries = Object.entries(Object.getOwnPropertyDescriptors(value)).sort(
                ([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)
              )
              return Object.fromEntries(
                entries.map(([key, descriptor]) => {
                  if (descriptor.get || descriptor.set || !descriptor.enumerable) {
                    throw new WorkflowError(
                      'INVALID_TRANSPORT',
                      'Getters, setters and non-enumerable properties are not supported'
                    )
                  }
                  return [key, visit(descriptor.value)]
                })
              )
            } finally {
              ancestors.delete(value)
            }
          }
          default:
            throw new WorkflowError(
              'INVALID_TRANSPORT',
              'Use JSON values; undefined is only supported as a void root result'
            )
        }
      }

      return visit(input)
    }),
    encode: SchemaGetter.transform((value) => value)
  })
)

const TransportEnvelope = Schema.Union([
  Schema.Tuple([Schema.Literal('void')]),
  Schema.Tuple([Schema.Literal('json'), JsonValueSchema])
])
const Transport = Schema.fromJsonString(TransportEnvelope)

export function milliseconds(value: Duration): number {
  let duration: number
  if (Number.isFinite(value)) {
    // SAFETY: Number.isFinite only accepts primitive finite numbers at runtime.
    duration = value as number
  } else {
    const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(String(value))
    if (!match) throw new WorkflowError('INVALID_DURATION', `Invalid duration: ${value}`)
    // SAFETY: the regular expression captures only keys of UNITS.
    duration = Number(match[1]) * UNITS[match[2] as keyof typeof UNITS]
  }
  if (!Number.isSafeInteger(duration) || duration < 0) {
    throw new WorkflowError(
      'INVALID_DURATION',
      'Durations must be nonnegative safe integer milliseconds'
    )
  }
  return duration
}

export function identifier(value: string, label: string): void {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof, eslint/no-control-regex -- Validate untyped configuration and intentionally reject ASCII control characters.
  if (typeof value !== 'string' || !value || value.length > 256 || /[\u0000-\u001f]/.test(value)) {
    throw new WorkflowError(
      'INVALID_IDENTIFIER',
      `${label} must contain 1–256 printable characters`
    )
  }
}

export function positiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) {
    throw new WorkflowError(
      'INVALID_CONFIGURATION',
      `${label} must be an integer between 1 and 2147483647`
    )
  }
}

/** The envelope distinguishes a void result from a legitimate JSON object. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Validate arbitrary handler output before persisting it.
export function encode(value: unknown): string {
  const envelope =
    value === undefined
      ? (['void'] as const)
      : (['json', Schema.decodeUnknownSync(JsonValueSchema)(value)] as const)
  const result = Schema.encodeSync(Transport)(envelope)
  if (Buffer.byteLength(result) > MAX_BYTES) {
    throw new WorkflowError('PAYLOAD_TOO_LARGE', `Encoded payload exceeds ${MAX_BYTES} bytes`)
  }
  return result
}

export function decode<T>(value: string): T {
  try {
    const envelope = Schema.decodeUnknownSync(Transport)(value)
    const [kind, data] = envelope
    // SAFETY: values were validated by the registered Standard Schema before encoding and are revalidated at handler boundaries.
    return (kind === 'void' ? undefined : data) as T
  } catch {
    throw new WorkflowError('CORRUPT_STORAGE', 'Invalid transport envelope')
  }
}

export async function validate<I, O>(
  schema: StandardSchemaV1<I, O>,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Standard Schema intentionally accepts unknown external input.
  value: unknown,
  label: string
): Promise<O> {
  const result = await schema['~standard'].validate(value)
  if (result.issues) {
    throw new WorkflowError(
      'VALIDATION_FAILED',
      `${label}: ${result.issues.map((issue) => issue.message).join('; ')}`
    )
  }
  // Reject schemas which transform into non-durable representations (Date, class instances, ...).
  encode(result.value)
  return result.value
}
