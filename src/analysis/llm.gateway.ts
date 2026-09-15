import { Injectable } from '@nestjs/common';
import { createGoogle } from '@ai-sdk/google';
import { APICallError, generateText, NoObjectGeneratedError, Output } from 'ai';
import type { z } from 'zod';
import { AppSettings } from '../config/settings.js';
import { IntegrationError } from '../common/failure.js';

export const LLM_GATEWAY = Symbol('LLM_GATEWAY');
export interface LlmJob {
  role: 'extract' | 'author' | 'judge'; model: string; system: string; prompt: string;
}
export interface LlmResponse<T> {
  data: T;
  usage: { inputTokens: number | null; outputTokens: number | null; totalTokens: number | null };
}
export interface LlmGateway {
  complete<T>(schema: z.ZodType<T>, job: LlmJob, signal: AbortSignal): Promise<LlmResponse<T>>;
}

@Injectable()
export class GoogleGateway implements LlmGateway {
  constructor(private readonly settings: AppSettings) {}
  async complete<T>(schema: z.ZodType<T>, job: LlmJob, signal: AbortSignal): Promise<LlmResponse<T>> {
    if (!this.settings.env.GOOGLE_GENERATIVE_AI_API_KEY) {
      throw new IntegrationError('GOOGLE_KEY_MISSING', 'Set GOOGLE_GENERATIVE_AI_API_KEY to execute LLM activities');
    }
    const google = createGoogle({ apiKey: this.settings.env.GOOGLE_GENERATIVE_AI_API_KEY });
    return callGoogle(google(job.model), schema, job, signal);
  }
}

// A separate function permits protocol-level tests with a real Google provider
// and an injected HTTP transport. Production never falls back to fake outputs.
export async function callGoogle<T>(
  model: ReturnType<ReturnType<typeof createGoogle>>,
  schema: z.ZodType<T>, job: LlmJob, signal: AbortSignal,
): Promise<LlmResponse<T>> {
  try {
    const result = await generateText({
      model, output: Output.object({ schema }),
      system: job.system, prompt: job.prompt,
      maxOutputTokens: job.role === 'author' ? 12_000 : 8_000,
      maxRetries: 0,
      abortSignal: signal,
    });
    if (!result.output) throw new IntegrationError('LLM_EMPTY_OUTPUT', 'Google returned no structured output', true);
    return {
      data: schema.parse(result.output),
      usage: {
        inputTokens: result.usage.inputTokens ?? null,
        outputTokens: result.usage.outputTokens ?? null,
        totalTokens: result.usage.totalTokens ?? null,
      },
    };
  } catch (error) {
    signal.throwIfAborted();
    if (APICallError.isInstance(error)) {
      throw new IntegrationError(`GOOGLE_HTTP_${error.statusCode ?? 'UNKNOWN'}`,
        'Google API request failed; check credentials, model availability and quota',
        error.statusCode === 429 || (error.statusCode !== undefined && error.statusCode >= 500));
    }
    if (NoObjectGeneratedError.isInstance(error)) {
      throw new IntegrationError('LLM_INVALID_OUTPUT', 'Model did not produce the required structured result', true);
    }
    throw error;
  }
}
