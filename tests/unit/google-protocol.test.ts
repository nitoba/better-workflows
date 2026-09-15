import { test, expect } from 'bun:test';
import { createGoogle } from '@ai-sdk/google';
import { z } from 'zod';
import { callGoogle } from '../../src/analysis/llm.gateway.js';
test('actual AI SDK Google provider encodes the requested model and parses structured output', async () => {
  let requestUrl = ''; let body = '';
  const provider = createGoogle({ apiKey: 'test-key', fetch: Object.assign(async (url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    requestUrl = String(url); body = String(init?.body);
    return new Response(JSON.stringify({ candidates: [{ content: { role: 'model', parts: [{ text: '{"answer":"supported"}' }] }, finishReason: 'STOP', index: 0 }],
      usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 5, totalTokenCount: 15 } }), { headers: { 'content-type': 'application/json' } });
  }, { preconnect: () => {} }) });
  const result = await callGoogle(provider('gemini-3.8-flash'), z.object({ answer: z.string() }),
    { role: 'judge', model: 'gemini-3.8-flash', system: 'Return an object', prompt: 'Test' }, new AbortController().signal);
  expect(requestUrl).toContain('gemini-3.8-flash'); expect(body).toContain('application/json');
  expect(result.data.answer).toBe('supported'); expect(result.usage.totalTokens).toBe(15);
});
test('Google HTTP quota errors remain retryable activity errors without hidden SDK retries', async () => {
  let requests = 0;
  const provider = createGoogle({ apiKey: 'test-key', fetch: Object.assign(async () => { requests++; return new Response(JSON.stringify({ error: { code: 429, message: 'quota', status: 'RESOURCE_EXHAUSTED' } }), { status: 429, headers: { 'content-type': 'application/json' } }); }, { preconnect: () => {} }) });
  try {
    await callGoogle(provider('gemini-3.8-flash'), z.object({ answer: z.string() }),
      { role: 'author', model: 'gemini-3.8-flash', system: 'test', prompt: 'test' }, new AbortController().signal);
    throw new Error('Expected failure');
  } catch (error) { expect(error).toMatchObject({ code: 'GOOGLE_HTTP_429', retryable: true }); }
  expect(requests).toBe(1);
});
