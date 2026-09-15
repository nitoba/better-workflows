import { z } from 'zod';
import type { LlmGateway, LlmJob, LlmResponse } from '../../src/analysis/llm.gateway.js';
import type { PaperAnalysis, Review } from '../../src/analysis/analysis.schemas.js';
export const QUOTE = 'Persisted checkpoints prevent repeated completed work after restart.';
export const SECOND_QUOTE = 'The experiment is synthetic and does not measure network partitions.';
export function analysisFixture(): PaperAnalysis {
  return { title: 'Synthetic Research Fixture', summary: 'This synthetic experiment studies recovery of recorded task results after process restarts. It does not establish guarantees for arbitrary external systems.',
    researchQuestion: 'Can persisted checkpoints support recovery?', methodology: 'A synthetic workload of 100 tasks and ten restarts.',
    keyPoints: [
      { title: 'Recovery', detail: 'Completed work can be replayed from persisted results.', evidence: [{ chunkId: 'chunk-0001', quote: QUOTE }] },
      { title: 'Scope', detail: 'The experiment did not test network partitions.', evidence: [{ chunkId: 'chunk-0001', quote: SECOND_QUOTE }] },
    ], results: ['The durable variant completed 100 tasks.'], limitations: ['Synthetic workload; no network partitions.'],
    extractionCaveats: ['Visual contents were not interpreted.'] };
}
export function reviewFixture(decision: Review['decision'] = 'accept'): Review {
  return { decision, fidelity: 95, coverage: 90, clarity: 95,
    reasoning: decision === 'accept' ? 'The claims are supported by the supplied synthetic source.' : 'Clarify the synthetic scope and avoid claiming general guarantees.',
    issues: decision === 'accept' ? [] : [{ severity: 'major', description: 'Clarify the limited evaluation scope.' }],
    revisionInstructions: decision === 'accept' ? [] : ['State explicitly that the experiment is synthetic.'] };
}
export function atomFixture(ids = ['2609.00001v1']): string {
  return `<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom">${ids.map(id => `<entry><id>http://arxiv.org/abs/${id}</id><title>Synthetic Research Fixture</title><summary>A synthetic integration fixture.</summary><published>2026-09-01T00:00:00Z</published><updated>2026-09-01T00:00:00Z</updated><author><name>Test Author</name></author><category term="cs.SE"/></entry>`).join('')}</feed>`;
}
export class ScriptedLlm implements LlmGateway {
  readonly calls: LlmJob[] = [];
  readonly judgeCalls = new Map<string, number>();
  decision: 'revise-once' | 'reject' | 'always-revise' = 'revise-once';
  holdExtract = false;
  private releases: (() => void)[] = [];
  release(): void { this.holdExtract = false; for (const release of this.releases.splice(0)) release(); }
  async complete<T>(schema: z.ZodType<T>, job: LlmJob, signal: AbortSignal): Promise<LlmResponse<T>> {
    this.calls.push(job);
    if (this.holdExtract && job.role === 'extract') await new Promise<void>((resolve, reject) => {
      const abort = () => reject(signal.reason);
      signal.addEventListener('abort', abort, { once: true });
      this.releases.push(() => { signal.removeEventListener('abort', abort); resolve(); });
      if (signal.aborted) abort();
    });
    signal.throwIfAborted();
    let data: unknown;
    if (job.role === 'extract') data = { overview: 'Synthetic recovery experiment.', claims: [{ claim: 'Checkpoints support recovery.', quote: QUOTE }], caveats: ['Synthetic experiment.'] };
    else if (job.role === 'author') data = analysisFixture();
    else {
      const count = this.judgeCalls.get(job.prompt) ?? 0; this.judgeCalls.set(job.prompt, count + 1);
      const decision = this.decision === 'reject' ? 'reject' : this.decision === 'always-revise' || count === 0 ? 'revise' : 'accept';
      data = reviewFixture(decision);
    }
    return { data: schema.parse(data), usage: { inputTokens: 100, outputTokens: 100, totalTokens: 200 } };
  }
}
