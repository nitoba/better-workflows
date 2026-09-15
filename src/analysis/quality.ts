import type { PaperAnalysis, Review, QualityReport } from './analysis.schemas.js';
export interface SourceChunk { id: string; text: string }
export function normalizeEvidence(text: string): string { return text.replace(/\s+/gu, ' ').trim(); }
export function evaluateAnalysis(analysis: PaperAnalysis, review: Review, source: readonly SourceChunk[]): QualityReport {
  const chunks = new Map(source.map(chunk => [chunk.id, normalizeEvidence(chunk.text)]));
  const evidenceErrors: string[] = [];
  for (const [index, point] of analysis.keyPoints.entries()) {
    for (const evidence of point.evidence) {
      const text = chunks.get(evidence.chunkId);
      if (!text) evidenceErrors.push(`keyPoints[${index}]: unknown chunk ${evidence.chunkId}`);
      else if (!text.includes(normalizeEvidence(evidence.quote))) {
        evidenceErrors.push(`keyPoints[${index}]: quote not found in ${evidence.chunkId}`);
      }
    }
  }
  const approved = review.decision === 'accept' && review.fidelity >= 85 && review.coverage >= 80
    && review.clarity >= 80 && !review.issues.some(issue => issue.severity !== 'minor') && evidenceErrors.length === 0;
  return { reviewer: review, evidenceErrors, approved, terminal: approved || review.decision === 'reject' };
}
