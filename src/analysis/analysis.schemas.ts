import { z } from 'zod';
export const EvidenceSchema = z.object({
  chunkId: z.string(), quote: z.string().min(12).max(800),
});
export const ChunkNotesSchema = z.object({
  overview: z.string().max(2500),
  claims: z.array(z.object({ claim: z.string().max(1200), quote: z.string().min(12).max(800) })).max(12),
  caveats: z.array(z.string().max(1000)).max(8),
});
export type ChunkNotes = z.infer<typeof ChunkNotesSchema>;
export const AnalysisSchema = z.object({
  title: z.string().min(1).max(1000),
  summary: z.string().min(40).max(7000),
  researchQuestion: z.string().max(2000), methodology: z.string().max(4000),
  keyPoints: z.array(z.object({
    title: z.string().max(250), detail: z.string().max(2200),
    evidence: z.array(EvidenceSchema).min(1).max(4),
  })).min(2).max(8),
  results: z.array(z.string().max(2000)).max(8),
  limitations: z.array(z.string().max(1200)).max(8),
  extractionCaveats: z.array(z.string().max(1000)).max(8),
});
export type PaperAnalysis = z.infer<typeof AnalysisSchema>;
export const ReviewSchema = z.object({
  decision: z.enum(['accept', 'revise', 'reject']),
  fidelity: z.number().min(0).max(100), coverage: z.number().min(0).max(100), clarity: z.number().min(0).max(100),
  reasoning: z.string().min(10).max(4000),
  issues: z.array(z.object({ severity: z.enum(['minor', 'major', 'critical']), description: z.string().max(2000) })).max(15),
  revisionInstructions: z.array(z.string().max(2000)).max(15),
});
export type Review = z.infer<typeof ReviewSchema>;
export const QualityReportSchema = z.object({
  reviewer: ReviewSchema, evidenceErrors: z.array(z.string()),
  approved: z.boolean(), terminal: z.boolean(),
});
export type QualityReport = z.infer<typeof QualityReportSchema>;
