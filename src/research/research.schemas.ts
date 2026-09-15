import { z } from 'zod';
import { SearchRequestSchema, PaperSchema } from '../arxiv/arxiv.schemas.js';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
export const LanguageSchema = z.enum(['pt-BR', 'en']);
export const ResearchInputSchema = SearchRequestSchema.extend({
  requestId: z.string().uuid(), recipient: z.string().email().max(254),
  language: LanguageSchema,
  maxReviewRounds: z.number().int().min(1).max(4),
  paperConcurrency: z.number().int().min(1).max(4),
  analysisModel: z.string(), judgeModel: z.string(),
});
export type ResearchInput = z.infer<typeof ResearchInputSchema>;
export const CreateResearchSchema = z.object({
  requestId: z.string().uuid(), recipient: z.string().email().max(254),
  query: z.string().trim().min(1).max(500).optional(),
  arxivIds: SearchRequestSchema.shape.arxivIds.optional(),
  maxResults: z.number().int().min(1).max(10).default(3),
  maxReviewRounds: z.number().int().min(1).max(4).default(3),
  language: LanguageSchema.default('pt-BR'),
}).strict().refine(v => Boolean(v.query) !== Boolean(v.arxivIds?.length), 'Provide either query or nonempty arxivIds, not both');
export type CreateResearch = z.infer<typeof CreateResearchSchema>;
export const PaperInputSchema = z.object({
  runId: z.string().uuid(), paper: PaperSchema, language: LanguageSchema,
  maxReviewRounds: z.number().int().min(1).max(4), analysisModel: z.string(), judgeModel: z.string(),
});
export type PaperInput = z.infer<typeof PaperInputSchema>;
export const PaperOutcomeSchema = z.object({
  paperId: z.string(), title: z.string(), executionId: z.string(),
  status: z.enum(['approved', 'rejected', 'failed']),
  analysis: ArtifactRefSchema.nullable(), review: ArtifactRefSchema.nullable(),
  rounds: z.number().int().nonnegative(), reason: z.string(),
});
export type PaperOutcome = z.infer<typeof PaperOutcomeSchema>;
export const ResearchResultSchema = z.object({
  runId: z.string(), papers: z.array(PaperOutcomeSchema),
  digest: ArtifactRefSchema, messageId: z.string(),
  approved: z.number(), rejected: z.number(), failed: z.number(),
});
export type ResearchResult = z.infer<typeof ResearchResultSchema>;
