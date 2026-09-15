import { z } from 'zod';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
export const ChunkSchema = z.object({
  id: z.string(), index: z.number().int().nonnegative(), start: z.number().int().nonnegative(), end: z.number().int().nonnegative(), artifact: ArtifactRefSchema,
});
export const DocumentSchema = z.object({
  markdown: ArtifactRefSchema, extraction: ArtifactRefSchema, totalPages: z.number().int().positive(),
  characters: z.number().int().positive(), chunks: z.array(ChunkSchema).min(1).max(100),
});
export type Document = z.infer<typeof DocumentSchema>;
