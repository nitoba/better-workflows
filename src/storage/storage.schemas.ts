import { z } from 'zod';
export const ArtifactRefSchema = z.object({
  key: z.string().regex(/^[a-f0-9]{64}$/), sha256: z.string().regex(/^[a-f0-9]{64}$/),
  bytes: z.number().int().nonnegative(), mediaType: z.enum(['application/pdf', 'text/markdown', 'application/json', 'text/plain']),
});
export type ArtifactRef = z.infer<typeof ArtifactRefSchema>;
export interface ArtifactRecord extends ArtifactRef { runId: string; label: string }
