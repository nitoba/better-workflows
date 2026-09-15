import { Injectable } from '@nestjs/common';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { z } from 'zod';
import { AppSettings } from '../config/settings.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import type { ArtifactRef } from '../storage/storage.schemas.js';
import { IntegrationError } from '../common/failure.js';
const exec = promisify(execFile);
const ParsedSchema = z.object({ markdown: z.string().min(100), totalPages: z.number().int().positive(), warnings: z.array(z.string()) });

@Injectable()
export class LiteparseService {
  constructor(private readonly settings: AppSettings, private readonly files: ArtifactsService) {}
  async parse(pdf: ArtifactRef, signal: AbortSignal) {
    await this.files.read(pdf);
    const tempRoot = join(this.settings.dataDir, 'tmp');
    await mkdir(tempRoot, { recursive: true });
    const directory = await mkdtemp(join(tempRoot, 'parse-'));
    const output = join(directory, 'extraction.json');
    try {
      await exec(this.settings.env.NODE_BINARY, [join(this.settings.projectDir, 'scripts/liteparse-worker.mjs'),
        this.files.path(pdf.key), output, String(this.settings.env.PDF_MAX_PAGES), String(this.settings.env.OCR_ENABLED)], {
        signal, timeout: this.settings.env.PDF_TIMEOUT_MS, killSignal: 'SIGKILL', maxBuffer: 64_000,
      });
      const parsed = ParsedSchema.parse(JSON.parse(await readFile(output, 'utf8')));
      if (parsed.markdown.length > this.settings.env.DOCUMENT_MAX_CHARS) {
        throw new IntegrationError('DOCUMENT_TOO_LARGE', `Extracted Markdown exceeds ${this.settings.env.DOCUMENT_MAX_CHARS} characters; no content was silently truncated`);
      }
      return parsed;
    } catch (error) {
      if (signal.aborted || error instanceof IntegrationError) throw error;
      throw new IntegrationError('PDF_EXTRACTION_FAILED', 'LiteParse failed or exceeded its limits. Check PDF_MAX_PAGES, OCR_ENABLED and NODE_BINARY.');
    } finally { await rm(directory, { recursive: true, force: true }); }
  }
}
