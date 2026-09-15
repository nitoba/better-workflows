import { Injectable } from '@nestjs/common';
import { resolve } from 'node:path';
import { z } from 'zod';

const Environment = z.object({
  HOST: z.string().default('127.0.0.1'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  API_TOKEN: z.string().min(16).default('local-development-token-change-me'),
  DATA_DIR: z.string().default('./data'),
  GOOGLE_GENERATIVE_AI_API_KEY: z.string().default(''),
  ANALYSIS_MODEL: z.string().default('gemini-3.8-flash'),
  JUDGE_MODEL: z.string().default('gemini-3.8-flash'),
  ARXIV_CONTACT_EMAIL: z.string().email().default('developer@example.com'),
  SMTP_HOST: z.enum(['127.0.0.1', 'localhost', 'mailpit']).default('127.0.0.1'),
  SMTP_PORT: z.coerce.number().int().min(1).max(65535).default(1025),
  MAIL_FROM: z.string().email().default('research@local.test'),
  NODE_BINARY: z.string().default('node'),
  PDF_MAX_BYTES: z.coerce.number().int().min(1024).max(100_000_000).default(25_000_000),
  PDF_MAX_PAGES: z.coerce.number().int().min(1).max(300).default(80),
  PDF_TIMEOUT_MS: z.coerce.number().int().min(1000).max(240_000).default(120_000),
  DOCUMENT_MAX_CHARS: z.coerce.number().int().min(2000).max(600_000).default(240_000),
  CHUNK_CHARS: z.coerce.number().int().min(1000).max(30_000).default(16_000),
  OCR_ENABLED: z.enum(['true', 'false']).default('false').transform(v => v === 'true'),
  PAPER_CONCURRENCY: z.coerce.number().int().min(1).max(4).default(2),
  LLM_CONCURRENCY: z.coerce.number().int().min(1).max(6).default(2),
  LOG_LEVEL: z.enum(['debug', 'log', 'warn', 'error']).default('log'),
});

@Injectable()
export class AppSettings {
  readonly env = Environment.parse(process.env);
  readonly dataDir = resolve(this.env.DATA_DIR);
  readonly projectDir = resolve(import.meta.dirname, '../../');
  readonly arxivIntervalMs = 3100;
}
