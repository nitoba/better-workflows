import { Injectable, NotFoundException } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { AppSettings } from '../config/settings.js';
import { AppDatabase } from './database.js';
import { ArtifactRefSchema } from './storage.schemas.js';
import type { ArtifactRef, ArtifactRecord } from './storage.schemas.js';
import { IntegrationError } from '../common/failure.js';

interface Row { key: string; run_id: string; label: string; media_type: ArtifactRef['mediaType']; bytes: number; sha256: string }

@Injectable()
export class ArtifactsService {
  readonly directory: string;
  constructor(private readonly settings: AppSettings, private readonly database: AppDatabase) {
    this.directory = join(settings.dataDir, 'artifacts');
  }
  path(key: string): string {
    if (!/^[a-f0-9]{64}$/.test(key)) throw new NotFoundException('Artifact not found');
    return join(this.directory, key);
  }
  async put(runId: string, label: string, content: string | Uint8Array, mediaType: ArtifactRef['mediaType']): Promise<ArtifactRef> {
    const buffer = typeof content === 'string' ? Buffer.from(content, 'utf8') : Buffer.from(content);
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    const key = createHash('sha256').update(JSON.stringify([runId, label, sha256])).digest('hex');
    await mkdir(this.directory, { recursive: true });
    const temporary = join(this.directory, `${key}.${randomUUID()}.tmp`);
    try {
      await writeFile(temporary, buffer, { flag: 'wx', mode: 0o600 });
      await rename(temporary, this.path(key));
    } finally { await unlink(temporary).catch(() => undefined); }
    this.database.db.query(`INSERT INTO artifacts(key,run_id,label,media_type,bytes,sha256) VALUES (?,?,?,?,?,?) ON CONFLICT(key) DO NOTHING`)
      .run(key, runId, label, mediaType, buffer.byteLength, sha256);
    return { key, sha256, bytes: buffer.byteLength, mediaType };
  }
  record(runId: string, key: string): ArtifactRecord {
    const row = this.database.db.query<Row, [string, string]>('SELECT * FROM artifacts WHERE run_id = ? AND key = ?').get(runId, key);
    if (!row) throw new NotFoundException('Artifact not found in this research run');
    return { key: row.key, runId: row.run_id, label: row.label, mediaType: row.media_type, bytes: row.bytes, sha256: row.sha256 };
  }
  list(runId: string): ArtifactRecord[] {
    return this.database.db.query<Row, [string]>('SELECT * FROM artifacts WHERE run_id = ? ORDER BY label,key').all(runId)
      .map(row => ({ key: row.key, runId: row.run_id, label: row.label, mediaType: row.media_type, bytes: row.bytes, sha256: row.sha256 }));
  }
  async read(ref: ArtifactRef): Promise<Buffer> {
    ArtifactRefSchema.parse(ref);
    const buffer = await readFile(this.path(ref.key));
    if (buffer.byteLength !== ref.bytes || createHash('sha256').update(buffer).digest('hex') !== ref.sha256) {
      throw new IntegrationError('ARTIFACT_CORRUPTED', `Artifact ${ref.key} failed integrity verification`);
    }
    return buffer;
  }
  async text(ref: ArtifactRef): Promise<string> { return (await this.read(ref)).toString('utf8'); }
}
