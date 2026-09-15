import { Injectable } from '@nestjs/common';
import type { OnApplicationShutdown } from '@nestjs/common';
import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { AppSettings } from '../config/settings.js';

@Injectable()
export class AppDatabase implements OnApplicationShutdown {
  readonly db: Database;
  constructor(settings: AppSettings) {
    mkdirSync(settings.dataDir, { recursive: true });
    this.db = new Database(join(settings.dataDir, 'application.sqlite'), { create: true, strict: true });
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS research_runs (
        id TEXT PRIMARY KEY, input_json TEXT NOT NULL, execution_id TEXT UNIQUE,
        desired TEXT NOT NULL DEFAULT 'running', terminal INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS paper_runs (
        execution_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES research_runs(id),
        paper_id TEXT NOT NULL, title TEXT NOT NULL, result_json TEXT
      );
      CREATE INDEX IF NOT EXISTS paper_runs_parent ON paper_runs(run_id);
      CREATE TABLE IF NOT EXISTS artifacts (
        key TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES research_runs(id),
        label TEXT NOT NULL, media_type TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS artifacts_parent ON artifacts(run_id);
      CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS arxiv_search_cache (cache_key TEXT PRIMARY KEY, papers_json TEXT NOT NULL, expires_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS network_gates (name TEXT PRIMARY KEY, last_at INTEGER NOT NULL);
    `);
  }
  getReceipt(key: string): unknown | undefined {
    const row = this.db.query<{ value_json: string }, [string]>('SELECT value_json FROM receipts WHERE key = ?').get(key);
    return row ? JSON.parse(row.value_json) : undefined;
  }
  putReceipt(key: string, value: unknown): void {
    this.db.query('INSERT INTO receipts(key,value_json) VALUES (?,?) ON CONFLICT(key) DO NOTHING').run(key, JSON.stringify(value));
  }
  onApplicationShutdown(): void { this.db.close(); }
}
