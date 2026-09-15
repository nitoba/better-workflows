import { Injectable, ConflictException, NotFoundException } from '@nestjs/common';
import { AppDatabase } from '../storage/database.js';
import { ResearchInputSchema } from '../research/research.schemas.js';
import type { ResearchInput, PaperOutcome } from '../research/research.schemas.js';
export type DesiredState = 'running' | 'paused' | 'interrupted';
export interface RunRow { id: string; input_json: string; execution_id: string | null; desired: DesiredState; terminal: number; revision: number; created_at: string; updated_at: string }
export interface ChildRow { execution_id: string; run_id: string; paper_id: string; title: string; result_json: string | null }

@Injectable()
export class RunRepository {
  constructor(private readonly database: AppDatabase) {}
  create(input: ResearchInput): RunRow {
    const encoded = JSON.stringify(ResearchInputSchema.parse(input));
    const now = new Date().toISOString();
    this.database.db.query(`INSERT INTO research_runs(id,input_json,created_at,updated_at) VALUES (?,?,?,?) ON CONFLICT(id) DO NOTHING`)
      .run(input.requestId, encoded, now, now);
    const row = this.get(input.requestId);
    if (row.input_json !== encoded) throw new ConflictException('requestId already belongs to a different request');
    return row;
  }
  get(id: string): RunRow {
    const row = this.database.db.query<RunRow, [string]>('SELECT * FROM research_runs WHERE id = ?').get(id);
    if (!row) throw new NotFoundException('Research run not found');
    return row;
  }
  input(id: string): ResearchInput { return ResearchInputSchema.parse(JSON.parse(this.get(id).input_json)); }
  recent(limit = 50): RunRow[] { return this.database.db.query<RunRow, [number]>('SELECT * FROM research_runs ORDER BY created_at DESC LIMIT ?').all(limit); }
  active(after: string): RunRow[] { return this.database.db.query<RunRow, [string]>('SELECT * FROM research_runs WHERE terminal=0 AND id > ? ORDER BY id LIMIT 100').all(after); }
  markTerminal(id: string): void { this.database.db.query('UPDATE research_runs SET terminal=1 WHERE id=?').run(id); }
  bind(id: string, executionId: string): void {
    this.database.db.query('UPDATE research_runs SET execution_id = ?, updated_at = ? WHERE id = ? AND (execution_id IS NULL OR execution_id = ?)')
      .run(executionId, new Date().toISOString(), id, executionId);
  }
  setDesired(id: string, state: DesiredState): RunRow {
    const update = this.database.db.transaction(() => {
      const current = this.get(id);
      if (current.terminal) throw new ConflictException('This run has already finished');
      if (current.desired === 'interrupted' && state !== 'interrupted') throw new ConflictException('Interrupted runs are terminal; use a new requestId');
      if (current.desired !== state) this.database.db.query('UPDATE research_runs SET desired = ?, revision = revision + 1, updated_at = ? WHERE id = ?')
        .run(state, new Date().toISOString(), id);
      return this.get(id);
    });
    return update();
  }
  registerChild(runId: string, executionId: string, paperId: string, title: string): void {
    this.database.db.query('INSERT INTO paper_runs(execution_id,run_id,paper_id,title) VALUES (?,?,?,?) ON CONFLICT(execution_id) DO NOTHING')
      .run(executionId, runId, paperId, title);
  }
  children(runId: string): ChildRow[] {
    this.get(runId);
    return this.database.db.query<ChildRow, [string]>('SELECT * FROM paper_runs WHERE run_id = ? ORDER BY paper_id').all(runId);
  }
  finishPaper(outcome: PaperOutcome): void {
    this.database.db.query('UPDATE paper_runs SET result_json = ? WHERE execution_id = ?').run(JSON.stringify(outcome), outcome.executionId);
  }
}
