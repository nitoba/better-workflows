import type {
  ExecutionSnapshot,
  HistoryPage,
  SignalDefinition,
  WorkflowClass,
  WorkflowInput
} from '../types'

/** Keeps the engine and its types out of the public client's declaration graph. */
export interface ClientBackend {
  start<W extends WorkflowClass>(
    workflow: W,
    input: WorkflowInput<W>,
    key?: string
  ): Promise<{ executionId: string; created: boolean }>
  resultVersion(workflow: WorkflowClass): number
  describe(workflow: WorkflowClass, id: string): Promise<ExecutionSnapshot>
  row(
    workflow: WorkflowClass,
    id: string
  ): Promise<{ readonly result_json: string | null; readonly version: number }>
  history(workflow: WorkflowClass, id: string, after?: number, limit?: number): Promise<HistoryPage>
  control(
    workflow: WorkflowClass,
    id: string,
    action: 'run' | 'pause' | 'cancel',
    reason: string
  ): Promise<void>
  signal<I, O>(
    workflow: WorkflowClass,
    id: string,
    signal: SignalDefinition<I, O>,
    payload: I,
    key: string
  ): Promise<{ accepted: boolean }>
}
