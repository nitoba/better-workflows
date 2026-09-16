import type {
  ExecutionSnapshot,
  HistoryPage,
  SignalDefinition,
  WorkflowContractClass,
  WorkflowInput
} from '../types'

/** Keeps the engine and its types out of the public client's declaration graph. */
export interface ClientBackend {
  start<W extends WorkflowContractClass>(
    workflow: W,
    input: WorkflowInput<W>,
    key?: string
  ): Promise<{ executionId: string; created: boolean }>
  resultVersion(workflow: WorkflowContractClass): number
  describe(workflow: WorkflowContractClass, id: string): Promise<ExecutionSnapshot>
  row(
    workflow: WorkflowContractClass,
    id: string
  ): Promise<{
    readonly event_sequence: number
    readonly result_json: string | null
    readonly version: number
    readonly state:
      | 'accepted'
      | 'running'
      | 'waiting'
      | 'blocked'
      | 'continued'
      | 'completed'
      | 'failed'
      | 'cancelled'
    readonly continued_to: string | null
  }>
  wait(
    workflow: WorkflowContractClass,
    id: string,
    afterRevision: number,
    options: { readonly signal?: AbortSignal | undefined; readonly timeout?: number | undefined }
  ): Promise<void>
  history(
    workflow: WorkflowContractClass,
    id: string,
    after?: number,
    limit?: number
  ): Promise<HistoryPage>
  control(
    workflow: WorkflowContractClass,
    id: string,
    action: 'run' | 'pause' | 'cancel',
    reason: string
  ): Promise<void>
  signal<I, O>(
    workflow: WorkflowContractClass,
    id: string,
    signal: SignalDefinition<I, O>,
    payload: I,
    key: string
  ): Promise<{ accepted: boolean }>
}
