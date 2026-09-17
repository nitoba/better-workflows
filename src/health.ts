import { Inject, Injectable } from '@nestjs/common'
import { WorkflowsRuntime } from './internal/runtime'
import type { WorkflowsLiveness, WorkflowsReadiness } from './types'

interface WorkflowsHealthRuntime {
  liveness(): WorkflowsLiveness
  readiness(): Promise<WorkflowsReadiness>
}

/**
 * Nest service exposing explicit workflow liveness and readiness checks.
 * No HTTP controller is registered automatically; applications choose their routes.
 */
@Injectable()
export class WorkflowsHealth {
  constructor(@Inject(WorkflowsRuntime) private readonly runtime: WorkflowsHealthRuntime) {}

  /**
   * Check whether the local runtime is alive without querying storage.
   * @returns Current process liveness state.
   */
  liveness(): WorkflowsLiveness {
    return this.runtime.liveness()
  }

  /**
   * Check storage, schema and required background loops for readiness.
   * @returns Current readiness state; notifier degradation is reported separately.
   */
  readiness(): Promise<WorkflowsReadiness> {
    return this.runtime.readiness()
  }
}
