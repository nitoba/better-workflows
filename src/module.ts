import { Module } from '@nestjs/common'
import type { DynamicModule } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'
import { WorkflowsAdmin, WORKFLOWS_ADMIN_BACKEND } from './admin'
import { WorkflowClient } from './client'
import { getWorkflowToken } from './decorators'
import type { WorkflowClass, WorkflowsAsyncOptions, WorkflowsOptions } from './types'
import { WORKFLOWS_OPTIONS, WorkflowsRuntime } from './internal/runtime'

const adminProviders = [
  {
    provide: WORKFLOWS_ADMIN_BACKEND,
    inject: [WorkflowsRuntime],
    useFactory: (runtime: WorkflowsRuntime) => runtime.adminBackend()
  },
  WorkflowsAdmin
]

@Module({})
export class WorkflowsModule {
  static forRoot(options: WorkflowsOptions): DynamicModule {
    return {
      module: WorkflowsModule,
      global: true,
      imports: [DiscoveryModule],
      providers: [
        { provide: WORKFLOWS_OPTIONS, useValue: options },
        WorkflowsRuntime,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsAdmin]
    }
  }

  static forRootAsync(options: WorkflowsAsyncOptions): DynamicModule {
    return {
      module: WorkflowsModule,
      global: true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        {
          provide: WORKFLOWS_OPTIONS,
          useFactory: options.useFactory,
          inject: [...(options.inject ?? [])]
        },
        WorkflowsRuntime,
        ...adminProviders
      ],
      exports: [WorkflowsRuntime, WorkflowsAdmin]
    }
  }

  /** Registers typed clients only. Handlers remain ordinary providers in their owning feature modules. */
  static forFeature(workflows: readonly WorkflowClass[]): DynamicModule {
    const providers = workflows.map((workflow) => ({
      provide: getWorkflowToken(workflow),
      inject: [WorkflowsRuntime],
      useFactory: (runtime: WorkflowsRuntime) => {
        runtime.registerContract(workflow)
        return new WorkflowClient(runtime, workflow)
      }
    }))
    return {
      module: WorkflowsModule,
      providers,
      exports: providers.map((provider) => provider.provide)
    }
  }
}
