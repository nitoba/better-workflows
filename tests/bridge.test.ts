import { expect, test } from 'bun:test'
import { Exit, Layer, ManagedRuntime, Option, Schema } from 'effect'
import { NodeCrypto } from '@effect/platform-node'
import { DurableDeferred, Workflow, WorkflowEngine } from 'effect/unstable/workflow'
import { interpretAsync } from '../src/internal/bridge'
import { FailureSchema } from '../src/internal/wire'

const approval = DurableDeferred.make('approval', { success: Schema.String, error: FailureSchema })

test('the actual Effect engine suspends and replays async handlers without catch/finally on suspension', async () => {
  const events: string[] = []
  const definition = Workflow.make('bridge-test', {
    payload: { id: Schema.String },
    success: Schema.String,
    error: FailureSchema,
    idempotencyKey: (input) => input.id
  })
  const handler = definition.toLayer(() =>
    interpretAsync<string, WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance>(
      async (dispatch) => {
        events.push('round')
        try {
          return await dispatch(DurableDeferred.await(approval))
        } catch {
          events.push('catch')
          return 'caught'
        } finally {
          events.push('finally')
        }
      }
    )
  )
  const runtime = ManagedRuntime.make(
    handler.pipe(Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(NodeCrypto.layer))
  )
  try {
    const executionId = await runtime.runPromise(
      definition.execute({ id: 'one' }, { discard: true })
    )
    await new Promise((resolve) => setTimeout(resolve, 20))
    const suspended = await runtime.runPromise(definition.poll(executionId))
    expect(Option.isSome(suspended) && suspended.value._tag).toBe('Suspended')
    expect(events).toEqual(['round'])
    await runtime.runPromise(
      DurableDeferred.done(approval, {
        token: DurableDeferred.tokenFromExecutionId(approval, {
          workflow: definition,
          executionId
        }),
        exit: Exit.succeed('approved')
      })
    )
    const output = await runtime.runPromise(definition.execute({ id: 'one' }))
    expect(output).toBe('approved')
    expect(events).toEqual(['round', 'round', 'finally'])
  } finally {
    await runtime.dispose()
  }
})
