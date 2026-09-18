# better-workflows — Alpha 0.1.0-alpha.9

## Tema da release

**Contract-first Activities & Worker Isolation**

A alpha.9 deve estender para Activities o mesmo modelo introduzido para Workflows na alpha.5:

```text
Simple mode
contract === implementation

Advanced mode
contract !== implementation
```

A mudança deve permitir que aplicações distribuídas compartilhem somente os contratos das activities entre:

```text
workflow/orchestrator packages
worker packages
shared contracts packages
test packages
```

sem importar:

```text
implementações Nest
repositories
SDKs externos
database clients
HTTP clients
worker-only dependencies
```

Decorators continuam sendo a API principal.

A experiência simples atual deve permanecer funcionando sem mudanças.

---

# 1. Problema atual

Hoje uma classe de Activities representa simultaneamente:

```text
activity contract
+
method schemas
+
activity names/versions
+
queue/default policies
+
Nest provider
+
worker implementation
+
DI dependencies
+
type usado por ctx.activities()
```

Exemplo atual:

```ts
@Activities({
  queue: ReportsQueue,
})
export class ReportActivities {
  constructor(private readonly repository: ReportRepository) {}

  @Activity({
    name: "reports.generate",
    version: 1,
    input: GenerateInput,
    output: GenerateOutput,
  })
  async generate(
    input: GenerateInputType,
    ctx: ActivityContext,
  ): Promise<GenerateOutputType> {
    return this.repository.generate(input);
  }
}
```

O workflow chama:

```ts
await ctx.activities(ReportActivities).generate(input, {
  stepId: "generate",
});
```

Portanto o orchestrator precisa importar:

```text
ReportActivities
```

mas essa mesma classe pode importar:

```text
ReportRepository
AWS SDK
Stripe SDK
database services
worker-only modules
```

Mesmo quando o orchestrator nunca executará essa classe.

Isso cria acoplamento desnecessário entre:

```text
workflow orchestration
e
activity execution
```

---

# 2. Objetivo

Introduzir um modo opcional de separar:

```text
Activity contract

de

Activity implementation
```

Mantendo:

```text
NestJS decorators
Nest dependency injection
typed ctx.activities()
Standard Schema
queue routing
retry
timeout
per-key concurrency
versioning
telemetry
dead-letter semantics
```

A API simples atual continua sendo a opção recomendada quando contrato e implementação vivem naturalmente no mesmo módulo.

---

# 3. API simples — permanece funcionando

Nada deve mudar neste código:

```ts
@Activities({
  queue: ReportsQueue,
})
export class ReportActivities {
  constructor(private readonly repository: ReportRepository) {}

  @Activity({
    name: "reports.generate",
    version: 1,
    input: GenerateInput,
    output: GenerateOutput,
  })
  async generate(
    input: GenerateInputType,
    ctx: ActivityContext,
  ): Promise<GenerateOutputType> {
    return this.repository.generate(input);
  }
}
```

Registro:

```ts
WorkflowsModule.forFeature({
  name: "reports",

  activities: [ReportActivities],

  queues: [
    {
      queue: ReportsQueue,
      concurrency: 4,
    },
  ],
});
```

Uso:

```ts
await ctx.activities(ReportActivities).generate(input, {
  stepId: "generate",
});
```

Internamente:

```text
ReportActivities
      │
      ├── contract
      └── implementation
```

Ou:

```text
contract === implementation
```

---

# 4. API avançada — novo `@ActivitiesContract`

Adicionar:

```ts
@ActivitiesContract(...)
```

O decorator representa somente o contrato.

Exemplo:

```ts
@ActivitiesContract({
  queue: ReportsQueue
})
export abstract class ReportActivities {
  @Activity({
    name: 'reports.generate',
    version: 1,
    input: GenerateInput,
    output: GenerateOutput,
    retry: {
      maxAttempts: 3
    },
    timeout: '2m'
  })
  abstract generate(
    input: GenerateInputType,
    ctx: ActivityContext
  ): Promise<GenerateOutputType>
}
```

Essa classe possui:

```text
activity method contracts
schemas
names
versions
queue
retry policies
timeouts
concurrency-key resolvers
TypeScript signatures
```

Mas NÃO possui:

```text
implementation
DI dependencies
Nest provider lifecycle
worker state
```

`@ActivitiesContract()` deve ser metadata-only.

Não aplicar:

```ts
@Injectable()
```

automaticamente.

---

# 5. Implementação do contrato

`@Activities()` deve ganhar um segundo modo.

Além de:

```ts
@Activities({
  queue: ReportsQueue,
})
class ReportActivities {}
```

deve aceitar:

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler {}
```

Exemplo completo:

```ts
@ActivitiesContract({
  queue: ReportsQueue
})
export abstract class ReportActivities {
  @Activity({
    name: 'reports.generate',
    version: 1,
    input: GenerateInput,
    output: GenerateOutput
  })
  abstract generate(
    input: GenerateInputType,
    ctx: ActivityContext
  ): Promise<GenerateOutputType>
}
```

Worker:

```ts
@Activities(ReportActivities)
export class ReportActivitiesHandler implements ReportActivities {
  constructor(
    private readonly repository: ReportRepository,
    private readonly storage: ObjectStorage,
    private readonly notifications: NotificationService,
  ) {}

  async generate(
    input: GenerateInputType,
    ctx: ActivityContext,
  ): Promise<GenerateOutputType> {
    return this.repository.generate(input, ctx.signal);
  }
}
```

`@Activities(ReportActivities)` deve:

```text
apply Injectable()

associate handler → contract

not copy durable identity from handler name
```

---

# 6. Simetria desejada

Depois da alpha.9:

```text
Workflow:

@WorkflowContract(...)
abstract class Contract {}

@Workflow(Contract)
class Handler {}
```

Activities:

```text
@ActivitiesContract(...)
abstract class Contract {}

@Activities(Contract)
class Handler {}
```

Essa simetria deve ser deliberada.

---

# 7. `@Activity` continua sendo o decorator de método

Não introduzir:

```ts
@ActivityContract(...)
```

nesta versão.

Manter:

```ts
@Activity(...)
```

tanto no simple mode quanto no advanced mode.

Simple:

```ts
@Activities(...)
class Emails {
  @Activity(...)
  async send(...) {}
}
```

Advanced:

```ts
@ActivitiesContract(...)
abstract class Emails {
  @Activity(...)
  abstract send(...): Promise<void>
}
```

Isso reduz a quantidade de conceitos públicos.

---

# 8. Suporte a `@Activity` em métodos abstratos

O decorator atual não pode depender da existência de:

```text
descriptor.value
```

porque um método abstrato não possui implementação runtime.

Modificar `@Activity()` para aceitar:

```ts
@Activity({...})
abstract send(...): Promise<void>
```

Ele deve registrar metadata usando:

```text
prototype
+
property key
```

mesmo sem função concreta.

---

# 9. Registro explícito dos métodos decorados

Existe um detalhe importante:

métodos abstratos podem não existir em:

```ts
Object.getOwnPropertyNames(Contract.prototype);
```

porque não possuem implementação JavaScript.

Portanto não descobrir activity contracts percorrendo apenas properties runtime do prototype.

`@Activity()` deve registrar explicitamente o método em metadata.

Criar algo equivalente a:

```text
ACTIVITY_METHODS_METADATA
```

contendo:

```text
method names decorados
```

Exemplo:

```ts
["generate", "send", "archive"];
```

O Registry passa a descobrir methods pela metadata explícita.

Isso também reduz dependência de reflection estrutural acidental.

---

# 10. Metadata interna

Separar:

```text
Activities contract metadata

de

Activities handler metadata
```

Introduzir algo equivalente a:

```ts
ACTIVITIES_CONTRACT_METADATA;
ACTIVITIES_HANDLER_METADATA;
ACTIVITY_METHODS_METADATA;
```

Manter:

```ts
ACTIVITY_METADATA;
```

para method definitions.

---

# 11. Simple mode metadata

Isto:

```ts
@Activities({
  queue: ReportsQueue,
})
class ReportActivities {}
```

deve internamente produzir:

```text
ACTIVITIES_CONTRACT_METADATA
      │
      └── defaults

ACTIVITIES_HANDLER_METADATA
      │
      └── contract = ReportActivities
```

E aplicar:

```text
Injectable
```

Assim:

```text
contract = ReportActivities

handler = ReportActivities
```

---

# 12. Advanced mode metadata

Contrato:

```ts
@ActivitiesContract({
  queue: ReportsQueue,
})
abstract class ReportActivities {}
```

produz:

```text
ACTIVITIES_CONTRACT_METADATA
```

sem `Injectable`.

Handler:

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler {}
```

produz:

```text
ACTIVITIES_HANDLER_METADATA
  contract = ReportActivities
```

e:

```text
Injectable
```

---

# 13. Compatibilidade de metadata

Se `ACTIVITIES_METADATA` atualmente for publicamente observável ou necessário por compatibilidade interna, preservá-lo no simple mode.

Mas código novo deve trabalhar preferencialmente sobre:

```text
ACTIVITIES_CONTRACT_METADATA
ACTIVITIES_HANDLER_METADATA
```

Não deixar o Registry manter permanentemente dois modelos conceituais diferentes.

Normalizar cedo.

---

# 14. Novo modelo interno

Criar representação semelhante a:

```ts
interface RegisteredActivitiesContract {
  readonly contract: ActivityContractClass;

  readonly defaults: ActivityDefaults;

  readonly methods: readonly ActivityMethodContract[];

  owner?: Feature;
}
```

Implementação:

```ts
interface RegisteredActivitiesImplementation {
  readonly contract: ActivityContractClass;

  readonly handler: ActivityImplementationClass;

  readonly instance: object;
}
```

Cada método resolvido continua contendo algo equivalente a:

```ts
interface ActivityMethodContract {
  readonly contract: ActivityContractClass;

  readonly method: string;

  readonly options: ResolvedActivityOptions;
}
```

E activity executável:

```ts
interface RegisteredActivity extends ActivityMethodContract {
  readonly handler: ActivityImplementationClass;

  readonly invoke: (input, context) => Promise<unknown>;
}
```

---

# 15. Durable identity

A identidade da activity continua exclusivamente:

```text
activity name
+
activity version
```

Exemplo:

```ts
@Activity({
  name: 'reports.generate',
  version: 2,
  ...
})
```

Nunca usar:

```text
ReportActivitiesHandler

constructor.name

method implementation source

filename
```

como identidade persistida.

---

# 16. Contrato é autoridade

No advanced mode, todas as políticas duráveis pertencem ao contrato:

```text
queue

activity name

activity version

input schema

output schema

retry

timeout

concurrency key
```

A implementação não pode redefinir essas políticas.

Portanto isto NÃO deve existir:

```ts
@Activities(ReportActivities, {
  timeout: '10m'
})
```

ou:

```ts
@Activity({...})
async generate(...)
```

novamente no handler.

O handler apenas implementa.

---

# 17. Handler deve ser semanticamente simples

Ideal:

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler
  implements ReportActivities
{
  async generate(input, ctx) {
    ...
  }

  async archive(input, ctx) {
    ...
  }
}
```

Sem duplicação de decorators por método.

---

# 18. Tipos públicos

Adicionar conceitos equivalentes a:

```ts
ActivityContractClass;
ActivityImplementationClass;
```

`ActivityContractClass` deve aceitar abstract constructors.

Algo conceitual:

```ts
export type ActivityContractClass<T = object> = abstract new (
  ...args: any[]
) => T;
```

Usar forma compatível com as regras de lint/type safety do projeto.

---

# 19. Type checking do handler

Isto deve compilar:

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler
  implements ReportActivities
{
  async generate(
    input: GenerateInputType,
    ctx: ActivityContext
  ): Promise<GenerateOutputType> {
    ...
  }
}
```

Isto deve falhar em TypeScript:

```ts
@Activities(ReportActivities)
class InvalidHandler {
  async generate(input: number): Promise<boolean> {
    return true;
  }
}
```

O overload do class decorator deve preservar structural validation.

---

# 20. Missing methods

Mesmo com type safety, runtime precisa validar.

Se Nest override, JavaScript ou cast produzir:

```text
handler sem método requerido
```

bootstrap deve falhar:

```text
MISSING_ACTIVITY_HANDLER
```

Exemplo de mensagem:

```text
ReportActivitiesHandler.generate
does not implement
ReportActivities.generate
```

---

# 21. Extra methods

Handler pode possuir métodos auxiliares não decorados:

```ts
@Activities(ReportActivities)
class Handler {
  async generate(...) {}

  private normalize(...) {}

  async helper(...) {}
}
```

Somente métodos presentes no contract são durable activities.

Não descobrir activities no handler.

---

# 22. `WorkflowContext.activities()`

Essa é a principal API consumidora.

Advanced mode:

```ts
const reports = ctx.activities(ReportActivities);
```

onde:

```text
ReportActivities = abstract contract
```

Não:

```ts
ctx.activities(ReportActivitiesHandler);
```

Handler avançado deve ser rejeitado.

Erro:

```text
INVALID_ACTIVITY_CONTRACT
```

---

# 23. ActivityClient

`ActivityClient<T>` precisa funcionar com abstract contracts.

Exemplo:

```ts
const reports = ctx.activities(ReportActivities);
```

deve inferir:

```ts
reports.generate(
  GenerateInputType,
  StepOptions
): Promise<GenerateOutputType>
```

O `ActivityContext` presente na assinatura do contrato continua sendo removido da proxy durable.

---

# 24. Métodos com e sem `ActivityContext`

Compatibilidade atual deve permanecer.

Isto continua válido:

```ts
@Activity(...)
async total(
  input: TotalInput
): Promise<number>
```

assim como:

```ts
@Activity(...)
async total(
  input: TotalInput,
  ctx: ActivityContext
): Promise<number>
```

O contrato advanced também pode optar por declarar o context.

Preferencialmente a documentação deve recomendar declará-lo quando a implementação utiliza:

```text
AbortSignal

heartbeat

attempt

idempotencyKey
```

---

# 25. `activityContracts`

A propriedade pública existente:

```ts
activityContracts;
```

passa a receber contracts explicitamente.

Advanced:

```ts
WorkflowsModule.forFeature({
  name: "report-orchestrator",

  activityContracts: [ReportActivities],
});
```

Esse processo não constrói:

```text
ReportActivitiesHandler
```

nem suas dependências.

---

# 26. `activities`

Advanced worker:

```ts
WorkflowsModule.forFeature({
  name: "report-workers",

  activities: [ReportActivitiesHandler],

  queues: [
    {
      queue: ReportsQueue,
      concurrency: 8,
    },
  ],
});
```

O Registry resolve automaticamente:

```text
ReportActivitiesHandler
          │
          ▼
ReportActivities
```

Não exigir:

```ts
activities: [
  {
    contract: ReportActivities,
    handler: ReportActivitiesHandler,
  },
];
```

para o caso normal.

---

# 27. Registration implies contract

Assim como workflow handlers implicitamente registram seus contracts:

```text
activities: [ReportActivitiesHandler]
```

deve implicitamente registrar:

```text
ReportActivities
```

no mesmo feature.

Não exigir simultaneamente:

```ts
activities: [ReportActivitiesHandler],

activityContracts: [ReportActivities]
```

Isso deve ser detectado como duplicação.

---

# 28. Simple mode

Simple continua:

```ts
activities: [ReportActivities];
```

porque:

```text
contract === handler
```

Não há distinção visível para usuários atuais.

---

# 29. Handler usado como contract

No advanced mode, rejeitar:

```ts
activityContracts: [ReportActivitiesHandler];
```

Mensagem:

```text
ReportActivitiesHandler is an activities handler;
register ReportActivities instead
```

Também rejeitar:

```ts
ctx.activities(ReportActivitiesHandler);
```

---

# 30. Exports

Hoje um feature pode exportar activity capabilities.

Advanced mode:

```ts
exports: {
  activities: [ReportActivities];
}
```

Sempre exportar contract.

Não:

```ts
ReportActivitiesHandler;
```

Simple mode permanece:

```ts
exports: {
  activities: [ReportActivities];
}
```

porque são a mesma classe.

---

# 31. Private queues

Preservar a capacidade atual de esconder uma queue atrás de um activity contract.

Exemplo:

```text
ReportsWorkerFeature

owns:
ReportsQueue

exports:
ReportActivities contract

does NOT export:
ReportsQueue
```

Consumidores podem chamar:

```ts
ctx.activities(ReportActivities);
```

sem ganhar capacidade de registrar seus próprios handlers na queue privada.

Não quebrar essa propriedade modular.

---

# 32. Queue ownership

`@ActivitiesContract()` continua podendo definir:

```ts
@ActivitiesContract({
  queue: ReportsQueue
})
```

A queue reference faz parte da definição do contract.

A configuração operacional:

```text
local concurrency
global concurrency
per-key concurrency
```

continua pertencendo ao feature/root queue registration.

Não mover capacidade operacional para o contract.

---

# 33. Defaults

A hierarquia continua conceitualmente:

```text
root defaults
        ↓
owner feature defaults
        ↓
ActivitiesContract defaults
        ↓
Activity method options
```

No simple mode:

```text
Activities defaults
=
ActivitiesContract defaults
```

Implementation handlers não adicionam uma nova camada de defaults.

---

# 34. Contract package

A nova arquitetura deve permitir:

```text
packages/
  contracts/
    reports.activities.ts
    reports.workflow.ts
    queues.ts

apps/
  api/

  orchestrator/

  activity-worker/
    reports-activities.handler.ts
```

`packages/contracts/reports.activities.ts`:

```ts
@ActivitiesContract({
  queue: ReportsQueue
})
export abstract class ReportActivities {
  @Activity({
    name: 'reports.generate',
    version: 1,
    input: GenerateInput,
    output: GenerateOutput
  })
  abstract generate(
    input: GenerateInputType,
    ctx: ActivityContext
  ): Promise<GenerateOutputType>
}
```

Nenhum worker dependency deve entrar nesse arquivo.

---

# 35. Orchestrator package

```ts
WorkflowsModule.forFeature({
  name: "reports-orchestrator",

  workflows: [GenerateReportHandler],

  activityContracts: [ReportActivities],
});
```

Workflow:

```ts
@Workflow(GenerateReportWorkflow)
class GenerateReportHandler {
  async run(input, ctx) {
    return ctx
      .activities(ReportActivities)
      .generate(input, { stepId: "generate" });
  }
}
```

O orchestrator nunca importa:

```text
ReportActivitiesHandler
```

---

# 36. Worker package

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler
  implements ReportActivities
{
  constructor(
    private readonly repository:
      ReportRepository
  ) {}

  async generate(input, ctx) {
    ...
  }
}
```

Registro:

```ts
WorkflowsModule.forFeature({
  name: "reports-workers",

  activities: [ReportActivitiesHandler],

  queues: [
    {
      queue: ReportsQueue,
      concurrency: 8,
    },
  ],
});
```

---

# 37. API-only process

API-only continua sem activities.

Exemplo:

```ts
WorkflowsModule.forFeature({
  clients: [GenerateReportWorkflow],
});
```

Nenhuma mudança necessária.

---

# 38. Registry

Refatorar Registry para normalizar activities por contract.

Adicionar mappings equivalentes a:

```text
implementation class
→
contract class
```

e:

```text
contract class
→
resolved method contracts
```

A collection executável continua indexada durablemente por:

```text
activity name + version
```

---

# 39. Registry identity maps

Conceitualmente:

```ts
activityProviders: Map<
  ActivityContractClass,
  readonly ActivityMethodContract[]
>;

activityHandlerContracts: Map<
  ActivityImplementationClass,
  ActivityContractClass
>;

activities: Map<ActivityNameVersion, RegisteredActivity>;
```

Evitar mapear durable contract primariamente pelo handler.

---

# 40. Duplicate handler

Isto deve falhar:

```ts
@Activities(ReportActivities)
class HandlerA {}

@Activities(ReportActivities)
class HandlerB {}
```

quando ambos são registrados no mesmo execution domain.

Erro:

```text
DUPLICATE_ACTIVITY_HANDLER
```

---

# 41. Duplicate contract

Dois JavaScript contract classes diferentes declarando a mesma activity identity:

```text
reports.generate@1
```

continuam proibidos quando aparecem no mesmo resolved catalog.

Erro atual/equivalente:

```text
DUPLICATE_ACTIVITY
```

---

# 42. Contract registration dedupe

O mesmo contract importado/reexportado através de múltiplos Nest modules deve continuar resolvendo para o mesmo owner/capability quando a module graph realmente representa o mesmo feature.

Não introduzir duplicações por import topology.

---

# 43. `useExisting`

Preservar o suporte atual de reusar providers existentes.

Advanced example:

```ts
@Injectable()
class ExistingReportWorker
  implements ReportActivities
{
  ...
}
```

Se a aplicação quiser associar esse instance a um decorated handler contract, a registration existente deve continuar possível através do mecanismo de `HandlerRegistration`.

Uma opção:

```ts
@Activities(ReportActivities)
class ReportActivitiesBinding implements ReportActivities {
  // shape
}
```

Mas não criar ceremony desnecessária.

Preferencialmente fazer o existing registration funcionar diretamente com uma implementation class que carregue:

```text
ACTIVITIES_HANDLER_METADATA
```

---

# 44. Não adicionar service locator

Não resolver handlers utilizando:

```text
moduleRef.get() global

container scan por class name

string lookup
```

Preservar dependency graph explícito do Nest.

---

# 45. Singleton rules

As mesmas regras atuais permanecem:

```text
activity handlers = singleton providers

request scope = unsupported

transient handlers = unsupported
```

Background execution não possui HTTP request scope.

Advanced handlers devem passar pelas mesmas validações.

---

# 46. Worker routing

Activity transport já possui envelope contendo:

```text
activityName
activityVersion
```

Essa arquitetura não deve mudar.

Worker recebe:

```text
queue delivery
      │
      ▼
activityName@version
      │
      ▼
RegisteredActivity
      │
      ▼
handler implementation
```

Somente a origem do `RegisteredActivity` muda:

```text
contract metadata
+
handler implementation
```

Não alterar wire protocol.

---

# 47. Dead letters

A semântica alpha.6 deve permanecer.

Se:

```text
reports.generate@1
```

está no contract mas nenhum worker com implementation correspondente está deployado:

```text
UNKNOWN_ACTIVITY
ou
UNKNOWN_ACTIVITY_VERSION
```

continua sendo operational dead-letter conforme o comportamento atual.

Não transformar missing implementation em business failure.

---

# 48. Requeue após deployment

Cenário importante:

```text
orchestrator dispatches
ReportActivities.generate@1

worker deployment estava sem
ReportActivitiesHandler

→ DLQ

deploy corrigido

admin.requeueDeadLetter()

→ contract resolves
→ handler executes
```

Adicionar teste contract-first específico.

---

# 49. Telemetry

Toda telemetry continua usando contract identity:

```text
activity.name

activity.version

queue.name
```

Nunca:

```text
ReportActivitiesHandler
```

como dimensão operacional.

Handler name pode aparecer apenas em debug logs internos se realmente necessário, mas não como stable semantic attribute.

---

# 50. Tracing

Trace propagation não muda.

Fluxo:

```text
workflow.round
      │
      ▼
activity.dispatch
      │
      ▼
persisted envelope
      │
      ▼
activity.execute
```

`activity.execute` resolve contract identity e implementação, mas trace attributes continuam baseados no contract.

---

# 51. Health

Readiness de workers deve verificar:

```text
configured activity contracts
+
registered implementations
+
worker loops
```

Advanced handler missing durante bootstrap deve ser configuration error quando o feature declarou:

```text
activities: [...]
```

Contract-only orchestrator não deve exigir implementation.

---

# 52. Operational diagnostics

`admin.stats()` não precisa mudar o modelo público apenas por esta feature.

Queue/activity identities continuam iguais.

Nenhum handler-class dimension deve ser adicionado.

---

# 53. Schedules

Schedules da alpha.8 não devem ser alterados funcionalmente.

Se scheduled workflow utiliza:

```ts
ctx.activities(ReportActivities);
```

deve funcionar com advanced contract exatamente como um workflow iniciado externamente.

Adicionar regression test:

```text
schedule
→ contract-first workflow
→ contract-first activity
→ worker
→ result
```

---

# 54. Workflow simple + advanced activity

Suportar:

```text
simple workflow
+
advanced activities
```

Exemplo:

```ts
@Workflow({...})
class WorkflowA {}

@ActivitiesContract(...)
abstract class ActivitiesA {}

@Activities(ActivitiesA)
class ActivitiesAHandler {}
```

---

# 55. Advanced workflow + simple activity

Também:

```text
advanced workflow
+
simple activities
```

Todos os quatro cruzamentos precisam funcionar:

```text
simple workflow + simple activities

simple workflow + advanced activities

advanced workflow + simple activities

advanced workflow + advanced activities
```

---

# 56. Method metadata ownership

No advanced mode:

```text
@Activity
```

metadata existe somente no contract.

Não copiar metadata para handler prototype.

Isso evita drift.

---

# 57. Handler method decorators

Se alguém escrever:

```ts
@Activities(ReportActivities)
class ReportActivitiesHandler {
  @Activity({...})
  async generate(...) {}
}
```

rejeitar ou ignorar?

Recomendação:

**rejeitar explicitamente**.

Erro:

```text
ACTIVITY_HANDLER_REDECLARES_CONTRACT
```

Motivo:

duas fontes de verdade criariam possibilidade de:

```text
schema drift
version drift
queue drift
retry drift
```

---

# 58. Simple mode continua aceitando decorators no implementation

Porque no simple mode:

```text
implementation === contract
```

Logo:

```ts
@Activities(...)
class ReportActivities {
  @Activity(...)
  async generate(...) {}
}
```

continua correto.

---

# 59. Contract handler argument

`@Activities(Contract)` precisa rejeitar:

```text
simple Activities class

handler class

undecorated class
```

como contract argument quando não for um verdadeiro advanced contract.

Mesmo comportamento conceitual de:

```text
@Workflow(Contract)
```

---

# 60. Helper de normalização

Adicionar função interna/pública conforme necessário:

```ts
activitiesContractClass(...)
```

análoga a:

```ts
workflowContractClass(...)
```

Ela aceita internamente:

```text
simple activities class

advanced handler
advanced contract
```

e retorna o contract normalizado.

APIs públicas que exigem contract devem validar e rejeitar handler avançado.

---

# 61. Type-level distinction

Adicionar types que permitam distinguir:

```text
ActivityContractClass

ActivityImplementationClass
```

sem expor Nest internals desnecessariamente.

`WorkflowContext.activities()` deve receber:

```text
ActivityContractClass
```

e não `Type<T>` concreto do Nest.

Isso é necessário para aceitar abstract classes.

---

# 62. Não vazar `Type<T>` como requisito de contract

Contratos abstratos não devem precisar ser constructible concretamente.

Refatorar APIs hoje tipadas como:

```ts
Type<T>;
```

quando semanticamente representam activity contracts.

Use type próprio com:

```text
abstract new
```

---

# 63. Activity client typing

Preservar:

```ts
ActivityClient<InstanceType<C>>;
```

ou abstração equivalente.

O usuário continua escrevendo apenas:

```ts
ctx.activities(ReportActivities);
```

sem generic explícito.

---

# 64. Compile-time tests

Adicionar package type tests para garantir:

```text
abstract contract accepted

handler shape valid

wrong input rejected

wrong output rejected

missing method rejected

ctx.activities(contract) inferred

ctx.activities(handler) rejected
```

Esses testes precisam rodar contra o package construído, não apenas source TS.

---

# 65. Abstract decorated methods

Criar teste específico demonstrando:

```ts
@ActivitiesContract()
abstract class Contract {
  @Activity(...)
  abstract execute(
    input: Input,
    ctx: ActivityContext
  ): Promise<Output>
}
```

e garantir que:

```text
metadata exists

method discovered

schema resolved

worker routes correctly
```

Esse teste é obrigatório porque é o ponto mais diferente da implementação atual.

---

# 66. Activity method ordering

Discovery não deve depender de:

```text
Object.getOwnPropertyNames
```

para methods abstratos.

A metadata list deve possuir ordem determinística.

Ordenar ou preservar declaração de forma explícita.

Durable activity identity não depende de ordinal, mas deterministic catalog validation é desejável.

---

# 67. Inheritance

Não ampliar deliberadamente inheritance semantics nesta release.

Preservar o comportamento compatível que já existir.

Se contracts herdados forem suportados naturalmente:

```ts
abstract class BaseActivities {}

abstract class Reports extends BaseActivities {}
```

garantir deterministic discovery.

Se isso gerar ambiguidades, documentar como não suportado no contract-first inicial.

Não transformar alpha.9 em redesign de inheritance.

---

# 68. Versioning

Nenhuma mudança no modelo de activity version.

Continua:

```ts
@Activity({
  name: 'email.send',
  version: 1
})
```

Se uma nova versão incompatível for necessária:

```text
version = 2
```

Old queued work precisa continuar encontrando implementação compatível enquanto for necessário.

---

# 69. Multiple versions

Contract-first deve facilitar packages como:

```text
EmailActivitiesV1

EmailActivitiesV2
```

com handlers:

```text
EmailActivitiesV1Handler

EmailActivitiesV2Handler
```

caso ambos precisem coexistir.

Não criar API específica de version aliases nesta release.

---

# 70. Cross-process contracts

Adicionar teste PostgreSQL real:

```text
orchestrator process

imports:
contracts package only

worker process

imports:
contracts package
+
handler implementation
+
worker dependencies
```

Workflow do orchestrator despacha activity e worker conclui.

Esse é o principal acceptance test arquitetural.

---

# 71. Provar ausência de implementação

No teste acima, tornar impossível ao orchestrator importar a implementation.

Por exemplo, package fixture separado.

Validar que:

```text
worker-only dependency
```

não aparece no dependency graph/import graph do orchestrator fixture.

Não precisa realizar bundle analysis complexo; package fixture separado já demonstra a fronteira.

---

# 72. Node/Bun package compatibility

Adicionar built-package smoke tests para:

```text
Node 22.16

Node 24

Bun
```

com advanced activity contracts.

---

# 73. Testing module

`WorkflowsTestingModule` deve suportar contract-first activities.

Overriding/mocking handlers deve continuar possível.

Exemplo desejado:

```ts
testing.overrideActivity(
  ReportActivities,
  'generate',
  async input => ...
)
```

Se a API de testing atual não tem override nesse formato, não é obrigatório inventar uma feature nova.

Mas os mecanismos atuais devem resolver o contract corretamente.

---

# 74. Testing sem handler

Contract-only test configuration deve conseguir validar workflow orchestration sem instanciar worker implementation quando execution de activities estiver desabilitada.

---

# 75. Error taxonomy

Adicionar erros explícitos quando necessário:

```text
INVALID_ACTIVITIES_CONTRACT

DUPLICATE_ACTIVITY_HANDLER

MISSING_ACTIVITY_HANDLER

ACTIVITY_HANDLER_REDECLARES_CONTRACT
```

Evitar reaproveitar:

```text
MISSING_DECORATOR
```

para todos os casos se isso tornar diagnóstico ruim.

---

# 76. Error messages

Mensagens devem apontar sempre contract e handler.

Exemplo:

```text
ReportActivitiesHandler is registered as an activities handler.
Use ReportActivities when calling ctx.activities().
```

ou:

```text
ReportActivitiesHandler does not implement
ReportActivities.generate.
```

DX importa bastante nessa feature.

---

# 77. JSDoc

Adicionar documentação completa para:

```text
ActivitiesContract

Activities advanced overload

ActivityContractClass

ActivityImplementationClass
```

Atualizar:

```text
Activity

ActivityClient

WorkflowContext.activities

WorkflowsFeatureOptions.activities

WorkflowsFeatureOptions.activityContracts

FeatureExports.activities
```

---

# 78. README

Adicionar seção:

```text
Contract-first activities (optional)
```

logo depois da seção:

```text
Contract-first workflows
```

Mostrar primeiro a forma simples e depois advanced.

Explicar quando utilizar:

```text
monolith
→ simple

separate orchestrator / workers
→ contract-first
```

---

# 79. Exemplo completo

README/docs deve mostrar arquitetura:

```text
packages/contracts
├── report.workflow.ts
├── report.activities.ts
└── queues.ts

apps/api
└── imports workflow contracts

apps/orchestrator
├── workflow handlers
└── activity contracts

apps/worker
└── activity handlers
```

---

# 80. Architecture documentation

Atualizar boundaries para:

```text
Workflow:
contract / implementation separated optionally

Activity:
contract / implementation separated optionally
```

Explicar:

```text
durable identity comes from contracts

Nest DI belongs to handlers

transport routes by contract identity

queue ownership remains feature capability
```

---

# 81. No persistence migration

Essa feature NÃO deve exigir migration.

Ela altera:

```text
metadata
registry
types
Nest registration
package boundaries
```

Não altera:

```text
activity envelope
delivery rows
claims
workflow journal
DLQ schema
schedule schema
```

Se uma migration se tornar necessária, isso deve ser tratado como sinal de que a implementação está alterando mais do que deveria.

---

# 82. No transport migration

O envelope continua:

```text
activityName
activityVersion
executionId
stepId
...
```

Nenhum handler identity deve ser persistido.

---

# 83. Backward compatibility

Todo código alpha.8 deve continuar compilando.

Especialmente:

```ts
@Activities({
  queue: Queue
})
class Activities {
  @Activity(...)
  async execute(...) {}
}
```

e:

```ts
ctx.activities(Activities);
```

Sem migration de aplicação.

---

# 84. Não depreciar simple mode

Contract-first é uma opção avançada.

Não documentar simple mode como legado.

A filosofia é:

```text
simple by default

separate when architecture requires it
```

---

# 85. Registry refactor

A implementação deve evitar criar:

```text
simple activity registry path

advanced activity registry path
```

permanentes.

Normalizar ambos para:

```text
contract
+
optional implementation
```

o mais cedo possível.

---

# 86. Target architecture

Depois do Registry discovery:

```text
ActivityContract
       │
       ├── defaults
       ├── method contracts
       └── owner
              │
              ▼
       optional handler
              │
              ▼
        executable invoke
```

Todo restante do runtime trabalha nessa representação.

---

# 87. Feature registration

Para advanced:

```ts
activities: [ReportActivitiesHandler];
```

o feature registration precisa instanciar:

```text
handler
```

e não:

```text
abstract contract
```

Esse é um critério essencial.

---

# 88. Client-only/activity-contract-only registration

```ts
activityContracts: [ReportActivities];
```

não pode construir:

```text
ReportActivitiesHandler
```

nem tentar instanciar:

```text
ReportActivities
```

porque é abstract metadata-only.

---

# 89. Nest Discovery

Discovery deve diferenciar claramente:

```text
activity contract
activity handler
```

Não inferir por:

```text
class abstractness
constructor name
presence of dependencies
```

Usar somente metadata explícita.

---

# 90. Decorator order

Method decorators executam antes do class decorator.

A implementação de `@Activity()` em abstract methods deve funcionar sem depender de:

```text
ACTIVITIES_CONTRACT_METADATA
```

já estar presente naquele momento.

Portanto:

```text
@Activity
```

registra method metadata de forma independente.

Class-level validation acontece depois/bootstrap.

---

# 91. Invalid standalone `@Activity`

Uma classe com:

```ts
class Foo {
  @Activity(...)
  abstract/existingMethod(...)
}
```

mas sem:

```text
@Activities
ou
@ActivitiesContract
```

deve continuar falhando no Registry com diagnóstico apropriado.

---

# 92. Handler method name

Contract method name faz parte da binding JavaScript, mas NÃO da durable identity.

Exemplo:

```ts
@Activity({
  name: 'emails.send',
  version: 1
})
abstract send(...)
```

Durable identity:

```text
emails.send@1
```

Binding implementation:

```text
method = send
```

Renomear o method no contract ainda é uma mudança de source API e pode quebrar handler binding, mas não altera delivery já persistida desde que activity name/version continue existindo em um contract registrado.

Documentar isso.

---

# 93. Contract method vs durable name

Não assumir:

```text
method name === activity name
```

Continuar permitindo:

```ts
@Activity({
  name: 'payments.capture'
})
abstract process(...)
```

Transport usa:

```text
payments.capture
```

Handler dispatch usa:

```text
process
```

---

# 94. Dead-letter diagnostics

Se delivery aponta para:

```text
payments.capture@1
```

mas contract existe e handler method está ausente, isso deveria ser bootstrap error no worker, e não esperar chegar uma mensagem para descobrir.

Se contract inteiro/version não está registrado naquele worker:

```text
UNKNOWN_ACTIVITY
UNKNOWN_ACTIVITY_VERSION
```

continua DLQ quando delivery chega.

---

# 95. Worker startup validation

Antes de worker loops iniciarem, validar todos os advanced bindings:

```text
every decorated contract method
has callable implementation method
```

Fail fast.

---

# 96. Queue worker grouping

A alpha.4 unificou transport físico por logical queue.

Preservar:

```text
one queue
→ multiple activity contracts/versions
```

Contract-first não pode regressar para:

```text
one physical queue per handler
```

---

# 97. Observability privacy

Nenhuma nova telemetry deve incluir:

```text
handler constructor source
DI tokens
input/output
contract package path
```

Stable attributes continuam baseados em activity contract metadata.

---

# 98. No new public client injection

Não criar:

```ts
@InjectActivities()
```

Activities continuam sendo utilizadas dentro do workflow por:

```ts
ctx.activities(Contract);
```

A alpha.9 é sobre contract/implementation separation, não sobre transformar activities em serviços invocáveis fora do workflow.

---

# 99. Não permitir durable invocation direta pelo handler

Isto:

```ts
handler.generate(...)
```

continua sendo uma chamada JavaScript normal.

Somente:

```ts
ctx.activities(ReportActivities)
  .generate(...)
```

é durable.

Documentação deve reforçar isso.

---

# 100. Acceptance matrix

A implementação deve provar:

```text
1. simple workflow
   + simple activities

2. simple workflow
   + contract-first activities

3. contract-first workflow
   + simple activities

4. contract-first workflow
   + contract-first activities

5. scheduled contract-first workflow
   + contract-first activities

6. child workflow
   + contract-first activities

7. map/parallel
   + contract-first activities

8. saga forward/compensation
   + contract-first activities

9. DLQ/requeue
   + contract-first activities
```

---

# 101. Testes obrigatórios

Adicionar cobertura para:

```text
@ActivitiesContract metadata-only

@Activity on abstract method

advanced handler DI

handler compile-time conformance

runtime missing method

duplicate handler

handler passed as contract

contract-only feature

exports/imports visibility

private queue behind activity contract

useExisting

simple-mode regression

contract-first workflow integration

scheduled workflow integration

child workflow integration

saga integration

DLQ recovery

telemetry identity

PostgreSQL cross-process

Node package consumer

Bun package consumer
```

---

# 102. Teste de isolamento de dependências

Criar fixture:

```text
contracts package
```

que NÃO importa:

```text
worker implementation package
```

Orchestrator deve compilar e executar usando somente contracts.

Worker-only package pode conter dependência sentinela.

Confirmar que orchestrator fixture não precisa dela instalada/importada.

Esse teste demonstra o principal benefício da feature.

---

# 103. CI

Adicionar type/package tests ao pipeline já existente.

Não aumentar dependências runtime sem necessidade.

Nenhuma migration/database fixture nova deveria ser necessária especificamente para contract-first.

---

# 104. Changelog

Adicionar:

```text
0.1.0-alpha.9
```

com destaque para:

```text
optional contract-first activities

abstract activity contracts

Nest worker implementations

contract-only orchestrator packages

typed ctx.activities contracts

backward-compatible simple mode
```

---

# 105. Critérios de aceite

A alpha.9 está pronta quando:

1. Existe `@ActivitiesContract()`.

2. `@Activities({...})` continua funcionando sem mudança.

3. `@Activities(Contract)` associa handler Nest ao contract.

4. Contract não recebe `@Injectable()`.

5. Handler recebe `@Injectable()`.

6. `@Activity()` funciona em abstract methods.

7. Abstract activity methods são descobertos por metadata explícita.

8. Discovery não depende de runtime method properties para contracts abstratos.

9. `ctx.activities(AbstractContract)` funciona.

10. Type inference de input/output continua correta.

11. `ctx.activities(AdvancedHandler)` é rejeitado.

12. `activityContracts` aceita advanced contracts.

13. `activities` aceita advanced handlers.

14. Registrar handler implicitamente registra seu contract.

15. Contract-only registration não instancia handler.

16. Contract-only registration não instancia abstract contract.

17. `exports.activities` usa contracts.

18. Private queue encapsulation continua funcionando.

19. Queue/default/retry/timeout/key policies pertencem ao contract.

20. Handler não pode redefinir durable policies.

21. Missing handler methods falham no bootstrap.

22. Dois handlers do mesmo contract são rejeitados.

23. Simple mode permanece backward compatible.

24. No durable schema migration é necessária.

25. Activity transport wire não muda.

26. DLQ semantics não mudam.

27. Telemetry usa contract identity.

28. Schedules alpha.8 continuam funcionando.

29. Simple/advanced workflows cruzam corretamente com simple/advanced activities.

30. PostgreSQL cross-process funciona com package contracts separado.

31. `useExisting` continua suportado.

32. Nest singleton validation continua funcionando.

33. Package declaration graph continua sem Effect types.

34. Node 22.16 smoke passa.

35. Node 24 smoke passa.

36. Bun tests passam.

37. `bun run check` passa integralmente.

---

# 106. Fora do escopo da alpha.9

Não implementar nesta versão:

```text
workflow patch/version gates

cross-version continueAsNew

ActivityClient fora de WorkflowContext

@InjectActivities

direct background job API

activity schedules

activity-specific public clients

activity event subscriptions

dashboard

dynamic runtime activity registration

remote worker discovery

multiple handler implementations for same contract

traffic splitting between activity versions

automatic contract migration
```

---

# 107. Ordem de implementação

Preferir três PRs.

## PR 1 — Contract model + types

Implementar:

```text
ActivitiesContract

Activities overload

metadata separation

abstract Activity decorator support

explicit method metadata

ActivityContractClass

ActivityImplementationClass

activitiesContractClass()

ActivityClient typing
```

Adicionar compile-time tests.

---

## PR 2 — Registry + Nest integration

Implementar:

```text
contract normalization

handler binding

activityContracts registration

activities registration

exports/imports

private queues

useExisting

duplicate validation

worker startup validation
```

Adicionar modularity/Nest tests.

---

## PR 3 — Distributed validation + docs

Implementar:

```text
cross-process PostgreSQL fixture

contract-only package fixture

DLQ recovery

schedule regression

saga/map/child regression

telemetry regression

README

architecture docs

JSDoc

package smoke

changelog/version
```

---

# 108. Resultado esperado

Depois da alpha.9, uma aplicação distribuída poderá ter:

```text
packages/
└── contracts/
    ├── reports.workflow.ts
    ├── reports.activities.ts
    └── queues.ts

apps/
├── api/
│   └── workflow clients
│
├── orchestrator/
│   ├── workflow handlers
│   └── activity contracts
│
└── worker/
    ├── activity handlers
    ├── repositories
    └── external SDKs
```

Contrato:

```ts
@ActivitiesContract({
  queue: ReportsQueue
})
export abstract class ReportActivities {
  @Activity({
    name: 'reports.generate',
    version: 1,
    input: GenerateInput,
    output: GenerateOutput,
    retry: {
      maxAttempts: 3
    },
    timeout: '2m'
  })
  abstract generate(
    input: GenerateInputType,
    ctx: ActivityContext
  ): Promise<GenerateOutputType>
}
```

Worker:

```ts
@Activities(ReportActivities)
export class ReportActivitiesHandler implements ReportActivities {
  constructor(
    private readonly repository: ReportRepository,

    private readonly storage: StorageService,
  ) {}

  async generate(
    input: GenerateInputType,
    ctx: ActivityContext,
  ): Promise<GenerateOutputType> {
    return this.repository.generate(input, ctx.signal);
  }
}
```

Workflow:

```ts
@Workflow(GenerateReportWorkflow)
export class GenerateReportHandler implements GenerateReportWorkflow {
  async run(input, ctx) {
    return ctx.activities(ReportActivities).generate(input, {
      stepId: "generate",
    });
  }
}
```

Orchestrator registration:

```ts
WorkflowsModule.forFeature({
  name: "reports-orchestrator",

  workflows: [GenerateReportHandler],

  activityContracts: [ReportActivities],
});
```

Worker registration:

```ts
WorkflowsModule.forFeature({
  name: "reports-worker",

  activities: [ReportActivitiesHandler],

  queues: [
    {
      queue: ReportsQueue,
      concurrency: 8,
      globalConcurrency: 32,
    },
  ],
});
```

A arquitetura passa então a ser completamente simétrica:

```text
WorkflowContract
      │
      ▼
WorkflowHandler


ActivitiesContract
      │
      ▼
ActivitiesHandler
```

e o package compartilhado contém somente:

```text
contracts
schemas
queues
signals
schedule definitions
```

enquanto Nest providers e dependências de infraestrutura ficam exclusivamente nos deployments responsáveis por executá-los.

O objetivo final da alpha.9 é terminar a separação entre **protocolo durável** e **implementação executável** em todo o `better-workflows`, deixando a biblioteca preparada para a próxima etapa: evolução segura de workflow code e replay compatibility.
