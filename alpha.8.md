# better-workflows — Alpha 0.1.0-alpha.8

## Tema da release

**Durable Workflow Scheduling**

A alpha.8 deve adicionar ao `better-workflows` suporte de primeira classe para iniciar workflows automaticamente a partir de schedules persistentes.

A funcionalidade deve cobrir inicialmente:

```text
Cron schedules
Interval schedules

+
timezone
+
durable cursor
+
misfire handling
+
overlap control
+
distributed execution
+
admin / CLI
+
testing clock
+
observability
```

O objetivo não é criar um wrapper sobre:

```text
setTimeout
setInterval
@nestjs/schedule
```

O scheduler precisa fazer parte das garantias duráveis da biblioteca.

Se um processo parar, reiniciar ou múltiplas instâncias concorrerem, o schedule deve continuar possuindo comportamento determinístico e recuperável.

---

# 1. Contexto

Atualmente os workflows são iniciados explicitamente:

```ts
const handle = await client.start(input)
```

Isso significa que sempre existe algum produtor externo:

```text
HTTP
queue consumer
application service
CLI
outro workflow
```

Mas várias jornadas comuns são naturalmente temporais:

```text
relatório diário

sincronização a cada 15 minutos

fechamento mensal

billing recorrente

reconciliação noturna

expiração periódica

limpeza de dados

monitoramento

ETL

processamento semanal
```

A biblioteca já possui a maior parte da infraestrutura necessária:

```text
durable timers

database clock

idempotent workflow start

SQL transactions

distributed leases / fencing

outbox / wakeup mechanisms

continueAsNew

admin API

CLI

manual business clock para testes

observability

health / readiness
```

A alpha.8 deve aproveitar essas capacidades em vez de construir um scheduler paralelo baseado em memória.

---

# 2. Princípio central

Um schedule não executa código de negócio diretamente.

Ele apenas inicia uma execution de um workflow.

Modelo:

```text
Schedule
   │
   ▼
Occurrence
   │
   ▼
Workflow start
   │
   ▼
Durable workflow execution
```

Portanto o scheduler não conhece:

```text
Activity
WorkflowContext
business implementation
Nest provider internals
```

Ele conhece apenas:

```text
schedule identity

workflow contract

scheduled occurrence

input

policies
```

---

# 3. Relação com contract-first

A alpha.8 parte da semântica introduzida na alpha.5.

Schedules pertencem ao **workflow contract**.

Nunca ao handler concreto.

Advanced mode:

```ts
@WorkflowContract({
  name: 'reports.daily',
  version: 1,
  input: DailyReportInput,
  output: DailyReportOutput
})
export abstract class DailyReportWorkflow {
  abstract run(input: DailyReportInput, ctx: WorkflowContext): Promise<DailyReportOutput>
}
```

O schedule deve apontar para:

```text
DailyReportWorkflow
```

e não:

```text
DailyReportHandler
```

O nome JavaScript do handler continua irrelevante para identidade durável.

---

# 4. API de decorators

A API deve permanecer natural para usuários NestJS.

Adicionar inicialmente:

```ts
@Cron(...)
```

e:

```ts
@Interval(...)
```

Não criar um DSL separado como API principal.

---

# 5. `@Cron`

API desejada:

```ts
@Cron({
  name: 'reports.daily',

  expression: '0 8 * * *',

  timezone: 'America/Fortaleza'
})
@WorkflowContract({
  name: 'reports.generate-daily',
  version: 1,
  input: DailyReportInput,
  output: DailyReportOutput
})
export abstract class GenerateDailyReportWorkflow {
  abstract run(input: DailyReportInput, ctx: WorkflowContext): Promise<DailyReportOutput>
}
```

No simple mode:

```ts
@Cron({
  name: 'reports.daily',
  expression: '0 8 * * *',
  timezone: 'America/Fortaleza'
})
@Workflow({
  name: 'reports.generate-daily',
  version: 1,
  input: DailyReportInput,
  output: DailyReportOutput
})
export class GenerateDailyReport {
  async run(input, ctx) {
    // ...
  }
}
```

Ambos os estilos devem produzir exatamente o mesmo schedule contract internamente.

---

# 6. `@Interval`

API desejada:

```ts
@Interval({
  name: 'catalog.sync',

  every: '15m'
})
@WorkflowContract({
  name: 'catalog.sync',
  version: 1,
  input: SyncInput,
  output: SyncOutput
})
export abstract class SyncCatalogWorkflow {}
```

O interval é calculado a partir do cursor persistido do schedule.

Não deve significar:

```text
15 minutos depois que a execution anterior terminou
```

por padrão.

Deve significar uma timeline própria:

```text
08:00
08:15
08:30
08:45
```

independente da duração do workflow.

Overlap policy define o que acontece se a execution anterior ainda estiver ativa.

---

# 7. Input do workflow

Schedules precisam conseguir produzir o input do workflow.

Na primeira versão, suportar duas formas.

## 7.1 Input estático

```ts
@Cron({
  name: 'reports.daily',

  expression: '0 8 * * *',

  input: {
    kind: 'daily'
  }
})
```

Esse input deve ser validado pelo input schema do workflow durante bootstrap quando possível e novamente no momento do start.

---

# 7.2 Input derivado da ocorrência

Adicionar um contexto puro:

```ts
input: (occurrence) => ({
  date: occurrence.scheduledAt
})
```

Exemplo:

```ts
@Cron({
  name: 'reports.daily',

  expression: '0 8 * * *',

  timezone: 'America/Fortaleza',

  input: ({ scheduledAt }) => ({
    reportDate: scheduledAt
  })
})
```

O callback deve ser:

```text
síncrono

puro

sem DI

sem I/O

sem randomness

sem clock externo
```

Não permitir Promise nesta versão.

---

# 8. `ScheduleOccurrence`

Expor um tipo público simples:

```ts
interface ScheduleOccurrence {
  readonly schedule: string

  readonly scheduledAt: string

  readonly occurrence: number

  readonly trigger: 'scheduled' | 'catch-up' | 'manual'
}
```

Pode incluir outros campos estáveis se necessário.

Não expor tipos do Effect.

---

# 9. Identidade do schedule

Cada schedule precisa ter:

```text
name
```

explícito e estável.

Exemplo:

```ts
@Cron({
  name: 'reports.daily',
  ...
})
```

Não derivar identidade de:

```text
class name
handler name
filename
Nest provider token
cron expression
```

O schedule name deve ser único dentro do namespace.

---

# 10. Metadata

Criar metadata separada para scheduling.

Algo equivalente a:

```ts
SCHEDULE_METADATA
```

O contract pode ter:

```text
0 ou 1 schedule
```

nesta primeira versão.

Não suportar inicialmente:

```ts
@Cron(...)
@Cron(...)
@Workflow(...)
```

na mesma classe.

Se múltiplos schedules para um mesmo workflow forem necessários, isso pode ser adicionado posteriormente com uma API explícita.

Priorizar semântica simples nesta release.

---

# 11. Modelo interno

Normalizar schedule como algo próximo de:

```ts
interface RegisteredSchedule {
  readonly name: string

  readonly workflow: WorkflowContractClass

  readonly workflowName: string

  readonly workflowVersion: number

  readonly kind:
    | 'cron'
    | 'interval'

  readonly expression?: string

  readonly timezone?: string

  readonly intervalMs?: number

  readonly misfire: MisfirePolicy

  readonly overlap: OverlapPolicy

  readonly maxCatchUp: number

  readonly inputResolver: (...)
}
```

Depois do bootstrap o registry deve ser imutável.

---

# 12. Persistência

Schedules precisam ter storage próprio pertencente ao `better-workflows`.

Não persistir objetos internos do Effect.

Adicionar uma tabela equivalente a:

```text
better_workflows_schedules
```

Com dados como:

```text
namespace

schedule_name

workflow_name

workflow_version

kind

definition_hash

timezone

state

last_occurrence_at

next_occurrence_at

last_execution_id

revision

claim_owner

claim_until

created_at

updated_at
```

Os nomes podem ser ajustados ao padrão atual do projeto.

---

# 13. Definition hash

Persistir uma assinatura da configuração estática do schedule.

Exemplo conceitual:

```text
kind
expression
timezone
misfire
overlap
maxCatchUp
workflow name
workflow version
```

Gerar um:

```text
definition_hash
```

determinístico.

Objetivo:

detectar configuração divergente entre processos.

Se:

```text
worker A:
0 8 * * *

worker B:
0 9 * * *
```

para:

```text
reports.daily
```

o bootstrap deve falhar.

Erro esperado:

```text
SCHEDULE_CONFIGURATION_CONFLICT
```

Não depender da ordem de bootstrap.

---

# 14. Registry e ownership

Schedules devem pertencer ao feature que possui o workflow contract/handler.

Exemplo:

```ts
WorkflowsModule.forFeature({
  name: 'reports',

  workflows: [GenerateDailyReportHandler]
})
```

O schedule decorado no contract é descoberto automaticamente.

No simple mode:

```ts
workflows: [GenerateDailyReport]
```

também.

Um processo client-only:

```ts
clients: [GenerateDailyReportWorkflow]
```

pode conhecer o contract, mas NÃO deve automaticamente registrar ownership/execution do schedule.

Schedules são infraestrutura executável.

O owner é o feature que registra o workflow implementation.

---

# 15. Process roles

Expandir:

```ts
execution
```

para incluir schedules.

Exemplo:

```ts
execution: {
  workflows: {
    enabled: true
  },

  activities: {
    enabled: false
  },

  schedules: {
    enabled: true
  }
}
```

Default:

```text
enabled = true
```

quando schedules existem, mantendo comportamento consistente com os outros execution roles.

---

# 16. Deployment separado

Deve ser possível criar um processo dedicado:

```ts
execution: {
  workflows: {
    enabled: false
  },

  activities: {
    enabled: false
  },

  schedules: {
    enabled: true
  }
}
```

Esse processo:

```text
não executa workflows

não executa activities

apenas materializa occurrences
```

As executions iniciadas serão consumidas pelos orchestrators normais.

---

# 17. Scheduler distribuído

Múltiplas instâncias devem poder executar scheduling simultaneamente.

Exemplo:

```text
scheduler A

scheduler B

scheduler C
```

sem gerar duplicate starts.

Não exigir:

```text
rode exatamente uma instância
```

Usar o mesmo modelo de confiabilidade já adotado no projeto:

```text
SQL claim

lease

fencing

transaction

deterministic occurrence identity
```

---

# 18. Claim

Cada schedule elegível precisa ser reivindicado temporariamente por uma instância.

Conceitualmente:

```text
schedule
   │
   ▼
claim
   │
   ▼
compute occurrences
   │
   ▼
persist occurrence/start
   │
   ▼
advance cursor
```

Se o processo morrer:

```text
lease expires
```

e outro scheduler continua.

---

# 19. Não manter um timer em memória por schedule

Não implementar:

```text
10.000 schedules
=
10.000 setTimeouts
```

O scheduler deve consultar schedules devidos usando armazenamento.

Algo conceitualmente parecido com:

```sql
WHERE next_occurrence_at <= now
```

ordenado por:

```text
next_occurrence_at
```

e processado em batches.

---

# 20. Wake-up

A arquitetura pode usar polling leve para deadlines, assim como outros componentes temporais da biblioteca.

Se houver primitive/wakeup interno reutilizável já existente, utilizar.

Mas correctness não deve depender de um timer JavaScript individual por schedule.

---

# 21. Occurrence

Cada disparo é uma occurrence persistida.

Adicionar storage equivalente a:

```text
better_workflows_schedule_occurrences
```

Com:

```text
namespace

schedule_name

scheduled_at

sequence

trigger_type

state

execution_id

created_at
```

Identidade única:

```text
(namespace, schedule_name, scheduled_at)
```

ou outra identidade determinística equivalente.

Isso é fundamental para deduplicação.

---

# 22. Occurrence e execution precisam ser transacionais

Nunca permitir:

```text
occurrence marked emitted

CRASH

workflow never started
```

nem:

```text
workflow started

CRASH

occurrence cursor not advanced

workflow started again
```

Usar transação/outbox.

Uma abordagem recomendada:

```text
BEGIN

lock schedule

persist occurrence if absent

accept workflow run
ou
persist start outbox

advance schedule cursor

COMMIT
```

Depois:

```text
dispatch workflow idempotently
```

Seguir o modelo já utilizado no journal.

---

# 23. Workflow idempotency key da occurrence

Cada occurrence deve possuir idempotency key determinística.

Exemplo conceitual:

```text
@better-workflows/schedule
/
reports.daily
/
2026-09-17T11:00:00.000Z
```

Não usar:

```text
randomUUID()
```

como identidade da occurrence.

Isso garante:

```text
scheduler A
scheduler B
crash
replay
```

convergindo para a mesma execution.

---

# 24. Relação com workflow `idempotencyKey`

Schedule occurrences não devem depender da função:

```ts
WorkflowOptions.idempotencyKey
```

para sua própria deduplicação.

Um workflow pode possuir idempotency resolver para starts externos.

O schedule precisa de uma chave interna própria.

Portanto:

```text
external start identity
≠
scheduled occurrence identity
```

Mesmo que ambos acabem utilizando o mecanismo interno de dedupe da execution.

---

# 25. Misfire

Adicionar:

```ts
type ScheduleMisfirePolicy = 'skip' | 'latest' | 'catch-up'
```

Default recomendado:

```text
latest
```

---

# 26. `misfire: 'skip'`

Exemplo:

```text
cron:
a cada hora

runtime offline:
08
09
10
11

runtime volta:
11:30
```

Resultado:

```text
08 skipped
09 skipped
10 skipped
11 skipped

next:
12:00
```

Nenhum workflow é iniciado pelas occurrences perdidas.

Registrar operational events/metrics apropriados.

---

# 27. `misfire: 'latest'`

Mesmo caso:

```text
08
09
10
11
```

Ao voltar 11:30:

```text
executa somente occurrence 11:00
```

Depois:

```text
next = 12:00
```

---

# 28. `misfire: 'catch-up'`

Executar occurrences perdidas:

```text
08
09
10
11
```

Cada uma recebe sua própria scheduled timestamp.

Exemplo input resolver:

```ts
input: ({ scheduledAt }) => ({
  hour: scheduledAt
})
```

deve receber:

```text
08:00
09:00
10:00
11:00
```

e não:

```text
11:30
```

---

# 29. `maxCatchUp`

Catch-up precisa de proteção.

Adicionar:

```ts
maxCatchUp?: number
```

Default razoável:

```text
100
```

ou outro valor documentado.

Se houver 10.000 occurrences atrasadas:

```text
não criar 10.000 executions em um único tick
```

Processar no máximo:

```text
maxCatchUp
```

por ciclo.

O cursor não pode perder as demais.

Elas ficam elegíveis para batches posteriores.

---

# 30. Overlap

Adicionar:

```ts
type ScheduleOverlapPolicy = 'allow' | 'skip'
```

Default recomendado:

```text
allow
```

---

# 31. `overlap: 'allow'`

Cada occurrence cria uma execution.

Exemplo:

```text
08:00 → execution A still running

08:05 → execution B
08:10 → execution C
```

Correto.

---

# 32. `overlap: 'skip'`

Antes de materializar a occurrence, verificar se existe execution ainda ativa iniciada por esse schedule.

Active inclui:

```text
accepted

running

waiting

blocked

paused

cancelling
```

Também considerar continuation chains.

Se:

```text
A → continueAsNew → B
```

e B ainda está ativo:

```text
schedule continua ocupado
```

Não considerar A terminal `continued` como ausência de execução ativa.

A chain ainda está viva.

---

# 33. Occurrence skipped por overlap

Uma occurrence ignorada por overlap deve ser persistida como:

```text
skipped
```

com reason:

```text
overlap
```

para não voltar a ser processada depois.

Não simplesmente avançar cursor silenciosamente.

Isso é importante para:

```text
auditoria

metrics

admin
```

---

# 34. Future overlap policy

Não implementar nesta versão:

```text
overlap: 'queue'
```

ou:

```text
replace
cancel-previous
```

Essas semânticas são mais complexas.

Alpha.8 deve manter:

```text
allow
skip
```

---

# 35. Timezones

Cron deve aceitar:

```ts
timezone?: string
```

Exemplo:

```text
America/Fortaleza
America/Sao_Paulo
UTC
```

Validar no bootstrap.

Timezone inválida:

```text
INVALID_SCHEDULE_TIMEZONE
```

Interval schedules não precisam de timezone.

---

# 36. DST

Cron precisa respeitar o comportamento da implementação de Cron utilizada pelo Effect para:

```text
daylight saving transitions
missing local times
duplicated local times
```

Adicionar testes usando timezones que possuem DST.

Não implementar manualmente regras de timezone.

Usar o primitive de Cron da versão pinned do Effect quando apropriado para:

```text
parse
validation
next occurrence
```

Persistência continua sendo do `better-workflows`.

---

# 37. Cron validation

Expressão inválida deve falhar no bootstrap:

```text
INVALID_CRON_EXPRESSION
```

Não esperar a primeira execução.

---

# 38. Interval validation

Isto deve falhar:

```ts
@Interval({
  name: 'bad',
  every: 0
})
```

ou durations negativas.

Usar o parser de duration já existente na lib.

---

# 39. Pause / resume de schedule

Adicionar no admin:

```ts
await admin.pauseSchedule('reports.daily')
```

e:

```ts
await admin.resumeSchedule('reports.daily')
```

Pause significa:

```text
não gerar novas occurrences
```

Não interfere em executions já iniciadas.

---

# 40. Semântica de resume

Quando schedule ficou paused por várias horas/dias, resume precisa aplicar misfire policy.

Exemplo:

```text
paused 08:00
resume 13:00
```

Com:

```text
misfire=skip
```

não executa occurrences perdidas.

Com:

```text
latest
```

executa a mais recente.

Com:

```text
catch-up
```

materializa conforme `maxCatchUp`.

Persistir:

```text
paused_at
```

ou cursor suficiente para obter comportamento determinístico.

---

# 41. Admin API

Adicionar:

```ts
admin.listSchedules(options?)
```

```ts
admin.getSchedule(name)
```

```ts
admin.pauseSchedule(name)
```

```ts
admin.resumeSchedule(name)
```

```ts
admin.triggerSchedule(name)
```

Opcionalmente:

```ts
admin.historySchedule(name, ...)
```

se a ocorrência já possuir paginação reutilizável.

---

# 42. Schedule snapshot

Modelo público equivalente a:

```ts
interface ScheduleSnapshot {
  readonly name: string

  readonly workflow: string

  readonly workflowVersion: number

  readonly type: 'cron' | 'interval'

  readonly status: 'active' | 'paused'

  readonly lastOccurrence?: string

  readonly nextOccurrence: string

  readonly lastExecutionId?: string

  readonly revision: number
}
```

Adicionar detalhes adicionais úteis sem expor storage internals.

---

# 43. `triggerSchedule`

Manual trigger deve iniciar uma execution através da definição do schedule.

Exemplo:

```ts
await admin.triggerSchedule('reports.daily')
```

Isso gera occurrence com:

```text
trigger = manual
```

Não deve alterar:

```text
lastOccurrence
nextOccurrence
```

da timeline normal.

Manual trigger não significa:

```text
"fingir que agora é a occurrence cron"
```

---

# 44. Input do manual trigger

Na primeira versão, manual trigger utiliza o mesmo input resolver do schedule.

`scheduledAt` para manual trigger deve ser o database/business clock atual.

`trigger`:

```text
manual
```

permitirá ao resolver distinguir caso necessário.

---

# 45. Idempotência de manual trigger

Cada chamada manual deve gerar uma occurrence distinta.

Pode aceitar opcionalmente:

```ts
{
  idempotencyKey?: string
}
```

para operações automatizadas.

Mas não é obrigatório nesta primeira versão.

Se não houver key:

```text
cada trigger é novo
```

---

# 46. CLI

Adicionar:

```text
better-workflows schedules list
```

```text
better-workflows schedules show reports.daily
```

```text
better-workflows schedules pause reports.daily
```

```text
better-workflows schedules resume reports.daily
```

```text
better-workflows schedules trigger reports.daily
```

Opcional:

```text
better-workflows schedules occurrences reports.daily
```

O CLI deve usar:

```text
WorkflowsAdmin backend
```

Nunca duplicar SQL.

---

# 47. `WorkflowsAdmin.stats()`

Expandir snapshot operacional para schedules.

Adicionar algo equivalente a:

```ts
schedules: {
  active: 10,
  paused: 2,
  overdue: 1,
  oldestLagMs: 400
}
```

Sem retornar todas as definições.

Listagem detalhada pertence a:

```text
listSchedules
```

---

# 48. Health / Readiness

Scheduler habilitado precisa entrar no readiness.

Se:

```ts
execution.schedules.enabled = true
```

verificar:

```text
scheduler loop running

storage reachable

schedule registry valid

last successful scheduler pass recente
```

Um schedule atrasado individualmente NÃO torna necessariamente o processo unready.

Mas scheduler loop parado/stale pode tornar:

```text
degraded
```

ou:

```text
down
```

conforme duração.

---

# 49. Observability

Integrar scheduling ao vocabulário da alpha.7.

Metrics:

```text
better_workflows.schedule.occurrence

better_workflows.schedule.started

better_workflows.schedule.skipped

better_workflows.schedule.misfire

better_workflows.schedule.catch_up

better_workflows.schedule.manual_trigger

better_workflows.schedule.failure
```

Histogram:

```text
better_workflows.schedule.lag
```

Onde:

```text
actual materialization time
-
scheduled occurrence time
```

---

# 50. Metric attributes

Permitir:

```text
schedule.name

workflow.name

workflow.version

schedule.type

schedule.trigger

schedule.misfire_policy

schedule.overlap_policy

skip.reason
```

Não usar:

```text
executionId

occurrence timestamp

occurrence sequence

input

idempotency key
```

como metric labels.

---

# 51. Tracing

Criar spans curtos:

```text
better-workflows.schedule.tick
```

para scheduler pass, se não gerar volume excessivo.

Mais importante:

```text
better-workflows.schedule.trigger
```

Attributes:

```text
schedule.name

workflow.name

workflow.version

scheduled_at

trigger

misfire_policy

overlap_policy
```

Occurrence timestamp pode aparecer em span/log.

Não em metric label.

---

# 52. Trace propagation

A execution iniciada pelo schedule deve nascer dentro da trace de:

```text
schedule.trigger
```

de forma que seja possível navegar:

```text
schedule.trigger
      │
      ▼
workflow.start
      │
      ▼
workflow.round
```

Sem tornar trace context parte da identidade durável.

---

# 53. Logs

Structured logs:

Debug:

```text
schedule occurrence evaluated

schedule occurrence materialized

schedule occurrence skipped
```

Info:

```text
scheduler started

scheduler stopped

schedule paused

schedule resumed
```

Warn:

```text
misfire detected

catch-up limited by maxCatchUp

scheduler lease lost

scheduler loop recovered
```

Error:

```text
persistent scheduler failure
```

Nunca logar input do schedule automaticamente.

---

# 54. Testing module

Schedules precisam funcionar com:

```text
WorkflowsTestingModule
```

e manual business clock.

Exemplo:

```ts
const app = await testingApp(...)

await app.clock.advance('1d')
```

deve materializar as occurrences relevantes.

---

# 55. Cron testing

Exemplo:

```text
current test time:
07:59

cron:
08:00
```

Depois:

```ts
await clock.advance('2m')
```

esperado:

```text
uma execution criada para 08:00
```

Não depender de real timers.

---

# 56. Interval testing

```text
start:
08:00

interval:
15m
```

Após:

```ts
await clock.advance('1h')
```

esperado:

```text
08:15
08:30
08:45
09:00
```

dependendo da misfire policy e da forma como o testing harness avança deadlines.

---

# 57. Testing + catch-up

Precisa ser possível testar:

```text
process/scheduler disabled
clock advance 5h
scheduler enabled/flush
```

e verificar:

```text
skip

latest

catch-up
```

deterministicamente.

---

# 58. Scheduler test control

Adicionar ao testing harness algo equivalente a:

```ts
await testing.flush()
```

deve também processar schedules.

Ou adicionar:

```ts
await testing.flushSchedules()
```

somente se realmente necessário.

Preferir integrar ao flush/quiescence já existente.

---

# 59. Crash recovery

Adicionar testes com processo real.

Casos obrigatórios:

```text
crash antes de occurrence transaction

crash depois de occurrence insert

crash depois de workflow acceptance

crash antes de cursor advance

crash depois de cursor advance

crash durante catch-up
```

Nenhum caso pode criar duplicate workflow execution para a mesma occurrence.

---

# 60. Multi-process PostgreSQL

Criar teste com:

```text
scheduler A

scheduler B
```

concorrendo sobre o mesmo namespace/database.

Verificar:

```text
uma occurrence
=
uma execution
```

Mesmo quando os dois processos enxergam a ocorrência quase simultaneamente.

---

# 61. Lease expiration

Testar:

```text
scheduler A claims

SIGKILL scheduler A

lease expires

scheduler B continues
```

A occurrence não deve ser perdida.

---

# 62. Overlap tests

Cobrir:

```text
allow
skip
```

E especificamente:

```text
execution A
→ continueAsNew
→ execution B active
```

Com:

```text
overlap=skip
```

a chain deve continuar sendo considerada ativa.

---

# 63. Pause tests

Cobrir:

```text
pause

clock advance

resume

misfire=skip
misfire=latest
misfire=catch-up
```

---

# 64. Timezone tests

Adicionar:

```text
UTC

America/Fortaleza

timezone com DST
```

Verificar cálculo da próxima occurrence.

---

# 65. Configuration drift tests

Dois processos:

```text
reports.daily
```

mas diferentes:

```text
cron expression

timezone

misfire

overlap

workflow version
```

devem detectar conflict.

---

# 66. Schedule change policy

Uma questão importante:

o que acontece se o usuário alterar:

```text
0 8 * * *
```

para:

```text
0 9 * * *
```

entre deployments?

Na alpha.8, NÃO aplicar automaticamente.

Tratar como configuration drift:

```text
SCHEDULE_DEFINITION_CHANGED
```

e exigir reconciliação administrativa explícita.

Isso evita mudanças silenciosas na timeline.

---

# 67. Admin reconciliation

Adicionar operação equivalente a:

```ts
admin.updateScheduleDefinition(
  name,
  {
    confirm: ...
  }
)
```

ou um fluxo de:

```text
preview

apply
```

mais consistente com retention/migrations já existentes.

Entretanto, se isso ampliar demais o escopo, uma primeira versão aceitável é:

```text
bootstrap falha

CLI/admin oferece comando explícito:
schedules reconcile
```

O requisito central é:

```text
não alterar timeline automaticamente
```

---

# 68. Reconciliation policy

Ao aceitar uma nova definição, exigir política para o próximo cursor.

Exemplo:

```ts
{
  from: 'now'
}
```

ou:

```ts
{
  preserveCursor: true
}
```

Na primeira versão pode suportar apenas:

```text
from now
```

para manter semântica previsível.

Documentar que alterar cron definition é uma ação administrativa.

---

# 69. New schedule

Novo decorator em deployment:

```text
schedule ainda não existe no banco
```

deve ser registrado automaticamente.

Primeiro `nextOccurrence` deve ser calculado a partir do:

```text
database/business current time
```

Não criar catch-up histórico anterior à criação do schedule.

---

# 70. Removed schedule

Se schedule persistido existe, mas o decorator deixa de existir no deployment:

não deletar automaticamente.

Marcar como:

```text
orphaned
```

ou detectar:

```text
SCHEDULE_MISSING_FROM_REGISTRY
```

Uma opção razoável:

```text
não executar mais
status = orphaned
```

e permitir admin removal.

Não apagar histórico silenciosamente.

---

# 71. Schedule status

Expandir status para:

```ts
type ScheduleStatus = 'active' | 'paused' | 'orphaned'
```

Possivelmente:

```text
conflicted
```

apenas como admin diagnostic state, se necessário.

---

# 72. Remove schedule

Admin:

```ts
admin.removeSchedule(name)
```

deve exigir:

```text
paused
ou
orphaned
```

e confirmação explícita.

Não remover occurrence history automaticamente se retention ainda depender dela.

---

# 73. Retention de occurrences

Schedule occurrence history pode crescer indefinidamente.

Adicionar retention próprio.

Não incluir auto-cleanup por default.

Permitir no admin futuramente ou nesta versão:

```ts
previewScheduleRetention(...)
pruneScheduleRetention(...)
```

Se isso ampliar demais o escopo, pelo menos garantir que occurrences antigas possam ser removidas junto com execution retention quando seguro.

---

# 74. Relação com execution retention

Se a execution associada a uma occurrence for removida:

não quebrar schedule history.

Pode:

```text
execution_id nullable/tombstone
```

ou manter somente metadata mínima.

Não exigir que schedule occurrence preserve toda a execution para sempre.

---

# 75. Migrations

Adicionar migration forward-only.

Não editar migrations anteriores.

A migration deve criar:

```text
schedule definitions/state

schedule occurrences

indexes necessários
```

Índices esperados:

```text
namespace + next_occurrence_at + state

namespace + schedule_name

namespace + schedule_name + scheduled_at

execution_id
```

Validar query plans quando necessário.

---

# 76. SQLite

SQLite continua single-process.

Scheduling deve funcionar normalmente.

Leases distribuídos podem ter fast path local quando apropriado, mas não é obrigatório otimizar prematuramente.

Correctness primeiro.

---

# 77. PostgreSQL

Distributed scheduler deve utilizar database time.

Não utilizar relógio local da máquina para decidir ownership/deadline em produção.

Isso evita:

```text
clock skew entre hosts
```

---

# 78. Database time vs test clock

Em produção:

```text
schedule occurrence timeline
```

deve usar database/business clock definido pela arquitetura atual.

No testing module:

```text
manual business clock
```

deve controlar scheduling.

Leases continuam usando database/real time conforme as regras atuais de ownership.

---

# 79. Não usar workflow timer para implementar scheduler

Não criar um workflow infinito interno:

```text
SchedulerWorkflow
while true:
  sleep(...)
```

para cada schedule.

Scheduling é infraestrutura de dispatch, não workflow de aplicação.

Manter responsabilidades separadas.

---

# 80. Não usar `continueAsNew` como implementação do scheduler

Também não fazer:

```text
schedule
=
workflow infinito
+
continueAsNew
```

`continueAsNew` é ferramenta para workflows da aplicação.

O scheduler precisa continuar funcionando mesmo quando nenhum orchestrator estiver executando um schedule workflow interno.

---

# 81. Segurança

Schedule input estático ou derivado pode conter dados sensíveis.

Não adicionar automaticamente a:

```text
metrics

logs

traces
```

Somente metadata operacional.

---

# 82. Public API esperada

Adicionar:

```text
Cron

Interval

ScheduleOccurrence

ScheduleSnapshot

ScheduleStatus

ScheduleMisfirePolicy

ScheduleOverlapPolicy
```

Adicionar métodos em:

```text
WorkflowsAdmin
```

Não criar subpackage separado nesta versão a menos que haja necessidade clara.

Decorators pertencem ao package principal.

---

# 83. Decorator ordering

Isto deve funcionar independentemente da ordem visual dos decorators:

```ts
@Cron(...)
@WorkflowContract(...)
class ...
```

e:

```ts
@WorkflowContract(...)
@Cron(...)
class ...
```

desde que seja tecnicamente possível com TypeScript decorators.

Se ordering inevitavelmente afetar metadata, escolher uma ordem oficial e validar claramente.

Não permitir falha silenciosa.

---

# 84. Validation lifecycle

Validações puramente locais devem acontecer cedo:

```text
decorator/bootstrap
```

Exemplos:

```text
invalid cron

invalid timezone

invalid interval

invalid policies

invalid name
```

Validações que dependem do storage:

```text
definition drift
duplicate ownership
```

acontecem no bootstrap antes de iniciar scheduler workers.

---

# 85. Duplicate schedule names

Dois contracts declarando:

```text
reports.daily
```

devem falhar:

```text
DUPLICATE_SCHEDULE
```

Mesmo que workflows sejam diferentes.

---

# 86. Scheduler loop

Criar componente interno próprio:

```text
ScheduleRuntime

ou

Scheduler
```

Não transformar ainda mais `WorkflowsRuntime` em uma classe monolítica.

A alpha.8 é uma boa oportunidade para preservar separação interna.

Responsabilidade:

```text
discover due schedules

claim

materialize occurrences

advance cursor

report health

emit telemetry
```

---

# 87. Separação sugerida

Internamente:

```text
schedule-types / metadata

ScheduleRegistry

ScheduleStore

Scheduler

ScheduleAdministration

ScheduleTelemetry
```

Adaptar nomes ao estilo existente.

Evitar colocar todas as queries em `Journal` se scheduling possui domínio próprio significativo.

Pode compartilhar:

```text
SqlClient
clock
workflow acceptance
telemetry
```

---

# 88. Workflow start reutilizável

Não duplicar a lógica de:

```text
input validation

execution identity

accept

dispatch outbox
```

Criar/refatorar uma operação interna reutilizável para:

```text
external WorkflowClient.start

scheduled occurrence start

child start
```

quando possível sem mudar semântica.

Schedule start deve usar exatamente as mesmas garantias de acceptance.

---

# 89. Failure behavior

Se uma occurrence não consegue iniciar por erro transitório de storage:

```text
não marcar como emitted
```

Ela deve ser retriada.

Se input resolver falha:

como o resolver é puro e síncrono, isso representa configuração/programming error.

Não retry infinitamente silenciosamente.

Marcar occurrence/schedule com erro operacional observável e gerar:

```text
SCHEDULE_INPUT_ERROR
```

Uma opção adequada é deixar o schedule ativo, mas a occurrence permanece failed e entra em admin visibility.

Definir explicitamente o comportamento durante implementação.

---

# 90. Schedule operational failures

Adicionar estado de occurrence:

```ts
type ScheduleOccurrenceStatus = 'started' | 'skipped' | 'failed'
```

Para failure:

```text
reason_code
```

controlado pela biblioteca.

Admin deve conseguir inspecionar failures.

---

# 91. Não criar DLQ para schedules nesta versão

Schedule occurrence failure não precisa reutilizar Activity DLQ.

São domínios diferentes.

Pode haver no futuro:

```text
schedule dead letter
```

mas alpha.8 deve manter uma ocorrência `failed` administrável.

---

# 92. Retry de occurrence failed

Adicionar:

```ts
admin.retryScheduleOccurrence(...)
```

somente se a implementação ficar simples.

Caso contrário, fora do escopo inicial.

Mas pelo menos preservar informação suficiente para futura recuperação.

---

# 93. Documentation

Atualizar README com seção:

```text
Scheduled workflows
```

Mostrar:

```text
@Cron

@Interval

misfire

overlap

admin
testing
```

---

# 94. Architecture docs

Documentar:

```text
schedule ownership

occurrence identity

distributed claiming

crash windows

cursor advancement

misfire semantics

overlap semantics

database clock

testing clock

definition reconciliation
```

---

# 95. JSDoc

Todos os novos decorators/types/admin methods precisam de JSDoc com exemplos compiláveis.

`docs:check` continua obrigatório.

---

# 96. Changelog

Adicionar release:

```text
0.1.0-alpha.8
```

com:

```text
durable cron/interval schedules

misfire policies

overlap policies

distributed scheduling

admin/CLI

testing clock

telemetry
```

---

# 97. Test matrix

Cobrir no mínimo:

```text
SQLite Bun

SQLite Node

PostgreSQL

simple workflow

contract-first workflow

single process

multi-process scheduler

API-only

scheduler-only

orchestrator-only
```

---

# 98. Performance tests

Adicionar teste com número significativo de schedules.

Por exemplo:

```text
1.000 schedules
```

ou quantidade adequada ao ambiente de CI.

Objetivo:

garantir que scheduler não:

```text
crie timer individual por schedule

execute query por schedule a cada poll
```

Não precisa ser benchmark rigoroso.

Deve detectar regressões arquiteturais óbvias.

---

# 99. Acceptance tests principais

Criar testes ponta a ponta para:

## Cron normal

```text
08:00
→ 1 execution
```

## Interval normal

```text
15m
→ occurrences corretas
```

## Crash recovery

```text
schedule due
scheduler crash
restart
→ exatamente uma execution
```

## Multi scheduler

```text
A + B
→ exatamente uma execution
```

## Misfire skip

```text
offline
→ zero catch-up
```

## Misfire latest

```text
offline
→ uma execution
```

## Misfire catch-up

```text
offline
→ N executions
```

## maxCatchUp

```text
grande backlog
→ batches limitados
```

## overlap skip

```text
workflow ativo
→ occurrence skipped
```

## continueAsNew + overlap skip

```text
generation 0 continued
generation 1 active
→ still skipped
```

## pause/resume

Todas as misfire policies.

## manual trigger

Não altera timeline normal.

## contract-first

Schedule no contract e handler separado.

## config drift

Bootstrap falha.

## timezone

Cron correto.

## virtual time

Sem espera real.

---

# 100. Critérios de aceite

A alpha.8 está pronta quando:

1. Existe `@Cron`.

2. Existe `@Interval`.

3. Schedules pertencem ao workflow contract.

4. Simple e contract-first funcionam igualmente.

5. Schedule name é identidade explícita e persistida.

6. Cron expression é validada no bootstrap.

7. Timezone é validada.

8. Interval é durável e não depende de `setInterval`.

9. Cursor de scheduling é persistido.

10. Restart não perde occurrences.

11. Crash não duplica occurrence.

12. Múltiplos schedulers podem concorrer.

13. Occurrence possui identidade determinística.

14. Workflow start da occurrence é idempotente.

15. Existe `misfire: skip`.

16. Existe `misfire: latest`.

17. Existe `misfire: catch-up`.

18. Existe `maxCatchUp`.

19. Existe `overlap: allow`.

20. Existe `overlap: skip`.

21. Continuation chain é considerada ativa para overlap.

22. Pause e resume são persistentes.

23. Resume respeita misfire policy.

24. Manual trigger existe.

25. Manual trigger não altera cursor normal.

26. Admin consegue listar schedules.

27. Admin consegue inspecionar schedule.

28. Admin consegue pause/resume.

29. CLI possui operações equivalentes.

30. `WorkflowsAdmin.stats()` inclui scheduler state agregado.

31. Readiness observa scheduler quando habilitado.

32. Observability possui schedule metrics.

33. Observability possui schedule tracing.

34. Nenhum schedule input aparece automaticamente na telemetry.

35. WorkflowsTestingModule suporta schedules com business clock virtual.

36. PostgreSQL multi-process está coberto.

37. SQLite está coberto.

38. Configuration drift entre processos falha cedo.

39. Alteração de schedule definition não é aplicada silenciosamente.

40. New schedules iniciam cursor a partir da criação/deployment, não fazem catch-up histórico.

41. Removed schedules não são deletados silenciosamente.

42. Migrations são forward-only.

43. Package declarations continuam sem Effect types públicos.

44. Node 22.16 e Node 24 smoke tests continuam passando.

45. `bun run check` passa integralmente.

---

# 101. Fora do escopo da alpha.8

Não implementar nesta release:

```text
calendar schedules arbitrários

RRULE completo

one-shot scheduled start API

overlap=queue

overlap=replace

cancel previous execution

automatic schedule definition updates

multiple decorators/schedules no mesmo contract

event-based schedules

dashboard visual

ActivityContract separation

workflow patch/version gates

cross-version continueAsNew

schedule-specific DLQ

auto retention global

public SSE/WebSocket
```

Esses itens podem ser adicionados depois sobre a fundação desta release.

---

# 102. Ordem de implementação

Dividir preferencialmente em quatro PRs.

## PR 1 — Contracts, metadata e persistence

Implementar:

```text
@Cron

@Interval

public types

registry

validation

schedule store

migrations

definition hash

configuration drift

schedule discovery
```

Sem scheduler loop ainda.

---

## PR 2 — Runtime distribuído

Implementar:

```text
Scheduler

database cursor

claims / leases

occurrence persistence

workflow acceptance

crash-safe start

misfire policies

overlap policies

pause/resume
```

Adicionar testes SQLite e PostgreSQL.

---

## PR 3 — Admin, CLI e testing

Implementar:

```text
list/show

pause/resume

manual trigger

stats

CLI

virtual clock

testing harness

definition reconciliation básica
```

---

## PR 4 — Observability e hardening

Implementar:

```text
metrics

tracing

structured logs

health/readiness

privacy tests

multi-process crash tests

DST/timezone coverage

performance/backlog tests

docs

package smoke
```

---

# 103. Resultado esperado

Depois da alpha.8 deve ser possível escrever:

```ts
@Cron({
  name: 'reports.daily',

  expression: '0 8 * * *',

  timezone: 'America/Fortaleza',

  misfire: 'latest',

  overlap: 'skip',

  input: ({ scheduledAt }) => ({
    date: scheduledAt
  })
})
@WorkflowContract({
  name: 'reports.generate-daily',
  version: 1,
  input: DailyInput,
  output: DailyOutput
})
export abstract class GenerateDailyReportWorkflow {
  abstract run(input: DailyInputType, ctx: WorkflowContext): Promise<DailyOutputType>
}
```

Handler:

```ts
@Workflow(GenerateDailyReportWorkflow)
export class GenerateDailyReportHandler implements GenerateDailyReportWorkflow {
  constructor(private readonly reports: ReportsService) {}

  async run(input, ctx) {
    return ctx.activities(ReportActivities).generate(input, {
      stepId: 'generate'
    })
  }
}
```

Deployment:

```ts
WorkflowsModule.forRoot({
  namespace: 'reports',

  storage: postgres({
    connectionString: databaseUrl
  }),

  topology: 'distributed',

  execution: {
    workflows: {
      enabled: false
    },

    activities: {
      enabled: false
    },

    schedules: {
      enabled: true
    }
  }
})
```

O runtime deve então fornecer:

```text
durable timeline

distributed scheduler ownership

exactly-once occurrence identity
sobre
at-least-once workflow execution semantics

misfire recovery

overlap control

admin operations

virtual-time testing

OTLP observability
```

sem que o desenvolvedor precise operar um scheduler externo.

O objetivo final da alpha.8 é transformar o `better-workflows` de:

```text
"execute workflows duráveis quando alguém os inicia"
```

para:

```text
"execute e opere workflows duráveis também como processos temporais autônomos"
```

preservando a experiência NestJS e as garantias de durabilidade construídas nas versões anteriores.
