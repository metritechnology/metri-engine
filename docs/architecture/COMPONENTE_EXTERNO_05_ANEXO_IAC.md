# Anexo IaC — Metri Schedulers Hub

> **Documento padre:** [COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md](COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md)  
> **Naturaleza:** Implementación — infraestructura como código  
> **Alcance:** `template.yaml` completo y Makefile del stack

Este anexo existe para que el documento de arquitectura siga siendo un documento de
arquitectura. El IaC vive aquí porque es **implementación**, no diseño: cambia con cada
ajuste de despliegue sin que la arquitectura se mueva.

**Las decisiones están en el padre, no aquí.** Este anexo las materializa:

| Decisión | Dónde se argumenta | Cómo se materializa abajo |
|---|---|---|
| Tres funciones de responsabilidad única | §1.1 | `LambdaChronos` / `LambdaKairos` / `LambdaIris` |
| Ruteo por `EventPattern`, no por `switch` | §1.1 | `detail.trigger_type` en cada `Pattern` |
| IAM segregado por función | §1.1, §10 | Chronos sin acceso al payload; Iris sólo `GetItem` |
| DLQ estándar por función | §10.1 | `ChronosDLQ` / `KairosDLQ` / `IrisDLQ` + `DeadLetterConfig` |
| Reintento en el target, no en la Lambda | §10.1 | `RetryPolicy` por event source |
| Ledger de orden con lápidas | §10.3 | `HubStateTable` con TTL `expires_at` |
| Agrupar por entorno, nunca por tenant | §8.4 | `MetriJobsScheduleGroup` |

> Si al leer el YAML algo parece arbitrario, la razón está en el documento padre. No
> modifiques un recurso de aquí sin comprobar qué garantía sostiene.

> **Estado de validación:** el template pasa `sam validate --lint` sin hallazgos.
> Reprodúcelo tras cualquier cambio:
>
> ```bash
> sam validate --template template.yaml --region us-east-1 --lint
> ```

---

## 1. `template.yaml` — IaC SAM del Schedulers Hub

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: >
  metri-schedulers

  Hub de trampas temporales y telemetricas (Patron Boomerang).
  Tres funciones Go de responsabilidad unica: Chronos (CRON/EXACT_TIME),
  Kairos (provision TELEMETRY) e Iris (correlacion breach -> fired).
  No ejecuta acciones de negocio ni escribe en Metri Engine Core.

Parameters:
  Environment:
    Type: String
    AllowedValues: [development, staging, production]
    Default: production

  MetriEventBusName:
    Type: String
    Default: metri-event-bus
    Description: "Nombre del EventBridge Bus de Metri Engine Core. El Hub NO lo crea."

  MetriEventBusArn:
    Type: String
    Description: "ARN del mismo bus — destino de PutEvents de Kairos, Iris y del Scheduler"

  LedgerTtlDays:
    Type: Number
    Default: 21
    MinValue: 15
    Description: >
      Vida de las lapidas del ledger (seccion 10.3). El limite que manda NO es el
      MaximumEventAgeInSeconds del target (1 h): es la RETENCION DE LA DLQ, 14 dias.
      El runbook contempla reprocesar la DLQ a mano, asi que un 'created' rescatado
      al dia 10 encontraria la lapida caducada y recrearia un Schedule fantasma para
      un Job ya borrado. Por eso el minimo es 15 y el valor por defecto 21.

Globals:
  Function:
    Runtime: provided.al2023          # Go compilado AOT (bootstrap)
    Architectures: [arm64]            # Graviton: menor costo y cold-start
    MemorySize: 128                   # Huella minima: el trabajo es I/O, no CPU
    Timeout: 15
    Tracing: Active
    Environment:
      Variables:
        ENVIRONMENT:      !Ref Environment
        HUB_STATE_TABLE:  !Ref HubStateTable
        LEDGER_TTL_DAYS:  !Ref LedgerTtlDays

Resources:

  # ══ MENSAJERIA — una DLQ por funcion (§10.1) ══════════════════════════
  # Estandar, no FIFO: EventBridge no admite colas FIFO como target ni como
  # destino de DeadLetterConfig, y una FIFO exigiria un MessageGroupId que
  # EventBridge no puede proporcionar.
  ChronosDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "metri-scheduler-chronos-dlq-${Environment}"
      MessageRetentionPeriod: 1209600      # 14 dias de ventana de reproceso

  KairosDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "metri-scheduler-kairos-dlq-${Environment}"
      MessageRetentionPeriod: 1209600

  IrisDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "metri-scheduler-iris-dlq-${Environment}"
      MessageRetentionPeriod: 1209600

  # ══ STORAGE — estado interno del Hub (§10.3 ledger + §7 correlacion) ══
  HubStateTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "metri-scheduler-state-${Environment}"
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - { AttributeName: job_id, AttributeType: S }
      KeySchema:
        - { AttributeName: job_id, KeyType: HASH }
      TimeToLiveSpecification:
        AttributeName: expires_at
        Enabled: true
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      # Una fila por Job, dos responsabilidades separadas:
      #  1) last_applied_ulid  -> ledger de orden, TODOS los trigger_type.
      #     En un DELETE la fila sobrevive como LAPIDA (§10.3).
      #  2) action_payload + idempotency_hash -> correlacion, SOLO TELEMETRY.
      # Propiedad EXCLUSIVA de este SAM: Metri IoT jamas la lee ni la conoce.

  # ══ SCHEDULE GROUP — agrupacion por entorno, nunca por tenant (§8.4) ══
  MetriJobsScheduleGroup:
    Type: AWS::Scheduler::ScheduleGroup
    Properties:
      Name: !Sub "metri-jobs-${Environment}"
      Tags:
        - { Key: Component, Value: metri-schedulers }
        - { Key: Environment, Value: !Ref Environment }

  # ══ COMPUTE 1/3 — CHRONOS: trampas temporales ════════════════════════
  LambdaChronos:
    Type: AWS::Serverless::Function
    Metadata:
      BuildMethod: go1.x
    Properties:
      FunctionName: !Sub "metri-scheduler-chronos-${Environment}"
      CodeUri: cmd/chronos/
      Handler: bootstrap
      Description: "CRON / EXACT_TIME -> EventBridge Scheduler. No participa en T=0."
      ReservedConcurrentExecutions: 20     # acota la rafaga de un import masivo (§8.4)
      Environment:
        Variables:
          SCHEDULE_GROUP_NAME:          !Ref MetriJobsScheduleGroup
          SCHEDULER_EXECUTION_ROLE_ARN: !GetAtt SchedulerExecutionRole.Arn
          METRI_EVENT_BUS_ARN:          !Ref MetriEventBusArn   # destino del target, no PutEvents propio
      Events:
        OnTemporalJobMutation:
          Type: EventBridgeRule
          Properties:
            EventBusName: !Ref MetriEventBusName
            Pattern:                       # El bus filtra; Chronos no ramifica
              source: ["metri.engine"]
              detail-type:
                - "system.scheduled_job.created"
                - "system.scheduled_job.updated"
                - "system.scheduled_job.deleted"
              detail:
                trigger_type: ["CRON", "EXACT_TIME"]
            RetryPolicy:                   # §10.1 — reintento real, no retencion
              MaximumRetryAttempts: 20
              MaximumEventAgeInSeconds: 3600
            DeadLetterConfig:
              Arn: !GetAtt ChronosDLQ.Arn
      Policies:
        - Statement:
            - Effect: Allow
              Action:
                - scheduler:CreateSchedule
                - scheduler:UpdateSchedule
                - scheduler:DeleteSchedule
                - scheduler:GetSchedule
              # Acotado al grupo de Metri, no a toda la cuenta
              Resource: !Sub "arn:aws:scheduler:${AWS::Region}:${AWS::AccountId}:schedule/metri-jobs-${Environment}/metri-job-*"
            - Effect: Allow
              Action: iam:PassRole
              Resource: !GetAtt SchedulerExecutionRole.Arn
              Condition:                   # Impide reutilizar el PassRole para otro servicio
                StringEquals:
                  iam:PassedToService: scheduler.amazonaws.com
            - Effect: Allow                # Ledger de orden (§10.3) — NO el payload
              Action: dynamodb:UpdateItem
              Resource: !GetAtt HubStateTable.Arn
              Condition:                   # Acota Chronos a los atributos del ledger
                ForAllValues:StringEquals:
                  dynamodb:Attributes: [job_id, last_applied_ulid, expires_at]

  # ══ COMPUTE 2/3 — KAIROS: provision telemetrica ══════════════════════
  LambdaKairos:
    Type: AWS::Serverless::Function
    Metadata:
      BuildMethod: go1.x
    Properties:
      FunctionName: !Sub "metri-scheduler-kairos-${Environment}"
      CodeUri: cmd/kairos/
      Handler: bootstrap
      Description: "TELEMETRY -> provision_requested + contexto de correlacion. No espera el breach."
      Environment:
        Variables:
          METRI_EVENT_BUS_NAME: !Ref MetriEventBusName
      Events:
        OnTelemetryJobMutation:
          Type: EventBridgeRule
          Properties:
            EventBusName: !Ref MetriEventBusName
            Pattern:                       # Filtro complementario al de Chronos
              source: ["metri.engine"]
              detail-type:
                - "system.scheduled_job.created"
                - "system.scheduled_job.updated"
                - "system.scheduled_job.deleted"
              detail:
                trigger_type: ["TELEMETRY"]
            RetryPolicy:
              MaximumRetryAttempts: 20
              MaximumEventAgeInSeconds: 3600
            DeadLetterConfig:
              Arn: !GetAtt KairosDLQ.Arn
      Policies:                            # Sin scheduler:*: Kairos no crea trampas AWS
        - Statement:
            - Effect: Allow
              Action: [dynamodb:UpdateItem, dynamodb:PutItem, dynamodb:DeleteItem]
              Resource: !GetAtt HubStateTable.Arn
            - Effect: Allow
              Action: events:PutEvents
              Resource: !Ref MetriEventBusArn

  # ══ COMPUTE 3/3 — IRIS: correlacion de retorno (camino caliente) ═════
  LambdaIris:
    Type: AWS::Serverless::Function
    Metadata:
      BuildMethod: go1.x
    Properties:
      FunctionName: !Sub "metri-scheduler-iris-${Environment}"
      CodeUri: cmd/iris/
      Handler: bootstrap
      Description: "breach correlacionado -> system.scheduled_job.fired. Sin round-trip al Core."
      ReservedConcurrentExecutions: 50     # Aisla rafagas de planta del resto del stack
      Environment:
        Variables:
          METRI_EVENT_BUS_NAME: !Ref MetriEventBusName
      Events:
        OnAlertBreach:                     # §7 — retorno del trigger TELEMETRY
          Type: EventBridgeRule
          Properties:
            EventBusName: !Ref MetriEventBusName
            Pattern:
              source: ["metri.iot"]
              detail-type: ["system.iot.alert.breach"]
              detail:
                correlation_id: [{"exists": true}]   # Ignora breaches sin Job detras
            RetryPolicy:
              MaximumRetryAttempts: 20
              MaximumEventAgeInSeconds: 3600
            DeadLetterConfig:
              Arn: !GetAtt IrisDLQ.Arn
      Policies:                            # Solo lectura: Iris nunca muta la correlacion
        - Statement:
            - Effect: Allow
              Action: dynamodb:GetItem     # Sin escritura, deliberadamente
              Resource: !GetAtt HubStateTable.Arn
            - Effect: Allow
              Action: events:PutEvents
              Resource: !Ref MetriEventBusArn

  # ══ IAM — rol que AWS asume al disparar en T=0 ═══════════════════════
  # En CRON/EXACT_TIME el 'fired' lo publica EventBridge Scheduler con este
  # role. Ninguna Lambda del Hub se ejecuta en T=0.
  SchedulerExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "metri-scheduler-execution-${Environment}"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal: { Service: scheduler.amazonaws.com }
            Action: sts:AssumeRole
            Condition:                     # Confused-deputy guard
              StringEquals:
                aws:SourceAccount: !Ref AWS::AccountId
      Policies:
        - PolicyName: PublishFiredOnly
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action: events:PutEvents   # Unica accion concedida en toda la cuenta
                Resource: !Ref MetriEventBusArn

  # ══ OBSERVABILIDAD — alarmas prometidas en §8.4 y SCH-6 ══════════════
  ChronosDLQDepthAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub "metri-scheduler-chronos-dlq-${Environment}"
      AlarmDescription: >
        Mensajes en la DLQ de Chronos. Nadie los reintenta solos (§10.1):
        cada mensaje aqui es un Job que no quedo programado en AWS.
      Namespace: AWS/SQS
      MetricName: ApproximateNumberOfMessagesVisible
      Dimensions:
        - { Name: QueueName, Value: !GetAtt ChronosDLQ.QueueName }
      Statistic: Maximum
      Period: 300
      EvaluationPeriods: 1
      Threshold: 1
      ComparisonOperator: GreaterThanOrEqualToThreshold
      TreatMissingData: notBreaching

  IrisLatencyAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub "metri-scheduler-iris-latency-${Environment}"
      AlarmDescription: "SLO del camino caliente: latencia breach -> fired"
      Namespace: AWS/Lambda
      MetricName: Duration
      Dimensions:
        - { Name: FunctionName, Value: !Ref LambdaIris }
      ExtendedStatistic: p99
      Period: 60
      EvaluationPeriods: 3
      Threshold: 2000
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching

  # Metricas SEGREGADAS por funcion, nunca agregadas: una grafica unica de
  # "errores del Hub" esconderia que Iris esta cayendo mientras Chronos va bien.
  SchedulerDashboard:
    Type: AWS::CloudWatch::Dashboard
    Properties:
      DashboardName: !Sub "metri-schedulers-${Environment}"
      DashboardBody: !Sub |
        {
          "widgets": [
            {
              "type": "metric", "x": 0, "y": 0, "width": 12, "height": 6,
              "properties": {
                "title": "Errores por funcion (nunca agregados)",
                "region": "${AWS::Region}",
                "metrics": [
                  ["AWS/Lambda","Errors","FunctionName","${LambdaChronos}",{"label":"Chronos"}],
                  ["AWS/Lambda","Errors","FunctionName","${LambdaKairos}",{"label":"Kairos"}],
                  ["AWS/Lambda","Errors","FunctionName","${LambdaIris}",{"label":"Iris"}]
                ],
                "stat": "Sum", "period": 300
              }
            },
            {
              "type": "metric", "x": 12, "y": 0, "width": 12, "height": 6,
              "properties": {
                "title": "Profundidad de DLQ — cada mensaje es un Job no programado",
                "region": "${AWS::Region}",
                "metrics": [
                  ["AWS/SQS","ApproximateNumberOfMessagesVisible","QueueName","${ChronosDLQ.QueueName}",{"label":"Chronos"}],
                  ["AWS/SQS","ApproximateNumberOfMessagesVisible","QueueName","${KairosDLQ.QueueName}",{"label":"Kairos"}],
                  ["AWS/SQS","ApproximateNumberOfMessagesVisible","QueueName","${IrisDLQ.QueueName}",{"label":"Iris"}]
                ],
                "stat": "Maximum", "period": 300
              }
            },
            {
              "type": "metric", "x": 0, "y": 6, "width": 12, "height": 6,
              "properties": {
                "title": "Iris p99 — SLO del camino caliente breach -> fired",
                "region": "${AWS::Region}",
                "metrics": [["AWS/Lambda","Duration","FunctionName","${LambdaIris}"]],
                "stat": "p99", "period": 60,
                "annotations": {"horizontal":[{"label":"SLO 2s","value":2000}]}
              }
            },
            {
              "type": "metric", "x": 12, "y": 6, "width": 12, "height": 6,
              "properties": {
                "title": "Throttling de CreateSchedule — satura la DLQ en imports masivos",
                "region": "${AWS::Region}",
                "metrics": [["AWS/Lambda","Throttles","FunctionName","${LambdaChronos}"]],
                "stat": "Sum", "period": 300
              }
            },
            {
              "type": "text", "x": 0, "y": 12, "width": 24, "height": 3,
              "properties": {
                "markdown": "### Schedules activos frente a la cuota\nEventBridge Scheduler no publica una metrica nativa de conteo de schedules. Un job programado debe publicar `Metri/Schedulers ActiveSchedules` como metrica custom y alarmar al 70% de la cuota vigente. Ver Componente Externo 05 seccion 8.4."
              }
            }
          ]
        }

Outputs:
  ChronosFunctionArn:
    Description: "ARN de Chronos — provisionador temporal"
    Value: !GetAtt LambdaChronos.Arn

  KairosFunctionArn:
    Description: "ARN de Kairos — provisionador telemetrico"
    Value: !GetAtt LambdaKairos.Arn

  IrisFunctionArn:
    Description: "ARN de Iris — correlacionador de retorno"
    Value: !GetAtt LambdaIris.Arn

  HubStateTableName:
    Description: "Tabla de estado interno del Hub (ledger + correlacion)"
    Value: !Ref HubStateTable

  ScheduleGroupName:
    Description: "Schedule Group donde viven todas las trampas temporales"
    Value: !Ref MetriJobsScheduleGroup

  SchedulerExecutionRoleArn:
    Description: "Role que EventBridge Scheduler asume para publicar el fired en T=0"
    Value: !GetAtt SchedulerExecutionRole.Arn
```

## 2. Makefile — comandos de desarrollo

```makefile
FUNCS = chronos kairos iris

build:
	@for f in $(FUNCS); do \
	  GOARCH=arm64 GOOS=linux CGO_ENABLED=0 \
	    go build -tags lambda.norpc -o cmd/$$f/bootstrap ./cmd/$$f/ ; \
	done

test:
	go test ./... -v -race -cover

# Los casos limite viven aqui: sin estas pruebas el Hub parece correcto y no lo es
test-edge:
	go test ./internal/scheduler/ -run 'TestConflict|TestCronTranslation|TestPastEpoch' -v
	go test ./internal/ledger/    -run 'TestStaleUlid|TestTombstone'                    -v
	go test ./internal/event/     -run 'TestBookkeepingDeltaIgnored'                    -v

lint:
	golangci-lint run ./...

validate:
	sam validate --lint

deploy: build
	sam deploy --config-env $(ENV)

local-invoke: build
	sam local invoke LambdaChronos --event events/job_created_cron.json
```

