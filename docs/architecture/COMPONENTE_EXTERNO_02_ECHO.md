# Componente Externo 02 — Echo (Retry Engine)

**Nombre del Componente:** `metri-echo`
**Runtime:** Golang 1.22
**Infraestructura:** AWS SAM · Lambda · EventBridge · SQS · DLQ
**Patrón Arquitectónico:** EventBridge Consumer · Retry with Exponential Backoff · Dead Letter Queue

---

## Definición

**Echo** es el componente Lambda Golang que actúa como motor de recuperación autónoma del sistema.
Consume eventos `DOMAIN_FAULT_DETECTED` con `retryable: true` publicados por `metri-engine`
en EventBridge vía SQS, reintenta la operación fallida con backoff exponencial, y escala al
DLQ cuando los intentos se agotan.

> [!IMPORTANT]
> **Echo NO es parte de `metri-engine`.**
> `metri-engine` solo publica el evento al bus. Echo lo consume de forma completamente
> independiente. No comparten código de runtime ni estado. La única interfaz entre ellos
> es el contrato del evento `DOMAIN_FAULT_DETECTED` en EventBridge.
>
> **¿Por qué Golang para Echo?**
> Al igual que el Event Router, Echo utiliza Golang para actuar como un micro-componente
> ultraligero y nativo de integración. Go ofrece Goroutines para manejar la concurrencia
> al absorber latencias de red externas (reintentos HTTP, llamadas gRPC a `metri-engine`)
> y binarios ARM64 con tiempos de cold start en milisegundos — crítico para un componente
> de recuperación ante incidentes.

---

## Objetivo

Garantizar la **recuperación autónoma** de operaciones de infraestructura fallidas sin
intervención humana, preservando el aislamiento multitenant y la trazabilidad OTel end-to-end.

Objetivos específicos:

1. Consumir `DOMAIN_FAULT_DETECTED` con `retryable: true` del bus EventBridge vía SQS
2. Despachar la retentativa apropiada según `error_code` + `stage`
3. Aplicar backoff exponencial con jitter para evitar thundering herd
4. Escalar a DLQ + severidad `FATAL` cuando `MAX_ATTEMPTS` se agota
5. Instrumentar cada intento con su propio span OTel vinculado al `trace_id` original

---

## Stack Tecnológico

| Capa              | Tecnología                             |
| :---------------- | :------------------------------------- |
| Lenguaje          | Go 1.22                                |
| Infraestructura   | AWS SAM (Serverless Application Model) |
| Compute           | AWS Lambda (arm64)                     |
| Bus de entrada    | AWS EventBridge → SQS                  |
| Bus de salida     | AWS EventBridge (escalación a `FATAL`) |
| Dead Letter Queue | AWS SQS DLQ                            |
| Observabilidad    | OpenTelemetry + AWS X-Ray              |
| Build             | `GOARCH=arm64 GOOS=linux`              |

---

## DOMINIO I: Contrato de Responsabilidades

### Separación estricta entre `metri-engine` (Phase 10) y Echo

| Jurisdicción             | `metri-engine` HACE                                  | Echo HACE                                                    |
| :----------------------- | :--------------------------------------------------- | :----------------------------------------------------------- |
| **Detección**            | Detecta el error vía Railway Pattern                 | No detecta — solo consume                                    |
| **Publicación**          | Publica `DOMAIN_FAULT_DETECTED` a EventBridge        | No publica el evento inicial                                 |
| **Orquestación Sherlog** | `sherlog::process_fault` garantiza emit + record     | Nunca llama funciones internas de Sherlog                    |
| **Consumo**              | No consume su propio error                           | Consume el evento vía SQS                                    |
| **Retry**                | No reintenta — devuelve `DomainError` al cliente     | Reintenta con backoff exponencial                            |
| **Catálogo TOML**        | Lee `retryable` del catálogo. Lo empaca en el DTO    | No accede al catálogo — lee `retryable` del DTO              |
| **DLQ**                  | No escribe en DLQ                                    | Escala al DLQ cuando `MAX_ATTEMPTS` se agota                 |
| **Escalación**           | No cambia severidad                                  | Promueve a `FATAL` + `DOMAIN_FAULT_ESCALATED` al bus         |
| **`circuit_breaker`**    | Publica `circuit_breaker: true` si severity `:fatal` | **Descarta el retry** si `circuit_breaker=true` en el evento |

### Presupuesto de tiempo

```
metri-engine (Rust Native — tiempo estricto ~5ms)
  └─► Detecta error → build_error_dto → sherlog::process_fault (spawned thread)
         ├─ EventBridgeNotifier::notify  → EventBridge PutEvents
         └─ olap_channel.route           → Janus OLAPChannel (domain_fault)
  └─► Responde al cliente gRPC
  └─► Responsabilidad TERMINA aquí

Echo (Lambda Golang — arm64, 128MB, timeout 120s)
  └─► Consume DOMAIN_FAULT_DETECTED (retryable=true) vía SQS
  └─► main.go: loop local [attempt 0..MAX_ATTEMPTS] en una sola invocación
         └─ ExponentialBackoff(attempt) → time.Sleep → Dispatch
         └─ éxito → break
         └─ agotado → DLQ + DOMAIN_FAULT_ESCALATED (FATAL)
  └─► Cierra span OTel con trace_id del evento original
```

> [!NOTE]
> **Por qué el loop en una sola invocación:** Echo NO delega el conteo de reintentos a SQS
> (`maxReceiveCount: 1`). Todos los intentos ocurren dentro de la misma invocación Lambda
> (timeout 120s). El presupuesto total de sleeps: 700 + 1200 + 2100 + 4300 ≈ 8.3s —
> muy por debajo del límite. Si Lambda crashea, SQS lo entrega al DLQ directamente.

---

## DOMINIO II: Contrato de Entrada

### Evento `DOMAIN_FAULT_DETECTED` recibido de EventBridge (vía SQS)

```json
{
  "version": "0",
  "source": "metri-engine",
  "detail-type": "DOMAIN_FAULT_DETECTED",
  "detail": {
    "severity": "ERROR",
    "source": "metri-engine/janus",
    "tenant_id": "uuid-acme-corp",
    "user_id": "uuid-tech-juan",
    "error_code": "JNS_TX_001",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "retryable": true,
    "circuit_breaker": false,
    "timestamp": 1718221000045,
    "dto": {
      "error": {
        "code": "JNS_TX_001",
        "stage": "janus",
        "entity_type": "work_order",
        "retryable": true,
        "context": {
          "entity_ulid": "01J...",
          "operation": "create",
          "entity_type": "work_order"
        }
      }
    }
  }
}
```

> [!NOTE]
> **Campos que Echo consume:**
>
> - `detail.retryable` — ya filtrado por EchoRule (`true` siempre), pero Echo lo valida defensivamente
> - `detail.error_code` — determina la estrategia de retry en el dispatcher
> - `detail.trace_id` — propagado al span OTel de Echo para correlación causal
> - `detail.tenant_id` — aislamiento multitenant en logs y spans
> - `dto.error.context` — datos no-sensibles para reconstruir el reintento
>
> Los campos `sensitive: true` del schema ya vienen `[REDACTED]` desde `metri-engine` — Echo nunca ve datos sensibles.

### Structs Go

```go
// internal/event/domain_fault.go

type DomainFaultEvent struct {
    Version    string `json:"version"`
    Source     string `json:"source"`
    DetailType string `json:"detail-type"`
    Detail     Detail `json:"detail"`
}

type Detail struct {
    Severity       string  `json:"severity"`
    TenantID       string  `json:"tenant_id"`
    UserID         string  `json:"user_id"`
    ErrorCode      string  `json:"error_code"`
    TraceID        string  `json:"trace_id"`
    Retryable      bool    `json:"retryable"`
    CircuitBreaker bool    `json:"circuit_breaker"`
    Timestamp      int64   `json:"timestamp"`
    DTO            FaultDTO `json:"dto"`
}

type FaultDTO struct {
    Error FaultError `json:"error"`
}

type FaultError struct {
    Code       string          `json:"code"`
    Stage      string          `json:"stage"`
    EntityType string          `json:"entity_type"`
    TenantID   string          `json:"tenant_id"`
    TraceID    string          `json:"trace_id"`
    Retryable  bool            `json:"retryable"`
    Context    json.RawMessage `json:"context"`    // flexible — varía por error_code
}
```

---

## DOMINIO III: Algoritmo de Retry

### III.1 — Implementación en Go

```go
// internal/retry/retry.go

package retry

import (
    "context"
    "fmt"
    "log/slog"
    "math"
    "math/rand"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

const (
    MaxAttempts = 5
    BaseDelayMs = 500
    MaxDelayMs  = 30_000
)

// ExponentialBackoff calcula el delay del intento N con jitter aleatorio.
// Fórmula: min(base × 2ⁿ + jitter, maxDelay)
// El jitter evita thundering herd cuando múltiples errores ocurren simultáneamente.
func ExponentialBackoff(attempt int) time.Duration {
    expDelay := float64(BaseDelayMs) * math.Pow(2, float64(attempt))
    jitter    := rand.Intn(200)    // ±200ms de jitter
    total     := expDelay + float64(jitter)
    if total > MaxDelayMs {
        total = MaxDelayMs
    }
    return time.Duration(total) * time.Millisecond
}

// RetryHandler orquesta el ciclo de retry para un DOMAIN_FAULT_DETECTED.
// Instrumentado con span OTel propio vinculado al trace_id original.
type RetryHandler struct {
    Dispatcher  Dispatcher
    DLQ         DLQClient
    EBPublisher EventBridgePublisher
}

func (h *RetryHandler) Handle(ctx context.Context, event DomainFaultEvent, attemptCount int) error {
    tracer := otel.Tracer("echo")
    ctx, span := tracer.Start(ctx, "echo.handle-retryable-fault")
    defer span.End()

    span.SetAttributes(
        attribute.String("echo.error_code",     event.Detail.ErrorCode),
        attribute.Int   ("echo.attempt",         attemptCount),
        attribute.String("echo.tenant_id",       event.Detail.TenantID),
        attribute.String("echo.original_trace",  event.Detail.TraceID),
    )

    // Validación defensiva — EchoRule ya filtra retryable=true
    if !event.Detail.Retryable {
        span.SetStatus(codes.Ok, "non-retryable — discarded")
        slog.Info("Non-retryable fault — discarding", "error_code", event.Detail.ErrorCode)
        return nil
    }

    // Circuit breaker activo — el error original era FATAL, no reintentar
    // metri-engine publica circuit_breaker=true cuando severity=:fatal.
    // Echo no debe reintentar — enviarlo a DLQ directamente para análisis forense.
    if event.Detail.CircuitBreaker {
        span.SetStatus(codes.Error, "circuit-breaker-open")
        span.SetAttributes(attribute.Bool("echo.circuit_breaker", true))
        slog.Warn("Circuit breaker active — skipping retry, routing to DLQ",
            "error_code", event.Detail.ErrorCode,
            "tenant_id",  event.Detail.TenantID)
        return h.DLQ.Send(ctx, DLQMessage{
            Event:  event,
            Reason: "circuit-breaker-open",
        })
    }

    // Intentos agotados → DLQ + escalación a FATAL
    if attemptCount >= MaxAttempts {
        span.SetStatus(codes.Error, "max-attempts-exceeded")
        slog.Error("Max retry attempts reached — escalating to FATAL",
            "error_code", event.Detail.ErrorCode,
            "attempts",   attemptCount)

        if err := h.escalateToFatal(ctx, event); err != nil {
            slog.Error("Failed to escalate to FATAL", "error", err)
        }
        return h.DLQ.Send(ctx, DLQMessage{
            Event:  event,
            Reason: "max-attempts-exceeded",
        })
    }

    // Backoff exponencial antes del reintento
    delay := ExponentialBackoff(attemptCount)
    span.SetAttributes(attribute.Int64("echo.delay_ms", delay.Milliseconds()))
    slog.Info("Retrying fault",
        "attempt",    attemptCount,
        "delay_ms",   delay.Milliseconds(),
        "error_code", event.Detail.ErrorCode)

    time.Sleep(delay)

    // Despacha la estrategia de retry según error_code + stage
    if err := h.Dispatcher.Dispatch(ctx, event); err != nil {
        span.SetStatus(codes.Error, err.Error())
        return fmt.Errorf("retry dispatch failed: %w", err)
    }

    span.SetStatus(codes.Ok, "retry dispatched")
    return nil
}

func (h *RetryHandler) escalateToFatal(ctx context.Context, event DomainFaultEvent) error {
    return h.EBPublisher.PutEvent(ctx, EscalationEvent{
        Source:     "metri-echo",
        DetailType: "DOMAIN_FAULT_ESCALATED",
        Detail: map[string]any{
            "original_error_code": event.Detail.ErrorCode,
            "original_trace_id":   event.Detail.TraceID,
            "tenant_id":           event.Detail.TenantID,
            "severity":            "FATAL",
            "reason":              "max-retry-attempts-exhausted",
        },
    })
}
```

### III.2 — Escalera de tiempos (ejemplo: `JNS_TX_001`)

```
Intento 0  →  error inicial  →  DOMAIN_FAULT_DETECTED emitido por metri-engine
Intento 1  →  +700ms   (500ms × 2⁰ + ~200ms jitter)
Intento 2  →  +1200ms  (500ms × 2¹ + ~200ms jitter)
Intento 3  →  +2100ms  (500ms × 2² + ~200ms jitter)
Intento 4  →  +4300ms  (500ms × 2³ + ~200ms jitter)
Intento 5  →  MAX_ATTEMPTS → DLQ + FATAL → PagerDuty
```

---

## DOMINIO IV: Dispatcher por `error_code` + `stage`

Echo no tiene lógica monolítica — cada error_code tiene su **estrategia de retry específica**.

```go
// internal/dispatcher/dispatcher.go

package dispatcher

import (
    "context"
    "fmt"
    "log/slog"
)

// Dispatcher interfaz — sustituible en tests
type Dispatcher interface {
    Dispatch(ctx context.Context, event DomainFaultEvent) error
}

// EchoDispatcher despacha la estrategia correcta según stage + error_code
type EchoDispatcher struct {
    metriEngineClient metriEngineGRPCClient
    KinesisClient      KinesisRetryClient
    KMSClient          KMSRetryClient
}

func (d *EchoDispatcher) Dispatch(ctx context.Context, event DomainFaultEvent) error {
    key := event.Detail.DTO.Error.Stage + "/" + event.Detail.ErrorCode

    switch key {

    // ── Janus: transacción EAV (DynamoDB) fallida ────────────────────────────
    case "janus/JNS_TX_001":
        // Re-invoca metri-engine gRPC con la operación original.
        // El context contiene entity_ulid + operation + entity_type.
        return d.metriEngineClient.RetryOperation(ctx, RetryRequest{
            TenantID:   event.Detail.TenantID,
            TraceID:    event.Detail.TraceID,
            ErrorCode:  event.Detail.ErrorCode,
            Context:    event.Detail.DTO.Error.Context,
        })

    // ── Kinesis: PutRecordBatch fallido ──────────────────────────────────────
    case "kinesis/SYS_KNS_001":
        return d.KinesisClient.RetryPutRecords(ctx, event.Detail.DTO.Error.Context)

    // ── KMS: encrypt/decrypt fallido ─────────────────────────────────────────
    case "codice/COD_KMS_001":
        return d.KMSClient.RetryEncrypt(ctx, event.Detail.DTO.Error.Context)

    // ── Infraestructura genérica (INFRA_DDB_001 o similar) ───────────────────
    case "infra/INFRA_DDB_001":
        return d.metriEngineClient.RetryOperation(ctx, RetryRequest{
            TenantID:  event.Detail.TenantID,
            TraceID:   event.Detail.TraceID,
            ErrorCode: event.Detail.ErrorCode,
            Context:   event.Detail.DTO.Error.Context,
        })

    // ── Fallback: error_code sin handler específico ───────────────────────────
    default:
        slog.Warn("No retry strategy for error_code — using generic retry",
            "stage",      event.Detail.DTO.Error.Stage,
            "error_code", event.Detail.ErrorCode)
        return d.metriEngineClient.RetryOperation(ctx, RetryRequest{
            TenantID:  event.Detail.TenantID,
            TraceID:   event.Detail.TraceID,
            ErrorCode: event.Detail.ErrorCode,
            Context:   event.Detail.DTO.Error.Context,
        })
    }
}
```

> [!NOTE]
> **OCP aplicado:** Agregar soporte de retry para un nuevo `error_code` = agregar un nuevo
> `case` en el switch. Sin cambios en `RetryHandler` ni en `metri-engine`.

---

## DOMINIO IV.B: `main.go` — Lambda Handler y Loop de Retry

`attemptCount` es un **contador local** dentro de una única invocación Lambda.
Todos los intentos (hasta `MAX_ATTEMPTS`) ocurren en el mismo proceso, con `time.Sleep`
entre ellos. SQS no gestiona el conteo — Echo lo gestiona internamente.

```go
// cmd/echo_handler/main.go
package main

import (
    "context"
    "encoding/json"
    "log/slog"
    "os"
    "strconv"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "metri-echo/internal/dispatcher"
    "metri-echo/internal/dlq"
    "metri-echo/internal/event"
    "metri-echo/internal/grpc"
    "metri-echo/internal/retry"
)

var handler *retry.RetryHandler

func init() {
    maxAttempts, _ := strconv.Atoi(os.Getenv("ECHO_MAX_ATTEMPTS"))
    if maxAttempts == 0 {
        maxAttempts = 5
    }

    grpcClient := grpc.NewmetriEngineClient(os.Getenv("metri_ENGINE_GRPC_URL"))
    dlqClient  := dlq.NewSQSClient(os.Getenv("ECHO_DLQ_URL"))
    ebClient   := eventbridge.NewClient(os.Getenv("FAULT_BUS_NAME"))

    handler = &retry.RetryHandler{
        Dispatcher:  &dispatcher.EchoDispatcher{metriEngineClient: grpcClient},
        DLQ:         dlqClient,
        EBPublisher: ebClient,
        MaxAttempts: maxAttempts,
    }
}

// EchoHandler: invocado por SQS trigger (BatchSize=1)
// Todo el ciclo de retry ocurre en esta única invocación (timeout 120s).
func EchoHandler(ctx context.Context, sqsEvent events.SQSEvent) error {
    for _, record := range sqsEvent.Records {
        var fault event.DomainFaultEvent
        if err := json.Unmarshal([]byte(record.Body), &fault); err != nil {
            slog.Error("Failed to unmarshal fault event",
                "error", err, "record_id", record.MessageId)
            return err // SQS moverá al DLQ si falla el parsing
        }

        // Loop de retry: todos los intentos en una sola invocación Lambda.
        // attemptCount es local — no viene de SQS ni de estado externo.
        for attempt := 0; attempt <= handler.MaxAttempts; attempt++ {
            err := handler.Handle(ctx, fault, attempt)
            if err == nil {
                slog.Info("Fault resolved",
                    "error_code", fault.Detail.ErrorCode,
                    "attempt",    attempt)
                break
            }
            if attempt == handler.MaxAttempts {
                slog.Error("All retry attempts exhausted",
                    "error_code", fault.Detail.ErrorCode)
                // DLQ y escalación ya fueron enviados por Handle() en el último intento
            }
        }
    }
    return nil // Lambda reporta éxito — SQS no reencola
}

func main() {
    lambda.Start(EchoHandler)
}
```

> [!NOTE]
> `retry.RetryHandler.MaxAttempts` se convierte de campo constante a campo de instancia
> para que `init()` lo inyecte desde la variable de entorno `ECHO_MAX_ATTEMPTS`.
> Esto hace el componente configurable sin recompilación.

---

## DOMINIO V: SAM Template

```yaml
# template.yaml — metri-echo
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: >
  metri-echo

  Componente Lambda Golang que consume DOMAIN_FAULT_DETECTED (retryable=true)
  desde EventBridge vía SQS y reintenta la operación fallida con backoff exponencial.
  Escala a DLQ y severidad FATAL cuando MAX_ATTEMPTS se agota.

Parameters:
  Environment:
    Type: String
    AllowedValues: [development, staging, production]
    Default: production

  FaultEventBusName:
    Type: String
    Description: "Nombre del EventBridge Bus de errores de metri-engine"
    Default: "metri-domain-faults"

  metriEngineGrpcEndpoint:
    Type: String
    Description: "Lambda Function URL de metri-engine para reintentos"

Globals:
  Function:
    Runtime: provided.al2023    # Go binary compilado con GOARCH=arm64
    Architectures: [arm64]
    MemorySize: 128             # Go es liviano — bajo consumo de memoria
    Timeout: 120                # los reintentos con backoff pueden tomar hasta 2 min
    Environment:
      Variables:
        ENVIRONMENT:              !Ref Environment
        FAULT_BUS_NAME:           !Ref FaultEventBusName
        metri_ENGINE_GRPC_URL:   !Ref metriEngineGrpcEndpoint
        ECHO_MAX_ATTEMPTS:        "5"
        ECHO_BASE_DELAY_MS:       "500"
        ECHO_MAX_DELAY_MS:        "30000"
        AWS_REGION:               !Ref AWS::Region

Resources:

  # ── Dead Letter Queue — fallos que Echo no pudo resolver ──────────────────
  EchoDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "metri-echo-dlq-${Environment}"
      MessageRetentionPeriod: 1209600    # 14 días

  # ── SQS Queue para Echo — target de EchoRule en EventBridge ───────────────
  EchoQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "metri-echo-queue-${Environment}"
      RedrivePolicy:
        deadLetterTargetArn: !GetAtt EchoDLQ.Arn
        maxReceiveCount: 1   # Echo gestiona sus propios reintentos — no SQS

  # ── EventBridge Rule — filtra DOMAIN_FAULT_DETECTED retryable=true ────────
  EchoRule:
    Type: AWS::Events::Rule
    Properties:
      EventBusName: !Ref FaultEventBusName
      Description: "Enruta errores retryables a Echo para recuperación autónoma"
      EventPattern:
        source: ["metri-engine"]
        detail-type: ["DOMAIN_FAULT_DETECTED"]
        detail:
          retryable: [true]
      Targets:
        - Id: EchoSQS
          Arn: !GetAtt EchoQueue.Arn

  # ── Topic SNS para PagerDuty (DOMAIN_FAULT_ESCALATED) ─────────────────────
  EscalationSNSTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "metri-echo-escalation-${Environment}"
      # Subscripción a PagerDuty vía HTTPS endpoint
      Subscription:
        - Protocol: https
          Endpoint: !Sub "{{resolve:ssm:/metri/pagerduty/endpoint/${Environment}}}"

  # ── EventBridge Rule — DOMAIN_FAULT_ESCALATED → PagerDuty (FATAL) ────────
  # Captura los reintentos agotados que Echo escala con severidad FATAL.
  # Métrica diferente de DOMAIN_FAULT_DETECTED — rule separada para aislamiento.
  EscalationPagerDutyRule:
    Type: AWS::Events::Rule
    Properties:
      EventBusName: !Ref FaultEventBusName
      Description: "Alerta PagerDuty cuando Echo agota MAX_ATTEMPTS — FATAL de retry"
      EventPattern:
        source: ["metri-echo"]
        detail-type: ["DOMAIN_FAULT_ESCALATED"]
      Targets:
        - Id: EscalationSNS
          Arn: !Ref EscalationSNSTopic

  # ── Handler: EchoFunction — retry con backoff exponencial ────────────────
  EchoFunction:
    Type: AWS::Serverless::Function
    Metadata:
      BuildMethod: go1.x
    Properties:
      FunctionName: !Sub "metri-echo-${Environment}"
      CodeUri: cmd/echo_handler/
      Handler: bootstrap
      Description: "Reintenta DOMAIN_FAULT_DETECTED con backoff exponencial"
      ReservedConcurrentExecutions: 5   # no abrumar la infra durante incidentes

      Events:
        EchoQueueTrigger:
          Type: SQS
          Properties:
            Queue: !GetAtt EchoQueue.Arn
            BatchSize: 1    # un error a la vez — backoff requiere control individual
            FunctionResponseTypes:
              - ReportBatchItemFailures

      DeadLetterQueue:
        Type: SQS
        TargetArn: !GetAtt EchoDLQ.Arn

      Policies:
        - SQSPollerPolicy:
            QueueName: !GetAtt EchoQueue.QueueName
        - SQSSendMessagePolicy:
            QueueName: !GetAtt EchoDLQ.QueueName
        - EventBridgePutEventsPolicy:
            EventBusName: !Ref FaultEventBusName
        - SNSPublishMessagePolicy:
            TopicName: !GetAtt EscalationSNSTopic.TopicName
        - Statement:
            - Effect: Allow
              Action: [lambda:InvokeFunction, lambda:InvokeFunctionUrl]
              Resource: "*"    # acceso al endpoint gRPC de metri-engine

Outputs:
  EchoFunctionArn:
    Value: !GetAtt EchoFunction.Arn

  EchoDLQUrl:
    Value: !Ref EchoDLQ
    Description: "URL del DLQ — monitorear para fallos de retry no resueltos"

  EscalationTopicArn:
    Value: !Ref EscalationSNSTopic
    Description: "SNS Topic para PagerDuty — alertas de DOMAIN_FAULT_ESCALATED"

---

## DOMINIO VI: Variables de Entorno

| Variable | Descripción | Requerida | Ejemplo |
| :------- | :---------- | :-------: | :------ |
| `ENVIRONMENT` | Entorno de ejecución | ✅ | `production` |
| `FAULT_BUS_NAME` | EventBus de errores de metri-engine | ✅ | `metri-domain-faults` |
| `metri_ENGINE_GRPC_URL` | Lambda Function URL de metri-engine para reintentos | ✅ | `https://xyz.lambda-url.us-east-1.on.aws` |
| `ECHO_MAX_ATTEMPTS` | Máximo de reintentos antes de DLQ | ✅ | `5` |
| `ECHO_BASE_DELAY_MS` | Delay base para backoff exponencial | ✅ | `500` |
| `ECHO_MAX_DELAY_MS` | Tope máximo del backoff | ✅ | `30000` |
| `AWS_REGION` | Inyectada automáticamente por Lambda | ✅ | `us-east-1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint OTEL para traces | ⚪ | `https://otel.internal:4317` |

---

## DOMINIO VII: Estructura de Archivos

```

metri-echo/
├── template.yaml ← IaC SAM — todos los recursos AWS
├── Makefile
├── go.mod
├── go.sum
│
├── cmd/
│ └── echo_handler/
│ └── main.go ← EchoHandler — punto de entrada Lambda SQS
│
├── internal/
│ ├── event/
│ │ └── domain_fault.go ← structs: DomainFaultEvent, Detail, FaultDTO
│ │
│ ├── retry/
│ │ ├── retry.go ← RetryHandler + ExponentialBackoff
│ │ └── retry_test.go ← TDD Matrix (ECH-01..ECH-11)
│ │
│ ├── dispatcher/
│ │ ├── dispatcher.go ← EchoDispatcher — switch por error_code/stage
│ │ └── dispatcher_test.go
│ │
│ ├── grpc/
│ │ └── metri_engine_client.go ← metriEngineGRPCClient — re-invoca operaciones
│ │
│ ├── dlq/
│ │ └── dlq.go ← DLQClient — send-to-dlq + DLQMessage
│ │
│ └── otel/
│ └── tracer.go ← OpenTelemetry + AWS X-Ray setup
│
└── docs/
└── architecture/ → (este documento)

````

### Makefile

```makefile
BINARY = cmd/echo_handler/bootstrap

build:
	GOARCH=arm64 GOOS=linux go build -o $(BINARY) ./cmd/echo_handler/

test:
	go test ./... -v -race

deploy-dev:
	sam build && sam deploy --config-env development

deploy-prod:
	sam build && sam deploy --config-env production

local:
	sam local invoke EchoFunction --event events/domain_fault_retryable.json
````

---

## DOMINIO VIII: Matriz TDD

| ID       | Caso                                                            | Comportamiento esperado                           |
| :------- | :-------------------------------------------------------------- | :------------------------------------------------ |
| `ECH-01` | `retryable=false` recibido → descartar silencioso               | `Dispatch` nunca llamado, log `non-retryable`     |
| `ECH-02` | `attempt=0` → `Dispatch` llamado tras ~500ms de delay           | `dispatchCalls=1`, delay `>= 400ms`               |
| `ECH-03` | `attempt=4` → delay acotado por `MAX_DELAY_MS`                  | `delay < 30000ms`                                 |
| `ECH-04` | `attempt=5` → DLQ Send + escalación FATAL                       | `dlqCalls=1`, `ebCalls=1` con `severity=FATAL`    |
| `ECH-05` | `Dispatch` retorna error → `Handle` retorna error, nunca panic  | función retorna `err != nil`, sin panic           |
| `ECH-06` | span OTel hereda `trace_id` del evento original                 | `span.echo.original_trace = event.TraceID`        |
| `ECH-07` | `"janus/JNS_TX_001"` → llama `metriEngineClient.RetryOperation` | `grpcCalls=1` con `entity_ulid` correcto          |
| `ECH-08` | `"kinesis/SYS_KNS_001"` → llama `KinesisClient.RetryPutRecords` | `kinesisCalls=1` con `stream_name`                |
| `ECH-09` | key desconocido → llama `RetryOperation` genérico + log Warn    | `grpcCalls=1`, log contiene `"No retry strategy"` |
| `ECH-10` | `ExponentialBackoff(0)` → entre 500ms y 700ms                   | `500 <= result_ms <= 700`                         |
| `ECH-11` | `ExponentialBackoff(10)` → acotado por `MAX_DELAY_MS`           | `result_ms == 30000`                              |
| `ECH-12` | `circuit_breaker=true` → DLQ inmediato, `Dispatch` nunca llamado | `dlqCalls=1`, `reason=circuit-breaker-open`       |
| `ECH-13` | `circuit_breaker=true` → EB Publisher NO recibe escalación FATAL | `ebCalls=0` (no es max-attempts, es CB)           |

```go
// internal/retry/retry_test.go
package retry_test

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

// ── Mocks ────────────────────────────────────────────────────────────────────

type MockDispatcher struct{ mock.Mock }
func (m *MockDispatcher) Dispatch(ctx context.Context, e DomainFaultEvent) error {
    return m.Called(ctx, e).Error(0)
}

type MockDLQ struct{ calls []DLQMessage }
func (m *MockDLQ) Send(_ context.Context, msg DLQMessage) error {
    m.calls = append(m.calls, msg)
    return nil
}

type MockEBPublisher struct{ calls []EscalationEvent }
func (m *MockEBPublisher) PutEvent(_ context.Context, e EscalationEvent) error {
    m.calls = append(m.calls, e)
    return nil
}

func makeEvent(retryable bool) DomainFaultEvent {
    return DomainFaultEvent{Detail: Detail{
        ErrorCode: "JNS_TX_001", TenantID: "t",
        TraceID: "tr", Retryable: retryable,
        DTO: FaultDTO{Error: FaultError{Stage: "janus", Retryable: retryable}},
    }}
}

func makeEventWithCB(circuitBreaker bool) DomainFaultEvent {
    return DomainFaultEvent{Detail: Detail{
        ErrorCode:      "SYS_KNS_001", TenantID: "t",
        TraceID:        "tr", Retryable: true,
        CircuitBreaker: circuitBreaker,
        DTO: FaultDTO{Error: FaultError{Stage: "kinesis"}},
    }}
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestECH01_NonRetryableDiscarded(t *testing.T) {
    d := &MockDispatcher{}
    h := RetryHandler{Dispatcher: d, DLQ: &MockDLQ{}, EBPublisher: &MockEBPublisher{}}

    err := h.Handle(context.Background(), makeEvent(false), 0)

    assert.NoError(t, err)
    d.AssertNotCalled(t, "Dispatch")   // nunca despacha si retryable=false
}

func TestECH04_MaxAttemptsEscalatesToFatal(t *testing.T) {
    dlq := &MockDLQ{}
    eb  := &MockEBPublisher{}
    h   := RetryHandler{Dispatcher: &MockDispatcher{}, DLQ: dlq, EBPublisher: eb}

    _ = h.Handle(context.Background(), makeEvent(true), MaxAttempts)

    assert.Len(t, dlq.calls, 1)
    assert.Len(t, eb.calls, 1)
    assert.Equal(t, "FATAL", eb.calls[0].Detail["severity"])
}

func TestECH10_BackoffAttempt0(t *testing.T) {
    ms := ExponentialBackoff(0).Milliseconds()
    assert.GreaterOrEqual(t, ms, int64(500))
    assert.LessOrEqual(t,    ms, int64(700))
}

func TestECH11_BackoffCappedAtMax(t *testing.T) {
    ms := ExponentialBackoff(10).Milliseconds()
    assert.Equal(t, int64(MaxDelayMs), ms)
}

func TestECH12_CircuitBreakerSentsToDLQ(t *testing.T) {
    dlq := &MockDLQ{}
    eb  := &MockEBPublisher{}
    d   := &MockDispatcher{}
    h   := RetryHandler{Dispatcher: d, DLQ: dlq, EBPublisher: eb}

    err := h.Handle(context.Background(), makeEventWithCB(true), 0)

    assert.NoError(t, err)
    assert.Len(t, dlq.calls, 1)
    assert.Equal(t, "circuit-breaker-open", dlq.calls[0].Reason)
    d.AssertNotCalled(t, "Dispatch")           // nunca reintenta
}

func TestECH13_CircuitBreakerDoesNotEscalateToFatal(t *testing.T) {
    // circuit_breaker ≠ max-attempts: no debe publicar DOMAIN_FAULT_ESCALATED
    dlq := &MockDLQ{}
    eb  := &MockEBPublisher{}
    h   := RetryHandler{Dispatcher: &MockDispatcher{}, DLQ: dlq, EBPublisher: eb}

    _ = h.Handle(context.Background(), makeEventWithCB(true), 0)

    assert.Len(t, dlq.calls, 1)  // DLQ forense — sí
    assert.Len(t, eb.calls,  0)  // DOMAIN_FAULT_ESCALATED — NO
}
```

---

## Topología de Recursos AWS

```
metri-engine (Rust Native gRPC Service)
      │
      │  PutEvents → EventBridge "metri-domain-faults"
      │
      │  EchoRule: retryable=true → SQS "metri-echo-queue"
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  AWS Account — metri-echo                                       │
│                                                                 │
│  EventBridge EchoRule (retryable=true)                          │
│          │                                                      │
│          ▼                                                      │
│  SQS: metri-echo-queue (maxReceiveCount=1)                      │
│          │                                                      │
│          ▼                                                      │
│  Lambda: EchoFunction (arm64, Go, 128MB, timeout 120s)          │
│    ├─ RetryHandler.Handle (backoff exponencial)                 │
│    ├─ EchoDispatcher.Dispatch (switch por error_code)           │
│    └─ metriEngineGRPCClient.RetryOperation → metri-engine       │
│          │                                                      │
│    Si MAX_ATTEMPTS agotados:                                    │
│    ├─ EBPublisher.PutEvent → DOMAIN_FAULT_ESCALATED (FATAL)     │
│    └─ DLQClient.Send → metri-echo-dlq                           │
│                                                                 │
│  SQS DLQ: metri-echo-dlq (retención 14 días)                    │
│    → PagerDuty via EventBridge PagerDutyRule + análisis forense │
└─────────────────────────────────────────────────────────────────┘
```

---

## Referencia cruzada

- **Contrato de entrada**: `DOMAIN_FAULT_DETECTED` — publicado por [`10_FASE_GESTION_ERRORES_EDA.md`](10_FASE_GESTION_ERRORES_EDA.md)
- **Schema de errores persistidos**: [`models/domain_fault.json`](models/domain_fault.json)
- **Catálogo de errores retryables**: `metri-engine/config/errors/error_catalog.toml` campo `retryable`
- **Patrón**: [`COMPONENTE_EXTERNO_01_EVENT_ROUTER.md`](COMPONENTE_EXTERNO_01_EVENT_ROUTER.md) — mismo stack Golang/SAM
