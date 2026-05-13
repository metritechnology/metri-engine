# Componente Externo 02 — Hephaestus Bulk Compactor

**Nombre del Componente:** `metri-bulk-compactor`
**Runtime:** Golang 1.22 (Goroutines Concurrentes)
**Infraestructura:** AWS SAM · Lambda · Kinesis Data Firehose · DynamoDB · S3 · Glue Iceberg
**Patrón Arquitectónico:** Direct Storage Writer · Columnar Transmutation · WAF Bypass

---

## Definición

**Bulk Compactor** es el micro-daemon asincrónico de destilación masiva de datos de Metri Engine. Toma la suma en crudo del _Lago de Transacciones Datahike_ almacenadas como objetos estáticos S3, los pre-procesa matemáticamente en columnas Apache Arrow y los comprime como ficheros `.parquet` hacia el DataLake final.

> [!IMPORTANT]
> **Bulk Compactor NO usa el endpoint gRPC del Metri Engine ni atraviesa el AWS WAF.**
>
> El WAF CloudFront (`metri-engine-waf`) aplica la **Regla 8** (`MetriGrpcProtocolEnforcement`) que bloquea todo POST que no incluya `Content-Type: application/grpc-web`. Adicionalmente, la **Regla 2** limita a 1,000 req/5min por IP — inaceptable para lotes masivos. Bulk Compactor escribe **directamente** en los backends de infraestructura usando su IAM Role dedicado.

---

## Recursos AWS Reales (Ajustados a `template.yaml`)

| Recurso                 | Nombre Físico                     | Origen en template.yaml                                     |
| :---------------------- | :-------------------------------- | :---------------------------------------------------------- |
| **DynamoDB OLTP**       | `metri-datahike-prod`             | `DatahikeTableName` param / `MetriDatahikeTable` · PK: `id` |
| **S3 DataLake**         | `metri-lake-{AccountId}-{Region}` | `MetriDataLakeBucket`                                       |
| **Firehose streams**    | `metri-olap-stream-{entity-slug}` | prefix `KINESIS_STREAM_PREFIX = "metri-olap-stream"`        |
| **Glue Database**       | `metri_olap`                      | `MetriGlueDatabase` / `GLUE_DATABASE_NAME`                  |
| **KMS CMK**             | `alias/metri-engine-cmk`          | `MetriKmsMasterKey`                                         |
| **Schemas / Blacklist** | `MetriSchemasTable` · PK: `id`    | `CEDAR_POLICIES_TABLE`                                      |

### Naming de Firehose Streams (SSOT)

Los streams se crean vía `make sync-firehose` — **no están en `template.yaml`**. Hephaestus usa el mismo prefix:

```
entity_type:  meter_reading     → stream: metri-olap-stream-meter-reading
entity_type:  audit_log         → stream: metri-olap-stream-audit-log
entity_type:  domain_fault      → stream: metri-olap-stream-domain-fault
entity_type:  inventory_ledger  → stream: metri-olap-stream-inventory-ledger
```

---

## Objetivo

> Insertar masivamente datos en crudo **directamente** en los backends OLAP y OLTP de Metri con cero pérdida y aislamiento multitenant estricto, sin atravesar el AWS WAF CloudFront.

Objetivos específicos:

1. Consumir eventos Kinesis Firehose o disparos EventBridge Scheduler en lotes pesados
2. Transmutar buffers Row-Based a Columnar con Apache Arrow (Golang)
3. **OLAP + OLTP simultáneo:** cada lote se escribe a **ambos** canales en paralelo — sin routing condicional
4. **OLAP:** `PutRecordBatch` directo al stream `metri-olap-stream-{entity-slug}` → Glue `metri_olap` → Iceberg → Athena
5. **OLTP:** `BatchWriteItem` directo a `metri-datahike-prod` con formato EAV Datahike-compatible
6. Inyectar columnas fonéticas `_soundex_fts` para FTS O(1) en Trino/Athena
7. Preservar Checkpoints Kinesis ante fallos — solo se confirma si **ambos** canales tienen éxito

---

## Topología de Inserción Directa

```
Bulk Compactor Lambda (Golang — IAM Role, sin tocar WAF CloudFront)
│
├── Kinesis Firehose / EventBridge Scheduler (Cron)
│         │
│         ▼
│   groupByTenantAndEntity(records)
│   Arrow Transmutation: Row → Columnar
│   InjectFTSColumns: _soundex_fts pre-computadas
│         │
│         ├─── Goroutine A: OLAP (SIEMPRE) ─────────────────────────────
│         │         │
│         │         ▼
│         │   firehose:PutRecordBatch
│         │   Stream: metri-olap-stream-{entity-slug}
│         │         │
│         │         ▼
│         │   S3: metri-lake-{AccountId}-{Region}/iceberg-data/{entity_type}/
│         │         │
│         │         ▼
│         │   Glue: metri_olap.{entity_type} (Iceberg nativo)
│         │         │
│         │         ▼
│         │   Athena workgroup: metri-analytics
│         │
│         └─── Goroutine B: OLTP (SIEMPRE) ─────────────────────────────
│                   │
│                   ▼
│             dynamodb:BatchWriteItem
│             Tabla: metri-datahike-prod  (PK: id ULID)
│             Formato EAV Datahike Pool Model
│             (visible inmediatamente desde JVM Clojure)
│
│   ← Checkpoint ACK solo si Goroutine A + Goroutine B = éxito
```

> [!IMPORTANT]
> **Escritura dual obligatoria:** el Bulk Compactor escribe cada lote a OLAP **y** OLTP simultáneamente mediante Goroutines paralelas. No existe routing condicional basado en el `engine` del Códice. Ambos canales son destinos permanentes de cada batch.

---

## Dominio I: Transmutación Apache Arrow

```go
// internal/arrow/transmuter.go
package arrow

// TransmuteToColumnar convierte Row-Based a Arrow IPC binario (columnar).
func TransmuteToColumnar(rawRows []map[string]any) (arrow.Record, []byte, error) {
    schema  := inferArrowSchema(rawRows)
    builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    defer builder.Release()
    for _, row := range rawRows { appendRow(builder, row) }
    rec := builder.NewRecord()

    var buf bytes.Buffer
    w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
    if err := w.Write(rec); err != nil { return nil, nil, err }
    w.Close()
    return rec, buf.Bytes(), nil
}

// EmitParquet escribe el record como .parquet Snappy en S3.
// Path: s3://metri-lake-{AccountId}-{Region}/iceberg-data/{entity_type}/{ulid}.parquet
func EmitParquet(ctx context.Context, s3c *s3.Client, bucket, entityType string, rec arrow.Record) error {
    key := fmt.Sprintf("iceberg-data/%s/%s.parquet", entityType, ulid.Make().String())
    var buf bytes.Buffer
    pw, _ := pqarrow.NewFileWriter(rec.Schema(), &buf,
        parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
        pqarrow.DefaultWriterProps(),
    )
    pw.WriteBuffered(rec)
    pw.Close()
    _, err := s3c.PutObject(ctx, &s3.PutObjectInput{
        Bucket: &bucket, Key: &key, Body: bytes.NewReader(buf.Bytes()),
    })
    return err
}
```

> [!NOTE]
> El path `iceberg-data/` corresponde al `LocationUri` de `MetriGlueDatabase` definido en `template.yaml`. Glue Iceberg usa este prefix para el descubrimiento automático de particiones.

---

## Dominio II: Pre-Computación FTS Fonética

```go
// internal/fts/soundex_injector.go
// Inyecta columnas _soundex_fts pre-computadas.
// Aegis/Trino apunta al metadato fonético en lugar de hacer full scan.
func InjectFTSColumns(rows []map[string]any, ftsFields []string) []map[string]any {
    for i, row := range rows {
        for _, field := range ftsFields {
            if v, ok := row[field].(string); ok {
                rows[i][field+"_soundex_fts"] = soundex(strings.ToLower(tokenize(v)))
            }
        }
    }
    return rows
}
```

---

## Dominio III: Canal OLAP — Kinesis Firehose Directo

```go
// internal/olap/firehose_writer.go
package olap

const StreamPrefix = "metri-olap-stream" // = KINESIS_STREAM_PREFIX en template.yaml

// WriteToOLAP escribe el lote directo al stream Firehose del entity_type.
// NO pasa por CloudFront ni por el WAF de Metri Engine.
func WriteToOLAP(ctx context.Context, fh *firehose.Client, entityType string, ipcBytes []byte) error {
    // Naming convention: metri-olap-stream-{entity-slug}
    slug       := strings.ReplaceAll(entityType, "_", "-")
    streamName := fmt.Sprintf("%s-%s", StreamPrefix, slug)

    _, err := fh.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
        DeliveryStreamName: &streamName,
        Records:            []types.Record{{Data: ipcBytes}},
    })
    if err != nil {
        otel.RecordError("hephaestus.olap.failed", err,
            attribute.String("stream", streamName),
            attribute.String("entity_type", entityType),
        )
    }
    return err
}
```

### Flujo OLAP completo

```
Hephaestus PutRecordBatch
    → Stream: metri-olap-stream-{entity-slug}
    → FirehoseDeliveryRole (IAM Role del template.yaml)
    → S3: metri-lake-{AccountId}-{Region}/iceberg-data/{entity_type}/
    → Glue: metri_olap.{entity_type} (tabla Iceberg)
    → Athena workgroup: metri-analytics
```

---

## Dominio IV: Canal OLTP — DynamoDB BatchWriteItem Directo

La tabla `MetriDatahikeTable` (`metri-datahike-prod`) usa PK simple `id` (tipo String). Datahike serializa internamente cada datom como un ítem individual. Hephaestus replica este formato EAV:

```go
// internal/oltp/dynamo_writer.go
package oltp

const Table = "metri-datahike-prod" // = DatahikeTableName param en template.yaml

// WriteToOLTP inserta el lote directo en DynamoDB sin pasar por Cedar ni el WAF.
// CRÍTICO: tenant_id siempre del partition key Kinesis — NUNCA del payload.
func WriteToOLTP(ctx context.Context, ddb *dynamodb.Client, tenantID, entityType string, rows []map[string]any) error {
    var pending []types.WriteRequest

    for _, row := range rows {
        // PK: ULID string — coincide con el schema de MetriDatahikeTable (PK: "id", type S)
        eid := ulid.Make().String()

        // Datom base: entity identity + aislamiento multitenant P0
        for attr, val := range map[string]any{
            "tenant/id":   tenantID,   // OBLIGATORIO — Pool Model multitenant
            "entity/type": entityType,
            "entity/ulid": eid,
        } {
            pending = append(pending, datom(eid, attr, val, tenantID))
        }

        // Atributos del payload — ignorar cualquier tenant_id del row
        for k, v := range row {
            if k == "tenant_id" { continue }
            pending = append(pending, datom(eid, entityType+"/"+k, v, tenantID))
        }

        // Flush en chunks de 25 — límite hard de DynamoDB BatchWriteItem
        for len(pending) >= 25 {
            if err := flush(ctx, ddb, pending[:25]); err != nil { return err }
            pending = pending[25:]
        }
    }
    if len(pending) > 0 { return flush(ctx, ddb, pending) }
    return nil
}

// datom construye un ítem EAV compatible con el formato interno Datahike.
// PK: "id" (string ULID) — coincide con el KeySchema de MetriDatahikeTable.
func datom(eid, attr string, val any, tenantID string) types.WriteRequest {
    return types.WriteRequest{PutRequest: &types.PutRequest{
        Item: map[string]types.AttributeValue{
            "id":        &types.AttributeValueMemberS{Value: eid + "#" + attr},
            "e":         &types.AttributeValueMemberS{Value: eid},
            "a":         &types.AttributeValueMemberS{Value: attr},
            "v":         marshalValue(val),
            "tenant_id": &types.AttributeValueMemberS{Value: tenantID},
            "tx_at":     &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)},
        },
    }}
}

func flush(ctx context.Context, ddb *dynamodb.Client, reqs []types.WriteRequest) error {
    _, err := ddb.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
        RequestItems: map[string][]types.WriteRequest{Table: reqs},
    })
    return err
}
```

> [!CAUTION]
> `tenant_id` **NUNCA** proviene del payload. Siempre se toma del partition key del evento Kinesis Firehose, inyectado upstream por el `OLAPChannel` del Janus Router. Un datom sin `tenant_id` correcto = data breach P0.

> [!WARNING]
> La tabla `MetriDatahikeTable` no tiene `DeleteItem` permitido desde Lambda (inmutabilidad Datahike). Hephaestus tampoco requiere este permiso — solo `BatchWriteItem` y `PutItem`.

---

## Dominio V: Escritura Dual — Estrategia de Fan-Out

No existe routing condicional. Cada lote activa **ambos canales en paralelo** mediante Goroutines independientes. El resultado final del batch es la composición de ambas escrituras:

```go
// internal/fanout/dual_writer.go
package fanout

type DualResult struct {
    OLAPErr error
    OLTPErr error
}

// WriteDual ejecuta OLAP y OLTP en paralelo para el mismo lote.
// El Checkpoint Kinesis solo se confirma si ambos canales retornan nil.
func WriteDual(
    ctx context.Context,
    fh  *firehose.Client,
    ddb *dynamodb.Client,
    tenantID, entityType string,
    enriched []map[string]any,
    ipcBytes []byte,
) DualResult {
    var wg sync.WaitGroup
    var result DualResult
    wg.Add(2)

    // Goroutine A — OLAP: Kinesis Firehose → Glue Iceberg → Athena
    go func() {
        defer wg.Done()
        result.OLAPErr = olap.WriteToOLAP(ctx, fh, entityType, ipcBytes)
        if result.OLAPErr != nil {
            otel.RecordError("bulk_compactor.olap.failed", result.OLAPErr,
                attribute.String("entity_type", entityType),
                attribute.String("tenant_id", tenantID),
            )
        }
    }()

    // Goroutine B — OLTP: DynamoDB BatchWriteItem → metri-datahike-prod
    go func() {
        defer wg.Done()
        result.OLTPErr = oltp.WriteToOLTP(ctx, ddb, tenantID, entityType, enriched)
        if result.OLTPErr != nil {
            otel.RecordError("bulk_compactor.oltp.failed", result.OLTPErr,
                attribute.String("entity_type", entityType),
                attribute.String("tenant_id", tenantID),
            )
        }
    }()

    wg.Wait()
    return result
}

// BothSucceeded retorna true solo si ambos canales escribieron sin error.
func (r DualResult) BothSucceeded() bool {
    return r.OLAPErr == nil && r.OLTPErr == nil
}
```

---

## Dominio VI: Handler Principal — Escritura Dual

```go
// cmd/compactor_handler/main.go
func CompactorHandler(ctx context.Context, event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
    span, ctx := otel.StartSpan(ctx, "bulk_compactor.handler")
    defer span.End()

    batches := groupByTenantAndEntity(event.Records)
    var failures []events.KinesisFirehoseResponseRecord

    for key, batch := range batches {
        tenantID, entityType := parseKey(key)

        // 1. FTS fonético pre-computado
        enriched := fts.InjectFTSColumns(batch.Rows, batch.FTSFields)

        // 2. Arrow transmutation (columnar + IPC bytes)
        rec, ipcBytes, err := arrow.TransmuteToColumnar(enriched)
        if err != nil {
            otel.RecordError("bulk_compactor.transmute.failed", err)
            failures = append(failures, markFailed(batch.Records)...)
            continue
        }

        // 3. Parquet → S3 iceberg-data/ (copia analítica directa — no fatal)
        _ = arrow.EmitParquet(ctx, s3Client, lakeBucket, entityType, rec)

        // 4. Escritura dual simultánea: OLAP + OLTP en paralelo
        //    Sin routing condicional — ambos canales reciben el mismo lote.
        result := fanout.WriteDual(ctx, firehoseClient, dynamoClient,
            tenantID, entityType, enriched, ipcBytes)

        // 5. Checkpoint ACK solo si AMBOS canales tuvieron éxito
        if !result.BothSucceeded() {
            otel.RecordEvent("bulk_compactor.dual_write.partial_failure", map[string]any{
                "tenant_id":   tenantID,
                "entity_type": entityType,
                "olap_ok":     result.OLAPErr == nil,
                "oltp_ok":     result.OLTPErr == nil,
            })
            failures = append(failures, markFailed(batch.Records)...)
            continue
        }

        otel.RecordEvent("bulk_compactor.dual_write.ok", map[string]any{
            "tenant_id":   tenantID,
            "entity_type": entityType,
            "count":       len(batch.Rows),
        })
    }
    return buildFirehoseResponse(failures), nil
}

func main() { lambda.Start(CompactorHandler) }
```

---

## Dominio VII: IAM Role (Ajustado a `template.yaml`)

Como el Bulk Compactor escribe **siempre** a ambos canales, el IAM Role requiere permisos sobre Firehose **y** DynamoDB de forma permanente — no condicional.

```yaml
# template.yaml — BulkCompactorFunction (componente separado del MetriEngineFunction)
# Escritura dual: OLAP + OLTP siempre activos.
# EXCEPTO: sin acceso al Lambda URL del Engine ni al WAF CloudFront.

Policies:
  # OLAP: escritura directa a Kinesis Firehose (todos los streams OLAP)
  - Statement:
      Effect: Allow
      Action: [firehose:PutRecord, firehose:PutRecordBatch]
      Resource: !Sub "arn:aws:firehose:${AWS::Region}:${AWS::AccountId}:deliverystream/metri-olap-stream-*"

  # OLTP: escritura directa a MetriDatahikeTable (sin Delete — inmutabilidad Datahike)
  - Statement:
      Effect: Allow
      Action: [dynamodb:PutItem, dynamodb:BatchWriteItem]
      Resource: !Sub "arn:aws:dynamodb:${AWS::Region}:${AWS::AccountId}:table/metri-datahike-prod"

  # S3: escritura Parquet al Lake (MetriDataLakeBucket / iceberg-data/)
  - Statement:
      Effect: Allow
      Action: [s3:PutObject]
      Resource: !Sub "arn:aws:s3:::metri-lake-${AWS::AccountId}-${AWS::Region}/iceberg-data/*"

  # KMS: CMK compartida alias/metri-engine-cmk (MetriKmsMasterKey)
  - Statement:
      Effect: Allow
      Action: [kms:Decrypt, kms:GenerateDataKey*]
      Resource: !Sub "arn:aws:kms:${AWS::Region}:${AWS::AccountId}:alias/metri-engine-cmk"

  # PROHIBIDO: sin lambda:InvokeFunction sobre MetriEngineFunction
  # El WAF CloudFront (MetriWebACL / 8 reglas) no es atravesado en ningún momento
```

---

## Dominio VIII: Variables de Entorno

| Variable                | Valor (de `template.yaml`)        | Descripción                     |
| :---------------------- | :-------------------------------- | :------------------------------ |
| `S3_LAKE_BUCKET`        | `metri-lake-{AccountId}-{Region}` | `MetriDataLakeBucket`           |
| `KINESIS_STREAM_PREFIX` | `metri-olap-stream`               | Mismo que `MetriEngineFunction` |
| `OLTP_TABLE_NAME`       | `metri-datahike-prod`             | `DatahikeTableName` param       |
| `GLUE_DATABASE_NAME`    | `metri_olap`                      | `MetriGlueDatabase`             |
| `KMS_KEY_ALIAS`         | `alias/metri-engine-cmk`          | `MetriKmsAlias`                 |
| `ENVIRONMENT`           | `production`                      | Mismo valor que el Engine       |

---

## Dominio IX: Garantías Multitenant

| Capa              | Mecanismo                                                                      |
| :---------------- | :----------------------------------------------------------------------------- |
| **Firehose OLAP** | Stream por entity_type, `tenant_id` como columna en cada registro Arrow        |
| **DynamoDB OLTP** | Atributo `tenant/id` obligatorio en cada datom — mismo Pool Model de Datahike  |
| **S3 Parquet**    | Path `iceberg-data/{entity_type}/` — Glue Iceberg filtra por `_tenant` column  |
| **KMS**           | CMK compartida `alias/metri-engine-cmk` — Hephaestus solo cifra su propio lote |

---

## Dominio X: Tolerancia a Fallos

| Escenario | Acción | Impacto |
| :-------- | :----- | :------ |
| Lambda timeout (Arrow transmutation) | `recover()` + OTel — NO confirma Checkpoint | Kinesis reintenta el batch completo |
| OLAP falla, OLTP OK | `DualResult.OLAPErr != nil` → `ProcessingFailed` | Kinesis reintenta — ambos canales vuelven a escribir |
| OLTP falla, OLAP OK | `DualResult.OLTPErr != nil` → `ProcessingFailed` | Kinesis reintenta — OLAP recibe el batch otra vez (idempotente por ULID) |
| Ambos fallan | `ProcessingFailed` — Checkpoint NO confirmado | Kinesis reintenta — alerta CloudWatch |
| Stream `metri-olap-stream-*` no existe | `PutRecordBatch` error OLAP → `ProcessingFailed` | No hay fallback a solo-OLTP — ambos son obligatorios |
| Panic Golang en alguna Goroutine | `recover()` + OTel warning — NO confirma Checkpoint | Cero pérdida — Kinesis retiene el batch |

---

## Estructura del Proyecto

```
metri-hephaestus/
├── template.yaml                      ← IaC SAM independiente del metro-engine
├── Makefile
├── go.mod / go.sum
├── cmd/
│   └── compactor_handler/main.go      ← Lambda Handler (Kinesis + EventBridge Cron)
└── internal/
    ├── arrow/
    │   ├── transmuter.go              ← Row→Columnar + EmitParquet (apache-arrow Go v14)
    │   └── schema_inferrer.go
    ├── fts/
    │   └── soundex_injector.go        ← Columnas _soundex_fts O(1)
    ├── olap/
    │   └── firehose_writer.go         ← PutRecordBatch → metri-olap-stream-{slug}
    ├── oltp/
    │   └── dynamo_writer.go           ← BatchWriteItem → metri-datahike-prod (EAV)
    ├── schema/
    │   └── resolver.go                ← ResolveEngine via DescribeDeliveryStream
    └── otel/
        └── tracer.go                  ← OTel spans + CloudWatch structured logs
```

---

## Referencias Cruzadas

| Documento                                                                  | Vínculo                                                                                                                              |
| :------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- |
| [template.yaml](../../template.yaml)                                       | SSOT de recursos AWS: `MetriDataLakeBucket`, `MetriDatahikeTable`, `MetriGlueDatabase`, `MetriKmsMasterKey`, `KINESIS_STREAM_PREFIX` |
| [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md)                       | `OLTPChannel` y `OLAPChannel` — Hephaestus replica su comportamiento sin WAF                                                         |
| [03_FASE_INGESTION.md](03_FASE_INGESTION.md)                               | Pool Model multitenant — mismo esquema de aislamiento                                                                                |
| [02_FASE_MOTOR_SCHEMA_DRIVEN_CORE.md](02_FASE_MOTOR_SCHEMA_DRIVEN_CORE.md) | Códice SSOT — define `engine: "olap"/"oltp"` por entity_type                                                                         |
| [10_FASE_GESTION_ERRORES_EDA.md](10_FASE_GESTION_ERRORES_EDA.md)           | Ouroboros Error Framework — Checkpoints y OTel                                                                                       |
