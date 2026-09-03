# Medición de coste del camino OLAP — Puerta 0 de [PLAN_COSTO_OLAP.md](PLAN_COSTO_OLAP.md)

> **Fecha de medición:** 2 de septiembre de 2026
> **Cuenta:** `982592308819` (perfil `metri-dev`, us-east-1, cuenta aislada — no pertenece a una Organization)
> **Periodo medido:** 2026-08-01 → 2026-09-01
> **Veredicto contra el criterio vinculante de los $200:** Athena procesó **0 bytes** en agosto ⇒ su línea es **$0/mes**. El plan **se archiva tras la Fase 1**. Las fases 2-5 (oráculo, DataFusion, corte progresivo) no se pagan.

---

## 1. Método

Métricas de **uso** (CloudWatch) sobre facturación (Cost Explorer), porque en esta cuenta CE no es fiable (§2). Las métricas de uso son la evidencia primaria del criterio: Athena factura por TB escaneado; 0 bytes escaneados ⇒ $0, con independencia de lo que diga la factura.

```bash
aws ce get-cost-and-usage --time-period Start=2026-08-01,End=2026-09-01 \
  --granularity MONTHLY --metrics UnblendedCost --group-by Type=DIMENSION,Key=SERVICE
aws cloudwatch get-metric-statistics --namespace AWS/Athena --metric-name ProcessedBytes \
  --dimensions Name=WorkGroup,Value=metri-analytics --period 2592000 --statistics Sum …
aws cloudwatch get-metric-statistics --namespace AWS/Firehose --metric-name IncomingRecords …
aws firehose describe-delivery-stream --delivery-stream-name metri-olap-stream-audit-log
aws logs describe-log-groups
```

## 2. Números

| Línea | Medición de agosto | Coste mensual estimado |
|---|---|---|
| **Athena** (workgroup `metri-analytics`) | `ProcessedBytes` = **0 bytes**; `athena-results/` vacío; el lake entero son **254 objetos / 575 KiB** | **≈ $0** |
| **Firehose** | `IncomingRecords`: audit-log **525**, domain-fault **27**, inventory-ledger **0**, meter-reading **0** → 552 records ≈ 2,8 MB facturados (redondeo 5 KB/record) | ≈ $0 |
| **Data Streams** | `metri-iot-telemetry-stream`: 2 shards PROVISIONED. ~~Huérfano~~ **corregido en la sección 5**: es el sumidero de la regla IoT Core **habilitada** `metri_telemetry_generic_rule` (MQTT `metri/telemetry/+/+`); recibió **291.037 records de por vida** (julio) y 0 en agosto | ~$21,90 fijos — el coste del pipeline IoT inactivo; el investigable es el silencio de los dispositivos, no el stream |
| **CloudWatch Logs** | Ningún LogGroup del stack declara retención (32 grupos listados; solo 2 de stacks ajenos tienen 7 días). Función engine viva: **155,4 MB ingeridos en agosto**, 338,4 MB almacenados | ~$0,08/mes ingesta, almacenamiento creciente sin techo |
| **Lambda** | 59.275 invocaciones en agosto (función `metri-engine-MetriEngineFunction-nzuDmeFu5usS`) | centavos |
| **Cost Explorer** | Julio $0,0000040; agosto $0,0000089 (UnblendedCost total) | **No fiable**: contradice el uso observado (2 shards aprovisionados solos cuestan ~$22). Posible cuenta con facturación no visible por CE. Los números de decisión de este documento son métricas de uso, no CE |

**Caveat de alcance:** si existe otra cuenta productiva distinta de `982592308819`, repetir este procedimiento allí antes de dar por bueno el veredicto. Este documento es a la vez el registro y el procedimiento.

## 3. Decisión (criterio vinculante de la Puerta 0)

> *"si la línea de Athena está por debajo de ~$200/mes, este plan se archiva tras la Fase 1"*

Athena: **$0**, tres órdenes de magnitud por debajo del umbral. Las fases 2-5 del plan (oráculo, `DataFusionQueryEngine`, modo sombra, corte por entidad, retirada de `LocalS3QueryEngine`) **no se ejecutan**. Consecuencias de coordinación con [PLAN_REFACTORIZACION.md](PLAN_REFACTORIZACION.md):

- La **Fase 3 objetivo nº 2** (descomponer `execute_single_query`, 428 líneas) **vuelve a estar vigente**: el riesgo de "refactorizar código condenado" desaparece porque el código ya no está condenado.
- `LocalS3QueryEngine` **se queda**: los 1.555 líneas de motor SQL casero siguen siendo deuda técnica del plan mayor, no de este.
- `AthenaQueryEngine` sigue siendo el único motor OLAP de producción; `SqlDialect` sigue sin una segunda implementación viva. La costura (`IQueryEngine`, `SqlDialect`) queda documentada en el plan original si el volumen OLAP creciera alguna vez.

La Fase 1 (líneas fijas + batching) **se ejecuta igualmente**: era "gane quien gane la Fase 0" y su retorno es inmediato.

## 4. Hallazgo que modifica la Fase 1 (ítem 5)

El plan prescribía empaquetar ~7 records de ~700 B en **un record de Firehose de ~5 KB** (7,3× de ahorro en volumen facturado). **Ese ahorro no es alcanzable contra el destino actual:**

- Los 4 delivery streams entregan a **tablas Iceberg** (verificado con `firehose describe-delivery-stream`: `IcebergDestinationDescription`, `AppendOnly: false`, `UniqueKeys: ["id"]`).
- Para destino Iceberg, AWS exige que **cada record de Firehose sea un único objeto JSON válido** ("Firehose currently supports only single JSON item as record payload and doesn't support JSON arrays"). Concatenar objetos → `ICEBERG_BAD_DATA` → los records van a `errors/firehose/` y **no llegan a la tabla**. Hay objetos `iceberg-failed` del propio día de esta medición (2026-09-02, stream `domain-fault`).

Lo que sí se ejecuta del ítem 5, sin romper la entrega:

- **`put_records` en `IStreamWriter` implementado con `PutRecordBatch`** (el IAM ya lo permitía): N llamadas HTTP concurrentes por chunk pasan a 1 llamada por chunk de 50. Reduce presión de pool de conexiones y latencia de ingesta; **no cambia la factura** (Firehose factura por record, no por llamada API).
- Cada record individual mantiene su payload byte a byte idéntico al camino anterior (`put_record` + newline), que es el equivalente a la Puerta 1 de "suite golden byte a byte idéntica" a nivel de unidad.
- El ahorro 7,3× de Firehose queda documentado como **no aplicable**: a 552 records/mes valía ~$0,0000 anyway, y contra Iceberg el mecanismo propuesto habría **roto producción**.

## 5. Acciones operativas resultantes (fuera del repo)

### 5a. HALLAZGO MAYOR (2026-09-03): la entrega Firehose → Iceberg NUNCA funcionó

La investigación de los objetos `errors/firehose/iceberg-failed/` que motivó esta sección terminó en un hallazgo que reordena el diagnóstico completo:

- **El lake está vacío de verdad**: los únicos objetos del bucket (256) son errores, desde el **30 de julio** (el primer error es de `audit-log`). No existe ni un solo dato entregado a Iceberg: no hay prefijo `iceberg-data/`, ni `firehose-backup/`, ni `athena-results/`.
- **Causa raíz** (`Iceberg.UnsupportedSchemaEvolution`): *"Warehouse location does not exist. Ensure that WarehouseLocation under CatalogConfiguration is existing S3 location."* Los streams no declaran `WarehouseLocation` en su `CatalogConfiguration`; Firehose deriva la ubicación de la tabla Glue (`s3://…/iceberg-data/<tabla>`), un prefijo que **no existía en S3**, y rechaza cada record.
- **Impacto**: 236 objetos de error de `audit-log` y 20 de `domain-fault` — **todo audit_log y domain_fault ingerido desde el 30-jul se perdió del camino analítico** (sobrevive solo embebido en los objetos de error). El `ProcessedBytes = 0` de Athena no es solo "poco volumen": no había nada que escanear ni datos sobre los que consultar.
- **REMEDIADO el 2026-09-03** (detalle completo en el anexo de [PLAN_CORRECCIONES_PENDIENTES.md](PLAN_CORRECCIONES_PENDIENTES.md)): la causa final fueron las tablas Glue Iceberg **huecas** — apuntaban a metadata borrada del bucket. Tras recrear el stream `domain-fault` con `WarehouseLocation` explícito y materializar las 4 tablas por DDL de Athena, la entrega funciona de extremo a extremo (verificado con centinela consultable por Athena y con tráfico real en vivo). El dato perdido se reingestró: 634 audit_log + 44 domain_fault, conciliación exacta.
- Este hallazgo **refuerza el veredicto** de la sección 3 (no hay volumen OLAP que justifique DataFusion) y añade una deuda real fuera del alcance de este plan: la pérdida de datos desde julio y la herramienta de seeding desaparecida.

### 5b. Acciones

| Acción | Impacto | Estado |
|---|---|---|
| ~~Borrar el Data Stream huérfano~~ **Corregido**: `metri-iot-telemetry-stream` es el sumidero de la regla IoT habilitada `metri_telemetry_generic_rule` y recibió 291K records en julio. **NO borrar.** | El investigable es por qué la telemetría dejó de publicar en agosto (dispositivos) | Retirado como acción |
| Crear prefijos `iceberg-data/<tabla>/` (remedio del error de Firehose) | Reabre la entrega del camino OLAP | **Aplicado 2026-09-03** — verificar con el primer record |
| `aws logs put-retention-policy --log-group <engine> --retention-in-days 14` sobre los 3 LogGroups vivos de `metri-engine` | Los existentes no heredan la retención del template hasta el redeploy; el abandonado de stacks anteriores seguiría creciendo | **Aplicado el 2026-09-02** |
| Evaluar reingesta de los 256 objetos de error (`iceberg-failed`) a las tablas Iceberg | Recupera el audit_log/domain_fault perdido desde el 30-jul (los records viajan embebidos en `rawData`) | Pendiente de decisión |
| Restaurar el seeder de Firehose/Iceberg (`sync-firehose` citado por el template ya no existe en el repo) | Sin herramienta de seeding, cualquier reconfiguración de streams es manual | Pendiente — deriva de infraestructura del plan mayor |
| Si se confirma otra cuenta productiva, repetir esta medición allí | — | Pendiente de confirmación |
