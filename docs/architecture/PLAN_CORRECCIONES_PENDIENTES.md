# Plan de refactorización — Correcciones pendientes del camino OLAP

> **Componente:** `metri-engine` — lake OLAP (Firehose → Iceberg → Athena) y su motor de desarrollo
> **Verificado contra el código y la cuenta:** 3 de septiembre de 2026 — rutas, líneas, métricas y configuración AWS comprobadas sobre el árbol y la cuenta `982592308819`
> **Motivación:** la ejecución de [PLAN_COSTO_OLAP.md](PLAN_COSTO_OLAP.md) destapó que **la entrega Firehose → Iceberg nunca funcionó** (dato analítico perdido desde el 30-jul) y dejó un inventario de deudas operativas sin dueño
> **Naturaleza:** el sistema está en producción; ningún commit intermedio puede romperlo
> **Relación:** consume el veredicto de [MEDICION_COSTO_OLAP.md](MEDICION_COSTO_OLAP.md) (Fase 0 archiva DataFusion) y reactiva el ítem de `execute_single_query` del [PLAN_REFACTORIZACION.md](PLAN_REFACTORIZACION.md) Fase 3
> **ESTADO (2026-09-03):** Fases 0, 1, 2, 3, 4 y 6 **EJECUTADAS Y VERDES** — anexo de ejecución al final de esta sección. La causa raíz fue más profunda que la escrita aquí (metadata Iceberg borrada del lake, no solo prefijos ausentes); el remedio completo: stream `domain-fault` recreado con `WarehouseLocation` explícito + **4 tablas Iceberg materializadas por DDL de Athena**. Fase 1 desplegada en producción tras cerrarse el desmonte Cedar (con un incidente de empaquetado documentado y resuelto). Fase 5 (`execute_single_query`) **pendiente** — es la única abierta; el drift de WarehouseLocation en 3 streams queda como acción opcional agendada (`--recreate-on-catalog-drift`).

## Anexo de ejecución (2026-09-03)

| Fase | Resultado |
|---|---|
| **0 — Entrega Iceberg** | **VERDE.** Tres descubrimientos encadenados: (a) el prefijo solo no bastó — los centinelas A-C siguieron fallando con `WarehouseLocation does not exist`; (b) `CatalogConfiguration` es **inmutable** en streams existentes → se recreó `metri-olap-stream-domain-fault` con `WarehouseLocation: s3://…/iceberg-data/`; (c) el error mutó a `NoSuchTable`: las tablas Glue eran **Iceberg huecas** — su `metadata_location` apuntaba a metadata borrada del bucket (solo sobreviven objetos desde el 30-jul). Remedio final: borrar las 4 tablas huecas y materializarlas con DDL Iceberg de Athena (esquemas recuperados del StorageDescriptor del catálogo). Verificación: centinela entregado como Parquet + **consultable por Athena**; a las 17:17 UTC un **domain_fault real** (`INFRA_CEDAR_002`) cruzó el pipeline en vivo. Nota: `audit-log` no necesitó recreación — con la metadata materializada, el stream original entregó solo. |
| **2 — Vigilancia** | **VERDE.** `CloudWatchLoggingOptions` ON en los 4 streams (grupos `/aws/kinesisfirehose/metri-olap-*`; descubrimiento: exige `LogGroupName` **y** `LogStreamName` explícitos). Política `firehose-cloudwatch-logs` añadida al rol de entrega (no tenía permisos de logs — hubieran fallado en silencio). 12 alarmas creadas (3 por stream: `DeliveryToIceberg.FailedRowCount`, `DeliveryToS3.Success`, `FailedValidation.Records`) con acción hacia `metri-echo-escalation-production`. **Pendiente humano: el tópico SNS tiene 0 suscriptores** — sin añadir uno, las alarmas son dashboard, no página. |
| **3 — Reingesta** | **VERDE.** [scripts/ops/reingesta_iceberg_failed.py](../../scripts/ops/reingesta_iceberg_failed.py) (dry-run por defecto): 682 records emitidos, 0 fallos, 263 objetos archivados en `errors/firehose/reingested/`. Conciliación exacta por Athena: `audit_log` **634/634**, `domain_fault` **44/44 reales** + 1 orgánico vivo (los 48 ids del lote incluían 4 centinelas fallidos A-D). Cero objetos en `iceberg-failed/` — el lago ya no acumula errores. |
| **6 — Stream IoT** | **VERDE.** `metri-iot-telemetry-stream` en `ON_DEMAND`, ACTIVE, regla IoT intacta. ~$21,90/mes → ~$0 en reposo. |
| **1 — Deploy** | **VERDE (2026-09-03 18:24 UTC), con incidente de deploy documentado.** Desplegado tras cerrarse el desmonte Cedar (375 tests en verde). Verificado en vivo: LogGroup `metri-engine-metri-engine` con retención 14; `RUST_LOG: warn` (los WARN del QuotaSweeper llegan, los INFO no); `BucketKeyEnabled: true`; lifecycle `GLACIER_IR` a 90 días; **kinesis:\* = 0** en los 4 policies del rol con `firehose:PutRecordBatch` concedido; versión 211→212 publicada con alias `live` repuntado. **Incidente:** `sam deploy` empaquetó el template fuente (CodeUri `.`) en vez del artefacto de build — el binario subió anidado en `MetriEngineFunction/bootstrap` y la versión 211 crasheó al arrancar (~4 min, 50 `Runtime.ExitError`). Rollback quirúrgico: `update-function-code --publish` con el zip correcto (bootstrap en raíz) + `update-alias live → 212`. Lección operativa: con el `Metadata: BuildMethod: makefile` de este stack, desplegar SIEMPRE con `sam build && sam deploy` desde el mismo árbol (el build manual del directorio `.aws-sam/` no es equivalente). Evidencia de PutRecordBatch en tráfico orgánico: pendiente de la primera BulkIngest real post-deploy (el binario desplegado es el del commit 1c2bedc, cubierto por los 3 tests de olap_channel_tests). |
| **4 — Seeder** | **VERDE (2026-09-03, commit `c921f3b`).** [scripts vía Makefile: `make sync-iceberg`, `make sync-firehose`, `make sync-drift`] — binario Rust `src/bin/firehose-seeder.rs` + lógica pura testeada en `src/infrastructure/seeder.rs`. Puerta 4: (1) apply doble idempotente verificado en vivo — 2 corridas, 0 cambios, 0 fallos; (2) preflight Regla 05 en vivo: con un bucket inexistente aborta con mensaje accionable y exit 1 — el mismo estado que causó el incidente del 30-jul; (3) alta de entidad nueva: camino de creación implementado (`create-delivery-stream` con WarehouseLocation + UniqueKeys id + logging ON; requiere `--delivery-role-arn`) y ejercido en unidad — la demo en vivo queda para la próxima entidad OLAP real; (4) drift = 0 **parcial**: tablas 4/4 válidas y `domain-fault` conforme; `audit-log`, `inventory-ledger` y `meter-reading` mantienen el drift inmutable (sin `WarehouseLocation`) — el tráfico fluye porque sus tablas ya son válidas, y la normalización exige recrear el stream con `--recreate-on-catalog-drift --apply` (ventana de indisponibilidad por stream; queda como acción agendada, no urgente). |
| **5 — execute_single_query** | Pendiente, sin cambios. |

---

## Resumen en cinco líneas

Hay tres correcciones estructurales y tres de higiene. La estructural primera es **cerrar la herida**: verificar que la entrega Iceberg funciona (prefijos ya creados, sin verificar) y **reingestar los 256 objetos de error** que contienen el audit_log y domain_fault perdido. La segunda es **que no vuelva a pasar en silencio**: los 4 streams tienen CloudWatch logging OFF y cero alarmas — eso es exactamente por qué la pérdida duró 5 semanas. La tercera es **recuperar el gestor de streams**: el seeder `sync-firehose` citado por el template desapareció del repo y toda la configuración viva de Firehose hoy es manual. Las de higiene son: desplegar la Fase 1 del plan de coste ya commiteada, descomponer `execute_single_query` (reactivada por el archivo de DataFusion), y decidir el modo de facturación del stream IoT inactivo.

---

## Parte I — Diagnóstico verificado

| # | Deuda | Evidencia | Gravedad |
|---|---|---|---|
| 1 | **Entrega Iceberg sin verificar** | Remedio aplicado 2026-09-03 (prefijos `iceberg-data/<tabla>/`), pero **ningún record real ha probado el camino** desde entonces | Alta — todo lo demás depende de esto |
| 2 | **Dato perdido sin reingestar** | 256 objetos en `errors/firehose/iceberg-failed/` (236 `audit-log`, 20 `domain-fault`), cada uno con el record íntegro en `rawData` | Alta — el dato existe, solo no está en su tabla |
| 3 | **Detección inexistente** | `CloudWatchLoggingOptions: Enabled: false` en los 4 streams (verificado); **cero alarmas** de Firehose — las únicas alarmas de la cuenta son DLQ de IoT/notificaciones | Alta — la pérdida duró del 30-jul al 2-sep sin señal |
| 4 | **Seeder desaparecido** | El template remite a `make sync-firehose`/`make sync-iceberg`; **ninguno de los dos targets existe en el Makefile**. La config viva de 4 delivery streams (buffering, retry, UniqueKeys, prefijos) es innombrada e irrecuperable desde el repo | Media — es la causa de raíz de #1 |
| 5 | **Fase 1 costo-OLAP sin desplegar** | Commits `37235a9`…`225e566` en main: retención de logs, bucket key, lifecycle, IAM kinesis fuera, `RUST_LOG: warn` — nada vive en AWS hasta el deploy | Baja pero ya pagada |
| 6 | **`execute_single_query`** | `local_s3_query_engine.rs:196` — la función de 428 líneas del plan mayor, reactivada al archivarse DataFusion (su Fase 5 ya no la borra) | Media — deuda de mantenibilidad |
| 7 | **Stream IoT en PROVISIONED inactivo** | `metri-iot-telemetry-stream`: 291K records en julio, **0 en agosto**; es el sumidero de la regla IoT habilitada `metri_telemetry_generic_rule` — ~$21,90/mes fijos | Baja — coste, no corrección |
| 8 | W8001 preexistente (`HasDevCorsOrigin` sin usar, `template.yaml:48`) | `sam validate` | Trivial |

**Resuelto durante esta sesión (no entrar aquí):** `src/eda/outbox.rs` ya no existe (ítem 4 de la Fase 2 del plan mayor, cerrado); la retención de los 3 LogGroups vivos está aplicada por CLI.

## Parte II — Alcance

**Dentro:** el camino OLAP completo (ingesta, entrega, tabla, vigilancia, gestión de streams) en la cuenta `982592308819`; el motor de desarrollo (`LocalS3QueryEngine`).

**Fuera:** el silencio de los dispositivos IoT (por qué no publican desde agosto) — vive en el stack `metri-iot` y su repo; aquí solo se trata su **coste** (#7), no su causa. **Fuera:** `meter_reading` como camino de consulta (sin cambio de alcance respecto al plan anterior). **Fuera:** resucitar DataFusion — la Puerta 0 del plan de coste lo archivó; si el volumen OLAP crece órdenes de magnitud, ese plan ya está escrito.

## Parte III — Reglas

Las cuatro del plan mayor aplican íntegras. Este plan añade dos:

| # | Regla | Por qué |
|---|---|---|
| 05 | **Nada se da por corregido sin prueba end-to-end.** La entrega Iceberg se considera reparada solo cuando un record real aparece como dato consultable en la tabla — no cuando el comando del remedio termina sin error. | El remedio de los prefijos terminó bien y aun así queda por verificar; el bug vivió 5 semanas porque nadie miró el extremo final. |
| 06 | **Primero la vigilancia, después el cambio.** Las alarmas de la Fase 2 preceden a la reingesta y a cualquier reconfiguración de streams. | Reingestar o reconfigurar sin alarmas es repetir el modo de fallo con más datos en juego. |

## Parte IV — Las seis fases

### Fase 0 — Verificar el remedio de la entrega

**Bloqueante · ~1 hora · Regla 05**

1. Inyectar un **record centinela** por `firehose put-record` al stream `metri-olap-stream-domain-fault`: un domain_fault sintético válido, tenant `system`, `id` con prefijo `sentinel-` y `severity: DEBUG` — rastreable y distinguido de dato real.
2. Verificar (buffer 300 s): aparece `iceberg-data/domain_fault/**` y el record es consultable vía Athena (`SELECT … WHERE id LIKE 'sentinel-%'`).
3. Si falla: leer el objeto de error nuevo, corregir (siguiente sospechoso: fijar `WarehouseLocation` explícito vía `update-delivery-stream`, o recrear la tabla Iceberg con el seeder de la Fase 3) y repetir.
4. Borrar el centinela (`DELETE FROM … WHERE id LIKE 'sentinel-%'` vía Athena/MergeOnRead) y dejar el resultado escrito en la medición.

**Puerta 0**
- [ ] Existe en el bucket un objeto de **dato** (no error) bajo `iceberg-data/domain_fault/`
- [ ] Una query Athena devuelve el record centinela
- [ ] El centinela está borrado y el procedimiento documentado

### Fase 1 — Desplegar lo ya comprometido

**Días (agenda de deploy) · retorno inmediato · independiente**

1. Deploy del stack con los commits ya en main: `RetentionInDays`, `BucketKeyEnabled`, lifecycle Glacier IR, IAM sin `kinesis:*`, `RUST_LOG: warn`, `put_records`/PutRecordBatch.
2. Verificar post-deploy: LogGroup nuevo con retención 14; un BulkIngest real produce 1 llamada PutRecordBatch por chunk (log `warn` no lo mostrará — verificar por CloudWatch métrica `IncomingRecords` del stream vs invocaciones).
3. Micro-limpieza en el mismo deploy o separado (Regla 01): eliminar la condición `HasDevCorsOrigin` sin uso (W8001) o conectarla.

**Puerta 1**
- [ ] El stack desplegado tiene `MetriEngineLogGroup` con retención 14 y el grupo nuevo recibe los logs
- [ ] La ingesta real pasa por `PutRecordBatch` (evidencia en métricas, no en fe)
- [ ] `sam validate` sin warnings propios

### Fase 2 — Vigilancia antes de tocar nada más

**Horas · Regla 06 · prerrequisito de las fases 3 y 4**

1. `CloudWatchLoggingOptions: Enabled: true` en los 4 streams (hoy `false` en los cuatro — verificado).
2. Alarmas CloudWatch por stream, con el mismo destino de notificación que las alarmas DLQ existentes (`metri-iot-*-dlq-*` demuestran que el mecanismo ya existe en la cuenta):
   - `FailedPutCount > 0` (1 datapoint) — falla de escritura del engine.
   - `DeliveryToS3.Success < 1` (1 datapoint en 15 min) — falla de entrega.
   - Métricas Iceberg del destino (`IcebergCommit.Success < 1`) — falla de commit de tabla.
3. **La alarma que habría ahorrado este incidente:** un contador de objetos en `errors/firehose/` (métrica `BucketSizeBytes` no sirve por granularidad — usar la excepción del engine: `DomainError::infra(Infra005)` ya produce faults vía Sherlog; verificar que esas faults son visibles y alertables).

**Puerta 2**
- [ ] Los 4 streams loguean a CloudWatch
- [ ] Existe al menos una alarma por stream y una prueba documentada de disparo (inyectar un record inválido)
- [ ] Un runbook mínimo dice qué hacer cuando la alarma suena (mirar `errors/firehose/`, causa conocida: WarehouseLocation)

### Fase 3 — Reingesta del dato perdido

**Medio día · Regla 05 aplicada al revés: el dato sí existe, solo falta ponerlo donde debe**

1. Script one-shot versionado en `scripts/` (`reingesta_iceberg_failed.py` — la cuenta ya se opera con AWS CLI + Python): recorre `errors/firehose/iceberg-failed/`, decodifica `rawData` (un JSON por línea), y re-emite por **PutRecordBatch al stream de origen** (deducible del nombre del objeto) — reusa la entrega ya reparada, sin escribir contra Iceberg directamente.
2. **Dry-run por defecto**: imprime conteos y no emite. Solo con `--apply` emite.
3. Idempotencia gratis: `UniqueKeys: ["id"]` hace que Firehose haga merge — reingestar dos veces no duplica.
4. Conciliación: distintos `id` en `rawData` (esperado: 236 audit + 20 fault menos solapamiento por reintentos) vs counts en tabla vía Athena. Los objetos reingestrados se **archivan** (nuevo prefijo `errors/firehose/reingested/`), no se borran.
5. Si algún record vuelve a caer en error → la Fase 2 ya está mirando.

**Puerta 3**
- [ ] Conciliación por `id`: todo record distinto en `rawData` está consultable en su tabla
- [ ] Los 256 objetos están archivados bajo `reingested/`
- [ ] El script corre dos veces sin duplicar (idempotencia probada)

### Fase 4 — El seeder: la gestión de streams vuelve al repo

**2-3 días · causa raíz de la Fase 0**

**Decisión — binario Rust leyendo el Códice, no CloudFormation declarativo.** Razones: (a) los streams se crean **por entidad del Códice** (el diseño del template lo dice explícitamente: "CERO cambios en este archivo template.yaml" al añadir entidades — declararlos en SAM rompería esa propiedad); (b) Clojure quedó condenado por la purga del plan mayor, no hay que resucitarlo; (c) el engine ya tiene todo el contexto (registry, coercion, naming `<prefix>-<entity-slug>`) en Rust.

1. `src/bin/firehose-seeder.rs` (o `scripts/` si prefiere no engordar el binario Lambda): lee el registro del Códice (`config/models/`), lista los delivery streams vivos, y hace **upsert idempotente** de la configuración esperada: destino Iceberg (catálogo Glue, base `metri_olap`, tabla por entidad, `UniqueKeys: ["id"]`), `S3ErrorOutputPrefix`, logging ON (Fase 2), retry 300 s, y **`WarehouseLocation` explícito** — el parámetro cuya ausencia causó el incidente.
2. `make sync-firehose` restaurado; `make sync-iceberg` con el mismo criterio para las tablas Glue (hoy creadas a mano el 29-jul).
3. **Preflight obligatorio** (Regla 05 embebida en código): antes de cualquier write, verificar que la `WarehouseLocation` de cada tabla existe en S3 — fallar con mensaje accionable si no.
4. Test: correr el seeder dos veces produce "sin cambios" la segunda; un test de unidad verifica el preflight contra un prefijo inexistente.

**Puerta 4**
- [ ] `make sync-firehose` es idempotente (2 corridas → 0 cambios)
- [ ] El preflight detecta un `WarehouseLocation` inexistente antes de escribir
- [ ] Una entidad nueva OLAP se da de alta con el seeder sin tocar template ni consola
- [ ] La config viva de los 4 streams coincide con lo que el seeder declara (drift = 0)

### Fase 5 — `execute_single_query` (reactivada del plan mayor)

**Días · coordinada, no duplicada**

El archivo de DataFusion (Puerta 0 del plan de coste) devolvió a la Fase 3 del plan mayor su objetivo nº 2: `execute_single_query` (`local_s3_query_engine.rs:196`, 428 líneas) ya no va a ser borrada, así que **sí conviene descomponerla**. Este plan no reescribe esa fase: la activa con dos condiciones nuevas aprendidas aquí:

1. **Corpus primero (Regla 05 heredada):** las 6 queries golden del camino OLTP-local de este motor existen en los snapshots; capturar los resultados actuales de `execute_single_query` contra el lake local antes de mover una línea — es el mini-oráculo del motor de desarrollo.
2. **Red primero (Regla 02):** cada extracción (parsing, planning, ejecución de Arrow/Parquet, mapeo a `QueryResults`) entra con su test de caracterización, como se hizo esta semana en `cedar/evaluator.rs` (commit `a0a099c` del desmonte paralelo — mismo patrón).
3. Sin límite artificial de líneas: la puerta es estructural (las 4 responsabilidades en módulos con red propia), no numérica.

**Puerta 5**
- [ ] Corpus golden capturado y versionado antes del primer movimiento
- [ ] Cada extracción va en commit propio con su test, suite verde (Regla 03)
- [ ] `LocalS3QueryEngine` sigue pasando los mismos goldens byte a byte

### Fase 6 — Coste del pipeline IoT inactivo

**Horas · decisión reversible, fuera del repo**

**Decisión — modo ON_DEMAND, no borrado ni shards menores.** El stream es el sumidero de una regla IoT **habilitada**; borrarlo rompe el camino si los dispositivos vuelven. PROVISIONED con 2 shards cuesta ~$21,90/mes con cero tráfico; ON_DEMAND cuesta por uso (a 0 records/mes = ~$0) y absorbe sin cambio alguno el pico de 291K/mes que hubo en julio (64 shards equivalentes on-demand en el peor segundo — el tráfico histórico no lo acerca).

1. `aws kinesis update-stream-mode --stream-arn … --stream-mode-details StreamMode=ON_DEMAND` — reversible, sin borrar nada.
2. Reportar al equipo IoT el silencio desde agosto (causa raíz de verdad fuera de este repo).

**Puerta 6**
- [ ] El stream está en ON_DEMAND y la regla IoT sigue habilitada
- [ ] La factura de octubre ya no muestra la línea fija de shards

## Parte V — Riesgos

| Riesgo | Mitigación | Fase |
|---|---|---|
| **El remedio de los prefijos no basta** (p. ej. Firehose cacheó el estado del catálogo) | La Fase 0 existe exactamente para eso; siguiente paso escrito: `WarehouseLocation` explícito vía `update-delivery-stream` | 0 |
| **Reingesta duplica datos** si corre dos veces | `UniqueKeys: ["id"]` hace el merge idempotente; conciliación por `id` en la Puerta 3; dry-run por defecto | 3 |
| **Reingesta reabre el bug** si algo sigue roto | Regla 06: alarmas (Fase 2) ANTES de reingresar (Fase 3); los fallos nuevos caen en `errors/` ya vigilados | 2 → 3 |
| **El seeder "arregla" streams con config viva distinta** (drift aplicado por sorpresa) | Dry-run/diff en la primera corrida; la Puerta 4 exige drift = 0, no cambios a ciegas | 4 |
| **El centinela contamina dashboards** | Tenant `system`, `severity: DEBUG`, `id LIKE 'sentinel-%'`, borrado verificado en la Puerta 0 | 0 |
| **ON_DEMAND más caro si la telemetría vuelve fuerte** | Punto de equilibrio ≈ 2 shards sostenidos (~146K records/mes de forma constante); julio fue 291K pero agosto 0 — perfil espigado, donde on-demand gana; revisar en la factura de octubre | 6 |
| **Deploy de la Fase 1 con cambio de LogGroup** (el grupo nuevo arranca vacío) | Los 3 grupos viejos conservan 14 días de historia aplicada por CLI; ventana de transición sin pérdida de visibilidad | 1 |
| **Blanco móvil con el desmonte Cedar en paralelo** | Sin solapamiento de archivos (este plan toca `local_s3_query_engine.rs`, no `cedar/`); seguir Regla 03 por commit | 5 |

## Parte VI — Coordinación

| Plan | Efecto |
|---|---|
| PLAN_COSTO_OLAP (archivado tras Fase 1) | Este plan hereda su veredicto, ejecuta su Fase 1 pendiente de deploy y absorbe sus hallazgos (§5a/§5b de la medición) como fases 0-4 |
| PLAN_REFACTORIZACION Fase 3, objetivo nº 2 | **Reactivado** como Fase 5 de este plan (ya no es "código condenado") |
| PLAN_REFACTORIZACION Fase 1 (CI, goldens) | La Fase 5 de este plan depende del patrón red-first ya demostrado en `a0a099c`; el corpus OLTP-local alimenta el mini-oráculo |
| Stack `metri-iot` (otro repo) | Fase 6 toca solo el modo de facturación; el silencio de dispositivos se reporta, no se diagnostica aquí |

## Orden si solo hay tiempo para una cosa

La **Fase 2** (vigilancia): son horas, no depende de nadie, y convierte el siguiente incidente de "5 semanas sin darse cuenta" en "una alarma a los 15 minutos". La reingesta (Fase 3) y la Fase 0 son las siguientes por valor; el resto puede esperar.
