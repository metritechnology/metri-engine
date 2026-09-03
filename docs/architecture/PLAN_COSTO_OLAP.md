# Plan de refactorización — Coste del camino OLAP para logs

> **Componente:** `metri-engine` · camino OLAP de `audit_log` y `domain_fault`
> **Verificado contra el código:** 2 de septiembre de 2026 — rutas y líneas comprobadas sobre el árbol de trabajo
> **Motivación:** el presupuesto de AWS sube y las líneas sospechosas son Athena, Firehose/Kinesis y CloudWatch Logs
> **Naturaleza:** el sistema está en producción; ningún commit intermedio puede romperlo
> **Relación con el plan mayor:** hereda las cuatro reglas de [PLAN_REFACTORIZACION.md](PLAN_REFACTORIZACION.md) (Parte III) y consume su `EngineConfig` (Fase 4, ✅ hecha)

---

## Resumen en cinco líneas

El coste del camino OLAP se reparte en tres sitios y **solo uno justifica un refactor**. La ingesta (Firehose) se arregla batcheando, que es un cambio de veinte líneas y no una migración. Las líneas fijas (retención de logs, KMS sin bucket keys, shards aprovisionados) se arreglan en configuración. El único cambio estructural que se paga solo es **sustituir Athena por DataFusion embebido en el propio Lambda** para las entidades de log, aprovechando dos costuras que el motor ya tiene: `IQueryEngine` y `SqlDialect`.

Beneficio colateral que casi vale tanto como el ahorro: DataFusion también reemplaza a `LocalS3QueryEngine` — **1.555 líneas de motor SQL casero** que hoy hacen que desarrollo y producción ejecuten dos motores distintos.

---

## Parte I — Diagnóstico verificado

### Las costuras que hacen esto barato

El motor ya está diseñado para este cambio, aunque no fuera a propósito:

| Costura | Ubicación | Estado | Por qué sirve |
|---|---|---|---|
| `IQueryEngine` | `src/domain/protocols.rs:78` | **2 métodos, 2 implementaciones vivas** | `AthenaQueryEngine` y `LocalS3QueryEngine` ya prueban que el trait admite motores radicalmente distintos |
| `SqlDialect` | `src/aegis/sql/dialect.rs:5` | **12 métodos, 2 implementaciones** (`AthenaDialect`, `PostgresDialect`) | Toda la divergencia de sintaxis entre motores ya está aislada aquí |
| `IStreamWriter` | `src/domain/protocols.rs:91` | 1 método, sin variante batch | El punto donde se arregla la ingesta |

**El trait `IQueryEngine` no hay que cambiarlo.** Tiene forma de Athena (`start_query` → `execution_id` → `get_query_results` con polling), que a primera vista no encaja con un motor in-process. Pero `LocalS3QueryEngine` ya resolvió exactamente eso: guarda el SQL en un `HashMap<execution_id, sql>` y ejecuta en la segunda llamada (`local_s3_query_engine.rs:29`, comentario *«misma estrategia que el polling de Athena»*). Y el par de llamadas es consecutivo dentro de la misma invocación — `janus/router/olap.rs:128` y `:145` —, así que el mapa en memoria es seguro en Lambda. La estrategia está probada en el árbol.

### Lo que sí está mal

1. **El dialecto está hardcodeado.** `compile_athena_sql_with_time_frame` construye `let dialect = AthenaDialect;` en dos sitios (`src/aegis/sql/compiler.rs:58` y `:146`) en lugar de recibirlo. La costura existe pero está soldada. Es lo único que hay que abrir.

2. **`put_record` uno por uno.** `src/janus_router/olap_channel.rs:129` emite un `PutRecord` por registro. Firehose factura **redondeando cada record a 5 KB**; un `audit_log` (`interceptor.rs:61`) pesa ~700 B. Se paga 7,3× el volumen real.

3. **Ninguna `LogGroup` declara `RetentionInDays`.** Cero coincidencias en `template.yaml`. Los logs de Lambda se retienen indefinidamente a $0,50/GB de ingesta más almacenamiento acumulativo, con `RUST_LOG: info` y un `info!` por cada record ingerido (`olap_channel.rs:95`).

4. **`MetriDataLakeBucket` usa CMK KMS sin `BucketKeyEnabled`.** Cada objeto que escribe Firehose es una llamada `GenerateDataKey` facturada.

5. **El IAM concede `kinesis:PutRecord` sobre `metri-olap-stream-*`** (`template.yaml:736`) además de Firehose. Si hay Data Streams aprovisionados, son ~$11/shard/mes fijos por entidad, con o sin tráfico.

### El nombre miente

`compile_athena_sql*`, `athena_engine`, `ATHENA_MODE`, y el mensaje de error *«Athena query engine no está inicializado»* (`janus/router/olap.rs:117`) nombran al proveedor, no a la capacidad. Es la misma clase de deuda que la v3 del plan mayor purgó con las referencias al stack anterior. Se corrige en la Fase 3, que es movimiento puro.

---

## Parte II — Alcance

**Dentro:** el camino de consulta OLAP de `audit_log` y `domain_fault`. La agregación de records en ingesta, para todas las entidades OLAP.

**Fuera:** `meter_reading`. Es serie temporal IoT con un perfil de volumen distinto, y ahí el camino columnar sí se justifica. Se queda en Athena y no se toca.

**Fuera:** migrar la ingesta de Firehose a SQS. Se evaluó y **no se paga**: SQS a $0,40 por millón de requests deja el ahorro en ~2× frente al 7,3× que da la agregación de records, a cambio de una pieza de infraestructura nueva. Firehose se queda.

---

## Parte III — Reglas

Las cuatro del plan mayor (Parte III) aplican íntegras. Este plan añade una quinta, propia del cambio de motor:

| # | Regla | Por qué |
|---|---|---|
| 05 | **Oráculo antes que motor.** Ninguna línea de DataFusion se escribe antes de que exista un corpus de queries con resultados de Athena capturados y versionados. | Un motor analítico nuevo no falla con excepciones: **devuelve números distintos**. Sin oráculo, la regresión es invisible y llega al dashboard de un cliente. |

---

## Parte IV — Las seis fases

### Fase 0 — Medir antes de mover

**Bloqueante · ~1 hora · no es refactor, es decidir si el plan existe**

Todo lo que sigue asume que Athena es una línea grande de la factura. Eso no está verificado — es una hipótesis derivada de leer la arquitectura.

1. Desglose real de la factura por tipo de uso:

   ```bash
   aws ce get-cost-and-usage --time-period Start=2026-08-01,End=2026-09-01 --granularity MONTHLY --metrics UnblendedCost --group-by Type=DIMENSION,Key=USAGE_TYPE --output json
   ```

2. Bytes escaneados por Athena: métrica `ProcessedBytes` del workgroup `AthenaWorkGroup`, mensual.
3. Records ingeridos: métrica `IncomingRecords` de cada delivery stream `metri-olap-stream-*`.
4. Inventario de Data Streams aprovisionados: `aws kinesis list-streams` y sus shards.
5. Volumen de ingesta a CloudWatch Logs del LogGroup de la Lambda.

**Criterio de decisión, explícito y vinculante:** si la línea de Athena está por debajo de ~$200/mes, **este plan se archiva tras la Fase 1**. Las fases 2-5 son entre dos y tres semanas de trabajo; por debajo de esa cifra no se pagan y el esfuerzo rinde más en la Fase 3 pendiente del plan mayor.

#### Puerta 0

- [ ] Existe en el repo un documento con el coste mensual real de Athena, Firehose, Data Streams y CloudWatch Logs
- [ ] La cifra de `ProcessedBytes` mensual y de `IncomingRecords` mensual está registrada como línea base
- [ ] La decisión de continuar o archivar está tomada por escrito contra el criterio de los $200

---

### Fase 1 — Las líneas fijas y el batching

**Independiente de todo lo demás · días · retorno inmediato · se ejecuta gane quien gane la Fase 0**

1. **Retención de logs.** `RetentionInDays: 14` en el LogGroup de la Lambda (hoy no existe el recurso explícito: se crea implícito y sin retención). `RUST_LOG: warn` en producción. Revisar de paso los `info!` del camino caliente — `olap_channel.rs:95` y `:141` emiten dos líneas por ingesta.
2. **`BucketKeyEnabled: true`** en `MetriDataLakeBucket`.
3. **Lifecycle del lake.** Transición a Glacier Instant Retrieval a los 90 días. Ojo con la retención regulatoria del sector salud: la política debe alargar la vida del dato, no acortarla — nada de `ExpirationInDays` sin decisión de negocio explícita.
4. **Data Streams huérfanos.** Si el inventario de la Fase 0 encuentra shards aprovisionados sin consumidor, borrarlos y retirar `kinesis:*` del IAM (`template.yaml:736`), que hoy concede más de lo que el código usa — el motor solo habla Firehose.
5. **Agregación de records — el único cambio de código de esta fase.** Añadir `put_records(&self, stream, Vec<(partition_key, Vec<u8>)>)` a `IStreamWriter` y empaquetar en `OlapChannel` hasta ~5 KB de payload por record de Firehose antes de emitir. El bucle de chunks de 50 (`olap_channel.rs:105`) ya es el sitio natural. Implementarlo con `PutRecordBatch`, que el IAM ya permite.

**Regla 01 aplica:** los pasos 1-4 son configuración y van en commits separados del paso 5, que es código.

#### Puerta 1

- [ ] Un test verifica que N registros de ~700 B producen `⌈N/7⌉` llamadas a Firehose, no N
- [ ] La suite golden del camino OLAP sigue byte a byte idéntica tras el batching
- [ ] `RetentionInDays` declarado; ningún `LogGroup` del stack retiene indefinidamente
- [ ] La factura del mes siguiente muestra caída medible en CloudWatch Logs y KMS

---

### Fase 2 — El oráculo

**Prerrequisito duro de las fases 3-5 · Regla 05**

Esta fase no cambia nada en producción. Construye el instrumento sin el cual el resto es adivinar.

1. **Corpus de queries representativas.** Por cada entidad en alcance (`audit_log`, `domain_fault`), y por cada viz type del contrato (`KPI`, `TIMESERIES`, `TABLE`, `PIE`, `BUBBLE`, `CSV_EXPORT`), una query real. Los 6 goldens por viz type de la Fase 1 del plan mayor son el punto de partida, pero cubren el camino OLTP: aquí hace falta el OLAP.
2. **Dataset fijo.** Un snapshot Parquet determinista del lake, versionado o generado por script, sobre el que ambos motores leen exactamente los mismos bytes. Sin esto, cualquier diferencia se puede achacar a los datos.
3. **Captura del oráculo.** Ejecutar el corpus contra Athena y versionar `(sql, columnas, filas)`. Este es el artefacto central del plan.
4. **Arnés comparador parametrizado por motor.** Un test que recibe un `dyn IQueryEngine`, corre el corpus y compara contra el oráculo. Correrá primero contra Athena — donde debe pasar trivialmente — y después contra DataFusion sin cambiar una línea.
5. **Política de tolerancia, decidida ahora y no cuando falle.** Las agregaciones exactas (`count`, `sum`, `min`, `max`) deben coincidir bit a bit. Las aproximadas **no van a coincidir**: `format_percentile` (`dialect.rs:20`) compila a `approx_percentile` de Presto, y DataFusion usa otro algoritmo. Fijar por escrito la tolerancia relativa aceptable para percentiles y documentarla como consecuencia en el ADR de la Fase 4.

#### Puerta 2

- [ ] El corpus cubre las 2 entidades × 6 viz types, con sus resultados de Athena versionados
- [ ] El arnés comparador pasa contra `AthenaQueryEngine` dos veces seguidas con salida idéntica
- [ ] El arnés acepta cualquier `dyn IQueryEngine` sin cambios de código
- [ ] La política de tolerancia para agregaciones aproximadas está escrita

---

### Fase 3 — Abrir la costura del dialecto

**Movimiento puro · Regla 01: no cambia comportamiento**

1. `compile_athena_sql_with_time_frame` (`compiler.rs:46`) recibe `dialect: &dyn SqlDialect` en lugar de construirlo. Igual en el camino híbrido (`compiler.rs:146`). Los llamadores pasan `&AthenaDialect` — **el comportamiento no cambia en este commit**.
2. El dialecto se resuelve desde `EngineConfig` (`OnceLock`, ya existente desde la Fase 4 del plan mayor), no desde `env::var` — la Puerta 4 de aquel plan prohíbe lo segundo.
3. Renombrar `compile_athena_sql*` → `compile_olap_sql*`, `athena_engine` → `query_engine`, `ATHENA_MODE` → `OLAP_ENGINE_MODE`. Commit propio, mecánico, sin lógica.

#### Puerta 3

- [ ] `git grep "AthenaDialect" src/aegis/sql/compiler.rs` devuelve 0
- [ ] El diff muestra solo movimiento: nada añadido, nada eliminado, nada reordenado
- [ ] El oráculo de la Fase 2 sigue byte a byte idéntico
- [ ] Ningún identificador del camino OLAP nombra al proveedor

---

### Fase 4 — El motor DataFusion

**El trabajo real · aquí vive el riesgo**

**Por qué DataFusion y no DuckDB:** DataFusion es Rust puro y no rompe el cross-compile ARM64 a `provided.al2023`; DuckDB arrastra C++ al build. Además DataFusion ya vive en el ecosistema Arrow/Parquet que la lectura del lake necesita de todos modos.

1. **`DataFusionDialect: SqlDialect`** — las 12 funciones del trait. Las tres de riesgo real, cada una con su caso en el corpus antes de implementarse: `format_percentile` (algoritmo distinto — ver política de tolerancia), `format_regr_slope` y `format_exists` (`dialect.rs:37`, la inyección jerárquica).
2. **`DataFusionQueryEngine: IQueryEngine`** — misma estrategia de `HashMap<execution_id, sql>` que `LocalS3QueryEngine`, que ya está probada. Registra la tabla como `ListingTable` sobre `s3://<lake>/<entity>/_tenant=…/` y mapea el `RecordBatch` resultante a `QueryResults`.
3. **Guard de footprint.** Antes de ejecutar, estimar los bytes que la query va a tocar tras el pruning de particiones. Si superan el umbral configurado, **delegar en Athena**. DataFusion en Lambda está acotado por 10 GB de memoria y no hace spill como Athena; este guard es lo que impide que una query grande tumbe la invocación.
4. **Cablear como tercera rama** del switch en la raíz de composición (`grpc/server.rs:87`), con Athena de default. No se reemplaza nada todavía.
5. **ADR.** Contexto, decisión, consecuencias — incluida la tolerancia de la Fase 2 y el límite de memoria. Va en `docs/architecture/adr/`, siguiendo ADR-001..005.

#### Puerta 4

- [ ] El arnés de la Fase 2 pasa con `DataFusionQueryEngine`
- [ ] Toda divergencia numérica frente al oráculo está justificada una a una en el commit, o el commit se revierte (Regla 04)
- [ ] Existe un test que fuerza el guard de footprint y verifica la delegación a Athena
- [ ] El cold start del Lambda no empeora más de lo acordado en el ADR
- [ ] El ADR está escrito y enlazado desde el código

---

### Fase 5 — Corte progresivo y retirada

**Nada de big bang · el sistema está en producción**

1. **Modo sombra, 2 semanas.** Ambos motores ejecutan; se sirve el resultado de Athena; las divergencias se registran como `domain_fault` vía Sherlog (`src/eda/`, que ya existe para esto). Coste extra durante la ventana: la ejecución de DataFusion, que es despreciable.
2. **Corte por entidad, no global.** `audit_log` primero — es la entidad que motivó el plan y la de agregaciones más simples. `domain_fault` después, con dos semanas de margen entre ambas.
3. **`meter_reading` no se corta.** Sigue en Athena, según el alcance.
4. **Retirada.** Si la divergencia es cero durante 30 días tras el corte, `AthenaQueryEngine` **se conserva** como fallback configurable — es el destino del guard de footprint y del rollback. No se borra.
5. **`LocalS3QueryEngine` sí se borra.** Con DataFusion corriendo en ambos entornos, las 1.555 líneas del motor casero sobran, y con ellas desaparece `execute_single_query` (428 líneas), que es el objetivo nº 2 de la Fase 3 pendiente del plan mayor. Desarrollo y producción pasan a ejecutar **el mismo motor**, que es una ganancia de corrección independiente del ahorro.

#### Puerta 5

- [ ] Cero divergencias registradas durante 30 días tras el corte de `audit_log`
- [ ] La línea de Athena en la factura cae ≥90% respecto a la línea base de la Fase 0
- [ ] `LocalS3QueryEngine` ya no existe y los tests locales corren contra DataFusion
- [ ] `AthenaQueryEngine` sigue cableado y hay un test que ejerce el camino de fallback

---

## Parte V — Riesgos

| Riesgo | Mitigación | Fase |
|---|---|---|
| **El plan no se paga.** Athena puede ser una línea menor y todo esto ser esfuerzo mal invertido | Criterio de los $200 en la Puerta 0, vinculante y por escrito | 0 |
| **Divergencia numérica silenciosa.** Un motor analítico nuevo no lanza excepciones: devuelve otros números, y el error llega al dashboard de un cliente | Regla 05: oráculo antes que motor. Modo sombra 2 semanas antes de cortar | 2 → 5 |
| **Percentiles que nunca van a coincidir.** `approx_percentile` de Presto y el de DataFusion usan algoritmos distintos por diseño | Política de tolerancia decidida en la Fase 2, **antes** de ver el primer fallo, y registrada en el ADR | 2 → 4 |
| **Memoria de Lambda.** DataFusion no hace spill; 10 GB es el techo duro y una query grande tumba la invocación | Guard de footprint con delegación a Athena; `AthenaQueryEngine` nunca se borra | 4 |
| **Radio de impacto mayor que el alcance.** `IQueryEngine` sirve a *todo* el OLAP, no solo a los logs — un cambio global tocaría `meter_reading` | Corte por entidad, nunca por motor. `meter_reading` fuera de alcance y sin tocar | 5 |
| **La regla WAF de anti-inyección asume sintaxis Presto.** `template.yaml:258` (Regla 7, prioridad 45) filtra patrones SQL para Athena | Revisar la regla contra la sintaxis que emite `DataFusionDialect` antes del corte; es control de seguridad vivo | 4 → 5 |
| **Cold start y tamaño del binario.** DataFusion es una dependencia pesada en un Lambda que ya arranca un servidor gRPC | Medir cold start antes y después; umbral acordado en el ADR es criterio de la Puerta 4 | 4 |
| **Blanco móvil.** La Fase 3 del plan mayor sigue abierta sobre `execute_single_query` (428 líneas), que esta Fase 5 borra entera | Coordinar: **no descomponer `execute_single_query`** si este plan pasa la Puerta 0 — sería refactorizar código condenado | 0 → 5 |
| **Dependencia nueva y grande.** DataFusion cambia el perfil de mantenimiento del motor | Es la contrapartida honesta del ahorro; se declara en el ADR con su coste, no se esconde | 4 |
| **Batching que pierde registros.** Agrupar records antes de emitir abre una ventana donde un fallo pierde el lote entero | El lote vive dentro de una sola llamada `route()`; propagar el error como hoy (`olap_channel.rs:136`) y no ampliar la ventana más allá de la invocación | 1 |

---

## Parte VI — Coordinación con el plan mayor

| Ítem del plan mayor | Efecto de este plan |
|---|---|
| Fase 3, objetivo nº 2 — `execute_single_query` (428 líneas) | **Se cancela** si la Puerta 0 sale a favor: la Fase 5 borra el archivo entero |
| Fase 4 — `EngineConfig` en `OnceLock` (✅ hecha) | **Prerrequisito consumido**: el dialecto y el modo de motor se resuelven ahí, no con `env::var` |
| Fase 5B, ítem 3 — ADRs para decisiones vivas | Este plan aporta un ADR nuevo (Fase 4, paso 5) |
| Fase 1 — goldens por viz type (✅ hecha) | Punto de partida del corpus de la Fase 2, que lo extiende al camino OLAP |

---

## Si solo hay tiempo para una cosa

La **Fase 1**. Son días de trabajo en configuración y veinte líneas de código, no dependen de ninguna decisión, y si la sospecha sobre CloudWatch Logs es correcta pueden recuperar más dólares que todo el resto del plan junto. Las fases 2-5 son un refactor con riesgo real que solo se justifica si la Fase 0 enseña un número grande en la línea de Athena.
