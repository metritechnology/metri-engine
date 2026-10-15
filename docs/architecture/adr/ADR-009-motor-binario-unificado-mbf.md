# ADR-009 — Motor Binario Unificado HTAP Metri Vortex (`.mbf`) sobre AWS S3

**Estado:** propuesta · 2026-10-15
**Suite de diseño e implementación:** [`docs/mbf/README.md`](../../mbf/README.md)

## Contexto

La arquitectura dual actual de `metri-engine` divide los datos entre DynamoDB (`src/eav/`, 4 tablas incluyendo secuencias, schemas y caché, más 3 GSIs que multiplican por ~5× las WRUs) y el pipeline analítico Firehose + Glue + Athena SQL (`src/aegis/sql/` + `src/infrastructure/athena.rs`). Esto genera:
1. **Latencia y doble cobro en BI:** La función Lambda (`2048 MB`) permanece ociosa entre 1.000 ms y 3.500 ms esperando el *polling* de Athena, pagando tanto el escaneo de Athena (\$5/TB) como los GB-segundo de espera en Lambda.
2. **Fricción entre dominios:** Es imposible ejecutar en una sola transacción ACID o en una sola consulta sub-milisegundo el cruce entre el libro mayor contable (`inventory_ledger`, en OLAP), los lotes/movimientos de inventario y mano de obra (`inventory_batch`, `inventory_movement`, `labor_log`, en OLTP), las jerarquías ISA-95 (`asset` $\rightarrow$ `location`) y la telemetría IoT (`meter_reading`).
3. **Deriva numérica financiera y duplicación de motores:** Las agregaciones actuales convierten los valores a `f64` IEEE-754 (riesgo de pérdida de centavos en contabilidad) y el entorno local requiere mantener un motor SQL casero de 1.555 líneas (`LocalS3QueryEngine`) distinto del motor de producción.

## Decisión

Unificar los motores OLTP y OLAP en un único motor HTAP en Rust (**Metri Vortex**) basado exclusivamente en el formato binario propio **`.mbf` (Metri Binary Format)** almacenado en **AWS S3**:

1. **Formato Multimodal Alineado a 64 Bytes (`#[repr(C, align(64))]` Zero-Copy):** Cada archivo `.mbf` estructura los datos en 5 zonas físicas proyectables sin deserialización vía `bytemuck`:
   - **Zona 1 (PAX + MVCC + FSST):** Punto de acceso $O(1)$ con MPHF + Elias-Fano y vectores bi-temporales `tx_assert`/`tx_retract`.
   - **Zona 2 (Grafo CSR + Intervalos de Euler `[tin, tout]`):** Reemplaza el GSI-VAET en $O(1)$ y resuelve subárboles jerárquicos ISA-95 y herencia de `cost_center` con una comparación entera SIMD.
   - **Zona 3 (IoT Time-Series):** Timestamps *Delta-of-Delta* + floats *Chimp128* + pirámides de rollup (`1m`, `1h`, `1d`).
   - **Zona 4 (Motor Financiero y Contable Exacto):** Vectores `FixedDecimal128` (`i128` escala $10^6$, redondeo bancario *Half-to-Even* y método de Hamilton), sumas prefijas `FenwickPrefixSum128` (flujos P&L en $O(1)$), `FinancialBlockCheckpoint` (saldos `last_value` en $O(\log B)$), capas en tiempo real FIFO/LIFO/PMP/FEFO, matriz histórica multi-divisa ISO 4217 (`EmbeddedFxMatrix`), invariante de partida doble y cadena criptográfica Merkle SHA-256.
   - **Zona 5 (Hiper-Acelerador Estadístico de BI):** Sinopsis algebraicas fusionables por bloque (`NumericBlockSynopsis` de Welford/Chan, co-momentos bivariados de Pébay para Pearson/OLS, sketches KLL de 256B para percentiles y `RoaringBitmap` + `Xor8` para FTS/filtros).
2. **Protocolo ACID Serverless `HOT_HEAD.mbf` sobre S3 Conditional Writes:** Un único objeto caliente por tenant (`HOT_HEAD.mbf`) agrupa el manifiesto MVCC, el buffer L0, `UniqueClaim`, cuotas, secuencias y punteros `.mbf.idx`. Cada commit emite **1 sola llamada `PutObject` con `If-Match: <ETag>`**, y las lecturas aprovechan los **512 MB gratuitos de `/tmp` NVMe en Lambda** con revalidación `If-None-Match` (`304 Not Modified`) y deduplicación *Single-Flight*.
3. **Ejecución Directa AST IR $\rightarrow$ SIMD y Optimización Lambda Graviton:** `aegis::mbf` ejecuta directamente el AST de FlatBuffers sin generar SQL. `build.rs` pre-compila los 60 modelos del Códice en `.rodata` (Cold Start `< 2 ms`) y `tonic_web` se enlaza directamente con `lambda_http` sobre `RESPONSE_STREAM`, retirando `LambdaAdapterLayerArm64`.
4. **Rollout Gobernado por Oráculo (`Regla 05`):** Despliegue progresivo mediante `MBF_ENGINE_MODE = legacy | shadow | primary` antes de retirar DynamoDB, Firehose, Glue, Athena y `src/aegis/sql/`.

## Consecuencias

- **(+)** Reducción de **> 99 % en GB-segundo de Lambda para consultas BI** (de ~2.200 ms a ~15 ms) y **> 92 % en costos de almacenamiento y llamadas API**, con escala *Scale-to-Zero* real.
- **(+)** Exactitud contable bit-exacta al centavo (`FixedDecimal128`), 0 ms de desfase entre transacciones operativas/contables y dashboards BI, y paridad 100 % idéntica entre desarrollo local (`LocalFsObjectStore`) y producción (`AwsS3ObjectStore`).
- **(−)** La contención extrema de escritura sobre un mismo tenant (> 200 TX/s simultáneas en el mismo milisegundo) requiere *Group Commit* en memoria y reintentos CAS sobre `HOT_HEAD.mbf`; los percentiles sobre archivos históricos L1/L2 (> 4.096 filas) tienen una cota de aproximación KLL documentada de $\le 0,05\%$.
