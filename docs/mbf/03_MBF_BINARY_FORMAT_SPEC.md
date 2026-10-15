# Metri Binary Format (`.mbf` v1) Physical Specification

> **English summary:** This document provides the byte-level specification of the **Metri Binary Format (`.mbf` v1)**, the self-contained **`HOT_HEAD.mbf`** transactional manifest, and the **`catalog.mbf.idx`** synopsis mirror. All internal structures are strictly 64-byte aligned (`#[repr(C, align(64))]`) for zero-copy memory mapping and ARM64 NEON SIMD execution, featuring dedicated physical zones for OLTP PAX+MVCC, CSR Graphs & ISA-95 trees, IoT Time-Series, 128-bit Fixed-Point Financial Accounting, and Algebraic BI Synopses.

---

## 1. Filosofía de Diseño del Formato `.mbf`

El formato `.mbf` ha sido diseñado desde cero para resolver tres cuellos de botella de la computación en la nube (AWS Lambda + AWS S3):
1. **Cero Deserialización (Zero-Copy Cast):** Cada bloque dentro de `.mbf` está alineado a múltiplos exactos de **64 bytes** (el tamaño de una línea de caché L1 en procesadores ARM64 Graviton y x86_64). Cuando se descarga un rango de bytes de S3 o se mapea desde `/tmp` con `mmap`, el motor transforma `&[u8]` en `&[T]` en **0 nanosegundos** mediante `bytemuck::try_cast_slice`.
2. **Diseño Multimodal en 5 Zonas Especializadas:** A diferencia de Parquet (que trata igual un texto, un grafo jerárquico, una serie temporal de sensores y un libro mayor contable), `.mbf` separa físicamente los datos en **5 Zonas de Almacenamiento** optimizadas para cada estructura matemática.
3. **Lectura de Cola en 1 RTT y Podado sin Abrir Archivos (`.mbf.idx`):** El directorio completo del archivo, los Zone Maps, los Checkpoints Financieros y las Sinopsis Estadísticas viven al final del archivo (`MbfFooter`) y se replican en un archivo espejo ultracompacto (`.mbf.idx`, ~0,5 % del peso de los datos).

---

## 2. Variantes de Archivos de la Familia `.mbf`

| Extensión | Rol en la Jerarquía S3 | Tamaño Típico | Mutabilidad | Mecanismo S3 |
| :--- | :--- | :--- | :--- | :--- |
| **`HOT_HEAD.mbf`** | Manifiesto activo del tenant + Buffer L0 OLTP/Contable (hasta 4.096 filas) + `UniqueClaim` Xor8 + Contadores (`Quota`, `Sequences`) + Punteros `.mbf.idx`. | 16 KB – 512 KB | Reemplazo atómico Copy-on-Write | `PutObject` con `If-Match: <ETag>` (1 solo PUT por TX/lote) y `GetObject` con `If-None-Match` (`304`). |
| **`*.mbf`** | Contenedor columnar/multimodal de Nivel 1 (Warm, 16–64 MB) y Nivel 2 (Cold Petabyte, 256 MB–1 GB). | 16 MB – 1 GB | **100 % Inmutable** | Escrito una vez con `If-None-Match: "*"`; leído vía `GetObject` con `Range: bytes=start-end`. |
| **`*.mbf.idx`** | Espejo compacto de cabeceras, `NumericBlockSynopsis`, `FinancialBlockCheckpoint`, `EmbeddedFxMatrix` y filtros `Xor8` de todos los archivos `.mbf` de un período. | 32 KB – 2 MB | Reemplazo al compactar | Permite podar el 99,98 % de Petabytes y responder KPIs/Balances sin abrir los archivos `.mbf` pesados. |

---

## 3. Mapa Físico a Nivel de Byte de un Archivo `.mbf`

```text
Offset 0x00000000 ┌──────────────────────────────────────────────────────────────────────────┐
                  │ 1. MBF HEADER (64 Bytes Fijos — Alineado a 64B)                          │
                  │    Magic: "METRIBIN" (8B) | Version: 1 (2B) | Flags (2B) | RowCount (4B) │
                  │    TenantHash (16B) | MinTx..MaxTx (16B) | MinTs..MaxTs (16B)            │
Offset 0x00000040 ├──────────────────────────────────────────────────────────────────────────┤
                  │ 2. ZONA 1: PAX + MVCC BI-TEMPORAL (Núcleo Entidades OLTP / CMMS)         │
                  │    ├── 2.1 EntityDirectory: MPHF Seed + Elias-Fano Monotonic Offsets     │
                  │    ├── 2.2 MVCC Visibility Slab: tx_assert[u64], tx_retract[u64], Bitmap │
                  │    ├── 2.3 FSST String Symbol Tables (Tabla de símbolos de 255 entradas) │
                  │    └── 2.4 Micro-Blocks Columnares (4.096 filas/bloque, alineados a 64B) │
Padding 64B       ├──────────────────────────────────────────────────────────────────────────┤
                  │ 3. ZONA 2: GRAFO NATIVO CSR + JERARQUÍA ISA-95 + COST CENTERS            │
                  │    ├── 3.1 Forward CSR: fwd_offsets[u32], fwd_targets[u32], attr_id[u16] │
                  │    ├── 3.2 Reverse CSR (VAET O(1)): rev_offsets[u32], rev_sources[u32]   │
                  │    └── 3.3 Euler Tour Tree Slab: tin[u32], tout[u32], depth[u16], cc[u32]│
Padding 64B       ├──────────────────────────────────────────────────────────────────────────┤
                  │ 4. ZONA 3: IOT TIME-SERIES SLAB (Telemetría de Sensores y Horómetros)    │
                  │    ├── 4.1 Delta-of-Delta Bitpacked Timestamps                           │
                  │    ├── 4.2 Chimp128 XOR Compressed f64 Streams + LZ4 Frames              │
                  │    └── 4.3 Multi-Resolution Pyramid Rollups (1m, 1h, 1d pre-agregados)   │
Padding 64B       ├──────────────────────────────────────────────────────────────────────────┤
                  │ 5. ZONA 4: MOTOR FINANCIERO, CONTABLE Y LIBRO MAYOR EXACTO (ERP/BANCARIO)│
                  │    ├── 5.1 Vectores FixedDecimal128 (i128 escalado 10^-6, Half-to-Even)  │
                  │    ├── 5.2 Vectores FenwickPrefixSum128 (Flujos P&L en rango en O(1))    │
                  │    ├── 5.3 FinancialBlockCheckpoints (Saldos As-Of / last_value O(log B))│
                  │    ├── 5.4 ActiveBatchSlab (Capas FIFO / LIFO / PMP-WAC / FEFO)          │
                  │    ├── 5.5 EmbeddedFxMatrix (Tasas históricas ISO 4217 en 1 ciclo SIMD)  │
                  │    └── 5.6 LedgerAuditHeaders (Cadena Criptográfica Merkle SHA-256)      │
Padding 64B       ├──────────────────────────────────────────────────────────────────────────┤
                  │ 6. ZONA 5: HIPER-ACELERADOR BI (Sinopsis Estadísticas + Índices Roaring) │
                  │    ├── 6.1 NumericBlockSynopsis Array (Welford: n, sum, mean, M2, min/max│
                  │    ├── 6.2 BivariateCoMoments Array (sum_xy para Pearson y OLS Lineal)   │
                  │    ├── 6.3 CompactKllSketches (256B/bloque para Mediana y P90/P95/P99)   │
                  │    └── 6.4 Xor8 Filters + Trigram Inverted RoaringBitmaps (FTS)          │
Padding 64B       ├──────────────────────────────────────────────────────────────────────────┤
                  │ 7. MBF FOOTER & DIRECTORY INDEX (Últimos 16–64 KB — Tail Range Read)     │
                  │    ├── 7.1 ZoneOffsetsTable & ColumnChunkDirectory                       │
                  │    ├── 7.2 File-Level Global Synopses & Fiscal Lock Epoch Watermark      │
                  │    └── 7.3 MbfFooterTrailer (64B Fijos: CRC32C, HMAC-SHA256, "MBFFOOTR") │
EOF               └──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Definición Exacta de Estructuras Binarias (`#[repr(C, align(64))]`)

### 4.1. Cabecera Principal (`MbfHeader` — 64 Bytes) y Cola (`MbfFooterTrailer` — 64 Bytes)

```rust
use bytemuck::{Pod, Zeroable};

pub const MBF_MAGIC: [u8; 8] = *b"METRIBIN";
pub const MBF_FOOTER_MAGIC: [u8; 8] = *b"MBFFOOTR";

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct MbfHeader {
    pub magic: [u8; 8],              // 0..8:   "METRIBIN"
    pub version: u16,                // 8..10:  Versión del formato (1)
    pub flags: u16,                  // 10..12: Bit 0: HotHead, Bit 1: Encrypted, Bits 2..7: ZoneMask
    pub row_count: u32,              // 12..16: Total de filas/entidades en este archivo
    pub tenant_hash: [u8; 16],       // 16..32: Hash truncado BLAKE3/UUID de 128 bits del tenant_id
    pub min_tx_id: u64,              // 32..40: Menor transacción ULID (epoch ms)
    pub max_tx_id: u64,              // 40..48: Mayor transacción ULID (epoch ms)
    pub min_timestamp_ms: i64,       // 48..56: Menor timestamp de negocio (para pruning temporal)
    pub max_timestamp_ms: i64,       // 56..64: Mayor timestamp de negocio
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct MbfFooterTrailer {
    pub directory_offset: u64,       // 0..8:   Offset absoluto donde inicia el Directory Index
    pub directory_len: u32,          // 8..12:  Longitud en bytes del Directory Index
    pub header_crc32c: u32,          // 12..16: CRC32C de MbfHeader
    pub directory_crc32c: u32,       // 16..20: CRC32C del Directory Index completo
    pub schema_fingerprint_hi: u32,  // 20..24: Parte alta del hash del esquema Códice
    pub fiscal_lock_epoch_ms: i64,   // 24..32: Marca de agua de cierre contable del tenant
    pub hmac_tag_truncated: [u8; 24],// 32..56: Tag de integridad criptográfica (192 bits)
    pub footer_magic: [u8; 8],       // 56..64: "MBFFOOTR"
}
```

### 4.2. Zona 1: PAX + MVCC Bi-Temporal + Compresión FSST
- **Tamaño de Micro-Bloque:** `4.096` filas (`ROW_GROUP_SIZE = 4096`). Este número es potencia de 2 ($2^{12}$), lo que hace que la máscara de visibilidad de un bloque completo ocupe exactamente **512 bytes (8 líneas de caché de 64 bytes)**.
- **Directorio de Entidades (`EAVT` Point Lookup en $O(1)$):**
  - Los `entity_id` (ULID/UUID de 128 bits) se ordenan lexicográficamente y sus prefijos se comprimen con **Elias-Fano** + una tabla de salto **MPHF (Minimal Perfect Hash Function)**. Buscar una entidad por ID requiere 1 acceso a memoria sin búsqueda en árboles B.
- **Vectores MVCC (`tx_assert: [u64; N]`, `tx_retract: [u64; N]`):**
  - Si `tx_retract[row] == u64::MAX`, la versión de la fila está actualmente activa (y su bit en `active_bitmap: RoaringBitmap` es `1`).
  - Para consultas de auditoría *Time-Travel* (`AS OF tx_target`), el motor evalúa con SIMD: `tx_assert[i] <= tx_target && tx_retract[i] > tx_target`.

### 4.3. Zona 2: Grafo CSR Bidireccional y Jerarquías ISA-95
Cada relación `type: "reference"` del Códice (`asset_id`, `location_id`, `part_id`, `work_order_id`, `parent_location_id`, `parent_asset_id`) se materializa en arreglos CSR:
- **Forward CSR:** `fwd_offsets: [u32; N + 1]`, `fwd_targets: [u32; E]`, `fwd_attr_ids: [u16; E]`.
- **Reverse CSR (Reemplazo del GSI-VAET):** `rev_offsets: [u32; N + 1]`, `rev_sources: [u32; E]`, `rev_attr_ids: [u16; E]`. Obtener todas las órdenes de trabajo o movimientos de inventario que apuntan a un activo `A` es simplemente leer el slice `&rev_sources[rev_offsets[A] as usize .. rev_offsets[A + 1] as usize]`.
- **Codificación de Recorrido de Euler para Árboles ISA-95 (`EulerTreeNode` — 16 Bytes):**
  ```rust
  #[repr(C, align(16))]
  #[derive(Clone, Copy, Debug, Pod, Zeroable)]
  pub struct EulerTreeNode {
      pub tin: u32,                     // Tiempo de entrada en el DFS del árbol
      pub tout: u32,                    // Tiempo de salida en el DFS (Subárbol = [tin..=tout])
      pub effective_cost_center_id: u32,// ID en diccionario del cost_center (propio o heredado)
      pub depth: u16,                   // Nivel de profundidad en la jerarquía ISA-95
      pub flags: u16,                   // Bit 0: has_children (para TreeMeta en UI sin sub-queries)
  }
  ```

### 4.4. Zona 3: Telemetría IoT (`Chimp128` + Pirámides Multi-Resolución)
Para entidades como `meter_reading`:
- Los datos se agrupan por `(asset_row_id: u32, metric_attr_id: u16)`.
- **Timestamps (`i64`):** Se almacena $t_0$ en la cabecera de la serie, $\Delta_1 = t_1 - t_0$, y a partir de ahí la segunda derivada $\Delta^2_i = (t_i - t_{i-1}) - (t_{i-1} - t_{i-2})$. En sensores que reportan cada 60 segundos, $\Delta^2_i = 0$, ocupando **1 solo bit por lectura**.
- **Valores (`f64`):** Algoritmo **Chimp128** (evolución de Gorilla XOR de Facebook) que explota la repetición de bits significativos en sensores industriales y médicos.
- **Pirámide Embebida:** Al final de la Zona 3 se adjuntan vectores pre-agregados (`count`, `sum`, `min`, `max`, `first`, `last`) en resoluciones de **1 hora** y **1 día**, acelerando gráficos de tendencia anual en 100×.

### 4.5. Zona 4: Motor Financiero, Contable y Libro Mayor Exacto
Diseñada específicamente para información financiera, contable, costos de mantenimiento e inventario auditable:

1. **Vector Monetario y Decimal Exacto (`FixedDecimal128`):**
   - Arreglo contiguo `&[i128]` alineado a 64 bytes (4 valores `i128` por línea de caché).
   - Escala canónica: $1.000.000$ ($10^6$ micro-unidades = $1,00$ unidad; $10.000$ micro-unidades = $0,01$ céntimos).
2. **Vector de Sumas Prefijas (`FenwickPrefixSum128`):**
   - Arreglo paralelo `&[i128]` donde la posición $k$ contiene $\sum_{j=0}^{k} v_j$.
   - Permite calcular cualquier suma financiera en un intervalo de tiempo $[t_1, t_2]$ en **$O(1)$ (`prefix[i2] - prefix[i1 - 1]`)**.
3. **Checkpoints de Cierre Contable (`FinancialBlockCheckpoint` — 64 Bytes):**
   - Almacena el último saldo conocido (`closing_balance_micro: i128` y `closing_value_micro: i128`) por cada combinación `(part_id, location_id, effective_cost_center_id, currency_iso_code)` al final del bloque.
   - Resuelve métricas con `"default_rollup": "last_value"` (como `running_balance` y `running_financial_value` en [`inventory_ledger.json`](../../config/models/inventory_ledger.json)) en **$O(\log B)$**.
4. **Slab de Capas de Lotes Activos (`ActiveBatchLayer` — 32 Bytes):**
   ```rust
   #[repr(C, align(32))]
   #[derive(Clone, Copy, Debug, Pod, Zeroable)]
   pub struct ActiveBatchLayer {
       pub batch_row_id: u32,           // 0..4:   Fila del inventory_batch
       pub part_row_id: u32,            // 4..8:   Fila del repuesto (part)
       pub location_row_id: u32,        // 8..12:  Almacén (location)
       pub currency_iso: u16,           // 12..14: Divisa ISO 4217
       pub flags: u16,                  // 14..16: Estado del lote
       pub received_or_expiry_ms: i64,  // 16..24: Timestamp para ordenamiento FIFO/LIFO/FEFO
       pub remaining_qty_micro: i64,    // 24..32: Cantidad disponible (escala 10^-4)
   }
   ```
5. **Cabecera de Integridad Contable (`LedgerAuditHeader` — 64 Bytes):**
   - Contiene `prev_block_sha256: [u8; 32]` y `block_merkle_root: [u8; 32]`, más la verificación de partida doble ($\Sigma \text{Débitos} - \Sigma \text{Créditos} = 0$).

### 4.6. Zona 5: Hiper-Acelerador Estadístico de BI (`NumericBlockSynopsis` — 64 Bytes)
Cada columna numérica con `aggregatable: true` posee un arreglo de `NumericBlockSynopsis` (1 entrada de 64 bytes por cada bloque de 4.096 filas):

```rust
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct NumericBlockSynopsis {
    pub valid_count: u64,            // 0..8:   N de valores no nulos y activos
    pub null_count: u64,             // 8..16:  N de valores nulos
    pub sum: f64,                    // 16..24: Suma exacta del bloque
    pub mean: f64,                   // 24..32: Media de Welford (μ)
    pub m2: f64,                     // 32..40: Momento central de segundo orden Σ(x - μ)²
    pub min: f64,                    // 40..48: Mínimo (Zone Map inferior)
    pub max: f64,                    // 48..56: Máximo (Zone Map superior)
    pub kll_sketch_offset: u64,      // 56..64: Offset al sketch KLL de 256B para Percentiles
}
```
Además, para pares de atributos frecuentemente correlacionados en el Códice (ej. `duration_minutes` vs. `hourly_rate_cents`, o `reading_value` vs. `downtime`), la sección `BivariateCoMoments` guarda $\sum (x_i - \mu_x)(y_i - \mu_y)$, permitiendo calcular **Correlación de Pearson (`CORRELATION`)** y **Regresión Lineal (`LINEAR_REGRESSION`)** en $O(1)$ por bloque.

### 4.7. Expansión Sin Límite de Presupuesto: `MbfHyperSynopsis512` (512 Bytes — 8 Líneas de Caché CPU)
Para escenarios empresariales donde se prioriza desbloquear el máximo absoluto de capacidades matemáticas, actuariales, de confiabilidad y espectrales en $O(1)$ sin importar el presupuesto de metadatos ($\sim 0,05\%$ de overhead en S3), cada bloque soporta el descriptor extendido **`MbfHyperSynopsis512`** (`512 bytes`, `#[repr(C, align(64))]`), documentado al detalle en [`09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md`](09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md):
- **Línea 0 (0x000..0x03F):** Momentos centrales exactos de Pébay de 1er a 4to orden (`valid_count`, `null_count`, `sum_exact_i128`, `pebay_mean_m1`, `pebay_m2`, `pebay_m3` para **Skewness $\gamma_1$**, y `pebay_m4` para **Kurtosis $\gamma_2$** y **Test de Normalidad de Jarque-Bera**).
- **Línea 1 (0x040..0x07F):** Extremos, apertura/cierre cronológico (`first_value_bits`, `last_value_bits` para `default_rollup: "last_value"`) y estadísticos suficientes logarítmicos (`sum_ln_x`, `sum_ln_x_sq`) para **Media/Varianza Geométrica** y distribución **Log-Normal**.
- **Línea 2 (0x080..0x0BF):** Ingeniería de Confiabilidad y Supervivencia Industrial (`sum_reciprocal_x` para **Media Armónica**, `weibull_beta_mle`, `weibull_eta_mle`, `censored_count`, `censored_exposure_sum` para **Weibull 2P/3P, Kaplan-Meier, MTBF, MTTR y Hazard Rate $h(t)$**) + Estadística Robusta (**Mediana $P_{50}$, MAD y Rango Intercuartílico IQR**).
- **Línea 3 (0x0C0..0x0FF):** Econometría de Series Temporales (`sum_lag1_cross_prod`, `sum_lag2_cross_prod` para **Autocorrelación ACF**, `sum_diff1_sq` para **Durbin-Watson**, pendiente OLS temporal $\beta_1$, estados **EMA rápida/lenta (MACD)** y acumuladores **CUSUM**).
- **Línea 4 (0x100..0x13F):** Diagnóstico Espectral IoT (`signal_rms`, `crest_factor`, `spectral_entropy`, `dominant_freq_hz` y `psd_bands_energy: [f32; 8]` precalculados mediante FFT/Goertzel).
- **Líneas 5 y 6 (0x140..0x1BF):** 16 Cuantiles Ancla de Cola Extrema ($P_{0.1}$ a $P_{99.99}$) + Media Recortada al 5% (`TrimmedMean5%`).
- **Línea 7 (0x1C0..0x1FF):** Sketch **HyperLogLog++** condensado (48 bytes) para `COUNT(DISTINCT)` exacto/casi-exacto en $O(1)$, mini-Bloom filter de 64 bits y checksum `CRC64-NVMe`.

### 4.8. Zona 5B: Motor Híbrido de Búsqueda Multimodal (`MbfHybridSearchIndex`)
Reemplaza el antiguo índice `GSI-FTS` de DynamoDB ([`src/eav/fts/`](../../src/eav/fts/)) por una zona auto-contenida de 6 capas para búsquedas de texto difusas, fonéticas, semánticas y geoespaciales en $< 1,5\text{ ms}$:
1. **Diccionario Léxico FST (*Finite State Transducer*):** Permite intersección en tiempo real con **Autómatas Deterministas de Damerau-Levenshtein ($k \le 2$)** en $< 40\ \mu\text{s}$.
2. **Tabla Hash Estática SymSpell (*Symmetric Delete* $d \le 2$):** Resolución de errores tipográficos en **$O(1)$ ($< 5\ \mu\text{s}$)**.
3. **Índice N-Gram Posicional (2-gram + 3-gram) sobre Roaring Bitmaps SIMD + Pesos BM25F:** Incluye vector de longitudes de campo normalizadas (`u8`) para ranking multifactorial `BM25F` ponderado según el Códice (`code`, `title`, `description`).
4. **Diccionario Fonético Industrial Bilingüe (`SpanishIndustrialMetaphone` + `DoubleMetaphone`):** Empareja errores auditivos severos en español e inglés técnico (`"balbula"` $\leftrightarrow$ `"válvula"`, `"chiler"` $\leftrightarrow$ `"chiller"`, `"suiche"` $\leftrightarrow$ `"switch"`).
5. **Índice Vectorial Nativo (`HNSW` + Cuantización Binaria/Asimétrica `RaBitQ`):** Comprime embeddings de 1.024 dimensiones (`document_chunk.semantic_embedding` con `amazon.titan-embed-text-v2`) en bloques de 128 bytes evaluables vía `VPOPCNTQ` / `CNT` SIMD + fusión **Reciprocal Rank Fusion (RRF)**.
6. **Índice Espaciotemporal Jerárquico (`Uber H3` `u64` + Morton Z-Order + R-Tree MBR):** Indexa `latitude`/`longitude` en `work_order`, `preventive_maintenance`, `labor_log` y `location`.

### 4.9. Zona 6B: Matriz de Co-Momentos Multivariados (`MbfCrossCovarianceMatrix`)
Almacena la matriz triangular superior de co-momentos centrales $\sum (x_i - \mu_i)(x_j - \mu_j)$ entre los $M$ atributos numéricos de la entidad, permitiendo calcular **Matrices de Correlación de Pearson $M \times M$, Regresión Lineal Múltiple (OLS vía Cholesky), PCA (Componentes Principales) y Distancia de Mahalanobis** en $O(1)$ sin descomprimir las columnas.

