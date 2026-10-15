# Folder Structure, Files, and Rust Module Map

> **English summary:** This document maps the exact directory structure, new Rust modules (`src/mbf/`, `src/aegis/mbf/`), modified infrastructure adapters (`src/infrastructure/`), and public trait/struct signatures required to implement the **Metri Vortex (`.mbf`)** engine in `metri-engine`.

---

## 1. Árbol Completo de Directorios y Archivos Propuestos

A continuación se detalla la estructura exacta de carpetas y archivos dentro del repositorio `metri-engine`, indicando qué archivos son nuevos (`[NEW]`), cuáles se modifican (`[MODIFY]`) y cuáles se eliminan en la Fase 6 (`[DELETE FASE 6]`):

```text
metri-engine/
├── Cargo.toml                                      # [MODIFY] Crates bytemuck, roaring, crc32fast + [profile.release] LTO fat
├── Makefile                                        # [MODIFY] Flags Graviton ARM64 (+neon,+crc,+crypto,+lse) en build-lambda
├── build.rs                                        # [MODIFY] Pre-compilación de 60 JSON Códice + error_catalog.toml -> .rodata
├── template.yaml                                   # [MODIFY] MemorySize: 1769, Timeout: 30, retiro de tablas DDB/Athena/LWA en Fase 6
├── config/
│   ├── errors/
│   │   └── error_catalog.toml                      # [MODIFY] Nuevos códigos MBF_CORRUPT_001, MBF_CAS_CONFLICT_001, MBF_LEDGER_*
│   └── models/                                     # [SSOT INTACTO] Los 60 modelos JSON gobiernan automáticamente el layout .mbf
├── docs/
│   ├── README.md                                   # [MODIFY] Enlace a docs/mbf/README.md y ADR-009
│   ├── architecture/adr/
│   │   └── ADR-009-motor-binario-unificado-mbf.md  # [NEW] Decisión arquitectónica formal del salto a .mbf
│   └── mbf/                                        # [NEW] Carpeta maestra de documentación del motor .mbf (8 documentos)
└── src/
    ├── lib.rs                                      # [MODIFY] Declara `pub mod mbf;`
    ├── main.rs                                     # [MODIFY] Inicialización desde .rodata + despacho directo lambda_http
    ├── domain/
    │   ├── errors.rs                               # [MODIFY] Enum ErrorCode sincronizado con error_catalog.toml
    │   └── protocols.rs                            # [MODIFY] Puertos ISP: IByteRangeReader, IConditionalObjectWriter, IMbfBlockCache
    ├── codice/
    │   ├── registry.rs                             # [MODIFY] Generación determinista de MbfEntityLayout por cada modelo JSON
    │   └── mbf_layout.rs                           # [NEW] Clasificador DRY de atributos en Zonas 1, 2, 3, 4 y 5
    ├── mbf/                                        # [NEW] NÚCLEO DEL FORMATO BINARIO PROPIO Y TRANSACCIONES S3
    │   ├── mod.rs                                  # [NEW] Raíz del módulo mbf
    │   ├── format/
    │   │   ├── mod.rs                              # [NEW] Exporta estructuras de formato
    │   │   ├── header.rs                           # [NEW] MbfHeader (64B) y constantes mágicas
    │   │   ├── footer.rs                           # [NEW] MbfFooterTrailer (64B), DirectoryIndex y validación CRC32C/HMAC
    │   │   ├── hot_head.rs                         # [NEW] Contenedor HotHeadManifest y 8 Celdas de Dominio (HOT_HEAD_c0..c7.mbf)
    │   │   ├── catalog_idx.rs                      # [NEW] Serializador/Lector del espejo ultracompacto .mbf.idx
    │   │   ├── schema_evolution.rs                 # [NEW] LazyDefaultProjection (ConstantVector) y Widening Cast SIMD zero-downtime
    │   │   ├── crypto_seal.rs                      # [NEW] MbfTenantCryptoSeal (Keyed BLAKE3 por Tenant + AES-256-GCM-SIV PII)
    │   │   └── codec.rs                            # [NEW] Trait ColumnCodec (OCP) y adaptadores Zero-Copy bytemuck
    │   ├── zones/
    │   │   ├── mod.rs                              # [NEW] Exporta las zonas especializadas (Zonas 1 a 6B)
    │   │   ├── pax_mvcc.rs                         # [NEW] Zona 1: MPHF + Elias-Fano, vectores MVCC tx_assert/retract, FSST
    │   │   ├── csr_graph.rs                        # [NEW] Zona 2: Grafo CSR directo/inverso (VAET) + Árbol Euleriano ISA-95
    │   │   ├── iot_timeseries.rs                   # [NEW] Zona 3: Delta-of-Delta timestamps + Chimp128 + UN/CEFACT SI normalizer
    │   │   ├── financial_ledger.rs                 # [NEW] Zona 4: FixedDecimal128, FenwickPrefixSum, Checkpoints, FIFO/PMP, FX, Merkle
    │   │   ├── bi_synopses.rs                      # [NEW] Zona 5: MbfHyperSynopsis512 (Pébay M1..M4, Weibull, FFT/PSD, KLL, HLL++)
    │   │   ├── hybrid_search.rs                    # [NEW] Zona 5B: FST Levenshtein DFA + SymSpell + BM25F + Metaphone + RaBitQ HNSW + H3
    │   │   └── cross_covariance.rs                 # [NEW] Zona 6B: MbfCrossCovarianceMatrix (Pearson MxM, OLS Cholesky, PCA, Mahalanobis)
    │   └── tx/
    │       ├── mod.rs                              # [NEW] Coordinación ACID Lock-Free de 3 capas sobre S3
    │       ├── group_commit.rs                     # [NEW] AdaptiveGroupCommitCoalescer (0 ms en baja carga, batching HTTP/2 en ráfagas)
    │       ├── manifest_cas.rs                     # [NEW] MbfWriteCoordinator (8-Cell Domain Sharding, If-Match CAS, FiscalLock)
    │       ├── auto_rebase.rs                      # [NEW] CommutativeDeltaAutoRebase (< 150 µs en HTTP 412 con TxConflictSignature Bloom)
    │       ├── piggyback_compactor.rs              # [NEW] Compactación L0 -> L1 y reordenamiento multidimensional por Curva de Hilbert
    │       └── state_overlays.rs                   # [NEW] QuotaCounter, SequenceRegistry e ISessionStore sobre HOT_HEAD.mbf
    ├── aegis/
    │   ├── mod.rs                                  # [MODIFY] Registra `pub mod mbf;`
    │   ├── formula/
    │   │   └── functions_registry.rs               # [MODIFY] Registra funciones financieras, de confiabilidad Weibull y espectrales
    │   ├── mbf/                                    # [NEW] EJECUTOR VECTORIZADO SIMD DE BI, FINANZAS, CONFIABILIDAD Y BÚSQUEDA HÍBRIDA
    │   │   ├── mod.rs                              # [NEW] Punto de entrada de Aegis sobre .mbf + MbfExecutionFlightRecorder
    │   │   ├── planner.rs                          # [NEW] Podado cuádruple (.mbf.idx) y selección de plan físico
    │   │   ├── executor.rs                         # [NEW] Ejecución SIMD de filtros, proyecciones y joins multi-serie
    │   │   ├── stats_simd.rs                       # [NEW] 35+ funciones matemáticas (Pébay M1..M4, Jarque-Bera, Weibull, ACF, PCA, OLS)
    │   │   ├── fuzzy_vector_search.rs              # [NEW] Ejecutor de búsqueda multimodal (FST + SymSpell + BM25F + RaBitQ HNSW + RRF)
    │   │   ├── financial_analytics.rs              # [NEW] Balances As-Of O(log B), flujos O(1), TCO/WIP y depreciación NIC 16 + IoT
    │   │   └── graph_traversal.rs                  # [NEW] Operadores de subárbol ISA-95 y alcance k-hop sobre CSR
    │   ├── oltp/                                   # [DELETE FASE 6] Retirado tras superar el Oráculo en Modo Sombra
    │   └── sql/                                    # [DELETE FASE 6] Retirado (fin de sea-query y Presto/Athena SQL)
    ├── janus_router/
    │   ├── router.rs                               # [MODIFY] Enruta hacia UnifiedMbfChannel
    │   ├── unified_mbf_channel.rs                  # [NEW] Canal único HTAP para Transact, CompositeTransact y BulkIngest
    │   ├── oltp_channel.rs                         # [DELETE FASE 6]
    │   └── olap_channel.rs                         # [DELETE FASE 6]
    ├── infrastructure/
    │   ├── s3_object_store.rs                      # [NEW] AwsS3ObjectStore (HTTP/2 Range Coalescing, Single-Flight, CAS, 304)
    │   ├── lambda_tmp_cache.rs                     # [NEW] LambdaTieredCache (RAM + 450 MB en /tmp NVMe con TinyLFU)
    │   ├── local_fs_object_store.rs                # [NEW] LocalFsObjectStore para desarrollo local y tests deterministas
    │   ├── athena.rs                               # [DELETE FASE 6]
    │   ├── dynamodb.rs                             # [DELETE FASE 6]
    │   ├── kinesis.rs                              # [DELETE FASE 6]
    │   └── local_s3_query_engine.rs                # [DELETE FASE 6] (1.555 líneas eliminadas)
    └── bin/
        └── mbf-migrator.rs                         # [NEW] Herramienta CLI para migrar datos de DynamoDB/Parquet hacia .mbf
```

---

## 2. Responsabilidades y Contratos por Módulo Clave

### 2.1. `src/codice/mbf_layout.rs`
Deriva el esquema físico de cada entidad sin duplicar configuración:
- Entrada: `&EntityModel` (cargado desde `config/models/<entity>.json`).
- Salida: `MbfEntityLayout` inmutable que especifica el índice de cada atributo en la Zona 1, las aristas de referencia en la Zona 2, las series de tiempo en la Zona 3, las columnas monetarias/decimales `FixedDecimal128` en la Zona 4 y los atributos agregables en la Zona 5.

### 2.2. `src/mbf/zones/financial_ledger.rs`
Contiene toda la lógica contable de precisión:
- `FixedDecimal128`: Tipo envoltorio `#[repr(transparent)]` sobre `i128` con escala $10^6$, operaciones `checked_add`, `checked_sub`, `mul_div_bankers` y `apportion_hamilton`.
- `ActiveBatchSlab`: Ejecuta la imputación de costos de inventario bajo `ValuationMethod::{Fifo, Lifo, Wac, Fefo}` y produce simultáneamente el delta de `inventory_batch`, la fila de `inventory_movement` y el asiento de `inventory_ledger` con su `FinancialBlockCheckpoint`.
- `LedgerAuditHeader`: Verifica que $\sum \text{Débitos} == \sum \text{Créditos}$ y calcula el encadenamiento criptográfico `SHA-256(prev_hash || block_merkle_root)`.

### 2.3. `src/aegis/mbf/planner.rs` y `executor.rs`
Sustituyen por completo al compilador SQL y al antiguo ejecutor OLTP:
- Reciben `&fb::AnalyticsRequest` (el AST IR de FlatBuffers ya validado por `janus` y enriquecido con los filtros Zero-Trust de Cedar).
- Seleccionan entre tres caminos de ejecución óptima:
  1. **`ExecutionPath::SynopsisOnly` ($O(B)$, $< 1\text{ ms}$):** Resuelve KPIs, promedios, desviaciones estándar, correlaciones, regresiones lineales, percentiles KLL y saldos financieros `last_value` / flujos `sum` leyendo únicamente `.mbf.idx` y `HOT_HEAD.mbf`.
  2. **`ExecutionPath::GraphIndexedScan` ($O(K)$, $< 5\text{ ms}$):** Resuelve primero el subárbol ISA-95 (`[tin, tout]`) o las referencias inversas CSR (`VAET`), obteniendo un `RoaringBitmap` de filas candidatas, y proyecta solo los bloques que intersectan con dicho bitmap.
  3. **`ExecutionPath::VectorizedColumnScan` ($< 20\text{ ms}$):** Descarga por HTTP/2 Range las columnas estrictamente necesarias de los bloques que superaron los Zone Maps y Xor8 Filters, aplicando predicados SIMD.
