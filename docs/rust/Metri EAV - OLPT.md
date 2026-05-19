# Metri EAV — Motor OLTP Inmutable en Rust

**Nombre:** `Metri EAV`
**Tipo:** Base de Datos Relacional Inmutable EAV (Entity-Attribute-Value)
**Lenguaje:** Rust (compiled to ARM64 — AWS Lambda `provided.al2023`)
**Storage:** AWS DynamoDB (Single Table Design)
**Reemplaza:** Datahike (Clojure/Datalog sobre DynamoDB)
**Especialización:** BI Analítico — tiempos mínimos y estables de respuesta

---

## MÓDULO 0: Motivación — Por qué reemplazar Datahike

### Problemas de Datahike en producción

| Problema | Impacto | Root Cause |
|:--|:--|:--|
| Cold start JVM ~4s | Latencia inaceptable en Lambda | JVM + Clojure bootstrap + Datahike init |
| `Node not found in storage` | Corrupción bajo concurrencia alta | Datahike serializa el índice completo como un solo blob en DynamoDB |
| Sin LIMIT nativo | Todo pasa por memoria | Datahike carga TODOS los datoms, filtra in-memory |
| Sort in-memory | O(n log n) sobre dataset completo | Datahike no tiene ORDER BY — se ordena post-query |
| Aggregation in-memory | CPU-bound en Lambda | COUNT/SUM/AVG se computan en Clojure sobre vectores |
| FTS via scan lineal | O(n) por cada búsqueda full-text | Sin índice invertido — Damerau-Levenshtein sobre todos los datoms |
| Latencia variable P99 ~800ms | Dashboards BI inconsistentes | GC pauses + índice monolítico + deserialización Nippy |

### Objetivos de Metri EAV

| Métrica | Datahike actual | Metri EAV objetivo |
|:--|:--|:--|
| Cold start | ~4,000ms | < 50ms (Rust native binary) |
| Point lookup (GET by ULID) | ~15ms | < 2ms |
| Filtered query (100 rows) | ~120ms | < 10ms |
| Aggregation KPI | ~200ms | < 5ms (pre-computed) |
| FTS fuzzy search | ~350ms | < 15ms (binary trie) |
| P99 variance | ±400ms | ±2ms (no GC, no JIT) |
| Write (single entity) | ~25ms | < 5ms |

---

## MÓDULO I: Modelo de Datos — EAV Inmutable con Índices Binarios

### I.1 — Estructura Fundamental: El Datom

Metri EAV almacena toda la información como **datoms** — tuplas inmutables de 5 elementos:

```
[entity, attribute, value, tx, op]
```

| Campo | Tipo Rust | Descripción |
|:--|:--|:--|
| `entity` | `u64` | ID interno (ULID encoded como u128, particionado a u64) |
| `attribute` | `u16` | ID numérico del atributo (max 65,535 attrs por tenant) |
| `value` | `DatomValue` (enum) | Valor tipado: Str, U64, F64, Bool, Bytes, Ref(u64) |
| `tx` | `u64` | Transaction ID (monotónico, epoch-based) |
| `op` | `bool` | `true` = assert, `false` = retract |

> [!IMPORTANT]
> **Inmutabilidad:** Un datom NUNCA se modifica ni elimina. Un UPDATE es un par `[retract antiguo, assert nuevo]` en la misma TX. Esto habilita time-travel y auditoría perfecta sin costo adicional.

### I.2 — Encoding Binario: Zero-Copy DatomValue

```rust
/// Encoding binario compacto — tag byte + payload.
/// Diseñado para comparación lexicográfica directa (memcmp-safe).
#[repr(u8)]
enum DatomValue {
    Null       = 0x00,
    Bool(bool) = 0x01,  // 1 byte: 0x00 | 0x01
    U64(u64)   = 0x02,  // 8 bytes big-endian (epoch, counters)
    F64(f64)   = 0x03,  // 8 bytes IEEE 754 con bit-flip para orden
    Str(Box<[u8]>) = 0x04,  // length-prefixed UTF-8
    Ref(u64)   = 0x05,  // FK — entity ID de la referencia
    Bytes(Box<[u8]>) = 0x06, // blob opaco (JSON, maps)
}
```

> [!TIP]
> **Ventaja sobre Datahike:** Los valores se almacenan en formato binario comparable. No hay deserialización Nippy, no hay overhead de Clojure persistent data structures. La comparación de valores es `memcmp` directo — O(1) para numéricos, O(k) para strings donde k = longitud del prefijo común.

### I.3 — Attribute Registry (Compilado en Bootstrap)

Cada atributo del modelo JSON se compila a un `AttributeDescriptor`:

```rust
struct AttributeDescriptor {
    id: u16,                    // ID numérico compacto
    ident: String,              // "asset/name", "work_order/status"
    value_type: ValueType,      // Str, U64, F64, Bool, Ref, Bytes
    cardinality: Cardinality,   // One | Many
    unique: Uniqueness,         // None | Identity | Value
    index: bool,                // B-tree index en AVET
    fts: bool,                  // Full-text search index
    is_dimension: bool,         // Para BI pre-aggregation
    is_measure: bool,           // Para BI pre-aggregation
    entity_ref: Option<String>, // FK target entity type
}
```

El registry se construye al arranque desde los 51 modelos JSON — idéntico al Bootstrapper del Códice pero compilado a un `Vec<AttributeDescriptor>` con lookup O(1) por `u16` ID.

---

## MÓDULO II: Arquitectura de Storage — Los 4 Índices Canónicos

### II.0 — Filosofía: Datomic's 4-Index Model sobre DynamoDB

Datomic/Datahike usan 4 índices ordenados sobre datoms `[E, A, V, T]` para cubrir TODOS los patrones de acceso posibles. Metri EAV replica este modelo pero **materializado directamente en DynamoDB** con sort keys binarios — eliminando la necesidad de un índice monolítico serializado (el cuello de botella de Datahike).

```
┌──────────────────────────────────────────────────────────────────┐
│  Los 4 índices canónicos — cada uno es una "vista" del mismo    │
│  conjunto de datoms, ordenada de forma diferente:               │
│                                                                  │
│  EAVT  →  "Dame todo sobre esta entidad"                        │
│  AEVT  →  "Dame todas las entidades con este atributo"          │
│  AVET  →  "Dame la entidad que tiene este valor en este attr"   │
│  VAET  →  "¿Quién apunta a esta entidad?" (reverse references) │
│                                                                  │
│  + FTS  →  "Búsqueda fuzzy por texto" (índice auxiliar)         │
└──────────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> **Cada write genera items en los 4 índices.** Un entity con 7 atributos genera 7 × 4 = 28 items en DynamoDB (más items FTS para atributos `fts: true` y items VAET solo para `type: reference`). El costo es proporcional al número de atributos, no al número de entidades — ideal para el modelo Schema-Driven de Metri donde los modelos tienen 5-15 atributos.

### II.1 — Tabla DynamoDB: Single Table Design

```
┌──────────────────────────────────────────────────────────────────┐
│  Table: metri-eav-prod                                           │
│  Billing: PAY_PER_REQUEST (On-Demand)                            │
│  Encryption: KMS (MetriKmsMasterKey)                             │
│  PITR: Enabled                                                   │
│  TTL: Disabled (datos inmutables — nunca se borran)              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Main Table — EAVT (Entity → Attribute → Value → Tx)            │
│    PK (String): T#<tenant_id>#E#<entity_id>                     │
│    SK (Binary): <attr_id(2B)><tx_id(8B)><op(1B)>                │
│    Attrs: v(B), tt(N), et(S)                                    │
│                                                                  │
│  GSI-AEVT — (Attribute → Entity → Value → Tx)                   │
│    PK (String): T#<tenant_id>#A#<attr_id>                       │
│    SK (Binary): <entity_id(8B)><tx_id(8B)><op(1B)>              │
│    Projected: v, tt, et                                          │
│                                                                  │
│  GSI-AVET — (Attribute → Value → Entity → Tx)                   │
│    PK (String): T#<tenant_id>#AV#<attr_id>                      │
│    SK (Binary): <value_prefix(32B)><entity_id(8B)><tx_id(8B)>   │
│    Projected: v, tt, et                                          │
│                                                                  │
│  GSI-VAET — (Value → Attribute → Entity → Tx)                   │
│    PK (String): T#<tenant_id>#R#<ref_entity_id>                 │
│    SK (Binary): <attr_id(2B)><entity_id(8B)><tx_id(8B)><op(1B)>│
│    Projected: tt, et                                             │
│                                                                  │
│  GSI-FTS — (Full-Text Search — Trigram)                          │
│    PK (String): T#<tenant_id>#FTS#<trigram>                     │
│    SK (Binary): <attr_id(2B)><entity_id(8B)>                    │
│    Projected: (keys only)                                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> **Tenant Isolation by Design:** El `tenant_id` es prefijo de TODOS los Partition Keys en TODOS los índices. Es IMPOSIBLE hacer un cross-tenant query accidental — DynamoDB no permite queries sin PK. Esto es superior al modelo Datahike donde el tenant filter era una cláusula Datalog que podía omitirse por bug.

### II.2 — EAVT: El Índice Primario (Main Table)

**Pregunta que responde:** _"Dame todo sobre la entidad X"_ — el `d/pull` de Datomic.

```
PK: "T#tnt_01J#E#01JXYZ..."                         // Tenant + Entity ULID
SK: <attr_id(2B)><tx_id(8B)><op(1B)>                 // 11 bytes fijos

Attributes:
  v:  Binary    // DatomValue encoded (tag byte + payload)
  tt: Number    // TX timestamp (epoch ms) — para time-travel display
  et: String    // Entity type ("asset", "work_order") — para type discrimination
```

**Operaciones soportadas:**

| Query | DynamoDB Operation | Complejidad |
|:--|:--|:--|
| Pull entity completo | `Query PK=T#tnt#E#ulid` → todos los datoms | O(A) donde A = num attrs |
| Pull atributo específico | `Query PK=..., SK begins_with attr_id` | O(log A) |
| Pull as-of TX T | `Query PK=..., SK between [attr, 0] and [attr, T]` + Last | O(log T) por attr |
| Entity history | `Query PK=..., ScanIndexForward=true` → todos los datoms cronológicos | O(A × T) |

```rust
/// Reconstituir una entidad completa desde EAVT.
/// Para cada atributo, toma el datom con max(tx) donde op=true.
fn pull_entity(tenant: &str, entity_id: u64) -> HashMap<u16, DatomValue> {
    // Query: PK = "T#{tenant}#E#{entity_id}", sin filtro de SK
    // Post-process: group by attr_id, keep max tx where op=true
    // Resultado: mapa attr_id → valor actual
}

/// Pull as-of: entidad en un punto del tiempo.
fn pull_as_of(tenant: &str, entity_id: u64, as_of_tx: u64) -> HashMap<u16, DatomValue> {
    // Query: PK = "T#{tenant}#E#{entity_id}"
    //        SK <= build_sk(0xFFFF, as_of_tx, true)
    // Post-process: group by attr_id, keep max tx <= as_of_tx where op=true
}
```

### II.3 — AEVT: Índice por Atributo-Entidad

**Pregunta que responde:** _"Dame todas las entidades que tienen este atributo"_ — el scan por tipo de entidad, listados, y queries sin filtro de valor.

```
GSI-AEVT:
  PK: "T#tnt_01J#A#0x0004"                           // Tenant + Attribute ID (ej: asset/status)
  SK: <entity_id(8B)><tx_id(8B)><op(1B)>              // 17 bytes
  Projected: v, tt, et
```

**Operaciones soportadas:**

| Query | Uso en Metri | Ejemplo |
|:--|:--|:--|
| Listar entidades por tipo | `WHERE entity/type = :asset` | Dashboard: lista de assets |
| Scan + filter in-memory | `WHERE entity/type = :work_order` + filter por valor | Tabla de work orders |
| Count por atributo | `SELECT COUNT(*) WHERE entity/type = :asset` | KPI: total assets |
| Paginación nativa | `ExclusiveStartKey` = último entity_id visto | Cursor-based pagination |

> [!TIP]
> **Ventaja clave de AEVT:** Cuando el query solo filtra por `entity/type` (ej: "lista todos los assets"), AEVT resuelve en **1 sola query DynamoDB** con paginación nativa — Datahike cargaba TODOS los datoms de TODAS las entidades y filtraba in-memory.

### II.4 — AVET: Índice por Valor (Value Lookup)

**Pregunta que responde:** _"¿Qué entidad tiene este valor en este atributo?"_ — unique lookups, filtros por valor exacto, range queries sobre valores.

```
GSI-AVET:
  PK: "T#tnt_01J#AV#0x0003"                          // Tenant + Attribute ID (ej: asset/tag)
  SK: <value_prefix(32B)><entity_id(8B)><tx_id(8B)>   // 48 bytes
  Projected: v, tt, et
```

**Encoding del value_prefix para orden lexicográfico:**

```rust
/// Genera el prefijo binario del valor para el SK de AVET.
/// Diseñado para que DynamoDB ordene los valores correctamente con memcmp.
fn value_to_sort_prefix(val: &DatomValue) -> [u8; 32] {
    let mut buf = [0u8; 32];
    match val {
        DatomValue::Null => { buf[0] = 0x00; }
        DatomValue::Bool(b) => { buf[0] = 0x01; buf[1] = if *b { 1 } else { 0 }; }
        DatomValue::U64(n) => {
            buf[0] = 0x02;
            buf[1..9].copy_from_slice(&n.to_be_bytes()); // big-endian = orden correcto
        }
        DatomValue::F64(f) => {
            buf[0] = 0x03;
            // IEEE 754 bit-flip: para que memcmp ordene floats correctamente
            let bits = f.to_bits();
            let ordered = if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) };
            buf[1..9].copy_from_slice(&ordered.to_be_bytes());
        }
        DatomValue::Str(s) => {
            buf[0] = 0x04;
            let len = s.len().min(31);
            buf[1..1+len].copy_from_slice(&s.as_bytes()[..len]); // primeros 31 bytes
        }
        DatomValue::Ref(id) => {
            buf[0] = 0x05;
            buf[1..9].copy_from_slice(&id.to_be_bytes());
        }
        _ => { buf[0] = 0xFF; } // Bytes — no indexable por valor
    }
    buf
}
```

**Operaciones soportadas:**

| Query | DynamoDB Operation | Ejemplo Metri |
|:--|:--|:--|
| Unique lookup (identity) | `Query PK=AV#tag, SK begins_with "A-X7K2M9P"` | Buscar asset por tag |
| Unique check (value) | `Query PK=AV#serial, SK begins_with "SN-123"` | Validar serial único |
| Range scan numérico | `Query PK=AV#total_cost, SK between [0x02+min] and [0x02+max]` | WOs con costo > 50K |
| Enum filter | `Query PK=AV#status, SK begins_with "OPEN"` | WOs con status OPEN |
| String prefix | `Query PK=AV#name, SK begins_with "Bomba"` | Assets que empiezan con "Bomba" |

> [!IMPORTANT]
> **AVET es SELECTIVO — solo se construye para atributos con `index: true`, `unique: identity|value`, `is_dimension: true`, o `fts: true`.** Esto minimiza el costo de escritura. De los ~400 atributos totales del sistema, solo ~80 generan items AVET.

### II.5 — VAET: Índice de Referencias Inversas (Reverse Refs)

**Pregunta que responde:** _"¿Quién apunta a esta entidad?"_ — el `d/pull` con reverse references de Datomic. Esencial para navegación de grafo y hierarchy traversal.

```
GSI-VAET:
  PK: "T#tnt_01J#R#01JXYZ..."                        // Tenant + Referenced Entity ULID
  SK: <attr_id(2B)><entity_id(8B)><tx_id(8B)><op(1B)> // 19 bytes
  Projected: tt, et
```

**Operaciones soportadas:**

| Query | Uso en Metri | Ejemplo |
|:--|:--|:--|
| Reverse lookup | _"¿Qué work_orders apuntan a este asset?"_ | Asset detail → WOs relacionadas |
| Hierarchy children | _"¿Qué locations tienen parent = X?"_ | Tree view de locations |
| has_children check | _"¿Existe algún child de esta location?"_ | Icono expandible en tree |
| Cascade check | _"¿Alguien referencia esta entidad?"_ | Pre-delete integrity check |
| Count dependents | _"¿Cuántas WOs tiene este asset?"_ | KPI: WOs por asset |

```rust
/// ¿Quién apunta a entity_id via el atributo attr_id?
/// Ejemplo: todos los work_orders donde work_order/asset_id = "01JXYZ..."
fn reverse_refs(tenant: &str, ref_entity_id: u64, attr_id: Option<u16>) -> Vec<u64> {
    // Query GSI-VAET:
    //   PK = "T#{tenant}#R#{ref_entity_id}"
    //   SK begins_with attr_id (si especificado) — o scan completo
    // Retorna: entity_ids que referencian a ref_entity_id
}

/// has_children: ¿existe al menos 1 child? (para iconos de tree view)
/// Usa Limit=1 para cortocircuitar — O(1) constante.
fn has_children(tenant: &str, parent_entity_id: u64, child_attr_id: u16) -> bool {
    // Query GSI-VAET:
    //   PK = "T#{tenant}#R#{parent_entity_id}"
    //   SK begins_with child_attr_id
    //   Limit = 1
    // Retorna: true si hay al menos 1 resultado
}
```

> [!TIP]
> **VAET solo se actualiza para atributos con `type: reference`.** De los ~400 atributos, solo ~50 son references. Cada write de un reference genera 1 item VAET adicional. Para `cardinality: many` (ej: `work_order/assignees`), se genera 1 item VAET por cada UUID en el array.

### II.6 — GSI-FTS: Índice de Búsqueda Full-Text (Auxiliar)

**No es un índice canónico Datomic** — es una extensión de Metri EAV para reemplazar el scan lineal de Datahike con un índice trigram invertido.

```
GSI-FTS:
  PK: "T#tnt_01J#FTS#bom"                            // Tenant + Trigram (3 chars lowercase)
  SK: <attr_id(2B)><entity_id(8B)>                    // 10 bytes (keys-only projection)
```

Detalle completo en **MÓDULO V**.

### II.7 — Mapa de Índices por Operación del Sistema

| Operación Metri | Índice primario | Índice secundario | Notas |
|:--|:--|:--|:--|
| `d/pull` (GET entity) | **EAVT** | — | Reconstituye entidad completa |
| `d/pull` reverse ref (`_children`) | **VAET** | EAVT (para pull de cada child) | Tree navigation |
| `d/q` WHERE `entity/type = X` | **AEVT** | — | Listado por tipo |
| `d/q` WHERE `attr = value` | **AVET** | — | Filtro por valor exacto |
| `d/q` WHERE `attr IN [v1, v2]` | **AVET** | — | Filtro por set de valores |
| `d/q` WHERE `attr > value` (range) | **AVET** | — | Range scan numérico |
| `d/q` WHERE `ref_attr → entity` | **VAET** | EAVT (join) | FK traversal |
| Unique constraint check | **AVET** | — | `unique: identity\|value` |
| FTS fuzzy search | **FTS** | EAVT (reconstituir entities) | Trigram intersection |
| Time-travel (as-of TX T) | **EAVT** | — | SK ≤ T, max per attr |
| Transaction log | **EAVT** | — | Query por entity, filter by tx range |
| KPI aggregation | **AEVT** o **AVET** | — | Streaming aggregate |
| has_children (tree) | **VAET** | — | Limit=1, O(1) |
| Cascade integrity check | **VAET** | — | Pre-delete scan |

### II.8 — Binary Sort Keys: La Ventaja Competitiva

Todos los Sort Keys son **binarios puros** — zero-copy, memcmp-comparable:

```rust
/// EAVT SK: attr_id + tx_id + op = 11 bytes fijos
fn build_eavt_sk(attr_id: u16, tx_id: u64, op: bool) -> [u8; 11] {
    let mut sk = [0u8; 11];
    sk[0..2].copy_from_slice(&attr_id.to_be_bytes());
    sk[2..10].copy_from_slice(&tx_id.to_be_bytes());
    sk[10] = if op { 1 } else { 0 };
    sk
}

/// AEVT SK: entity_id + tx_id + op = 17 bytes fijos
fn build_aevt_sk(entity_id: u64, tx_id: u64, op: bool) -> [u8; 17] {
    let mut sk = [0u8; 17];
    sk[0..8].copy_from_slice(&entity_id.to_be_bytes());
    sk[8..16].copy_from_slice(&tx_id.to_be_bytes());
    sk[16] = if op { 1 } else { 0 };
    sk
}

/// AVET SK: value_prefix + entity_id + tx_id = 48 bytes
fn build_avet_sk(val: &DatomValue, entity_id: u64, tx_id: u64) -> [u8; 48] {
    let mut sk = [0u8; 48];
    sk[0..32].copy_from_slice(&value_to_sort_prefix(val));
    sk[32..40].copy_from_slice(&entity_id.to_be_bytes());
    sk[40..48].copy_from_slice(&tx_id.to_be_bytes());
    sk
}

/// VAET SK: attr_id + source_entity_id + tx_id + op = 19 bytes
fn build_vaet_sk(attr_id: u16, source_entity_id: u64, tx_id: u64, op: bool) -> [u8; 19] {
    let mut sk = [0u8; 19];
    sk[0..2].copy_from_slice(&attr_id.to_be_bytes());
    sk[2..10].copy_from_slice(&source_entity_id.to_be_bytes());
    sk[10..18].copy_from_slice(&tx_id.to_be_bytes());
    sk[18] = if op { 1 } else { 0 };
    sk
}
```

> [!TIP]
> **Por qué binario y no string:** DynamoDB compara sort keys byte-a-byte. Con encoding big-endian, los números se ordenan correctamente sin parseo. Un range scan sobre `tx_id > 1000` es un simple `SK > 0x00010000000003E8...` — cero deserialización, cero CPU en Lambda.

### II.9 — Costo de Escritura por Índice

| Índice | Items por datom | Condición | WCU estimado |
|:--|:--|:--|:--|
| **EAVT** (Main Table) | 1 | Siempre | 1 WCU por datom |
| **AEVT** (GSI) | 1 | Siempre | Automático (GSI) |
| **AVET** (GSI) | 1 | Solo si `index\|unique\|is_dimension\|fts` | Automático (GSI) |
| **VAET** (GSI) | 1 | Solo si `type: reference` | Automático (GSI) |
| **FTS** (GSI) | ~N trigrams | Solo si `fts: true` | ~8-12 items por valor FTS |

**Ejemplo: CREATE work_order (14 atributos, 3 references, 2 FTS):**
- EAVT: 14 items
- AEVT: 14 items (auto-projected)
- AVET: ~6 items (status, priority, due_date, work_order_number, title, location_id)
- VAET: 3 items (asset_id, location_id, assigned_group_id) + N (assignees many)
- FTS: ~24 items (title ~12 trigrams + work_order_number ~12 trigrams)
- **Total: ~61 items** en 1 `BatchWriteItem` (DynamoDB max 25/batch → 3 batches paralelos)

---

## MÓDULO III: Motor de Transacciones (Write Path)

### III.1 — Transaction Pipeline

```
gRPC TransactionRequest
    │
    ├─ [1] Deserializar payload (Protobuf → Rust struct)
    ├─ [2] Attribute Registry lookup — validar campos
    ├─ [3] Generar TX ID (monotónico, epoch-based)
    ├─ [4] Para cada atributo del payload:
    │       ├─ Si UPDATE: generar par [retract old, assert new]
    │       │   └─ Old value: point-read del datom actual
    │       └─ Si CREATE: generar [assert]
    ├─ [5] FK Integrity: batch-verify entity refs existen
    ├─ [6] Auto-generate: sequential/stochastic codes
    ├─ [7] Build DynamoDB BatchWriteItem (max 25 items/batch)
    ├─ [8] Conditional write con TX ordering guarantee
    └─ [9] Return ULID + TX ID
```

### III.2 — Garantías ACID

| Propiedad | Mecanismo |
|:--|:--|
| **Atomicity** | `TransactWriteItems` de DynamoDB (hasta 100 items, ACID nativo) |
| **Consistency** | `ConditionExpression` previene TX ordering conflicts |
| **Isolation** | Snapshot isolation via TX ID monotónico — readers ven TX ≤ su snapshot |
| **Durability** | DynamoDB multi-AZ replication — datos durables en commit |

### III.3 — Projections ACID (Outbox, Sagas, Calendar)

Idéntico al patrón actual de Datahike pero usando `TransactWriteItems`:

```rust
// Una sola TransactWriteItems incluye:
// 1. Entity datoms (N items)
// 2. Outbox event (1 item) — si disable_eda == false
// 3. Saga projections (M items) — si shadow_sagas_mapping existe
// 4. Calendar event (1 item) — si calendar_mapping existe
// 5. Quota confirmation (1 item) — si disable_quota == false
//
// Todo pasa o todo falla — cero dangling state.
```

---

## MÓDULO IV: Motor de Consultas (Read Path) — Optimizado para BI

### IV.1 — Query Compiler: AST IR → DynamoDB Queries

El compilador traduce el AST IR de Janus (idéntico al actual) a un plan de ejecución DynamoDB:

```
AST IR (del Janus Router)
    │
    ├─ [1] Plan Selection:
    │       ├─ Point lookup (filter by ULID) → Main Table GET
    │       ├─ Single attr filter → GSI-1 AVET Query
    │       ├─ Multi attr filter → Parallel AVET Queries + Intersection
    │       ├─ FTS query → GSI-4 Trigram + Post-filter
    │       └─ Full scan + filter → GSI-2 AEVT por entity_type + filter
    │
    ├─ [2] Projection pushdown:
    │       Solo leer los atributos del SELECT (no pull [*])
    │
    ├─ [3] Pagination pushdown:
    │       DynamoDB Limit + ExclusiveStartKey = cursor nativo
    │
    ├─ [4] Pre-aggregation (KPI/PIE):
    │       Compute SUM/COUNT/AVG durante el scan — O(1) memoria
    │
    └─ [5] Result assembly:
            Reconstruct entity maps desde datoms
```

### IV.2 — Set Intersection: El Truco Clave para Filtros Compuestos

```
Query: "work_orders WHERE status = 'OPEN' AND priority = 'HIGH'"

Plan:
  1. GSI-1 Query: PK="T#tnt#A#status"  SK begins_with "OPEN"  → Set A (entity IDs)
  2. GSI-1 Query: PK="T#tnt#A#priority" SK begins_with "HIGH" → Set B (entity IDs)
  3. Intersection: A ∩ B → entity IDs que cumplen ambos filtros
  4. Batch GET: Main Table → reconstruct entities con solo los attrs del SELECT
```

> [!IMPORTANT]
> **Ambas queries GSI corren en PARALELO** (tokio::join!). La intersección es O(min(|A|,|B|)) con HashSet. Para 10,000 work_orders con 500 OPEN y 200 HIGH, la intersección produce ~20 resultados en < 1ms de CPU.

### IV.3 — Streaming Aggregation (Zero-Allocation KPIs)

```rust
/// Para KPI queries (COUNT/SUM/AVG), no necesitamos reconstituir entities.
/// Leemos SOLO el atributo de la métrica y agregamos durante el scan.
struct StreamAggregator {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl StreamAggregator {
    fn ingest(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min { self.min = value; }
        if value > self.max { self.max = value; }
    }
    fn avg(&self) -> f64 { self.sum / self.count as f64 }
}
```

> [!TIP]
> **Ventaja sobre Datahike:** Datahike carga TODOS los datoms de la entidad (`pull [*]`) y luego agrega en Clojure. Metri EAV lee SOLO el atributo de la métrica directamente del GSI-1 AVET — nunca toca los demás atributos. Para un KPI de `SUM(total_cost)` sobre 10,000 work_orders, esto reduce I/O de ~70KB (7 attrs × 10K) a ~10KB (1 attr × 10K).

---

## MÓDULO V: Full-Text Search — Índice Trigram Binario

### V.1 — Indexación

Al escribir un datom con `fts: true`, se generan trigrams:

```
"Bomba Hidráulica" → trigrams: ["bom", "omb", "mba", "ba ", "a h", " hi", "hid", ...]
```

Cada trigram se escribe como un item en GSI-4:

```
PK: "T#tnt_01J#FTS#bom"
SK: <attr_id>#<entity_id>
```

### V.2 — Búsqueda Fuzzy (Damerau-Levenshtein ≤ 1)

```
Input: "bomba" → trigrams: ["bom", "omb", "mba"]
                + DL1 expansions: ["boma", "obmba", ...]

Plan:
  1. Query GSI-4 para cada trigram → 3 queries paralelas
  2. Intersección ponderada: entities con ≥ 2/3 trigrams = match
  3. Post-filter: Damerau-Levenshtein distance ≤ 1 sobre el valor real
  4. Rank por score = (trigrams matched / total trigrams)
```

---

## MÓDULO VI: Time-Travel y Auditoría

### VI.1 — Snapshot Reads

```rust
/// Un snapshot es simplemente un TX ID. Leer "as-of" TX T significa:
/// para cada (entity, attribute), tomar el datom con max(tx) donde tx ≤ T y op = true.
///
/// Implementación: Query con SK ≤ T, Limit 1, ScanIndexForward=false
/// → DynamoDB retorna el datom más reciente ≤ T en O(1).
fn read_as_of(entity_id: u64, attr_id: u16, as_of_tx: u64) -> Option<DatomValue> {
    // SK range: [attr_id, 0, 0] .. [attr_id, as_of_tx, 1]
    // Limit 1, ScanIndexForward=false → último datom vigente
}
```

### VI.2 — History Query (Auditoría completa)

```rust
/// Retorna TODOS los datoms de un entity, incluyendo retracts.
/// Ideal para audit trail: "¿quién cambió qué y cuándo?"
fn entity_history(entity_id: u64) -> Vec<Datom> {
    // Query Main Table: PK = "T#tnt#E#entity_id"
    // Sin filtro de SK → todos los datoms, ordenados por tx
}
```

---

## MÓDULO VII: Integración con Metri Engine

### VII.1 — Contrato gRPC (Compatibilidad total)

Metri EAV expone el **mismo contrato gRPC** que el engine Clojure actual. El Janus Router no cambia — solo cambia la implementación del `IJanusWriteChannel`:

```
                    ┌─────────────────────┐
                    │   Janus Router      │
                    │   (Clojure — sin    │
                    │    cambios)         │
                    └────────┬────────────┘
                             │
                    ┌────────▼────────────┐
                    │  gRPC call to       │
                    │  Metri EAV Lambda   │
                    │  (Rust ARM64)       │
                    └────────┬────────────┘
                             │
                    ┌────────▼────────────┐
                    │  DynamoDB           │
                    │  (metri-eav-prod)   │
                    └─────────────────────┘
```

### VII.2 — API del Motor Rust

```protobuf
// Extensión de metri.proto para el motor EAV nativo
service MetriEavService {
  // Transacción ACID — reemplaza d/transact de Datahike
  rpc Transact(EavTransactRequest) returns (EavTransactResponse);
  
  // Query compilada — reemplaza d/q de Datahike
  rpc Query(EavQueryRequest) returns (stream EavQueryChunk);
  
  // Point lookup — reemplaza d/pull de Datahike
  rpc Pull(EavPullRequest) returns (EavPullResponse);
  
  // FTS — reemplaza el scan lineal de Datahike
  rpc Search(EavSearchRequest) returns (EavSearchResponse);
}
```

### VII.3 — Migración Gradual (Feature Flag)

```
Fase 1: Dual-Write
  Janus escribe a Datahike Y a Metri EAV simultáneamente.
  Reads siguen de Datahike. Validación de consistencia.

Fase 2: Shadow-Read
  Reads van a ambos motores en paralelo.
  Respuesta de Datahike al cliente, Metri EAV se compara.
  Métricas de latencia y correctitud.

Fase 3: Cutover
  Reads de Metri EAV, Datahike como fallback.
  Feature flag por tenant.

Fase 4: Datahike Sunset
  Eliminar Datahike. Metri EAV es el SSOT.
```

---

## MÓDULO VIII: Superioridad sobre Datomic/Datahike

| Característica | Datomic/Datahike | Metri EAV |
|:--|:--|:--|
| Runtime | JVM (GC pauses, cold start) | Rust native (zero GC, < 50ms cold) |
| Storage | Blob index en DynamoDB | Datoms individuales en DynamoDB (granular I/O) |
| Query | Datalog in-memory | DynamoDB GSI push-down + parallel intersection |
| Aggregation | Post-query in-memory | Streaming aggregation durante scan |
| FTS | Scan lineal O(n) | Trigram index O(k) |
| Pagination | In-memory (load all, take N) | DynamoDB Limit + cursor nativo |
| Latency P99 | ~800ms (GC, JIT) | < 15ms (deterministic) |
| Cost model | Datahike lee el índice completo por query | Metri EAV lee solo los items necesarios |
| Tenant isolation | Cláusula Datalog (bypassable) | Partition Key (impossible to bypass) |
| BI optimization | Ninguna | Pre-computed aggregates, streaming metrics |
| Binary search | No | Lexicographic binary sort keys en DynamoDB |

---

## MÓDULO IX: Crates y Dependencias

```toml
[dependencies]
# AWS SDK
aws-sdk-dynamodb = "1.x"
aws-config = "1.x"

# Async runtime
tokio = { version = "1", features = ["full"] }

# Serialization
serde = { version = "1", features = ["derive"] }
prost = "0.13"              # Protobuf
tonic = "0.12"              # gRPC server

# Lambda
lambda_runtime = "0.13"

# Utils
ulid = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
bytes = "1"
```

---

## MÓDULO X: Tabla de Errores

| Código | Cuándo | Severidad |
|:--|:--|:--|
| `EAV_001` | Entity not found (ULID inválido) | WARNING |
| `EAV_002` | Attribute not in registry | ERROR |
| `EAV_VAL_001` | Value type mismatch | ERROR |
| `EAV_REF_001` | FK reference not found | ERROR |
| `EAV_REF_002` | FK type confusion (entity type mismatch) | ERROR |
| `EAV_TX_001` | DynamoDB TransactWriteItems failed | ERROR |
| `EAV_TX_002` | TX ordering conflict (conditional check) | RETRY |
| `EAV_UNIQUE_001` | Unique constraint violation (identity) | ERROR |
| `EAV_UNIQUE_002` | Unique constraint violation (value) | ERROR |
| `EAV_FTS_001` | FTS index write failed | WARNING |
| `EAV_QUERY_001` | Query compilation failed | ERROR |
| `EAV_QUERY_002` | Query execution timeout | ERROR |

---

## MÓDULO XI: Optimizaciones Nivel 20/10 (Específicas para CMMS)

Un CMMS como Metri Engine tiene demandas asimétricas que rompen bases de datos tradicionales: jerarquías profundas de activos, tsunamis de telemetría IoT, y grafos densos de órdenes de trabajo. Para llevar Metri EAV al límite de lo posible, aplicamos estas 4 optimizaciones de dominio.

### XI.1 — Jerarquías O(1) vía Materialized Paths

**El Problema:** Obtener un árbol completo de activos (Motor -> Bomba -> Válvula -> Sensor) usando el índice `VAET` requiere $N$ queries secuenciales (una por cada nivel de profundidad).

**La Solución 20/10:** Inyectamos el patrón *Materialized Path* directamente en el EAV.
* Cuando se crea/mueve un `location` o `asset`, el motor Rust calcula su ruta completa de ancestros y la guarda como un array de strings en un atributo de sistema: `sys/hierarchy_path = ["01J_MOTOR", "01J_BOMBA"]`.
* Al tener el flag `index: true`, DynamoDB genera items en el índice **AVET** para cada elemento del array.
* **Resultado:** Para obtener *todos* los descendientes de un Motor (sin importar la profundidad), el Janus Router hace **UNA SOLA QUERY O(1)**:
  `Query AVET: PK="T#tnt#AV#hierarchy_path" SK begins_with "01J_MOTOR"`
  Devuelve el sub-árbol completo en < 5ms.

### XI.2 — Compresión Columnar para IoT (Telemetría de Alta Velocidad)

**El Problema:** Escribir un `meter_reading` (IoT) genera múltiples datoms. Si ingresan 1,000 lecturas por segundo, escribir a nivel granular de EAV en DynamoDB consumirá miles de dólares al mes en WCU.

**La Solución 20/10:** *Hybrid EAV con Epoch-Bucketing.*
* Identificamos la entidad `meter_reading` en el registry como `telemetry_entity: true`.
* El motor Rust intercepta las escrituras de esta entidad y en lugar de generar datoms individuales por atributo, empaqueta todos los atributos de un minuto/hora en un solo blob binario (Parquet in-memory o Zstd) dentro de la **Main Table (EAVT)**.
* Costo reducido: De 10 WCU por lectura a 1 WCU por cada 50 lecturas empaquetadas. (Reducción de costos AWS del 98%).
* Janus Firehose sigue exportando al Data Lake OLAP como siempre.

### XI.3 — Resolución de Grafo O(1) (Batch-Pulling N+1)

**El Problema:** La query Datalog `pull [* {:work_order/asset [* {:asset/location [*]}]}]` obliga a hacer un JOIN triple. Datahike lo hace secuencial: lee WO (15ms) -> lee Asset (15ms) -> lee Location (15ms) = 45ms.

**La Solución 20/10:** *BatchGetItem Pipeline en el Executor.*
* Metri EAV lee todas las *Work Orders* solicitadas de una vez (Ej: 50 WOs).
* Escanea los datoms en memoria (CPU O(n)), extrae todos los `DatomValue::Ref(asset_id)`. Des-duplica con un `HashSet`.
* Hace un *ÚNICO* `BatchGetItem` a la Main Table de DynamoDB pidiendo los 20 *Assets* únicos implicados.
* Repite para `location_id`.
* **Resultado:** El tiempo de resolución es constante $O(profundidad\_grafo)$, no $O(filas)$. Las 50 WOs con todos sus joins se resuelven en ~12ms (2 roundtrips de red), no en 750ms.

### XI.4 — Sharding Transaccional (Hot Partitions Multi-Tenant)

**El Problema:** En el índice **AEVT**, la Partition Key es `T#<tenant>#A#<attr_id>`. Si el *Golden Tenant* de Metri tiene 2 millones de `work_orders`, la partición `T#GOLDEN#A#entity_type` superará el límite de 1,000 WCU/s o 3,000 RCU/s físicos de AWS.

**La Solución 20/10:** *Dynamic Write Sharding.*
* El motor añade un sufijo de fragmentación aleatoria (0 a $S-1$) al PK del GSI en tiempo de escritura: `T#GOLDEN#A#entity_type#3`.
* $S$ (número de shards) se configura dinámicamente en el modelo (ej: `shards: 10`).
* **Resultado:** Capacidad infinita por tenant. Evita los estrangulamientos silenciosos (Throttling) sin importar cuánto crezca la base de datos empresarial de Metri.

---

## MÓDULO XII: Mitigación de Límites Físicos de AWS

Para operar a escala extrema (10M+ registros por tenant) sin incidentes, el motor Rust implementa defensas nativas contra los límites hard-coded de DynamoDB.

### XII.1 — Límite de TransactWriteItems (100 operaciones)

**El Riesgo:** DynamoDB permite un máximo absoluto de 100 operaciones por transacción ACID. Si un payload genera más de 100 datoms (ej. una entidad con 20 atributos, 15 referencias que generan índices VAET, y 5 campos indexados con FTS que generan docenas de trigrams), la llamada a `TransactWriteItems` fallará con un error HTTP 400.

**La Mitigación (Chunking Transaccional Degradado):**
* El motor Rust cuenta los items resultantes ANTES de enviarlos a DynamoDB.
* Si `total_items <= 100`: Ejecuta la transacción ACID atómica estándar.
* Si `total_items > 100`: Pasa automáticamente a un modelo de **Eventual Consistency Controlada**:
  1. Ejecuta una transacción ACID prioritaria con los items CORE (los datoms de la tabla principal EAVT, las validaciones de unicidad críticas en AVET y el evento del Outbox). Esto garantiza la integridad base del sistema.
  2. Encola asíncronamente (vía SQS o buffer en memoria de Rust) la creación de los índices secundarios no-críticos (trigrams de FTS, VAET para dependencias de solo lectura masivas).
  3. Si falla la escritura asíncrona de los índices secundarios, el sistema se autorrepara reprocesando el evento a través de un worker idempotente.

### XII.2 — Hot Partitions y Throttling en AEVT

**El Riesgo:** Cada partición física de DynamoDB soporta máximo 1,000 WCU/s o 3,000 RCU/s. Si un tenant gigante tiene 5 millones de `work_orders`, la Partition Key `T#tenant#A#entity_type` en el índice AEVT recibirá **todo** el tráfico de lectura y escritura para ese tipo de entidad. Esto causaría un *Throttling* severo.

**La Mitigación (Dynamic Write Sharding & Scatter-Gather):**
* Implementamos una estrategia de **Write Sharding**. Para atributos de altísima cardinalidad, el motor añade un sufijo de shard al final del PK: `T#tenant#A#entity_type#<0_a_S>`.
* El factor $S$ (shards) es dinámico. Por defecto $S=1$, pero para entidades masivas $S=10$. El motor distribuye las escrituras de manera uniforme y determinista (ej. hash del ULID módulo $S$).
* En la lectura (Query Compiler), el motor detecta el sharding y lanza $S$ tareas paralelas asíncronas (`tokio::spawn`) haciendo un **Scatter-Gather**.
* Como los Sort Keys son binarios y las respuestas vienen pre-ordenadas desde DynamoDB, la Lambda en Rust solo necesita hacer un `merge` lineal de las respuestas, manteniendo el paginado en O(S) de manera extremadamente eficiente.

### XII.3 — Límite de 1024 Bytes en Sort Keys (AVET)

**El Riesgo:** El Sort Key de DynamoDB tiene un límite físico estricto de 1024 bytes. Los valores de texto largos excederían esto, causando rechazos en la escritura del índice AVET (que usa el valor como prefijo del Sort Key para búsquedas).

**La Mitigación (Truncamiento Predictivo + Desempate en Memoria):**
* El diseño actual ya previene el error físico truncando el valor a los primeros 31 bytes (`buf[1..1+len]`) al construir el SK binario.
* **Resolución de Colisiones:** ¿Qué pasa si hay dos valores gigantescos que son **idénticos** en sus primeros 31 bytes (ej. una descripción larguísima)?
  1. El índice AVET proyecta el valor *completo* en el atributo `v` asociado al item.
  2. Cuando el Query Compiler busca por un string exacto que supera los 31 bytes, busca en DynamoDB usando `begins_with` con los 31 bytes de prefijo.
  3. DynamoDB devuelve *todos* los candidatos (colisiones) que coinciden en el prefijo.
  4. El motor Rust ejecuta un filtro exacto final en memoria (`memcmp` del valor completo en `v`) antes de devolver los resultados.
* Esto garantiza que las búsquedas siempre devuelvan el resultado exacto, sin violar jamás los límites de la base de datos y manteniendo un rendimiento O(1) en la red.

---

## MÓDULO XIII: Las 3 Soluciones para Calificación 10/10

### XIII.1 — Cursor de Paginación Estable Multi-Índice

**El Problema:** Cuando un query involucra intersección paralela de múltiples índices (ej. `AVET status=OPEN` ∩ `AVET priority=HIGH`), DynamoDB devuelve un `LastEvaluatedKey` separado por cada índice. Un cursor que solo guarda una clave es ambiguo — no puede representar el estado de múltiples scans en vuelo simultáneamente.

**La Solución: Composite Opaque Cursor (Base64 + MessagePack)**

El motor Rust serializa el estado completo de la paginación en un cursor compuesto, opaco para el cliente.

```rust
/// Estado de paginación completo — uno por cada índice participante en el query.
#[derive(Serialize, Deserialize)]
struct IndexCursorState {
    index_name: String,           // "AVET", "AEVT", "VAET"
    last_pk: String,              // Último PK evaluado en ese índice
    last_sk: Vec<u8>,             // Último SK evaluado (binario, serializado como bytes)
    exhausted: bool,              // true = ese índice ya no tiene más páginas
}

/// Cursor compuesto: N cursores por índice + metadatos del query original.
#[derive(Serialize, Deserialize)]
struct CompositeCursor {
    version: u8,                  // v1 — para compatibilidad futura
    query_fingerprint: u64,       // Hash del AST IR original (detecta cursor inválido si cambia el query)
    page_size: u32,
    index_states: Vec<IndexCursorState>,
    intersection_resume_set: Vec<u64>, // Entity IDs del overlap parcial de la última página
}

impl CompositeCursor {
    /// Serializa a Base64(MessagePack) — opaco, URL-safe, < 512 bytes en casos normales.
    fn encode(&self) -> String {
        let bytes = rmp_serde::to_vec(self).unwrap();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
    }

    /// Deserializa y valida que el fingerprint coincida con el query actual.
    fn decode(token: &str, current_fingerprint: u64) -> Result<Self, EavError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)?;
        let cursor: CompositeCursor = rmp_serde::from_slice(&bytes)?;
        if cursor.query_fingerprint != current_fingerprint {
            return Err(EavError::InvalidCursor("Query changed between pages"));
        }
        Ok(cursor)
    }
}
```

**Flujo de Paginación con Intersección Paralela:**

```
Página 1:
  ├─ Query AVET[status=OPEN]  → 200 entities, LastKey=K1  → Set A (200)
  ├─ Query AVET[priority=HIGH]→ 150 entities, LastKey=K2  → Set B (150)
  ├─ Intersección A ∩ B       → 25 matches → devolver 25 al cliente
  └─ Cursor = encode({K1, K2, intersection_resume_set=[]})

Página 2 (cliente envía cursor):
  ├─ decode(cursor) → resume desde K1 y K2
  ├─ Query AVET[status=OPEN]  desde K1 → 200 más → Set A2
  ├─ Query AVET[priority=HIGH]desde K2 → 150 más → Set B2
  ├─ Intersección A2 ∩ B2     → 30 matches → devolver 30
  └─ Cursor actualizado
```

> [!IMPORTANT]
> El `query_fingerprint` (hash del AST IR) **invalida automáticamente** cursores si el cliente cambia los filtros entre páginas. Esto previene silenciosamente resultados corruptos — el motor retorna `EAV_QUERY_003: CursorStaleness` y el cliente reinicia desde la página 1.

---

### XIII.2 — Optimistic Locking para Escrituras Concurrentes

**El Problema:** En un CMMS, dos técnicos pueden intentar cambiar el estado de la misma Work Order simultáneamente (ej. ambos pulsan "Cerrar WO" en sus tablets al mismo tiempo). Sin control de concurrencia, la segunda escritura silenciosamente sobreescribe la primera — un datom con `tx_id=1002` puede retractar el datom `tx_id=1001` sin saber que ya fue retractado por `tx_id=1001`.

**La Solución: TX-Version Optimistic Locking vía ConditionExpression**

Cada entidad tiene un atributo de sistema `sys/version` (u64 monotónico) almacenado en EAVT. El motor lo usa como vector de versión para `ConditionExpression`:

```rust
/// Al construir la transacción de UPDATE, el motor:
///   1. Lee el sys/version actual de la entidad (en el mismo roundtrip del Pull).
///   2. Incluye una ConditionCheck en el TransactWriteItems que falla si version cambió.
///   3. Si la condición falla → el cliente recibe EAV_TX_003 (Concurrent Modification).

struct OptimisticWrite {
    entity_id: u64,
    expected_version: u64,     // Versión que el cliente leyó
    new_version: u64,          // expected_version + 1
    datoms: Vec<Datom>,
}

fn build_transact_items(write: &OptimisticWrite) -> Vec<TransactWriteItem> {
    let mut items = vec![
        // ── 1. ConditionCheck: versión no cambió desde que el cliente leyó ──
        TransactWriteItem::ConditionCheck(ConditionCheck {
            table_name: TABLE_NAME,
            key: eavt_pk(write.entity_id),
            condition_expression: "version = :expected_v",
            expression_attribute_values: {":expected_v": write.expected_version},
        }),

        // ── 2. Retract versión vieja ──────────────────────────────────────────
        TransactWriteItem::Put(build_datom(
            write.entity_id, SYS_VERSION_ATTR, write.expected_version,
            write.new_tx_id, op=false  // retract
        )),

        // ── 3. Assert versión nueva ───────────────────────────────────────────
        TransactWriteItem::Put(build_datom(
            write.entity_id, SYS_VERSION_ATTR, write.new_version,
            write.new_tx_id, op=true   // assert
        )),

        // ── 4. Datoms de negocio (atributos del payload) ──────────────────────
        // ... (retract old + assert new para cada attr modificado)
    ];
    items
}
```

**Flujo de Concurrencia Resuelta:**

```
T=0: Técnico A lee WO-0042 → version=5, status=IN_PROGRESS
T=0: Técnico B lee WO-0042 → version=5, status=IN_PROGRESS

T=1: Técnico A envía → status=REVIEW, expected_version=5
     ConditionCheck(version=5) ✅ PASA → version=6, status=REVIEW → COMMIT OK

T=2: Técnico B envía → status=CLOSED, expected_version=5
     ConditionCheck(version=5) ❌ FALLA (version ya es 6)
     → EAV_TX_003: ConcurrentModification
     → Cliente muestra: "La WO fue modificada. Recarga para ver cambios."
     → Técnico B ve status=REVIEW actualizado y decide si cierra igual.
```

> [!TIP]
> **El cliente siempre incluye la versión leída en el request.** El campo `entity_version` se expone en el `EavPullResponse` proto y el Janus Router lo propaga transparentemente al `EavTransactRequest`. El técnico nunca ve el número — la UI lo maneja automáticamente. Si el cliente **no envía versión** (ej. operación de bulk import), el motor omite el ConditionCheck y acepta la escritura (modo `last-writer-wins`).

---

### XIII.3 — Schema Evolution sin Downtime

**El Problema:** El Attribute Registry se compila en bootstrap desde los modelos JSON. Si un cliente añade un nuevo campo `work_order/urgency_score` al modelo, las `work_orders` existentes en DynamoDB no tienen ese datom — son "invisibles" para AEVT. Un query de listado que ordena por `urgency_score` no encontraría las entidades viejas (tienen `null` implícito).

**La Solución: 3-Phase Schema Evolution Protocol**

#### Fase 1 — Additive Migration (Nuevo atributo, sin rotura)

```
1. El deployer añade el campo al modelo JSON con un `default_value`.
2. En el bootstrap, el motor detecta que `urgency_score` (attr_id=0x0042) es NUEVO
   (no existe en el registry anterior guardado en MetriSchemasTable).
3. El motor escribe el nuevo AttributeDescriptor en MetriSchemasTable con
   estado: "BACKFILL_PENDING".
4. La Lambda comienza a recibir tráfico normalmente — las entidades NUEVAS ya
   tienen el atributo. Las entidades VIEJAS retornan null para urgency_score.
5. Un proceso de Backfill asíncrono (Lambda dedicada o Step Function) itera
   sobre AEVT[entity/type=work_order] y escribe un datom de default para cada
   entidad antigua: [entity_id, 0x0042, default_value, BACKFILL_TX_ID, true].
6. Cuando el backfill termina, el estado cambia a "ACTIVE". El índice AVET
   ahora incluye todas las entidades, nuevas y viejas.
```

#### Fase 2 — Rename Migration (Renombrar atributo)

```
1. Se añade el nuevo nombre al modelo como un alias: "alias_of": "old_name".
2. El motor escribe AMBOS datoms (old_attr_id y new_attr_id) en cada write
   durante el período de transición.
3. Los readers pueden usar cualquiera de los dos nombres.
4. Tras completar el backfill del alias, se depreca el original.
```

#### Fase 3 — Remove Migration (Eliminar atributo — sin borrar datoms)

```
1. El atributo se marca "deprecated: true" en el modelo JSON.
2. El motor deja de escribir NUEVOS datoms para ese attr_id.
3. Los datoms EXISTENTES permanecen en DynamoDB (inmutabilidad garantizada).
4. El Query Compiler filtra attr_id deprecados del pull [*] automáticamente.
5. Los datoms históricos siguen siendo accesibles via time-travel as-of queries.
```

**Registry Version en MetriSchemasTable:**

```
PK: "SCHEMA_REGISTRY"
Attributes:
  version:    Number   // Incrementa en cada deploy que cambia el schema
  attrs:      Binary   // Vec<AttributeDescriptor> serializado con MessagePack
  deployed_at: Number  // Epoch ms
  prev_version: Number // Para rollback instantáneo
```

```rust
/// Al arranque, el motor compara el registry en DynamoDB con el compilado en el binario.
/// Si hay discrepancias → aplica las migraciones additive/alias/deprecate automáticamente.
async fn bootstrap_registry(ddb: &DynamoDbClient) -> AttributeRegistry {
    let stored   = load_registry_from_ddb(ddb).await;    // Versión en producción
    let compiled = compile_registry_from_models();        // Versión en el binario actual

    let migration = diff_registries(&stored, &compiled);

    for new_attr in &migration.added {
        // Escribe en MetriSchemasTable con estado BACKFILL_PENDING
        // Dispara Step Function de backfill si hay entidades existentes
        register_new_attr(ddb, new_attr, BackfillStrategy::Async).await;
    }

    for deprecated in &migration.removed {
        mark_deprecated(ddb, deprecated).await;
    }

    // Retorna el registry merged (stored + new additions) listo para servir tráfico
    merge_registries(stored, compiled)
}
```

> [!IMPORTANT]
> **Zero-Downtime Garantizado:** La Lambda nunca espera el backfill para arrancar. Arranca con el registry `merged` (entidades viejas sin el nuevo campo = null implícito), y el backfill rellena el vacío asíncronamente. El único caso que requiere coordinación explícita es si el nuevo atributo tiene `unique: identity` — en ese caso el backfill corre de manera sincrónica antes de activar el índice AVET para ese atributo.

---

## Próximos Pasos de Implementación

1. **Binary Encoding Spec** — Especificación byte-a-byte de `DatomValue` (tag + payload por tipo).
2. **SAM Template Update** — Añadir `metri-eav-prod` con sus 4 GSIs + IAM policies al `template.yaml`.
3. **AttributeRegistry PoC** — Parser JSON → `Vec<AttributeDescriptor>` con diff + bootstrap protocol.
4. **Cursor Codec** — Implementar `CompositeCursor` encode/decode con `rmp_serde` + `base64`.
5. **Optimistic Lock PoC** — Test de concurrencia: 2 writers en paralelo, verificar que solo 1 gana.
6. **Backfill Worker** — Lambda Step Function que itera AEVT y aplica default datoms en batch.

---
---

# PARTE II — Arquitectura de Módulos Rust (`src/eav/`)

> Esta sección documenta la estructura de implementación Rust del motor EAV:
> carpetas, archivos, tipos, firmas de funciones y dependencias entre módulos.

**Definición:** El módulo `eav/` es el motor de base de datos inmutable y append-only de Metri Engine. Implementa el modelo Datom (Entity-Attribute-Value-Transaction) con 4 índices canónicos sobre DynamoDB, reemplazando Datahike/Clojure con latencias determinísticas sub-milisegundo.

**Objetivo:** Proveer una capa de persistencia ACID, multitenant, y schema-driven que soporte consultas analíticas sin full-table-scan, time-travel, y full-text search, ejecutándose dentro del límite de 50ms de cold start en AWS Lambda ARM64.

---

## Estructura de Carpetas

```
metri-engine/
└── src/                         ← raíz compartida con src/metri/ (Clojure)
    ├── metri/                   # Clojure — existente (NO tocar)
    │   ├── aegis/
    │   ├── janus/
    │   └── ...
    │
    └── eav/                     # ⚠️ NUEVO — Rust puro, carpeta hermana de src/metri/
        ├── mod.rs               # Re-exports públicos del módulo
        │
        ├── types/               # ── MÓDULO 1: Tipos Fundamentales ──
        │   ├── mod.rs
        │   ├── datom.rs         # Struct Datom + DatomValue enum (12 tipos)
        │   ├── encoding.rs      # Builders de Sort Key binario (EAVT/AEVT/AVET/VAET)
        │   └── value_type.rs    # ValueType enum + tag bytes (0x01..0x0C)
        │
        ├── registry/            # ── MÓDULO 2: Attribute Registry ──
        │   ├── mod.rs
        │   ├── descriptor.rs    # AttributeDescriptor struct (u16 id, name, type, flags)
        │   ├── registry.rs      # AttributeRegistry: Vec<Descriptor> + HashMap lookups
        │   ├── compiler.rs      # JSON model → AttributeDescriptor (parser Códice)
        │   └── migration.rs     # diff_registries(), BackfillStrategy, schema evolution
        │
        ├── writer/              # ── MÓDULO 3: Write Path ──
        │   ├── mod.rs
        │   ├── transact.rs      # transact() → TransactWriteItems (ACID)
        │   ├── chunker.rs       # Chunking transaccional (límite 100 items DynamoDB)
        │   ├── enricher.rs      # auto-generate: ulid, created_at, sys/version, tenant_id
        │   ├── outbox.rs        # build_outbox_item() para EDA
        │   └── optimistic.rs    # build_condition_check() para Optimistic Locking
        │
        ├── reader/              # ── MÓDULO 4: Read Path ──
        │   ├── mod.rs
        │   ├── pull.rs          # eav_pull(): reconstituir entidad desde EAVT
        │   ├── query.rs         # eav_query(): selección de índice + ejecución
        │   ├── assembler.rs     # datoms → HashMap<String, Value> (row de negocio)
        │   └── as_of.rs         # time-travel: eav_pull_as_of(entity_id, tx_id)
        │
        ├── index/               # ── MÓDULO 5: Los 4 Índices Canónicos ──
        │   ├── mod.rs
        │   ├── eavt.rs          # Main table: PK=T#tenant#E#eid, SK=attr+tx binario
        │   ├── aevt.rs          # GSI-AEVT: PK=T#tenant#A#type, SK=eid+tx binario
        │   ├── avet.rs          # GSI-AVET: PK=T#tenant#AV#attr, SK=value+eid binario
        │   └── vaet.rs          # GSI-VAET: PK=T#tenant#V#ref_eid, SK=attr+eid binario
        │
        ├── fts/                 # ── MÓDULO 6: Full-Text Search ──
        │   ├── mod.rs
        │   ├── trigram.rs       # generate_trigrams(text) → Vec<String>
        │   ├── index_writer.rs  # build_fts_items() → DynamoDB items del índice FTS
        │   └── searcher.rs      # fts_search(): trigram intersection + EAVT reconstituir
        │
        ├── cursor/              # ── MÓDULO 7: Paginación ──
        │   ├── mod.rs
        │   └── composite.rs     # CompositeCursor: encode()/decode() Base64+MessagePack
        │
        ├── hierarchy/           # ── MÓDULO 8: Materialized Paths ──
        │   ├── mod.rs
        │   └── path.rs          # build_hierarchy_path(), O(1) subtree query via AVET
        │
        ├── sharding/            # ── MÓDULO 9: Write Sharding ──
        │   ├── mod.rs
        │   └── shard.rs         # shard_key(tenant, entity, N), scatter_gather()
        │
        └── telemetry/           # ── MÓDULO 10: IoT Telemetry ──
            ├── mod.rs
            └── bucket.rs        # epoch_bucket(), binary blob compression
```


---

## MÓDULO 1: `types/` — Tipos Fundamentales

**Definición:** La representación en memoria de un Datom — la unidad mínima e inmutable de información del sistema.

**Objetivo:** Garantizar que todos los valores del sistema sean tipados en compile-time, zero-copy cuando sea posible, y que los Sort Keys binarios sean `memcmp`-comparables sin deserialización.

### `datom.rs`

```rust
/// La unidad atómica de información: [Entity, Attribute, Value, Transaction, Op]
/// Inmutable por diseño — nunca se modifica, solo se retracta y se re-asserta.
#[derive(Debug, Clone)]
pub struct Datom {
    pub entity_id: u64,      // ULID decodificado a u64 (big-endian, sortable)
    pub attr_id:   u16,      // ID del atributo en el AttributeRegistry (O(1) lookup)
    pub value:     DatomValue,
    pub tx_id:     u64,      // ULID de transacción (monotónico, time-ordered)
    pub op:        bool,     // true = assert, false = retract
    pub tenant_id: String,   // Isolation cardinal — presente en TODOS los datoms
}

#[derive(Debug, Clone)]
pub enum DatomValue {
    Str(String),      // tag 0x01
    U64(u64),         // tag 0x02 — 8 bytes big-endian
    I64(i64),         // tag 0x03 — offset-encoded
    F64(f64),         // tag 0x04 — IEEE 754 bit-flip para orden correcto
    Bool(bool),       // tag 0x05 — 1 byte
    Ulid(u128),       // tag 0x06 — referencia a otra entidad
    Bytes(Vec<u8>),   // tag 0x07 — blob binario (IoT telemetry)
    Epoch(i64),       // tag 0x08 — epoch-ms
    Json(String),     // tag 0x09 — JSON serializado
    Nil,              // tag 0x0A
    Array(Vec<DatomValue>), // tag 0x0B — genera múltiples items AVET
    Ref(u64),         // tag 0x0C — genera item VAET automáticamente
}
```

### `encoding.rs`

```rust
/// EAVT SK: attr_id (2B) + tx_id (8B) + op (1B) = 11 bytes fijos
pub fn build_eavt_sk(attr_id: u16, tx_id: u64, op: bool) -> [u8; 11];

/// AVET SK: type_tag (1B) + value_bytes (≤31B) + entity_id (8B) = ≤40 bytes
/// F64 usa IEEE 754 bit-flip. Epoch usa offset-encoding. memcmp-comparable.
pub fn build_avet_sk(value: &DatomValue, entity_id: u64) -> Vec<u8>;

/// VAET SK: attr_id (2B) + source_entity_id (8B) = 10 bytes fijos
pub fn build_vaet_sk(attr_id: u16, source_entity_id: u64) -> [u8; 10];
```

---

## MÓDULO 2: `registry/` — Attribute Registry

**Definición:** El catálogo compilado de todos los atributos del sistema. Transforma los modelos JSON del Códice en estructuras Rust estáticas cargadas en bootstrap.

**Objetivo:** Lookup O(1) por `attr_id` (u16) o por nombre de atributo durante todo el ciclo de vida de la Lambda, sin acceder a DynamoDB en caliente.

```rust
#[derive(Debug, Clone)]
pub struct AttributeDescriptor {
    pub id:            u16,
    pub name:          String,         // "work_order/status"
    pub entity_type:   String,
    pub value_type:    ValueType,
    pub cardinality:   Cardinality,    // One | Many
    pub indexed:       bool,           // genera item AVET
    pub unique:        Option<UniqueStrategy>,
    pub fts:           bool,           // genera trigrams FTS
    pub is_ref:        bool,           // genera item VAET
    pub deprecated:    bool,
    pub status:        AttrStatus,     // Active | BackfillPending | Deprecated
    pub default_value: Option<DatomValue>,
}

pub struct AttributeRegistry {
    by_id:     Vec<AttributeDescriptor>,      // O(1) por u16 ID
    by_name:   HashMap<String, u16>,          // "entity/attr" → id
    by_entity: HashMap<String, Vec<u16>>,     // "work_order" → [id1, id2, ...]
}
```

### `migration.rs`

```rust
pub enum BackfillStrategy { Async, Sync }

pub struct RegistryMigration {
    pub added:      Vec<AttributeDescriptor>,
    pub deprecated: Vec<u16>,
    pub renamed:    Vec<(u16, String)>,
}

pub fn diff_registries(stored: &AttributeRegistry, compiled: &AttributeRegistry)
    -> RegistryMigration;
```

---

## MÓDULO 3: `writer/` — Write Path

**Definición:** Pipeline ACID que transforma un payload de negocio en datoms y los persiste en DynamoDB.

**Objetivo:** Cada `transact()` es atómico, idempotente, y respeta los límites físicos de DynamoDB (100 items/TX).

```rust
pub struct TransactRequest {
    pub tenant_id:      String,
    pub entity_id:      Option<u64>,
    pub entity_type:    String,
    pub attributes:     HashMap<String, serde_json::Value>,
    pub entity_version: Option<u64>,   // Optimistic locking
    pub operation:      TxOperation,   // Assert | Retract | Upsert
}

/// Pipeline: Enrich → Build datoms → Build index items → Chunk → TransactWriteItems
pub async fn transact(
    req:      TransactRequest,
    registry: &AttributeRegistry,
    ddb:      &DynamoDbClient,
) -> Result<TransactResult, EavError>;
```

### `chunker.rs`

```rust
pub enum ChunkStrategy {
    SingleAtomic(Vec<TransactWriteItem>),           // ≤ 100 items → ACID puro
    DegradedConsistency {
        core_items:      Vec<TransactWriteItem>,    // EAVT + unique checks + Outbox
        secondary_items: Vec<TransactWriteItem>,    // VAET masivos + FTS (async)
    },
}

pub fn plan_chunks(items: Vec<TransactWriteItem>) -> ChunkStrategy;
```

---

## MÓDULO 4: `reader/` — Read Path

**Definición:** Pipeline de lectura que consulta los 4 índices y reconstituye entidades de negocio.

**Objetivo:** Responder a `eav_pull` y `eav_query` en < 5ms P99 usando siempre el índice óptimo.

```rust
/// Reconstituye entidad desde EAVT — equivalente a d/pull de Datahike.
pub async fn eav_pull(
    entity_id: u64, select: &[String], tenant_id: &str,
    registry: &AttributeRegistry, ddb: &DynamoDbClient,
) -> Result<HashMap<String, DatomValue>, EavError>;

/// Selecciona el índice óptimo para el QueryPlan.
pub fn select_query_plan(plan: &EavQueryPlan, registry: &AttributeRegistry) -> QueryPlan;

pub enum QueryPlan {
    PointLookup      { entity_id: u64 },
    AvetSingle       { attr_id: u16, value: DatomValue },
    AvetIntersection { filters: Vec<(u16, DatomValue)> },
    AevtScan         { entity_type: String, shard: Option<u8> },
    VaetLookup       { ref_entity_id: u64, attr_id: Option<u16> },
    FtsSearch        { term: String, attr_ids: Vec<u16> },
    AsOfSnapshot     { entity_id: u64, as_of_tx: u64 },
}

/// Agrupa datoms, toma max(tx_id) por attr_id donde op=true.
pub fn assemble_entity(datoms: Vec<Datom>, registry: &AttributeRegistry)
    -> HashMap<String, DatomValue>;
```

---

## MÓDULO 5: `index/` — Los 4 Índices Canónicos

| Módulo | Patrón de Acceso | PK | SK |
|:--|:--|:--|:--|
| `eavt.rs` | Reconstituir entidad | `T#<tenant>#E#<eid>` | `attr_id + tx_id + op` (11B) |
| `aevt.rs` | Listar por tipo | `T#<tenant>#A#<type>[#<shard>]` | `eid + tx_id + op` (17B) |
| `avet.rs` | Filtrar por valor | `T#<tenant>#AV#<attr>` | `tag + value_prefix + eid` (≤40B) |
| `vaet.rs` | Grafo inverso | `T#<tenant>#V#<ref_eid>` | `attr_id + src_eid` (10B) |

```rust
pub fn build_eavt_item(datom: &Datom, table: &str) -> Put;
pub fn build_aevt_item(datom: &Datom, entity_type: &str, shard: u8) -> Put;
pub fn build_avet_item(datom: &Datom, attr: &AttributeDescriptor) -> Option<Put>;
pub fn build_vaet_item(datom: &Datom, attr: &AttributeDescriptor) -> Option<Put>;
```

---

## MÓDULO 6: `fts/` — Full-Text Search

```rust
pub fn generate_trigrams(text: &str) -> Vec<String>;
// PK = T#<tenant>#FTS#<trigram>  SK = entity_id (8B)
pub fn build_fts_items(datom: &Datom, attr: &AttributeDescriptor) -> Vec<Put>;
pub async fn fts_search(term: &str, attr_ids: &[u16], tenant_id: &str,
    select: &[String], ddb: &DynamoDbClient,
) -> Result<Vec<HashMap<String, DatomValue>>, EavError>;
```

---

## MÓDULO 7: `cursor/` — Paginación

```rust
#[derive(Serialize, Deserialize)]
pub struct CompositeCursor {
    pub version:             u8,
    pub query_fingerprint:   u64,   // invalida cursor si cambia el query
    pub page_size:           u32,
    pub index_states:        Vec<IndexCursorState>,
    pub intersection_resume: Vec<u64>,
}
impl CompositeCursor {
    pub fn encode(&self) -> String;  // Base64(MessagePack) — URL-safe < 512 bytes
    pub fn decode(token: &str, fingerprint: u64) -> Result<Self, EavError>;
}
```

---

## MÓDULO 8: `hierarchy/` — Materialized Paths

```rust
/// sys/hierarchy_path = ["01J_MOTOR", "01J_BOMBA"]
/// AVET query: SK begins_with "01J_MOTOR" → subtree completo en 1 query
pub async fn build_hierarchy_path(
    entity_id: u64, parent_id: Option<u64>,
    tenant_id: &str, ddb: &DynamoDbClient,
) -> Result<Vec<String>, EavError>;
```

---

## MÓDULO 9: `sharding/` — Write Sharding

```rust
pub fn shard_key(entity_id: u64, total_shards: u8) -> u8;
pub async fn scatter_gather_aevt(
    entity_type: &str, tenant_id: &str, total_shards: u8,
    plan: &EavQueryPlan, ddb: &DynamoDbClient,
) -> impl Stream<Item = Result<Datom, EavError>>;
```

---

## MÓDULO 10: `telemetry/` — IoT Telemetry

```rust
/// -98% WCU: N lecturas IoT → 1 blob LZ4 por bucket de tiempo
pub fn epoch_bucket(timestamp_ms: i64, granularity_secs: u64) -> i64;
pub fn compress_readings(readings: &[IoTReading]) -> Vec<u8>;
pub fn decompress_readings(blob: &[u8]) -> Vec<IoTReading>;
```

---

## Dependencias entre Módulos

```
          registry/ (bootstrap)
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
  types/   writer/    reader/
    │         │          │
    └────┬────┘          ▼
         ▼             fts/
       index/ ──────────┘
         │
       cursor/
         │
    ┌────┴────────┐
    ▼             ▼
hierarchy/    sharding/
                  │
              telemetry/
```

**Regla:** `types/` sin dependencias internas. `registry/` solo depende de `types/`. Todos los demás dependen de ambos.

---

## `Cargo.toml` — Dependencias del módulo `eav/`

```toml
[dependencies]
aws-sdk-dynamodb = "1"
aws-config       = "1"
rmp-serde        = "1"           # MessagePack (CompositeCursor)
base64           = "0.22"        # URL-safe cursor encoding
tokio            = { version = "1", features = ["full"] }
ulid             = "1"
lz4_flex         = "0.11"        # IoT blob compression
xxhash-rust      = { version = "0.8", features = ["xxh64"] }  # cursor fingerprint
tracing          = "0.1"
```

