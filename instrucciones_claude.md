# Contexto y Objetivo Principal

Actúas como Arquitecto Core Senior de `metri-engine`, un motor analítico y OLTP escrito en Rust. El pipeline interno tiene 5 etapas secuenciales:
gRPC Request → translator.rs → janus/ast_compiler.rs → aegis/oltp/executor.rs → janus/normalizer.rs → grpc/service.rs → gRPC Response
Los contratos de salida están definidos en `metri.proto` e incluyen los nodos:
`VizMeta`, `TreeMeta`, `ChartDecoration`, `TableMeta`, `BreakdownSignal`, `AnalyticalSignal`, `RowSet`, `Pagination` y `QueryMetadata`.
**OBJETIVO FINAL:** Realizar todas las modificaciones necesarias en el código Rust de `metri-engine` para que cada nodo del contrato sea emitido correctamente, con todos sus campos requeridos poblados, sin respuestas vacías y 100% alineado con `metri.proto`.
**REGLAS:**

1. **Solo entorno local.** Ningún cambio toca producción.
2. **Cero respuestas vacías.** Todo componente debe devolver datos funcionales.
3. **Cero campos omitidos.** Cada nodo proto debe estar completamente hidratado.

---

# Fase 1 — Exploración: Motor Python de Grafo de Contratos

Diseña un script Python (`proto_graph_explorer.py`) cuyo único objetivo es mapear
dinámicamente la topología y relaciones de todos los nodos del contrato `metri.proto`.
**La función debe:**

- Inspeccionar recursivamente el `QueryResponse` recibido del gRPC y trazar el grafo completo de nodos encontrados vs nodos esperados según el proto.
- Para cada nodo (`VizMeta`, `TreeMeta`, `ChartDecoration`, `TableMeta`, `BreakdownSignal`, `Pagination`, etc.), verificar:
  - ¿Está presente? ¿Está vacío? ¿Tiene los campos requeridos?
  - ¿Los tipos de datos son correctos? (ej. no BigInt en un campo `double`)
  - ¿El `oneof payload` en `VizMeta` resuelve al tipo correcto según el `viz` hint?
- Emitir un reporte estructurado de **Nodos Encontrados vs Nodos Esperados** con
  un score por componente (0–100).
  **El resto de las fases depende de lo que esta exploración descubra.**

---

# Fase 2 — Ingestión de Datos de Prueba

Basado en los componentes descubiertos, inyecta entidades de prueba localmente
usando los esquemas en `metri-engine/config/models/` (50 modelos disponibles: `location.json`, `asset.json`, `work_order.json`, etc.).

- Para cada entidad requerida por los tests, usa `rpc Transact` o `rpc BulkIngest`
  para crear registros que ejerciten jerarquías (`parent_location_id`, `parent_asset_id`)
  y relaciones entre entidades.
- Esto garantiza la regla Anti-Empty antes de cualquier test de lectura.

---

# Fase 3 — Suite de Testing por Componentes (100+ casos)

Para cada nodo descubierto en Fase 1, diseña una batería de tests independiente:

- **Tree / Jerarquías:** raíces (`current_node_id=""`), hijos específicos, `inject_has_children=true`.
- **Table:** paginación por cursor, sort multi-campo, `ColumnSchema` completamente hidratado.
- **ChartDecoration:** bar, line, area, scatter con `x_dimension` y `y_dimensions` no vacíos.
- **KPI / AnalyticalSignal:** `value`, `intelligence.direction`, `intelligence.percentage`.
- **Pie / BreakdownSignal:** `signals` con keys semánticos (no `slice_0`, `slice_1`).
- **Pagination:** `has_next`, `next_cursor`, `page_size`, `links` correctamente calculados.
  Cada test debe validar el contrato contra el grafo explorado en Fase 1.

---

# Fase 4 — Revisión del Pipeline en 5 Etapas

Para cada tipo de componente con fallos detectados, rastrea el payload en cada etapa:

1. **`translator.rs`** — ¿El `TimeFrameContext` proto se traduce al FBS?
   Actualmente hay un `// TODO` en la línea de `time_frame: None`. Esto rompe
   cualquier query con filtro temporal.
2. **`janus/ast_compiler.rs`** — ¿El `output_cast` por defecto (`"KPI"`) es correcto
   cuando el usuario no lo especificó? Un `viz="tree"` sin `output_cast` explícito
   no debería defaultear a KPI.
3. **`aegis/oltp/executor.rs`** — ¿El filtro jerárquico (`passes_hierarchy`) cubre
   correctamente el caso `current_node_id` con ULID largo (ej. `01KRP14X...`)?
4. **`janus/normalizer.rs`** — ¿El `BreakdownSignal` usa el valor semántico real
   de la dimensión como key de `signals`, o usa `slice_0`, `slice_1`?
5. **`grpc/service.rs`** — ¿El `TableMeta.columns` se emite con `vec![]` siempre
   (vacío), en lugar de propagar las columnas reales del `RowSet`?

---

# Fase 5 — Correcciones en Rust (Solo Local) y Plan de Acción

Aplica todas las correcciones necesarias en `metri-engine`. Los puntos de intervención
confirmados por análisis estático del código son:
| Archivo | Línea | Problema | Corrección requerida |
|---|---|---|---|
| `grpc/translator.rs` | ~55 | `time_frame: None` (TODO sin implementar) | Mapear `TimeFrameContext` proto → `TimeFrameContextT` FBS |
| `janus/ast_compiler.rs` | ~144 | `output_cast` default `"KPI"` incorrecto | Default debe inferirse del campo `viz` cuando no hay `output_cast` |
| `janus/normalizer.rs` | ~189 | `pie` emite `slice_0` como key semántico | Usar el valor real de la primera columna dimensión como key |
| `grpc/service.rs` | ~361 | `TableMeta.columns: vec![]` siempre vacío | Propagar las columnas reales del `RowSet.columns` |
Para cada corrección:

1. Muestra el diff exacto del código Rust modificado.
2. Vuelve a compilar localmente (`cargo build`).
3. Ejecuta la suite de la Fase 3 y verifica que el score del componente pasa de X a 100.
4. Documenta el antes/después en el Plan de Acción.
   **Criterio de éxito:** Suite Python reporta 100/100 en todos los componentes,
   0 respuestas vacías, 0 campos proto omitidos, 0 tipos incorrectos.

---

## Entregables

1. `proto_graph_explorer.py` — Motor explorador de grafos con score por componente.
2. `seed_local_data.py` — Inyector de entidades basado en `config/models/`.
3. `test_suite_100.py` — 100+ casos de prueba organizados por componente visual.
4. Diffs de Rust con las correcciones aplicadas en `metri-engine` local.
5. Plan de Acción iterativo con scores antes/después por cada nodo proto.
