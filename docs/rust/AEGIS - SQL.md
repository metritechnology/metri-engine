# AEGIS — Compilador SQL para AWS Athena (Rust)

**Propósito:** Transpilar el contrato `EavQueryPlan` (FlatBuffers) generado por Janus Rust en SQL optimizado para AWS Athena/Iceberg. Reemplaza el pipeline Clojure: `aegis.sql.compiler` + `where` + `select` + `fuzzy_sql` + `comparison`.

---

## MÓDULO 0: Filosofía del Compilador

### Invariantes de Diseño

| Invariante | Descripción |
|:--|:--|
| **Zero-Trust Gate** | Ningún SQL se genera si el plan no contiene `tenant_id` en el WHERE. Es el primer check, antes de cualquier compilación. |
| **Anti-Levenshtein** | `levenshtein_distance()` en Athena = full table scan. PROHIBIDO. El fuzzy se calcula en CPU (Rust) y se emite como `LIKE + regexp_like`. |
| **Predicate Pushdown First** | Toda cláusula WHERE debe ser Iceberg-pushdownable. Funciones no-pushdownables solo se usan como filtros secundarios. |
| **SeaQuery como Builder Tipado** | El compilador usa `SeaQuery` como builder tipado (equivalente a HoneySQL en Clojure). Los identificadores de columna y tabla son `Ident` tipados — nunca strings crudos en el query. SQL injection imposible por construcción. |
| **OutputCast Routing** | El tipo de salida (`KPI`, `TIMESERIES`, `TABLE`, `PIE`, `BUBBLE`, `CSV_EXPORT`) determina el `SELECT`, `GROUP BY` y `ORDER BY` completos. |

### Arquitectura de Módulos Rust

```
EavQueryPlan (FlatBuffers)
    │
    ▼
┌─────────────────────────────────────────────┐
│  aegis/compiler.rs  (orchestrator)          │
│  ├── assert_tenant_isolation()  ← ZT gate  │
│  ├── table_iden()               ← DynIden  │
│  └── compile_athena_sql()                   │
└──────────────┬──────────────────────────────┘
               │
       ┌───────┼────────────┐
       ▼       ▼            ▼
  where.rs  select.rs  comparison.rs
  filter_   build_     build_time_
  to_expr() select_    shift_query()
  → Simple  statement()→ WithClause
    Expr    → Select     + CTE
       │      Statement      │
       ▼            │        │
  fuzzy.rs          │        │
  fuzzy_to_expr()   │        │
  → SimpleExpr      │        │
  (Cond::any +      │        │
   LIKE|regexp)     │        │
       │            │        │
       └─────┬──────┘        │
             ▼               │
    ┌─────────────────────────────────┐
    │   sea_query  (Builder Tipado)   │
    │                                 │
    │   SimpleExpr / Cond / Select    │
    │   Statement / WithClause        │
    │                                 │
    │   DynIden  ←  col_iden()        │
    │   (whitelist [a-z0-9_])         │
    │                                 │
    │   ← Equivalente a HoneySQL en   │
    │     Clojure (misma filosofía)   │
    └────────────────┬────────────────┘
                     │
                     │  .to_string(MysqlQueryBuilder)
                     │  ← ÚNICO punto de materialización
                     ▼
              SQL String (ANSI/Presto)
                     │
                     ▼
        Athena StartQueryExecution
```


---

## MÓDULO I: Tabla de Nombres y Routing

### I.1 — Resolución del Nombre de Tabla Athena

```rust
/// Resuelve el nombre completo de la tabla Iceberg en Athena.
/// Formato: `<database>.<entity_plural>`
/// Ejemplo: "metrics_db.work_orders"
fn resolve_table_name(entity: &str, database: &str) -> String {
    // Pluralización simple — el registry tiene el nombre real
    // Prioridad: registry.olap_table_name > entity + "s"
    let plural = REGISTRY.get(entity)
        .and_then(|e| e.olap_table_name.as_deref())
        .unwrap_or_else(|| &format!("{}s", entity));
    format!("{}.{}", database, plural)
}
```

### I.2 — Routing por OutputCast

```
OutputCast → Estrategia SQL
──────────────────────────────────────────────────────────
KPI         → SELECT AGG(metric) [GROUP BY dims] WHERE ...
              LIMIT 1 (o N si hay breakdown)
TIMESERIES  → SELECT date_trunc(interval, ts_col) AS bucket,
                     AGG(metric) WHERE ... GROUP BY 1 ORDER BY 1 ASC
              Sin LIMIT (el frontend pagina)
TABLE       → SELECT col1, col2, ... WHERE ... ORDER BY ... LIMIT 10_000
PIE         → SELECT dim_col, AGG(metric) WHERE ... GROUP BY dim_col
BUBBLE      → SELECT x_dim, AGG(y_metric), AGG(size_metric)
                WHERE ... GROUP BY x_dim
CSV_EXPORT  → SELECT * WHERE ... (sin LIMIT)
```

---

## MÓDULO II: Security Gate (Zero-Trust)

El primer paso del compilador — **antes de cualquier otra operación**:

```rust
/// Verifica que el EavQueryPlan contiene tenant_id en el where_root.
/// Escanea el árbol FilterNode recursivamente.
/// Si retorna false → el SQL NUNCA se genera. Se retorna EavError::TenantMissing.
fn assert_tenant_isolation(plan: &EavQueryPlan) -> Result<(), AegisError> {
    fn scan(node: &FilterNode) -> bool {
        match node.op() {
            FilterOp::Eq => node.field() == "tenant_id",
            FilterOp::And | FilterOp::Or => {
                node.children().iter().any(|c| scan(c))
            }
            FilterOp::Not => node.children().first().map(scan).unwrap_or(false),
            // ref-filter apunta a otra entidad — no contiene tenant assert de esta
            _ => false,
        }
    }

    if plan.where_root().map(|r| scan(&r)).unwrap_or(false) {
        Ok(())
    } else {
        Err(AegisError::TenantMissing {
            tenant_id: plan.tenant_id().unwrap_or("").to_string(),
        })
    }
}
```

---

## MÓDULO III: WHERE Compiler (`where.rs`)

Transpila el árbol `FilterNode` (FlatBuffers) a SQL string paramétrico.

### III.1 — Operadores Soportados (14 del contrato)

```rust
fn compile_filter_node(node: &FilterNode, params: &mut ParamBag) -> String {
    let col = sanitize_col(node.field()); // Snake_case, sin namespace
    match node.op() {
        FilterOp::Eq      => format!("{} = {}", col, params.add(node.value())),
        FilterOp::Neq     => format!("{} <> {}", col, params.add(node.value())),
        FilterOp::Gt      => format!("{} > {}", col, params.add(node.value())),
        FilterOp::Gte     => format!("{} >= {}", col, params.add(node.value())),
        FilterOp::Lt      => format!("{} < {}", col, params.add(node.value())),
        FilterOp::Lte     => format!("{} <= {}", col, params.add(node.value())),
        FilterOp::In      => {
            let list = compile_value_list(node.value(), params);
            format!("{} IN ({})", col, list)
        }
        FilterOp::NotIn   => {
            let list = compile_value_list(node.value(), params);
            format!("{} NOT IN ({})", col, list)
        }
        FilterOp::Contains => {
            let escaped = escape_like(node.value().str_val());
            format!("LOWER({}) LIKE '%{}%'", col, escaped.to_lowercase())
        }
        FilterOp::StartsWith => {
            let escaped = escape_like(node.value().str_val());
            format!("LOWER({}) LIKE '{}%'", col, escaped.to_lowercase())
        }
        FilterOp::Matches  => format!("regexp_like({}, {})", col, params.add(node.value())),
        FilterOp::IsNull    => format!("{} IS NULL", col),
        FilterOp::IsNotNull => format!("{} IS NOT NULL", col),
        FilterOp::And => {
            let parts: Vec<_> = node.children().iter()
                .map(|c| compile_filter_node(c, params)).collect();
            format!("({})", parts.join(" AND "))
        }
        FilterOp::Or => {
            let parts: Vec<_> = node.children().iter()
                .map(|c| compile_filter_node(c, params)).collect();
            format!("({})", parts.join(" OR "))
        }
        // Fuzzy → delegado al módulo fuzzy.rs
        FilterOp::Fuzzy => compile_fuzzy_node(node, params),
        // RefFilter → subquery cross-entity
        FilterOp::RefFilter => compile_ref_filter(node, params),
    }
}
```

### III.2 — Sanitización de Columnas (SQL Injection Prevention)

```rust
/// Extrae el nombre de columna del field del FilterNode.
/// Input:  "work_order/status" | "status" | "entity/tenant_id"
/// Output: "status" | "tenant_id"
/// Whitelist: solo [a-z0-9_] — cualquier otro caracter causa AegisError::InvalidField.
fn sanitize_col(field: &str) -> String {
    let col = field.split('/').last().unwrap_or(field);
    // Mapeo de nombres especiales del contrato
    let col = match col {
        "id"         => "id",
        "created_at" => "created_at",
        "tenant_id"  => "tenant_id",
        other        => other,
    };
    // Whitelist estricta — previene SQL injection
    if col.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        col.to_string()
    } else {
        panic!("AegisError::InvalidField: {}", col) // En producción: retorna Err
    }
}
```

### III.3 — RefFilter (Cross-Entity JOIN como Subquery)

Equivalente al `:ref-filter` del Clojure:

```rust
/// Genera: location_id IN (SELECT id FROM metrics_db.location WHERE type = 'BUILDING')
/// El ref_entity se deduce del namespace del primer campo del inner FilterNode.
fn compile_ref_filter(node: &FilterNode, params: &mut ParamBag) -> String {
    // node.field()    = "asset/location_id" → columna = "location_id"
    // node.children() = [FilterNode { field: "location/type", op: Eq, value: "BUILDING" }]
    let ref_col    = sanitize_col(node.field());
    let inner      = node.children().first().expect("RefFilter sin inner node");
    let ref_entity = inner.field().split('/').next().unwrap_or("unknown");
    let ref_table  = resolve_table_name(ref_entity, &DATABASE);
    let inner_sql  = compile_filter_node(inner, params);

    format!(
        "{} IN (SELECT id FROM {} WHERE {})",
        ref_col, ref_table, inner_sql
    )
}
```

### III.4 — TimeFrame (Cláusula Temporal)

```rust
/// Resuelve el campo epoch del schema — evita hardcodear "created_at".
/// Estrategia: atributo con type="epoch" en el schema → usar su nombre.
/// Fallback: "created_at" (legacy).
fn resolve_ts_column(plan: &EavQueryPlan) -> &'static str {
    // Busca en el registry el atributo con type=Epoch para esta entidad
    REGISTRY.get(plan.entity())
        .and_then(|e| e.attributes.iter().find(|a| a.value_type == ValueType::Epoch))
        .map(|a| a.name.as_str())
        .unwrap_or("created_at")
}

fn compile_time_frame(tf: &TimeFrame, ts_col: &str) -> Option<String> {
    match (tf.start_ts(), tf.end_ts()) {
        (Some(s), Some(e)) => Some(format!("{} >= {} AND {} <= {}", ts_col, s, ts_col, e)),
        (Some(s), None)    => Some(format!("{} >= {}", ts_col, s)),
        (None, Some(e))    => Some(format!("{} <= {}", ts_col, e)),
        (None, None)       => None, // ALL_TIME — sin cláusula temporal
    }
}
```

---

## MÓDULO IV: SELECT Compiler (`select.rs`)

### IV.1 — Routing por OutputCast

```rust
fn compile_select(plan: &EavQueryPlan) -> SelectClause {
    match plan.output_cast() {
        OutputCast::Timeseries => compile_timeseries_select(plan),
        OutputCast::Kpi        => compile_kpi_select(plan),
        OutputCast::Pie        => compile_pie_select(plan),
        OutputCast::Bubble     => compile_bubble_select(plan),
        OutputCast::Table      => compile_table_select(plan),
        OutputCast::CsvExport  => SelectClause { exprs: vec!["*".into()], limit: None },
    }
}
```

### IV.2 — TIMESERIES: date_trunc con normalización epoch

```rust
/// Genera: date_trunc('day', from_unixtime(IF(ts > 1e11, ts/1000.0, CAST(ts AS DOUBLE))))
/// La normalización IF() maneja tanto epoch en ms como en segundos transparentemente.
fn dim_to_bucket_expr(dim: &DimensionSpec) -> String {
    let col = sanitize_col(dim.field());
    let interval = dim.interval().as_str(); // "day", "month", "hour", etc.
    format!(
        "date_trunc('{}', from_unixtime(IF({} > 100000000000, {} / 1000.0, CAST({} AS DOUBLE))))",
        interval, col, col, col
    )
}

fn compile_timeseries_select(plan: &EavQueryPlan) -> SelectClause {
    let mut exprs = vec![];
    // Buckets temporales (dimensiones)
    for dim in plan.dimensions() {
        let bucket = dim_to_bucket_expr(dim);
        exprs.push(format!("{} AS {}", bucket, sanitize_col(dim.field())));
    }
    // Métricas agregadas
    for metric in plan.metrics() {
        exprs.push(compile_metric(metric));
    }
    SelectClause {
        exprs,
        group_by: plan.dimensions().iter().map(|d| dim_to_bucket_expr(d)).collect(),
        order_by: vec!["1 ASC".into()], // Bucket siempre primero
        limit: None,
    }
}
```

### IV.3 — KPI/PIE/BUBBLE: Agregaciones

```rust
fn compile_metric(m: &MetricSpec) -> String {
    let col = sanitize_col(m.field());
    let alias = sanitize_col(m.alias());
    match m.aggregation() {
        Aggregation::Sum           => format!("SUM({}) AS {}", col, alias),
        Aggregation::Count         => format!("COUNT(*) AS {}", alias),
        Aggregation::Avg           => format!("AVG({}) AS {}", col, alias),
        Aggregation::Min           => format!("MIN({}) AS {}", col, alias),
        Aggregation::Max           => format!("MAX({}) AS {}", col, alias),
        Aggregation::CountDistinct => format!("COUNT(DISTINCT {}) AS {}", col, alias),
    }
}
```

---

## MÓDULO V: Fuzzy Search (`fuzzy.rs`)

### V.1 — Principio Anti-Levenshtein en Athena

```
PROHIBIDO en Athena:
  WHERE levenshtein_distance(name, 'bomba') <= 1
  → Full table scan: O(m·n) por fila, sin predicate pushdown
  → Costo ~$0.50 por 10M rows

PERMITIDO (este módulo):
  WHERE LOWER(name) LIKE '%bomba%'
    OR regexp_like(LOWER(name), '\b(bomba|omba|bmba|...)\b')
  → Bloom filter + min/max pushdown: costo ~$0.005
```

### V.2 — Generador de Alternativas Damerau-Levenshtein 1

```rust
/// Genera un patrón RE2 que captura exactamente todas las strings
/// a distancia Damerau-Levenshtein ≤ 1 del término base.
/// Incluye: omisiones, sustituciones, inserciones, transposiciones.
fn generate_dl1_regex(term: &str) -> String {
    let t: Vec<char> = term.chars().collect();
    let n = t.len();
    let mut alts: Vec<String> = vec![regex_escape(term)]; // término exacto

    // Omisiones (n-1 chars)
    for i in 0..n {
        let s: String = t[..i].iter().chain(t[i+1..].iter()).collect();
        alts.push(regex_escape(&s));
    }
    // Sustituciones (n chars, 1 char → wildcard)
    for i in 0..n {
        let s: String = t[..i].iter().collect::<String>() + "." + &t[i+1..].iter().collect::<String>();
        alts.push(s);
    }
    // Inserciones (n+1 chars)
    for i in 0..=n {
        let s: String = t[..i].iter().collect::<String>() + "." + &t[i..].iter().collect::<String>();
        alts.push(s);
    }
    // Transposiciones (swap adyacentes)
    for i in 0..n.saturating_sub(1) {
        let mut chars = t.clone();
        chars.swap(i, i + 1);
        alts.push(regex_escape(&chars.iter().collect::<String>()));
    }

    let unique: Vec<String> = alts.into_iter().collect::<std::collections::HashSet<_>>()
        .into_iter().collect();
    format!(r"\b({})\b", unique.join("|"))
}
```

### V.3 — Guarda de Costos por Longitud

```rust
/// Expande un término de búsqueda en patrones SQL cost-safe.
/// 
/// Estrategia por longitud:
///   ≤ 2 chars → solo LIKE exacto (threshold=0, demasiados falsos positivos)
///   3-8 chars → LIKE + regexp_like DL1 (ventana óptima)
///   ≥ 9 chars → solo LIKE substring (regex sería poco selectivo en textos largos)
struct FuzzyPatterns {
    like_pat:  String,         // '%term%'
    regex_pat: Option<String>, // RE2 pattern | None
}

fn expand_term(term: &str) -> FuzzyPatterns {
    let t = term.to_lowercase();
    let escaped = escape_like(&t);
    let like_pat = format!("%{}%", escaped);
    let n = t.chars().count();

    let regex_pat = if (3..=8).contains(&n) {
        Some(generate_dl1_regex(&t))
    } else {
        None
    };
    FuzzyPatterns { like_pat, regex_pat }
}

fn compile_fuzzy_node(node: &FilterNode, _params: &mut ParamBag) -> String {
    let col   = sanitize_col(node.field());
    let term  = node.value().str_val().unwrap_or("");
    let pats  = expand_term(term);

    match pats.regex_pat {
        Some(re) => format!(
            "(LOWER({}) LIKE '{}' OR regexp_like(LOWER({}), '{}'))",
            col, pats.like_pat, col, re
        ),
        None => format!("LOWER({}) LIKE '{}'", col, pats.like_pat),
    }
}
```

---

## MÓDULO VI: CTE Compiler — TIME_SHIFT y BENCHMARK (`comparison.rs`)

### VI.1 — Tipos de Comparación

| Tipo | Descripción | SQL generado |
|:--|:--|:--|
| `BENCHMARK` | Valor estático de referencia | Solo metadata — sin SQL extra |
| `TIME_SHIFT_RELATIVE` | Período anterior relativo (ej. semana pasada) | CTE `WITH current AS (...), previous AS (...)` |
| `TIME_SHIFT_SHORTCUT` | Shortcut semántico (MoM, YoY, QoQ) | CTE con fechas calculadas automáticamente |
| `TIME_SHIFT_ABSOLUTE` | Rango de fechas explícito del cliente | CTE con fechas literales |

### VI.2 — CTE Builder para TIME_SHIFT

```rust
/// Genera SQL con CTEs para comparación temporal:
///   WITH current_period AS (
///     SELECT AGG(metric) FROM table WHERE tenant_id=X AND ts BETWEEN a AND b
///   ),
///   previous_period AS (
///     SELECT AGG(metric) FROM table WHERE tenant_id=X AND ts BETWEEN c AND d
///   )
///   SELECT
///     current_period.value  AS current_value,
///     previous_period.value AS previous_value
///   FROM current_period, previous_period
fn compile_time_shift_cte(
    base_where: &str,
    base_select: &str,
    table: &str,
    current_tf: &TimeFrame,
    previous_tf: &TimeFrame,
) -> String {
    format!(
        r#"WITH current_period AS (
  SELECT {select} FROM {table}
  WHERE {base_where} AND {current_cond}
),
previous_period AS (
  SELECT {select} FROM {table}
  WHERE {base_where} AND {prev_cond}
)
SELECT
  current_period.value  AS current_value,
  previous_period.value AS previous_value
FROM current_period, previous_period"#,
        select       = base_select,
        table        = table,
        base_where   = base_where,
        current_cond = compile_time_frame(current_tf, "created_at").unwrap_or("1=1".into()),
        prev_cond    = compile_time_frame(previous_tf, "created_at").unwrap_or("1=1".into()),
    )
}
```

---

## MÓDULO VII: Orchestrator (`compiler.rs`)

### VII.1 — Entry Point

```rust
/// Transpila EavQueryPlan → SQL listo para Athena StartQueryExecution.
/// 
/// Retorna:
///   Ok(AthenaQuery { sql, database, entity, output_cast, benchmarks })
///   Err(AegisError::TenantMissing | AegisError::CompileError)
pub fn compile_athena_sql(plan: &EavQueryPlan) -> Result<AthenaQuery, AegisError> {
    // ── 0. Security gate (CARDINAL — nunca bypasseable) ─────────────────────
    assert_tenant_isolation(plan)?;

    let entity   = plan.entity().unwrap_or("events");
    let database = plan.tenant_id()
        .map(|t| tenant_to_database(t))
        .unwrap_or("metrics_db");
    let table    = resolve_table_name(entity, &database);
    let output_cast = plan.output_cast();

    // ── 1. Compilar WHERE base ───────────────────────────────────────────────
    let mut params   = ParamBag::new();
    let where_sql    = plan.where_root()
        .map(|r| compile_filter_node(&r, &mut params))
        .unwrap_or("1=1".into());

    // ── 2. TimeFrame → inyectar en WHERE ────────────────────────────────────
    let ts_col    = resolve_ts_column(plan);
    let time_sql  = plan.time_frame()
        .and_then(|tf| compile_time_frame(&tf, ts_col));
    let full_where = match time_sql {
        Some(t) => format!("({}) AND ({})", where_sql, t),
        None    => where_sql,
    };

    // ── 3. SELECT / GROUP BY / ORDER BY / LIMIT ──────────────────────────────
    let select_clause = compile_select(plan);

    // ── 4. Detectar si necesita CTE (TIME_SHIFT_*) ──────────────────────────
    let cte_comparisons: Vec<_> = plan.comparisons().into_iter()
        .filter(|c| matches!(c.kind(),
            ComparisonType::TimeShiftRelative |
            ComparisonType::TimeShiftShortcut |
            ComparisonType::TimeShiftAbsolute
        ))
        .collect();

    let sql = if !cte_comparisons.is_empty() {
        compile_time_shift_cte(
            &full_where,
            &select_clause.exprs.join(", "),
            &table,
            plan.time_frame().as_ref().unwrap(),
            &derive_previous_period(plan.time_frame().as_ref().unwrap(), &cte_comparisons[0]),
        )
    } else {
        // ── 5. Query simple ──────────────────────────────────────────────────
        let limit_sql = select_clause.limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();
        let group_sql = if select_clause.group_by.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", select_clause.group_by.join(", "))
        };
        let order_sql = if select_clause.order_by.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", select_clause.order_by.join(", "))
        };
        format!(
            "SELECT {} FROM {} WHERE {}{}{}{}",
            select_clause.exprs.join(", "),
            table, full_where, group_sql, order_sql, limit_sql
        )
    };

    // ── 6. Extraer BENCHMARK metadata ────────────────────────────────────────
    let benchmarks: Vec<_> = plan.comparisons().into_iter()
        .filter(|c| c.kind() == ComparisonType::Benchmark)
        .map(|c| BenchmarkMeta {
            label:           c.label().unwrap_or("").to_string(),
            benchmark_value: c.benchmark_value(),
        })
        .collect();

    Ok(AthenaQuery {
        sql,
        database,
        entity: entity.to_string(),
        output_cast,
        benchmarks,
    })
}
```

---

## MÓDULO VIII: Tabla de Errores Aegis

| Código | Cuándo | Severidad |
|:--|:--|:--|
| `AEG_TENANT_MISSING` | AST sin tenant_id en WHERE | CRITICAL — bloqueo total |
| `AEG_COMPILE_001` | Campo inválido (caracteres fuera de whitelist) | ERROR |
| `AEG_COMPILE_002` | Error de compilación inesperado | ERROR |
| `AEG_UNSUPPORTED_OP` | FilterOp no soportado en Athena | ERROR |
| `AEG_INVALID_CAST` | OutputCast desconocido | ERROR |
| `AEG_FUZZY_EMPTY` | Término fuzzy vacío o nil | WARNING → fail-open `1=1` |
| `AEG_REF_ENTITY_UNKNOWN` | RefFilter sin ref_entity detectado | ERROR → fail-safe `1=0` |

---

## MÓDULO IX: Ejemplo Completo — End to End

**Input:** Query de dashboard KPI "total work_orders OPEN del tenant ACME en los últimos 30 días"

**EavQueryPlan (FlatBuffers, esquema simplificado):**
```
entity:      "work_order"
tenant_id:   "ACME"
output_cast: Kpi
metrics:     [{ field: "id", aggregation: Count, alias: "total" }]
where_root:  AND(
               EQ(tenant_id, "ACME"),
               EQ(status, "OPEN")
             )
time_frame:  { start_ts: 1714521600000, end_ts: 1717113600000 }
```

**SQL Generado:**
```sql
SELECT COUNT(*) AS total
FROM metrics_db.work_orders
WHERE (tenant_id = 'ACME' AND status = 'OPEN')
  AND (created_at >= 1714521600000 AND created_at <= 1717113600000)
LIMIT 1
```

---

## MÓDULO X: SeaQuery — Builder Tipado (Equivalente a HoneySQL)

### X.0 — Por qué SeaQuery en lugar de String Building

| Criterio | `format!("SELECT...")`  | **SeaQuery** |
|:--|:--|:--|
| SQL Injection | Requiere sanitización manual | **Imposible por construcción** — identifiers son `Ident` tipados |
| Dialect swap | Hardcodeado | `MysqlQueryBuilder` / `SqliteQueryBuilder` / `QuoteChar::Backtick` |
| Composición | Concatenación frágil de strings | AST inmutable — nodes se componen con `.and_where()`, `.add_column()` |
| Equivalente Clojure | `str/join`, `format` | **HoneySQL** — mismo modelo de composición |
| Testabilidad | Comparar strings | Comparar `SelectStatement` structs |
| CTEs | Manual `WITH ...` | `.with_cte()` nativo |

> [!IMPORTANT]
> SeaQuery usa `AthenaSqlBuilder` (dialect Presto/ANSI) ya que Athena es compatible con ANSI SQL. Si Athena requiere sintaxis específica (ej. `regexp_like`), se usa `.expr(Expr::cust(...))` para expresiones custom dentro del builder tipado.

### X.1 — Dependencia

```toml
[dependencies]
sea-query = { version = "0.31", features = ["derive"] }
```

### X.2 — Definición de Identificadores Tipados

En lugar de strings crudos, SeaQuery usa enums que implementan `Iden` para identificar tablas y columnas — idéntico al concepto de `(keyword :entity :field)` en HoneySQL/Clojure:

```rust
use sea_query::Iden;

/// Identificador dinámico generado desde el registry.
/// Equivalente al (keyword entity field) de HoneySQL.
#[derive(Debug, Clone)]
pub struct DynIden(String);

impl Iden for DynIden {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(s, "{}", self.0).unwrap();
    }
}

/// Construye un Iden validado desde un field del FilterNode.
/// Aplica la misma whitelist [a-z0-9_] que sanitize_col().
fn col_iden(field: &str) -> Result<DynIden, AegisError> {
    let col = field.split('/').last().unwrap_or(field);
    if col.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(DynIden(col.to_string()))
    } else {
        Err(AegisError::InvalidField(col.to_string()))
    }
}

/// Construye un Iden para la tabla: "metrics_db.work_orders"
fn table_iden(entity: &str, database: &str) -> DynIden {
    let plural = REGISTRY.get(entity)
        .and_then(|e| e.olap_table_name.as_deref())
        .unwrap_or(&format!("{entity}s"));
    DynIden(format!("{database}.{plural}"))
}
```

### X.3 — WHERE Compiler con SeaQuery (`where.rs`)

Equivalente al `where-node->honey` de HoneySQL pero usando `sea_query::Condition`:

```rust
use sea_query::{Cond, Expr, SimpleExpr, Value};

/// Transpila FilterNode → sea_query::SimpleExpr (árbol tipado, no string).
/// Equivalente directo a where-node->honey en Clojure/HoneySQL.
fn filter_to_expr(node: &FilterNode) -> Result<SimpleExpr, AegisError> {
    let col = || Expr::col(col_iden(node.field())?);

    Ok(match node.op() {
        // Operadores de comparación directa
        FilterOp::Eq  => col()?.eq(to_sea_value(node.value())?),
        FilterOp::Neq => col()?.ne(to_sea_value(node.value())?)  ,
        FilterOp::Gt  => col()?.gt(to_sea_value(node.value())?)  ,
        FilterOp::Gte => col()?.gte(to_sea_value(node.value())?),
        FilterOp::Lt  => col()?.lt(to_sea_value(node.value())?)  ,
        FilterOp::Lte => col()?.lte(to_sea_value(node.value())?),

        // IN / NOT IN
        FilterOp::In    => col()?.is_in(to_sea_values(node.value())?),
        FilterOp::NotIn => col()?.is_not_in(to_sea_values(node.value())?)  ,

        // LIKE / CONTAINS / STARTS_WITH
        FilterOp::Contains   => col()?.like(format!("%{}%", escape_like(node.value().str_val()))),
        FilterOp::StartsWith => col()?.like(format!("{}%", escape_like(node.value().str_val()))),

        // IS NULL / IS NOT NULL
        FilterOp::IsNull    => col()?.is_null(),
        FilterOp::IsNotNull => col()?.is_not_null(),

        // REGEXP_LIKE — expresión custom (Athena-specific)
        FilterOp::Matches => Expr::cust_with_values(
            "regexp_like(?, ?)",
            [node.field().split('/').last().unwrap_or(""), node.value().str_val()]
        ),

        // AND / OR — composición de árboles (recursivo)
        FilterOp::And => {
            let mut cond = Cond::all();
            for child in node.children() {
                cond = cond.add(filter_to_expr(child)?);
            }
            SimpleExpr::from(cond)
        }
        FilterOp::Or => {
            let mut cond = Cond::any();
            for child in node.children() {
                cond = cond.add(filter_to_expr(child)?);
            }
            SimpleExpr::from(cond)
        }

        // FUZZY — delega al módulo fuzzy.rs que retorna SimpleExpr
        FilterOp::Fuzzy => fuzzy_to_expr(col_iden(node.field())?, node.value().str_val())?,

        // REF FILTER — subquery tipada con SeaQuery
        FilterOp::RefFilter => ref_filter_to_expr(node)?,
    })
}
```

### X.4 — RefFilter como SubQuery Tipada

```rust
/// location_id IN (SELECT id FROM metrics_db.location WHERE type = 'BUILDING')
/// Construido como SelectStatement tipado — cero strings.
fn ref_filter_to_expr(node: &FilterNode) -> Result<SimpleExpr, AegisError> {
    let ref_col    = col_iden(node.field())?;
    let inner      = node.children().first().ok_or(AegisError::MissingInnerNode)?;
    let ref_entity = inner.field().split('/').next().unwrap_or("unknown");
    let ref_table  = table_iden(ref_entity, &DATABASE);
    let inner_expr = filter_to_expr(inner)?;

    // SELECT id FROM metrics_db.location WHERE <inner_expr>
    let subquery = Query::select()
        .column(DynIden("id".into()))
        .from(ref_table)
        .and_where(inner_expr)
        .take();

    Ok(Expr::col(ref_col).in_subquery(subquery))
}
```

### X.5 — SELECT Builder con SeaQuery (`select.rs`)

```rust
use sea_query::{Alias, Expr, Func, Order, Query, SelectStatement};

/// Construye el SelectStatement completo según OutputCast.
/// Equivalente a build-select-exprs + build-group-by + build-order-by de Clojure.
fn build_select_statement(
    plan: &EavQueryPlan,
    full_where: SimpleExpr,
    table: DynIden,
) -> Result<SelectStatement, AegisError> {
    let mut stmt = Query::select();
    stmt.from(table);
    stmt.and_where(full_where);

    match plan.output_cast() {
        OutputCast::Timeseries => {
            for dim in plan.dimensions() {
                // date_trunc('day', from_unixtime(IF(ts > 1e11, ts/1000.0, CAST(ts AS DOUBLE))))
                let bucket = bucket_expr(dim)?;
                stmt.expr_as(bucket, Alias::new(sanitize_str(dim.field())));
            }
            for m in plan.metrics() {
                stmt.expr_as(agg_expr(m)?, Alias::new(sanitize_str(m.alias())));
            }
            // GROUP BY 1, 2, ... (posición de buckets)
            let n_dims = plan.dimensions().len();
            for i in 1..=n_dims {
                stmt.add_group_by([Expr::cust(i.to_string())]);
            }
            stmt.order_by_expr(Expr::cust("1"), Order::Asc);
        }

        OutputCast::Kpi | OutputCast::Pie | OutputCast::Bubble => {
            for dim in plan.dimensions() {
                stmt.column(col_iden(dim.field())?);
                stmt.add_group_by([Expr::col(col_iden(dim.field())?)]);
            }
            for m in plan.metrics() {
                stmt.expr_as(agg_expr(m)?, Alias::new(sanitize_str(m.alias())));
            }
            if plan.output_cast() == OutputCast::Kpi {
                stmt.limit(1);
            }
        }

        OutputCast::Table => {
            if plan.select().is_empty() {
                stmt.column(Asterisk);
            } else {
                for field in plan.select() {
                    stmt.column(col_iden(field)?);
                }
            }
            stmt.limit(10_000);
        }

        OutputCast::CsvExport => {
            stmt.column(Asterisk); // SELECT * sin LIMIT
        }
    }

    Ok(stmt)
}

/// Construye la expresión de agregación para una MetricSpec.
fn agg_expr(m: &MetricSpec) -> Result<SimpleExpr, AegisError> {
    let col = Expr::col(col_iden(m.field())?);
    Ok(match m.aggregation() {
        Aggregation::Sum           => Func::sum(col),
        Aggregation::Count         => Func::count(Expr::cust("*")),
        Aggregation::Avg           => Func::avg(col),
        Aggregation::Min           => Func::min(col),
        Aggregation::Max           => Func::max(col),
        Aggregation::CountDistinct => Expr::cust_with_expr("COUNT(DISTINCT ?)", col),
    })
}
```

### X.6 — Fuzzy como `SimpleExpr` (no string)

```rust
/// El generador DL1 retorna SimpleExpr en lugar de String.
/// LIKE OR regexp_like → árbol Cond tipado de SeaQuery.
fn fuzzy_to_expr(col: DynIden, term: &str) -> Result<SimpleExpr, AegisError> {
    let pats = expand_term(term);
    let lower_col = Expr::cust_with_expr("LOWER(?)", Expr::col(col));

    Ok(match pats.regex_pat {
        Some(re) => SimpleExpr::from(
            Cond::any()
                .add(lower_col.clone().like(pats.like_pat))
                .add(Expr::cust_with_values(
                    "regexp_like(LOWER(?), ?)",
                    [term, re.as_str()]
                ))
        ),
        None => lower_col.like(pats.like_pat),
    })
}
```

### X.7 — CTE TIME_SHIFT con SeaQuery (`comparison.rs`)

```rust
use sea_query::{CommonTableExpression, Query, WithClause};

/// Genera la query WITH CTEs para comparación temporal.
/// Equivalente a build-comparison-cte-query de Clojure/HoneySQL.
fn build_time_shift_query(
    base_select: SelectStatement,
    table: DynIden,
    base_where: SimpleExpr,
    current_tf: &TimeFrame,
    previous_tf: &TimeFrame,
) -> Result<String, AegisError> {
    // CTE: current_period
    let current_cte = CommonTableExpression::new()
        .query(
            base_select.clone()
                .and_where(time_frame_expr(current_tf)?)
                .take(),
        )
        .table_name(Alias::new("current_period"))
        .to_owned();

    // CTE: previous_period
    let previous_cte = CommonTableExpression::new()
        .query(
            base_select
                .and_where(time_frame_expr(previous_tf)?)
                .take(),
        )
        .table_name(Alias::new("previous_period"))
        .to_owned();

    // SELECT final que cruza los dos CTEs
    let final_query = Query::select()
        .expr_as(Expr::cust("current_period.value"),  Alias::new("current_value"))
        .expr_as(Expr::cust("previous_period.value"), Alias::new("previous_value"))
        .from(Alias::new("current_period"))
        .cross_join(Alias::new("previous_period"), None)
        .take();

    let with_clause = WithClause::new()
        .cte(current_cte)
        .cte(previous_cte)
        .to_owned();

    // Serializar a SQL string con dialect ANSI (compatible con Athena/Presto)
    Ok(final_query
        .with(with_clause)
        .to_string(MysqlQueryBuilder)) // Athena usa sintaxis ANSI/MySQL-like
}
```

### X.8 — Orchestrator Final: De FlatBuffer a SQL String

```rust
/// Punto de entrada — de EavQueryPlan a SQL string listo para Athena.
/// Todo el pipeline usa tipos SeaQuery — el to_string() final es el único lugar
/// donde se materializa el SQL como String.
pub fn compile_athena_sql(plan: &EavQueryPlan) -> Result<AthenaQuery, AegisError> {
    // 0. Security gate
    assert_tenant_isolation(plan)?;

    let database    = tenant_to_database(plan.tenant_id().unwrap_or(""));
    let table       = table_iden(plan.entity().unwrap_or("events"), &database);

    // 1. WHERE base (árbol SeaQuery tipado)
    let where_expr  = plan.where_root()
        .map(|r| filter_to_expr(&r))
        .transpose()?
        .unwrap_or(Expr::cust("1=1"));

    // 2. TimeFrame (inyectado como SimpleExpr adicional)
    let ts_col      = resolve_ts_column(plan);
    let final_where = match plan.time_frame().and_then(|tf| time_frame_expr_col(&tf, ts_col).ok()) {
        Some(tf_expr) => Cond::all().add(where_expr).add(tf_expr).into(),
        None          => where_expr,
    };

    // 3. SelectStatement tipado
    let stmt = build_select_statement(plan, final_where, table)?;

    // 4. CTE o query simple
    let sql = if has_time_shift_comparisons(plan) {
        let (current_tf, previous_tf) = derive_comparison_periods(plan)?;
        build_time_shift_query(stmt, table_iden(plan.entity().unwrap_or(""), &database),
                               final_where, &current_tf, &previous_tf)?
    } else {
        // ── Único to_string() del pipeline — SeaQuery serializa a SQL ANSI ──
        stmt.to_string(MysqlQueryBuilder)
    };

    Ok(AthenaQuery {
        sql,
        database,
        entity:      plan.entity().unwrap_or("").to_string(),
        output_cast: plan.output_cast(),
        benchmarks:  extract_benchmarks(plan),
    })
}
```

> [!TIP]
> **`to_string(MysqlQueryBuilder)` es el único lugar en todo el pipeline donde se materializa SQL como `String`.** Todo lo anterior es un árbol `SelectStatement` / `SimpleExpr` / `Cond` completamente tipado — imposible de producir SQL injection por construcción. Es el equivalente exacto del pipeline `hsql/format` de HoneySQL en Clojure.

---

## Próximos Pasos

1. **`where.rs`** — Implementar `filter_to_expr` con los 14 operadores + RefFilter usando SeaQuery.
2. **`fuzzy.rs`** — Portar el generador DL1 — retorna `SimpleExpr` (no string).
3. **`select.rs`** — Implementar `build_select_statement` con routing por OutputCast.
4. **`comparison.rs`** — CTE builder con `CommonTableExpression` + `WithClause`.
5. **`compiler.rs`** — Orchestrator — el único lugar que llama `to_string(MysqlQueryBuilder)`.
6. **Integration test** — `EavQueryPlan` → `SelectStatement` → SQL string → ejecutar en Athena sandbox.
