# Metri Formula Engine — Contexto Profundo para Modelos de Lenguaje

> **Propósito:** Este documento proporciona el contexto técnico completo que un modelo de lenguaje (AWS Nova, Claude Sonnet, GPT) necesita para generar, validar, depurar y explicar fórmulas dentro del ecosistema Metri CMMS.
>
> **Audiencia:** Modelos de IA que asisten a usuarios del CMMS Metri en la creación de dashboards, KPIs, reportes y métricas calculadas.

---

## 1. ¿Qué es el Formula Engine?

El Motor de Fórmulas de Metri es un **evaluador de expresiones matemáticas escalares** implementado nativamente en Rust. Procesa cálculos derivados **fila por fila** sobre datos de activos, órdenes de trabajo, ubicaciones y lecturas de medidor de un sistema CMMS (Computerized Maintenance Management System).

### 1.1 — Concepto Fundamental: Fórmulas ≠ Agregaciones

Esta distinción es **CRÍTICA** y la fuente más común de errores:

| Concepto | Alcance | Ejemplos | Dónde se define |
|:---|:---|:---|:---|
| **Fórmula (escalar)** | Calcula un valor para **cada fila individual** | `health_score * 0.8 + criticality_index * 0.2` | Campo `measures[].formula` |
| **Agregación** | Reduce **múltiples filas** a un solo valor | `AVG`, `SUM`, `COUNT`, `MIN`, `MAX`, `MEDIAN` | Campo `metrics[].aggregation` |

**REGLA DE ORO:** Las funciones `AVG`, `SUM`, `COUNT`, `MIN`, `MAX`, `MEDIAN`, `STD_DEV`, `VARIANCE`, `PERCENTILE_*`, `CORRELATION`, `LINEAR_REGRESSION` **NO** son funciones válidas dentro de una fórmula. Son operadores de agregación que se definen por separado en la estructura de métricas.

### 1.2 — Pipeline de Dos Fases

Cuando un usuario necesita "el promedio de un cálculo derivado", se ejecuta un pipeline secuencial:

```
Fase 1: Formula (escalar, fila por fila)
  → Para cada fila: calcula la fórmula y añade el resultado como una columna virtual
  
Fase 2: Aggregation (conjunto, todas las filas)  
  → Toma la columna virtual creada en Fase 1 y aplica SUM/AVG/COUNT/etc.
```

**Ejemplo concreto — "Promedio de la eficiencia operativa":**

```json
{
  "measures": [
    {
      "name": "eficiencia",
      "formula": "(asset/uptime_hours / NULLIF(asset/total_hours, 0)) * 100"
    }
  ],
  "metrics": [
    {
      "entity": "asset",
      "attribute": "eficiencia",
      "aggregation": "AVG",
      "name": "avg_eficiencia"
    }
  ],
  "output_cast": "KPI"
}
```

---

## 2. Sintaxis de Fórmulas

### 2.1 — Gramática EBNF

```ebnf
formula       ::= expression
expression    ::= term ( ( '+' | '-' ) term )*
term          ::= power ( ( '*' | '/' | '%' ) power )*
power         ::= unary ( '^' unary )*
unary         ::= ( '-' )? factor
factor        ::= function_call | '(' expression ')' | literal | variable

function_call ::= FUNCTION_NAME '(' arg_list ')'
arg_list      ::= expression ( ',' expression )*

variable      ::= IDENTIFIER ( '/' IDENTIFIER )?
literal       ::= [0-9]+ ( '.' [0-9]+ )?
FUNCTION_NAME ::= [A-Z_]+
IDENTIFIER    ::= [a-zA-Z_] [a-zA-Z0-9_-]*
```

### 2.2 — Operadores Aritméticos

| Operador | Símbolo | Precedencia | Asociatividad | Comportamiento especial |
|:---|:---:|:---:|:---:|:---|
| Suma | `+` | 1 | Izquierda | — |
| Resta | `-` | 1 | Izquierda | — |
| Multiplicación | `*` | 2 | Izquierda | — |
| División | `/` | 2 | Izquierda | División por cero → `NaN` (se omite del resultado) |
| Módulo | `%` | 2 | Izquierda | Módulo por cero → `NaN` |
| Exponenciación | `^` | 3 | Derecha | `base ^ exponente` equivale a `POWER(base, exponente)` |
| Negación unaria | `-` | 4 | Derecha | Detectado automáticamente por contexto |

**Precedencia de evaluación (menor → mayor):**
```
1.  +  -            (suma, resta)
2.  *  /  %          (multiplicación, división, módulo)
3.  ^                (exponenciación)
4.  - (unario)       (negación)
5.  ()               (paréntesis — máxima precedencia)
6.  FUNC()           (llamadas a función)
```

### 2.3 — Variables (Referencias a Atributos)

Las variables referencian columnas del dataset. Soportan dos formatos:

| Formato | Ejemplo | Resolución |
|:---|:---|:---|
| **Bare** (sin namespace) | `health_score` | Busca directamente `row["health_score"]` |
| **Namespace** (dominio/atributo) | `asset/health_score` | Busca `row["asset/health_score"]` → fallback `row["health_score"]` |
| **Con guión** | `asset/health-score` | Se normaliza a `health_score` (guiones → underscores) |

**Restricciones:**
- Solo caracteres alfanuméricos, `_`, `-` y `/`
- No pueden empezar con un dígito
- El separador `/` solo aparece una vez (un nivel de namespace)
- Case-sensitive: `Revenue` ≠ `revenue`
- El namespace es semántico (indica dominio de negocio), no altera la resolución

### 2.4 — Literales Numéricos

| Tipo | Ejemplos | Notas |
|:---|:---|:---|
| Entero | `100`, `0`, `42` | — |
| Decimal | `3.14`, `0.5`, `100.00` | Punto decimal, no coma |
| Negativo | `-1`, `-0.05` | Precedido por negación unaria |

**No soportado:** notación científica (`1e6`), hexadecimal (`0xFF`), separadores de miles (`1_000`).

---

## 3. Catálogo Completo de Funciones

Todas las funciones son **case-insensitive** (`abs`, `ABS`, `Abs` son equivalentes).

### 3.1 — Funciones Matemáticas

| Función | Aridad | Descripción | Ejemplo | Resultado con `x=12, y=3` |
|:---|:---:|:---|:---|:---|
| `ABS(x)` | 1 | Valor absoluto | `ABS(-4.5)` | `4.5` |
| `ROUND(x)` | 1-2 | Redondeo. Segundo arg opcional = decimales | `ROUND(3.456, 2)` | `3.46` |
| `CEIL(x)` | 1 | Redondeo hacia arriba (techo) | `CEIL(3.1)` | `4.0` |
| `FLOOR(x)` | 1 | Redondeo hacia abajo (piso) | `FLOOR(3.9)` | `3.0` |
| `POWER(x, n)` | 2 | Potencia x^n | `POWER(2, 8)` | `256.0` |
| `SQRT(x)` | 1 | Raíz cuadrada. `x < 0` → indefinido | `SQRT(144)` | `12.0` |
| `LOG(x)` | 1 | Logaritmo natural (ln). `x ≤ 0` → indefinido | `LOG(2.718281828)` | `≈1.0` |
| `LOG10(x)` | 1 | Logaritmo base 10. `x ≤ 0` → indefinido | `LOG10(1000)` | `3.0` |
| `MOD(x, y)` | 2 | Residuo de la división. `y = 0` → indefinido | `MOD(17, 5)` | `2.0` |
| `SIGN(x)` | 1 | Signo: retorna `-1`, `0`, o `1` | `SIGN(-42)` | `-1.0` |

### 3.2 — Funciones de Protección contra Nulos y Ceros

| Función | Aridad | Descripción | Caso de uso crítico |
|:---|:---:|:---|:---|
| `NULLIF(x, y)` | 2 | Retorna `NULL` si `x == y`, sino retorna `x` | **División segura:** `valor / NULLIF(denominador, 0)` |
| `COALESCE(x, y, ...)` | 2+ | Retorna el primer argumento que NO sea `NULL`/`NaN` | **Valores por defecto:** `COALESCE(asset/score, 0)` |
| `IF(cond, then, else)` | 3 | Si `cond ≠ 0` retorna `then`, sino retorna `else` | **Condicionales:** `IF(revenue, profit/revenue * 100, 0)` |

### 3.3 — Funciones de Rango y Contención

| Función | Aridad | Descripción | Ejemplo |
|:---|:---:|:---|:---|
| `GREATEST(x, y, ...)` | 2+ | El mayor de todos los argumentos | `GREATEST(score_a, score_b, score_c)` |
| `LEAST(x, y, ...)` | 2+ | El menor de todos los argumentos | `LEAST(costo_a, costo_b)` |
| `CLAMP(x, lo, hi)` | 3 | Restringe `x` al rango `[lo, hi]` | `CLAMP(health_score, 0, 100)` |

### 3.4 — Funciones NO Disponibles en Fórmulas

Las siguientes funciones **NO EXISTEN** en el motor de fórmulas porque son agregaciones de conjunto, no operaciones escalares:

❌ `AVG()` ❌ `SUM()` ❌ `COUNT()` ❌ `MIN()` ❌ `MAX()` ❌ `MEDIAN()` ❌ `STD_DEV()` ❌ `VARIANCE()` ❌ `PERCENTILE_90/95/99()` ❌ `CORRELATION()` ❌ `LINEAR_REGRESSION()`

Si el usuario pide "promediar" o "sumar" valores, usar el campo `metrics[].aggregation` combinado con `output_cast: "KPI"`.

---

## 4. Estructura de la Petición gRPC (`QueryRequest`)

### 4.1 — Anatomía del Request

```json
{
  "tenant_id": "mi_empresa",
  "queries": {
    "nombre_unico_de_la_consulta": {
      "tenant_id": "mi_empresa",
      "entity": "asset",
      
      "output_cast": "KPI | TABLE | TIMESERIES | PIE | BUBBLE | CSV_EXPORT",
      
      "measures": [
        {
          "name": "alias_columna_resultado",
          "formula": "expresión escalar fila-por-fila"
        }
      ],
      
      "metrics": [
        {
          "entity": "asset",
          "attribute": "nombre_columna_o_alias_measure",
          "aggregation": "AVG | SUM | COUNT | MIN | MAX | MEDIAN | STD_DEV | VARIANCE",
          "name": "alias_metrica_resultado"
        }
      ],
      
      "dimensions": [
        {
          "entity": "asset",
          "attribute": "status",
          "interval": "day | week | month"
        }
      ],
      
      "filters": [
        {
          "criteria": {
            "field": "status",
            "op_ref": "EQ",
            "value": { "string_val": "ACTIVE" }
          }
        }
      ],
      
      "time_frame": {
        "type": "LAST_N_DAYS",
        "n_value": 30,
        "timezone": "America/Bogota"
      },
      
      "limit": 100,
      "sort": [{ "field": "health_score", "descending": true }]
    }
  }
}
```

### 4.2 — Tipos de OutputCast y Cuándo Usarlos

| OutputCast | Cuándo usar | Requiere metrics | Requiere dimensions |
|:---|:---|:---:|:---:|
| `TABLE` | Tablas de datos crudos, listados, exportaciones | Opcional | Opcional |
| `KPI` | Indicadores numéricos (1 sola fila agregada) | **Sí** | No |
| `TIMESERIES` | Gráficas de línea/área con eje temporal | **Sí** | **Sí** (con `interval`) |
| `PIE` | Gráficas de pastel, distribuciones | **Sí** | **Sí** |
| `BUBBLE` | Gráficas de dispersión/burbuja | **Sí** | **Sí** |
| `CSV_EXPORT` | Exportación masiva sin límites | No | No |

### 4.3 — Agregaciones Disponibles en `metrics[].aggregation`

| Valor | Descripción | Ejemplo de uso CMMS |
|:---|:---|:---|
| `COUNT` | Cantidad de registros | Total de órdenes de trabajo |
| `SUM` | Suma aritmética | Costo total de mantenimiento |
| `AVG` | Promedio aritmético | Salud promedio de la flota |
| `MIN` | Valor mínimo | Lectura más baja del medidor |
| `MAX` | Valor máximo | Costo máximo de una OT |
| `MEDIAN` | Mediana (percentil 50) | Tiempo mediano de resolución |
| `STD_DEV` | Desviación estándar | Variabilidad en lecturas de sensor |
| `VARIANCE` | Varianza muestral | Dispersión de costos |
| `PERCENTILE_90` | Percentil 90 | SLA: 90% de OTs resueltas en < X horas |
| `PERCENTILE_95` | Percentil 95 | Umbral de latencia |
| `PERCENTILE_99` | Percentil 99 | Outlier detection |
| `CORRELATION` | Correlación de Pearson | Relación salud ↔ costo |
| `LINEAR_REGRESSION` | Pendiente de regresión | Predicción de degradación |

---

## 5. Entidades del Dominio CMMS

Estas son las entidades principales disponibles en Metri y sus atributos más comunes:

| Entidad | Descripción | Atributos típicos |
|:---|:---|:---|
| `asset` | Activos físicos (equipos, máquinas) | `health_score`, `criticality`, `status`, `current_meter_reading`, `area_value`, `manufacturer`, `serial_number`, `location_id` |
| `work_order` | Órdenes de trabajo | `status`, `priority`, `total_cost`, `labor_cost`, `parts_cost`, `estimated_hours`, `actual_hours`, `assigned_to` |
| `location` | Ubicaciones físicas | `type`, `area_value`, `parent_id`, `latitude`, `longitude` |
| `meter_reading` | Lecturas de medidores/sensores | `reading_value`, `meter_type`, `asset_id`, `timestamp` |
| `part` | Repuestos e inventario | `quantity`, `unit_cost`, `reorder_point`, `stock_level` |
| `preventive_plan` | Planes de mantenimiento preventivo | `frequency`, `interval`, `last_execution`, `next_due_date` |

---

## 6. Patrones de Uso Comunes (Recetas)

### 6.1 — KPI Simple: Promedio de Salud

```json
{
  "output_cast": "KPI",
  "metrics": [{ "entity": "asset", "attribute": "health_score", "aggregation": "AVG", "name": "avg_health" }]
}
```

### 6.2 — KPI con Fórmula Derivada: Eficiencia Operativa

```json
{
  "output_cast": "KPI",
  "measures": [{ "name": "efficiency", "formula": "(asset/uptime_hours / NULLIF(asset/total_hours, 0)) * 100" }],
  "metrics": [{ "entity": "asset", "attribute": "efficiency", "aggregation": "AVG", "name": "avg_efficiency" }]
}
```

### 6.3 — Tabla con Columna Calculada

```json
{
  "output_cast": "TABLE",
  "measures": [{ "name": "roi", "formula": "(asset/revenue - work_order/total_cost) / NULLIF(work_order/total_cost, 0) * 100" }]
}
```

### 6.4 — Timeseries: Costo Mensual de Mantenimiento

```json
{
  "output_cast": "TIMESERIES",
  "dimensions": [{ "entity": "work_order", "attribute": "created_at", "interval": "month" }],
  "metrics": [{ "entity": "work_order", "attribute": "total_cost", "aggregation": "SUM", "name": "monthly_cost" }]
}
```

### 6.5 — PIE: Distribución por Estado

```json
{
  "output_cast": "PIE",
  "dimensions": [{ "entity": "asset", "attribute": "status" }],
  "metrics": [{ "entity": "asset", "attribute": "id", "aggregation": "COUNT", "name": "count_assets" }]
}
```

### 6.6 — División Segura (Patrón NULLIF)

```
INCORRECTO:  revenue / cost                         → Puede explotar si cost = 0
CORRECTO:    revenue / NULLIF(cost, 0)               → Retorna NULL si cost = 0
CORRECTO:    COALESCE(revenue / NULLIF(cost, 0), 0)  → Retorna 0 si cost = 0
```

### 6.7 — Score Ponderado con Normalización

```
CLAMP(
  (health_score * 0.4 + COALESCE(reliability_index, 50) * 0.3 + criticality * 0.3),
  0,
  100
)
```

### 6.8 — Margen de Rentabilidad (%)

```
ROUND(
  ((asset/revenue - work_order/total_cost) / NULLIF(asset/revenue, 0)) * 100,
  2
)
```

---

## 7. Errores y Diagnóstico

### 7.1 — Catálogo de Errores de Fórmulas

| Código | Error | Causa | Solución |
|:---|:---|:---|:---|
| `FML_001` | Token inesperado | Carácter no reconocido en la fórmula | Verificar sintaxis, solo usar `+`, `-`, `*`, `/`, `%`, `^`, `(`, `)`, `,` |
| `FML_002` | Paréntesis desbalanceados | `(` sin `)` o viceversa | Contar y balancear paréntesis |
| `FML_003` | Función desconocida | Nombre de función no registrado | Verificar nombre contra el catálogo (§3). **AVG, SUM, COUNT no son funciones de fórmula** |
| `FML_004` | Aridad incorrecta | Cantidad de argumentos errónea | `POWER` requiere 2 args, `CLAMP` requiere 3, `ABS` requiere 1 |
| `FML_005` | División por cero | Denominador evaluó a cero | Proteger con `NULLIF(denominador, 0)` |
| `FML_006` | Dominio matemático | `SQRT(-1)`, `LOG(0)`, `LOG(-5)` | Validar que los inputs sean positivos con `IF` o `ABS` |
| `FML_007` | Inyección SQL | Contiene `SELECT`, `DROP`, `--`, `;`, etc. | Remover fragmentos SQL. Las fórmulas son expresiones matemáticas, no SQL |
| `FML_008` | Fórmula muy larga | Excede 4096 caracteres | Simplificar o dividir en múltiples medidas |
| `FML_009` | Anidamiento excesivo | Más de 32 niveles de paréntesis | Simplificar la expresión |
| `FML_010` | Demasiados tokens | Más de 512 tokens | Simplificar la expresión |
| `FML_011` | Funciones anidadas excesivas | Más de 16 niveles de funciones anidadas | Reducir `ROUND(ABS(SQRT(POWER(...))))` |
| `FML_012` | Variable no resuelta | El atributo no existe en la entidad | Verificar nombre del atributo en el esquema |
| `FML_013` | Fórmula vacía | String vacío o solo whitespace | Proporcionar una expresión válida |

### 7.2 — Error Más Frecuente: `FML_003 — Unknown function: AVG`

Esto ocurre cuando el usuario intenta escribir `AVG(health_score)` como fórmula. La solución es:

```
❌ INCORRECTO (como fórmula):
  measures: [{ "name": "avg_health", "formula": "AVG(health_score)" }]

✅ CORRECTO (como métrica + output_cast):
  metrics: [{ "attribute": "health_score", "aggregation": "AVG", "name": "avg_health" }],
  output_cast: "KPI"
```

---

## 8. Comportamiento del Motor

### 8.1 — Tratamiento de NULL / NaN

| Situación | Comportamiento |
|:---|:---|
| Variable inexistente en el row | Resuelve a `NaN` |
| División por cero | Resultado = `NaN` |
| `SQRT(-1)`, `LOG(0)` | Resultado = indefinido (None → NaN) |
| `NULLIF(x, x)` | Resultado = `NaN` (equivale a NULL SQL) |
| Resultado final = `NaN` | **La columna se omite del JSON de respuesta** (no aparece como `null`, simplemente no se incluye) |
| `COALESCE(NaN, 42)` | Resultado = `42` (salta el NaN) |
| `IF(NaN, a, b)` | `NaN ≠ 0` es verdadero → resultado = `a` |

### 8.2 — Aritmética de Punto Flotante

- Todas las operaciones usan IEEE 754 `f64` (64-bit double precision)
- Precisión: ~15-17 dígitos significativos
- Rango: ±1.7 × 10^308
- Comparaciones de igualdad exacta pueden fallar por redondeo (ej: `0.1 + 0.2 ≠ 0.3`)
- Usar `ROUND()` para comparaciones con tolerancia

### 8.3 — Motor OLAP vs OLTP

El motor compila la misma fórmula de forma diferente según el plan de ejecución:

| Aspecto | OLTP (DynamoDB EAV) | OLAP (Athena/DuckDB) |
|:---|:---|:---|
| **Ejecución** | Evaluador RPN en Rust, fila por fila | SQL injection como projection en SELECT |
| **NULLs** | `f64::NAN` propagado | SQL `NULL` nativo |
| **Funciones** | Evaluadas por `FormulaEvaluator` | Transpiladas a SQL nativo |
| **División por cero** | `NaN` → omitido del JSON | SQL `NULL` |
| **Resolución de variables** | `OltpVariableResolver` (dual-lookup) | `OlapVariableResolver` (col_id_str) |

El usuario **no necesita preocuparse** por esta diferencia. La fórmula se escribe una sola vez y funciona en ambos motores.

---

## 9. Límites del Sistema

| Límite | Valor | Justificación |
|:---|:---:|:---|
| Longitud máxima de fórmula | 4096 chars | Previene payloads masivos |
| Tokens máximos | 512 | Limita complejidad del parser |
| Profundidad de paréntesis | 32 niveles | Previene stack overflow |
| Funciones anidadas | 16 niveles | Previene recursión excesiva |
| Aridad de variádicas | 64 args | `GREATEST(x1, ..., x64)` suficiente |
| Measures por query | 32 | Previene queries con 100+ columnas |
| Queries por batch | Sin límite práctico | Un dashboard puede enviar N consultas |

---

## 10. Seguridad

Las fórmulas pasan por validación de seguridad **antes** del parsing:

**Palabras clave SQL bloqueadas** (word-boundary matching):
`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `EXEC`, `EXECUTE`, `UNION`, `FROM`, `WHERE`, `JOIN`, `INTO`, `GRANT`, `REVOKE`, `TRUNCATE`

**Patrones literales bloqueados:**
`--`, `/*`, `*/`, `;`

**Ejemplo:**
```
"SELECT * FROM users" → RECHAZADO (FML_007)
"selection_count + 1" → ACEPTADO (word-boundary: "selection" ≠ "SELECT")
```

---

## 11. Guía de Generación para el Modelo de Lenguaje

### 11.1 — Reglas para Generar Fórmulas

1. **NUNCA usar funciones de agregación** (`AVG`, `SUM`, `COUNT`, etc.) dentro de un string de fórmula
2. **SIEMPRE proteger divisiones** con `NULLIF(denominador, 0)`
3. **SIEMPRE usar `COALESCE`** para atributos que podrían no existir en todas las filas
4. **PREFERIR notación namespace** (`asset/health_score`) sobre bare (`health_score`) para claridad semántica
5. **LIMITAR la complejidad**: fórmulas de más de 200 caracteres probablemente necesitan ser divididas
6. **USAR `CLAMP`** para scores y porcentajes que deben estar en un rango finito
7. **USAR `ROUND`** en resultados que serán mostrados al usuario (2 decimales para %, 0 para enteros)

### 11.2 — Cómo Responder "Quiero el promedio de X"

```
Usuario: "Quiero el promedio de la salud de los activos"
→ Usar metrics con aggregation AVG, NO una fórmula

Usuario: "Quiero el promedio de (revenue - cost) / revenue"
→ Usar measures + metrics encadenados (pipeline de dos fases)

Usuario: "Quiero health_score normalizado de 0 a 100"
→ Usar fórmula: CLAMP(health_score, 0, 100)
```

### 11.3 — Cómo Responder "Quiero una gráfica de X por Y"

```
Usuario: "Gráfica de costos por mes"
→ output_cast: "TIMESERIES" + dimension con interval: "month" + metric SUM(total_cost)

Usuario: "Distribución de activos por estado"
→ output_cast: "PIE" + dimension status + metric COUNT

Usuario: "Tabla de activos con ROI calculado"
→ output_cast: "TABLE" + measure con fórmula ROI
```

### 11.4 — Template de Respuesta para el LLM

Cuando generes una consulta, usa este formato:

```markdown
**Consulta: [Nombre descriptivo]**

**Fórmula escalar** (si aplica):
`(campo_a - campo_b) / NULLIF(campo_a, 0) * 100`

**Estructura:**
- Entity: `asset`
- Output Cast: `KPI`
- Measures: `[alias] = [fórmula]`
- Metrics: `AVG([alias])` 
- Filters: `status = ACTIVE`
- Time Frame: `Last 30 days`

**JSON:**
```json
{ ... }
```
```

---

## 12. Referencia Rápida de Archivos del Motor

| Archivo | Responsabilidad |
|:---|:---|
| `src/aegis/formula/token.rs` | Definición de tipos `Token`, `Operator` |
| `src/aegis/formula/lexer.rs` | Tokenización: `String → Vec<Token>` |
| `src/aegis/formula/parser.rs` | Shunting-Yard: `Vec<Token> → Vec<Token>` (RPN) |
| `src/aegis/formula/evaluator.rs` | Evaluación RPN: `(RPN, Row) → f64` |
| `src/aegis/formula/resolver.rs` | Trait `VariableResolver` + implementaciones OLTP/OLAP |
| `src/aegis/formula/functions.rs` | 16 funciones: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `POWER`, `SQRT`, `LOG`, `LOG10`, `MOD`, `SIGN`, `NULLIF`, `COALESCE`, `IF`, `GREATEST`, `LEAST`, `CLAMP` |
| `src/aegis/formula/functions_registry.rs` | Registry extensible de funciones (OCP) |
| `src/aegis/formula/compiler_olap.rs` | Compilador OLAP: fórmula → SQL projection |
| `src/aegis/formula/security.rs` | Validación anti-inyección SQL |
| `src/aegis/formula/errors.rs` | `FormulaError` enum con 13 variantes |
| `src/aegis/oltp/caster.rs` | Integración: evalúa fórmulas fila-por-fila antes del OutputCast |
| `src/aegis/oltp/aggregation.rs` | 14 estrategias de agregación (AVG, SUM, MEDIAN, etc.) |
| `src/aegis/sql/query_builder.rs` | Integración OLAP: inyecta fórmulas como SQL projections |
