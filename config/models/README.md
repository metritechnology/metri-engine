# Códice de Metri: Guía de Referencia y Modelado de Datos

Este directorio contiene las definiciones de esquema JSON que estructuran el ecosistema de datos de **Metri**. Este sistema, conocido internamente como **Códice**, actúa como la **única fuente de verdad (Single Source of Truth)** para la validación Estructural OLTP, la persistencia en el modelo EAV, y la proyección analítica en el Data Lake OLAP.

Esta guía está redactada con especificaciones técnicas detalladas para servir como referencia a desarrolladores, agentes autónomos y LLMs que necesiten consultar, mantener o ampliar los modelos de datos de Metri.

---

## I. Arquitectura y Bootstrap (Fail-Fast)

El motor analítico en Rust (`metri-engine`) escanea y procesa determinísticamente todos los archivos JSON de este directorio durante su fase de inicio (_cold start_).

- **Inicialización inmutable**: Compila todos los JSON en una estructura in-memory indexada en un `OnceLock<CodeRegistry>` de forma estática y de consulta $O(1)$.
- **Validación de Identidad**: Si detecta esquemas con nombres duplicados o colisiones SHA-256 de campos, el servidor ejecuta un pánico inmediato e interrumpe el arranque para evitar la corrupción de base de datos.

---

## II. Estructura de una Entidad (Modelo Códice)

Cada archivo JSON define un modelo estructural con las siguientes propiedades raíz:

| Campo                        | Tipo      | Requerido | Descripción                                                                                                                         |
| :--------------------------- | :-------- | :-------- | :---------------------------------------------------------------------------------------------------------------------------------- |
| `entity`                     | `string`  | **Sí**    | Identificador único de la entidad (ej: `asset`, `location`, `work_order`).                                                          |
| `engine`                     | `enum`    | No        | Canal del motor. `"oltp"` (por defecto, base de datos relacional EAV en DynamoDB) o `"olap"` (para telemetría y flujos Kinesis/S3). |
| `is_system`                  | `boolean` | No        | Indica si la entidad es reservada del sistema (ej: `audit_log`, `outbox_event`).                                                    |
| `is_sequence_scope_provider` | `boolean` | No        | Indica si esta entidad puede actuar como scope/contexto para contadores secuenciales (ej. `location`).                              |
| `track_history`              | `boolean` | No        | Activa la auditoría automática de historial de cambios en el almacén de transacciones.                                              |
| `attributes`                 | `array`   | **Sí**    | Lista de atributos estructurados que definen las columnas y propiedades de la entidad.                                              |
| `event_rules`                | `array`   | No        | Reglas de eventos y alertas automáticas disparadas por mutaciones de estado de esta entidad.                                        |

---

## III. Atributos de Datos: Tipos y Banderas

El arreglo de `attributes` define de forma estricta las propiedades de cada campo.

### 1. Tipos de Datos Soportados (`type`)

Códice traduce las declaraciones JSON a la enumeración estricta de Rust `AttrType` y posteriormente a variantes indexables de EAV `DatomValue`:

| Tipo JSON                                               | Tipo Rust (`AttrType`) | Tipo Almacenamiento EAV (`DatomValue`) | Descripción                                                                     |
| :------------------------------------------------------ | :--------------------- | :------------------------------------- | :------------------------------------------------------------------------------ |
| `"string"`                                              | `String`               | `Str(String)`                          | Cadenas de texto plano con soporte FTS.                                         |
| `"number"` <br> (alias: `"int"`, `"integer"`, `"long"`) | `Number`               | `Long(i64)`                            | Números enteros exactos (i64). **Ideal para dinero en céntimos (minor units)**. |
| `"decimal"` <br> (alias: `"double"`, `"float"`)         | `Decimal`              | `Double(f64)`                          | Números de coma flotante de precisión doble (f64).                              |
| `"epoch"`                                               | `Epoch`                | `Instant(i64)`                         | Timestamps en milisegundos Unix epoch.                                          |
| `"boolean"`                                             | `Boolean`              | `Bool(bool)`                           | Valores lógicos booleanos `true` o `false`.                                     |
| `"reference"`                                           | `Reference`            | `Ref(u64)` / `Array(Vec<String>)`      | Relaciones externas (1:1 o 1:N) que apuntan a otras entidades (`entityRef`).    |
| `"enum"`                                                | `Enum`                 | `Str(String)`                          | Restringe valores a una lista predefinida en `"options"`.                       |
| `"uuid"`                                                | `Uuid`                 | `Uuid(String)`                         | Identificadores únicos universales estándar.                                    |
| `"array"`                                               | `Array`                | `Array(Vec<String>)`                   | Arreglo plano de cadenas de texto.                                              |
| `"json"`                                                | `Json`                 | `Str(String)`                          | Bloque JSON libre para datos no indexables, evitando polución estructural.      |

---

### 1.1. Manejo Especial de Cadenas, Números y Estandarización de Divisas (Currency)

Para mantener la máxima integridad de los datos, Códice aplica reglas estrictas para el modelado de textos, importes numéricos y variables financieras.

#### A. Cadenas de Texto (`"string"`)

- **Validación de Formato (`"pattern"`)**: Habilita la validación por expresión regular (PCRE regex) directamente en la capa transaccional (ej: `^[A-Z]{3}$` para divisas, o `^WO-\\d{4}$` para códigos secuenciales).
- **FTS (Full-Text Search)**: Si se marca `"fts": true`, el atributo ingresa automáticamente en el pipeline de indexación para búsquedas aproximadas de texto.

#### B. Números Enteros vs Decimales (`"number"` vs `"decimal"`)

- **Tipo `"number"`** (y alias `"int"`, `"integer"`, `"long"`):
  - Mapea internamente a enteros exactos de 64 bits (`i64` / `DatomValue::Long`).
  - **Uso ideal**: Contadores, métricas discretas y **cantidades contables/financieras** (céntimos).
- **Tipo `"decimal"`** (y alias `"double"`, `"float"`):
  - Mapea a valores de coma flotante de precisión doble de 64 bits (`f64` / `DatomValue::Double`).
  - **Uso ideal**: Telemetría IoT, lecturas físicas de sensores, y áreas/coordenadas.

#### C. Estandarización Monetaria y Divisas (Currency Pattern)

Para evitar la imprecisión acumulada e inherente de los números flotantes en aritmética financiera (IEEE 754, ej: `0.1 + 0.2 = 0.30000000000000004`), el ecosistema de Metri aplica obligatoriamente la **Estandarización Financiera de Unidad Menor (Minor Units)**:

1. **Atributos Monetarios en Céntimos (`*_cents`)**:
   - Todos los atributos de costos, precios e importes monetarios deben declararse con el sufijo `_cents` (ej. `total_cost_cents`, `default_unit_cost_cents`).
   - Su tipo de dato debe ser estrictamente `"number"`.
   - **Ejemplo**: Un importe de `$150.25` se transmite y persiste como el entero exacto `15025`.
2. **Atributo Sibling de Divisa (`"currency"`)**:
   - Cualquier atributo monetario de céntimos debe estar obligatoriamente acompañado en el mismo modelo por un atributo de divisa.
   - El atributo de divisa debe definirse como:
     - Nombre: `"currency"`
     - Tipo: `"string"`
     - Banderas: `"is_dimension": true` para optimizar consultas analíticas en el Data Lake.
     - Validación: `"pattern": "^[A-Z]{3}$"` para obligar al formato ISO 4217 de tres letras mayúsculas.

---

### 2. Banderas Estructurales y Analíticas (Flags)

Cada atributo puede configurar banderas que modifican cómo se comporta en tiempo de ejecución, en las búsquedas OLTP y en las consultas del Data Lake:

```json
{
  "name": "total_cost_cents",
  "type": "number",
  "required": true,
  "unique": "identity",
  "index": true,
  "fts": true,
  "is_measure": true,
  "default_rollup": "sum",
  "sensitive": false
}
```

- **`required`** (`boolean`): Valida en tiempo de ingesta que el atributo esté presente y no sea nulo.
- **`unique`** (`string`): Estrategia de restricción de unicidad:
  - `"tenant"` (o `"tenant_scoped"`): **Recomendado para entidades de negocio**. Garantiza que el valor sea único exclusivamente dentro del ámbito del `tenant_id` actual (aislamiento multi-tenant). Permite que distintos clientes reutilicen la misma nomenclatura de tags (`A-001`), series o códigos.
  - `"identity"` (o `"global"`): Garantiza que el valor sea único **globalmente en todo el sistema** (reservado para identificadores del sistema o subdominios globales).
- **`index`** (`boolean`): Genera automáticamente un registro índice (AVET) en la base de datos para habilitar búsquedas de igualdad de alto rendimiento.
- **`fts`** (`boolean`): Incluye el campo dentro del motor de búsqueda de texto completo (_Full-Text Search_).
- **`sensitive`** (`boolean`): Marca campos con información sensible (PII) para ser censurados de forma automática en los registros de errores del backend.
- **`is_dimension`** (`boolean`): Identifica el atributo como una dimensión de categorización/filtrado para agregaciones en el Janus Router del Data Lake.
- **`is_measure`** / **`is_metric`** (`boolean`): Identifica atributos numéricos acumulables en análisis OLAP.
- **`default_rollup`** (`string`): Función agregadora analítica por defecto (`"sum"`, `"avg"`, `"min"`, `"max"`).

---

### 3. Configuraciones Especiales y Autogeneración

- **`entityRef`** (`string`): Para tipos `"reference"`, indica el nombre de la entidad destino.
- **`cardinality`** (`enum`): `"one"` para relaciones unitarias o `"many"` para relaciones múltiples.
- **`auto_generate`** (`object`): Permite establecer secuencias automáticas. Configura el patrón generador:
  - **Sequential**: Utiliza contadores secuenciales en base a un scope.
    ```json
    "auto_generate": {
      "strategy": "sequential",
      "prefix": "WO-",
      "padding": 4,
      "scope_resolution": "nearest_registered"
    }
    ```
  - **Stochastic Base36**: Genera identificadores criptográficos estocásticos compactos.
    ```json
    "auto_generate": {
      "strategy": "stochastic_base36",
      "prefix": "A-",
      "length": 7
    }
    ```

---

## IV. Reglas de Eventos (`event_rules`)

Las reglas de eventos permiten disparar notificaciones y registrar auditorías asíncronas en el Bus de Eventos (Moira) a través de transacciones reactivas en tiempo de ejecución.

```json
"event_rules": [
  {
    "rule_code": "WO_HIGH_COST_ALERT",
    "event_trigger_type": "on_update",
    "filter_conditions": [
      {
        "field_name": "total_cost_cents",
        "operator": "gt",
        "target_value": "5000000"
      }
    ],
    "detail_type_output": "system.alerts.high_cost_work_order",
    "description": "Alerta sistémica automática para órdenes de trabajo que superan los 50k en costo."
  }
]
```

### Propiedades de las Reglas:

1. **`rule_code`**: Identificador único global de la regla.
2. **`event_trigger_type`**: Tipo de disparo transaccional:
   - `"on_create"`: Ejecuta la regla tras insertar la entidad.
   - `"on_update"`: Ejecuta la regla tras actualizar la entidad.
   - `"on_delete"`: Ejecuta la regla al retractar la entidad.
3. **`filter_conditions`**: Lista de objetos que especifican cuándo debe dispararse el evento:
   - `field_name`: El nombre del atributo a evaluar.
   - `operator`: Operadores de comparación suportados (`"eq"`, `"neq"`, `"gt"`, `"lt"`, `"gte"`, `"lte"`).
   - `target_value`: El valor objetivo de comparación (guardado como string pero coercionado dinámicamente según el tipo de atributo).
4. **`detail_type_output`**: El tipo de evento con el que se emitirá al bus (ej: `"system.alerts.high_cost_work_order"`).

---

## V. Protocolos y formularios: los dos sistemas y sus fronteras

Desde el 2026-09-09 el catálogo reconoce **dos** sistemas de "definición →
instancia" (la capa `work_order_template` + paradas de ruta y el sistema de
tareas `task_template`/`work_order_task` fueron retirados). Ambos cuelgan
directamente de `work_order`:

| Sistema | Definición | Instancia | Frontera |
|---|---|---|---|
| **Procedimientos** | `procedure` → `procedure_field` (12 tipos de campo, scoring) | `work_order_procedure` → `work_order_procedure_field` | **SSOT del protocolo formal**: pasos tipados con puntaje, estilo MaintainX. Solo los `procedure` con `lifecycle_state=PUBLISHED` se instancian; `DRAFT` no se ofrece a nuevas OTs y `RETIRED` conserva histórico. |
| **Formularios** | `form_template` → `form_template_section` → `form_template_field` (10 tipos de respuesta) | `check_list` → `check_list_section` → `check_list_item` | **Checklists de inspección y solicitudes**: única vía de formularios para `check_list` y para `request`. |

Reglas de frontera:

1. Un protocolo de trabajo con scoring (aprobar/rechazar por puntaje) se define
   como `procedure`, nunca como `form_template`.
2. Una lista de verificación sí/no/respuesta corta se define como
   `form_template`, nunca como `procedure`.
3. Ninguna definición cuelga de otra capa: `procedure` y `form_template` se
   instancian sobre la OT (o la `request`) directamente.
4. Las tareas de una OT son ahora atributos y evidencias de la propia
   `work_order` (notas, horas de `labor_log`, adjuntos vía `file`): no existe
   entidad de tarea.

### La OT lleva 1..N procedimientos y 1..N checklists

Una `work_order` instancia **uno a varios** procedimientos y **una a varias**
checklists — no hay límite de uno por OT ni de plantillas distintas:

- Cada instancia de procedimiento (`work_order_procedure`) lleva
  `procedure_order` (1..N): el orden de ejecución dentro de la OT, copiado de
  `procedure.procedure_order` al instanciar y reordenable sin tocar la
  plantilla. Constraint: el mismo `procedure` no se instancia dos veces en la
  misma OT (`unique` tenant sobre `work_order_id + procedure_id`).
- Cada checklist (`check_list`) lleva `check_list_order` (1..N) y puede nacer
  de una `form_template` o construirse a mano. Constraint: la misma
  `form_template` no se instancia dos veces en la misma OT (`unique` tenant
  sobre `work_order_id + form_template_id`); las manuales, sin plantilla, no
  reclaman nada.
- Los N procedimientos y las N checklists de una OT son independientes entre
  sí: ni los protocolos generan checklists ni las checklists generan pasos.
