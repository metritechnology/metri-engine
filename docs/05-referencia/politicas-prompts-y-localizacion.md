# Referencia de Políticas Cedar, Prompts de IA y Localización (`config/`)

Este documento describe el 100% de los artefactos de configuración declarativa alojados en `config/policies/`, `config/prompts/` y `config/locales/`, los cuales complementan a `config/models/` ([modelos-codice.md](modelos-codice.md)) y `config/errors/error_catalog.toml` ([codigos-error.md](codigos-error.md)).

---

## 1. Políticas de Autorización Zero-Trust (`config/policies/`)

El subsistema [`src/cedar/`](../04-componentes/09-cedar.md) evalúa las políticas ABAC (Attribute-Based Access Control) escritas en Cedar Policy 3.

### 1.1 Archivos del Directorio `config/policies/`

| Archivo | Propósito | Consumidor en Rust |
| :--- | :--- | :--- |
| `config/policies/metri.cedar` | Conjunto canónico de reglas `permit` y `forbid` del motor Zero-Trust. | `src/cedar/engine.rs` (`CedarAuthorizer`) |
| `config/policies/cedar-schema.json` | Esquema formal de entidades (`Metri::User`, `Metri::MetriResource`) y grupos de acciones (`analytical`, `mutational`, `system`). | Validación de esquema Cedar y `src/cedar/evaluator/` |

### 1.2 Catálogo de Reglas en `metri.cedar`

| ID Regla | Efecto | Grupo de Acción | Condición ABAC | Invariante que Protege |
| :--- | :---: | :--- | :--- | :--- |
| **P00** | `permit` | `analytical` | `principal.grants.contains(resource.required_grant) && principal.query_scope != "NONE" && (principal.tenant_id == "system" \|\| principal.tenant_id == resource.tenant_id)` | Autoriza lecturas/consultas solo si el rol/grupo otorga el grant del dominio y un alcance distinto de `NONE`. |
| **P01** | `permit` | `mutational` | `principal.grants.contains(resource.required_grant) && (principal.tenant_id == "system" \|\| principal.tenant_id == resource.tenant_id)` | Autoriza mutaciones (`Transact`, `CompositeTransact`, `BulkIngest`) cuando el principal posee el permiso explícito `dominio:acción`. |
| **P02** | `permit` | `system` | `principal.grants.contains(resource.required_grant) && (principal.tenant_id == "system" \|\| principal.tenant_id == resource.tenant_id)` | Autoriza operaciones de sistema e infraestructura (`MatchRoutingRulesBatch`, etc.). |
| **P03** | `forbid` | *Todas* | `principal.tenant_id != "system" && principal.tenant_id != resource.tenant_id` | **Regla Cardinal Zero-Trust**: Prohíbe de forma absoluta cualquier acceso cruzado entre tenants (`forbid` prevalece siempre sobre `permit`). |
| **P04** | `forbid` | `analytical` | `resource.query_scope == "NONE"` | Denegación explícita cuando el alcance de consulta resuelto para el dominio es `NONE`. |
| **P05** | `forbid` | *Todas* | `principal.status == "SUSPENDED"` | Revocación inmediata de acceso para usuarios con estado `SUSPENDED`. |
| **P06** | `forbid` | *Todas* | `principal.user_type == "CONTRACTOR" && (!(resource has assigned_company_id) \|\| resource.assigned_company_id != principal.company_id)` | Aislamiento estricto de contratistas: solo pueden operar sobre recursos explícitamente asignados a su `company_id`. |
| **P07** | `forbid` | *Todas* | `resource.entity_type in ["tenant", "quota", "domain_quota", "domain_plugin", "tenant_plugin"] && principal.tenant_id != "system"` | Blindaje de entidades de plataforma: solo el tenant maestro (`system`) puede acceder o mutar entidades de gobierno multi-tenant. |

---

## 2. Plantillas de Prompts y Enrutamiento del Agente IA (`config/prompts/`)

El servicio gRPC `AgentConfigService` ([`src/grpc/agent_config_service.rs`](../04-componentes/14-grpc.md)) sirve las configuraciones, reglas de validación, rutas de navegación y plantillas de sistema para **Metri Q Assistant** desde el directorio `PROMPTS_DIR` (por defecto `config/prompts`).

### 2.1 Archivos de Configuración Raíz en `config/prompts/`

| Archivo | Propósito |
| :--- | :--- |
| `config/prompts/config.json` | Clasificador de intenciones por dominio (`telemetry`, `locations`, `navigation`, `form_autofill`, `tenants`, `deletions`, `formula_engine`), definiendo palabras clave (`keywords`), archivo de prompt asociado y herramientas MCP/funciones habilitadas (`tools`). |
| `config/prompts/navigation_routes.json` | Mapa canónico de rutas de la interfaz de usuario (`metri-app`) para navegación asistida por voz/texto (`navigate_to_view`). |
| `config/prompts/validation_rules.json` | Reglas declarativas de pre-validación para autocompletado de formularios (`autofill_active_form`). |

### 2.2 Dominios de Prompts Bilingües y Compactos (`config/prompts/<dominio>/`)

Cada subdirectorio contiene 4 variantes optimizadas por idioma y presupuesto de tokens (`es.txt`, `es_compact.txt`, `en.txt`, `en_compact.txt`):

| Subdirectorio | Rol en Metri Q Assistant | Herramientas Asociadas (`tools`) |
| :--- | :--- | :--- |
| `config/prompts/base/` | Instrucciones base del sistema, identidad del asistente, reglas de seguridad y formato de respuesta. | Transversal a todas las sesiones. |
| `config/prompts/telemetry/` | Consultas de consumo energético/hídrico, series temporales IoT, sostenibilidad ESG y gráficos analíticos. | `resolve_time_context`, `search_schemas`, `get_location_by_id` |
| `config/prompts/locations/` | Consulta de plantas, áreas, celdas, líneas y jerarquías físicas ISA-95 de ubicaciones y activos. | `list_locations`, `get_location_by_id`, `navigate_to_view` |
| `config/prompts/navigation/` | Redirección y apertura de vistas del dashboard, analítica, auditoría y módulos operativos. | `navigate_to_view` |
| `config/prompts/form_autofill/` | Creación y edición asistida de entidades rellenando formularios activos de la UI. | `autofill_active_form`, `navigate_to_view`, `list_tenants`, `list_locations`, `list_assets` |
| `config/prompts/tenants/` | Administración de cuentas corporativas, planes de suscripción (`starter`, `professional`, `enterprise`) y facturación. | `list_tenants`, `get_tenant_by_id`, `navigate_to_view` |
| `config/prompts/deletions/` | Flujos seguros con confirmación para baja o eliminación de activos, ubicaciones y recursos. | `list_assets`, `list_locations`, `list_tenants`, `get_location_by_id`, `get_asset_by_id` |
| `config/prompts/formula_engine/` | Construcción de fórmulas matemáticas personalizadas (`CLAMP`, `ROUND`, `NULLIF`, `COALESCE`) para consultas analíticas en Aegis. | `search_schemas` |

---

## 3. Catálogos de Internacionalización (`config/locales/`)

El directorio `config/locales/` almacena las etiquetas legibles por humanos (nombres de entidades, descripciones de atributos y etiquetas de opciones enum) que enriquecen las respuestas del RPC `Discovery` en `janus` para la construcción dinámica de interfaces en `metri-app`.

| Ruta | Idioma | Contenido |
| :--- | :---: | :--- |
| `config/locales/es.json` | Español | Diccionario global de etiquetas y metadatos de interfaz en español. |
| `config/locales/es/{asset,location,technical_specification,work_order}.json` | Español | Sobrescrituras y traducciones granulares por entidad para activos, ubicaciones, especificaciones técnicas y órdenes de trabajo. |
| `config/locales/en.json` | Inglés | Diccionario global de etiquetas y metadatos de interfaz en inglés. |
| `config/locales/en/{asset,location,technical_specification,work_order}.json` | Inglés | Traducciones granulares por entidad en inglés. |
