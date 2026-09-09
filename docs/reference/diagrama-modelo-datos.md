# Diagrama del Modelo de Datos (Códice)

> Fuente única: `config/models/*.json` (58 modelos). Este documento es un mapa visual
> de acompañamiento — la referencia campo a campo vive en
> [modelos-codice.md](modelos-codice.md) (generada). Si el catálogo cambia,
> regenerar las aristas: las relaciones de este doc derivan de los atributos
> `type: "reference"` + `entityRef` de cada modelo.

## Cómo leer los diagramas

- **Flecha sólida `──1──▶`**: `reference` con `cardinality: "one"` (belongs_to).
- **Flecha punteada `┈┈N┈┈▶`**: `reference` con `cardinality: "many"` (array de IDs).
- **Auto-referencia**: jerarquía dentro de la misma entidad (`asset.parent_asset_id`).
- Las etiquetas de flecha son el **nombre del atributo** en el modelo de origen.
- 🔒 = `is_system: true` (reservado del motor), ❄ = `engine: "olap"` (Data Lake).

Las relaciones **polimórficas** (nombre de entidad + id en campos string, sin
`entityRef`) no se dibujan como flechas; se listan al final.

---

## 1. Mapa de dominios del catálogo

```mermaid
flowchart LR
    subgraph CMMS["Núcleo CMMS (ejecución)"]
        WO["work_order<br/>labor_log · downtime · notes<br/>check_list · procedures"]
    end
    subgraph PLAN["Planificación"]
        PM["preventive_maintenance<br/>scheduled_job · reminder<br/>calendar_event"]
    end
    subgraph PROT["Protocolos (plantillas de trabajo)"]
        TT["procedure<br/>form_template"]
    end
    subgraph EXEC["Instancias de formulario"]
        CL["check_list · work_order_procedure"]
    end
    subgraph ACT["Activos y ubicaciones"]
        AS["asset · location · company"]
    end
    subgraph INV["Inventario"]
        PART["part · inventory_*"]
    end
    subgraph IOT["IoT y telemetría"]
        METER["meter_reading · iot_*"]
    end
    subgraph PEOPLE["Personas y acceso"]
        U["user · user_group · role<br/>shift_pattern · technician_shift"]
    end
    subgraph PLAT["Plataforma transversal 🔒"]
        PF["tenant · file · electronic_signature<br/>approval_* · outbox · audit_log · …"]
    end

    WO --> AS
    WO --> U
    WO --> CL
    CL --> TT
    PLAN --> WO
    METER --> AS
    INV --> AS
    IOT --> METER
    ACT --> U
    PEOPLE --> PLAT
```

Todo modelo OLTP persiste en el EAV de DynamoDB; los OLAP (❄ `audit_log`,
`domain_fault`, `meter_reading`, `inventory_ledger`) se entregan por Firehose a
tablas Iceberg en S3 y se consultan por Athena.

---

## 2. Núcleo de ejecución: `work_order` y su órbita

```mermaid
flowchart TB
    REQ["request<br/>(solicitud entrante)"] -. request_id .-> WO

    WO["📘 work_order<br/>work_order_number (secuencial scoped)<br/>status · priority · category<br/>SLA · check-in/out GPS"]
    WO -. parent_work_order_id .-> WO

    WO -->|"asset_id"| ASSET["asset"]
    WO -->|"location_id (scope ⭑)"| LOC["location"]
    WO -->|"client_id"| CO["company (CLIENT)"]
    WO -. "assignees" .-> U["user"]
    WO -. "assigned_group_ids" .-> UG["user_group"]
    WO -->|"preventive_maintenance_id"| PM["preventive_maintenance"]
    WO -->|"scheduled_job_id (unique ⫦)"| SJ["scheduled_job 🔒"]
    WO -->|"completed_by"| U

    NOTE["note<br/>(auditable)"] -->|"work_order_id"| WO
    NOTE -->|"author_id"| U

    LL["labor_log<br/>hora × tarifa"] -->|"work_order_id"| WO
    LL -->|"user_id"| U

    CL["check_list<br/>(inspección instanciada)"]
    WO -->|"work_order_id"| CL
    CLS["check_list_section"] -->|"check_list_id"| CL
    CLI["check_list_item<br/>(10 tipos de respuesta)"] -->|"check_list_section_id"| CLS

    WPROC["work_order_procedure<br/>(snapshot con scoring)"]
    WO -->|"work_order_id"| WPROC
    WPF["work_order_procedure_field<br/>(respuesta tipada value_*)"] -->|"work_order_procedure_id"| WPROC

    DT["downtime_log<br/>(parada no planificada)"] -->|"work_order_id"| WO
    DT -->|"asset_id"| ASSET
```

Restricción viva: `constraints` de `work_order` — **una OT por
(`scheduled_job_id`, `asset_id`)** por tenant: es la idempotencia definitiva del
loop preventivo (un mismo disparo puede materializar una OT por activo sin que
el reintento duplique ninguna).

⭑ `location_id` es `is_sequence_scope`: el contador `work_order_number` se
Scoped por ubicación (`WO-L-K92MXA-0043`); si solo viene `asset_id`, el scope se
resuelve vía `is_sequence_scope_via` (nearest_registered).

---

## 3. De la planificación y los protocolos a la orden de trabajo

Dos sistemas de "protocolo → instancia" cuelgan **directamente de la OT**. La
capa intermedia `work_order_template` (+ paradas de ruta) y el sistema de
tareas `task_template(_item)` + `work_order_task(_item)` se retiraron del
catálogo el 2026-09-09: los procedimientos son el único protocolo formal.

```mermaid
flowchart LR
    subgraph DEFS["Protocolos (definición)"]
        PR["procedure → procedure_field<br/>(12 tipos · scoring)"]
        FT["form_template → section → field<br/>(10 tipos)"]
    end

    subgraph GEN["Generación preventiva"]
        PM["preventive_maintenance<br/>(CRON o umbral de medidor)"]
        PM -->|"saga (shadow_sagas_mapping)"| SJ["scheduled_job 🔒"]
        SJ -->|"materializa + constraint unique"| WO2["work_order"]
        SEQ["sequence_registry 🔒"] -.->|"scope del contador"| WO2
    end

    subgraph RUN["Instancia runtime"]
        WO2 --> WPR2["work_order_procedure ← procedure"]
        WO2 --> CL2["check_list ← form_template"]
        REQ2["request"] -.->|"convert_to_work_order"| WO2
    end
```

| Sistema | Definición | Instancia | Uso |
|---|---|---|---|
| Procedures | `procedure(_field)` | `work_order_procedure(_field)` | Protocolo formal con scoring (estilo MaintainX) |
| Form templates | `form_template(_section/_field)` | `check_list(_section/_item)` | Checklists de inspección y `request` |

---

## 4. Activos, ubicaciones, telemetría e inventario

```mermaid
flowchart TB
    subgraph JER["Jerarquías"]
        LOC["location (ISA-95)"] -. parent_location_id .-> LOC
        ASSET["asset"] -->|location_id| LOC
        ASSET -. parent_asset_id .-> ASSET
        CO["company"] -. parent_company_id .-> CO
        ASSET -->|manufacturer_company_id| CO
        ASSET -->|vendor_provider_id| CO
    end

    subgraph TEL["Telemetría (CQRS)"]
        SUB["iot_subscription"] -->|device_profile_id| PROF["iot_device_profile"]
        HARV["iot_harvester_config"] -->|device_profile_id| PROF
        MR["meter_reading ❄<br/>(Firehose → Iceberg)"] -->|iot_subscription_id| SUB
        MR -->|device_profile_id| PROF
        MR -->|asset_id| ASSET
        MR -->|location_id| LOC
        AR["iot_alert_rule"] -->|asset_id / target_asset_id| ASSET
        CMD["iot_device_command"] -->|asset_id| ASSET
    end

    subgraph INV["Inventario"]
        P["part"]
        IB["inventory_batch"] -->|part_id| P
        IB -->|location_id| LOC
        IM["inventory_movement"] -->|inventory_batch_id| IB
        IM -->|part_id| P
        ITX["inventory_transfer"] -.->|"from_location_id / to_location_id"| LOC
        IL["inventory_ledger ❄"] -->|movement_id| IM
    end

    DT2["downtime_log"] -->|asset_id| ASSET
```

---

## 5. Personas y plataforma transversal

```mermaid
flowchart TB
    TEN["tenant 🔒"] --> LOGO["file 🔒"]
    U["user 🔒"] -->|tenant_id| TEN
    U -. role_ids .-> R["role 🔒"]
    U -. group_ids .-> UG["user_group"]
    UG -. allowed_locations .-> LOC["location"]
    UG -. allowed_assets .-> AS["asset"]
    R -. allowed_locations .-> LOC
    R -. allowed_assets .-> AS
    AK["api_key 🔒"] -->|role_id| R
    SP["shift_pattern"] -->|user_id| U
    SP -. user_group_id .-> UG
    TS["technician_shift"] -->|shift_pattern_id| SP

    subgraph EVID["Evidencias y firmas"]
        F["file 🔒"]
        ES["electronic_signature 🔒"] -->|graphical_file_id| F
        CLI["check_list_item"] -. response_signature_id .-> ES
    end

    subgraph APPROVALS["Aprobaciones 🔒"]
        API["approval_instance"]
        AP["approval_policy"] -->|"policy_id"| API
        ASE["approval_step_execution"] -->|"approval_instance_id"| API
        ASE -->|"electronic_signature_id"| ES
    end

    subgraph EDA["Eventos y auditoría 🔒"]
        OE["outbox_event<br/>(status: enum PENDING→DELIVERED)"]
        AL["audit_log ❄"]
        DF["domain_fault ❄"]
        ERR["event_routing_rule"] -. subscribed_rule_ids .-> WE["webhook_endpoint"]
    end
```

---

## 6. Relaciones polimórficas (sin `entityRef`)

Estas asociaciones guardan `(entity_type, entity_id)` como texto/uuid; el
catálogo no las valida estructuralmente:

| Origen | Campos | Destinos válidos |
|---|---|---|
| `file` | `owner_entity_type/id` | cualquier entidad (adjuntos) |
| `document_chunk` | `owner_entity_type/id` | dueño del documento RAG |
| `electronic_signature` | `signed_entity/id` | entidad firmada |
| `approval_instance` | `target_entity_name/id` | entidad bajo aprobación |
| `calendar_event` | `source_entity_type/id` | reminder · preventive_maintenance · work_order |
| `scheduled_job` | `parent_entity_ref` | preventive_maintenance · reminder · user |
| `inventory_movement` | `reference_entity/reference_id` | work_order · compra, etc. |

---

## 7. Deudas del modelo detectadas en la revisión (2026-09-09)

**Eliminado en esta revisión** (config muerta que el motor nunca leyó):

- Bloques raíz `"events"` formato legacy (`{create: "X_CREATED", …}`) en 12
  modelos — sustituidos por `event_rules`, que es lo único que compila el
  registry (`src/codice/registry.rs::parse_entity_model`).
- `"calendar_mapping"` en `preventive_maintenance` y `reminder` — 0 lectores en
  `src/`. Si se quiere proyectar a `calendar_event`, el mecanismo actual es
  `shadow_sagas_mapping` en la saga (`src/janus_router/saga.rs`).
- 13 claves `"default"` renombradas a `"default_value"` (6 modelos IoT) — el
  parser solo lee `default_value`; quedaban en silencio ignoradas.
- `outbox_event.status` era `type: "string"` con `options` (que el validador
  solo aplica a enums) → ahora `enum` con los 4 valores que `moira.rs` escribe
  realmente (PENDING · PROCESSING · FAILED · DELIVERED).

**Capa retirada por decisión de simplificación (2026-09-09):**

- `work_order_template` y `work_order_template_stop` ya no existen en el
  catálogo. Consecuencias aplicadas: `preventive_maintenance.template_id`
  eliminada (la saga sigue materializando la OT con asset + trazabilidad de
  pauta y job); los protocolos quedan anclados directamente a la OT. El
  validador descarta claves desconocidas,
  así que los payloads que aún envíen `template_id`/`work_order_template_id` no
  se rompen — metri-app debe dejar de enviarlos.
- Segunda ola, misma fecha: `work_order_task`, `work_order_task_item`,
  `task_template` y `task_template_item` retirados — **los procedimientos son
  el único protocolo formal**. Reencuadres aplicados: `note` ahora ancla a
  `work_order_id` (antes `work_order_task_id`); `labor_log` imputa solo a la OT
  (fuera `work_order_task_id` y `work_order_task_item_id`); `check_list` perdió
  su anclaje jerárquico a tareas.

**Fases 3-5 y 7 del [plan](../architecture/PLAN_REFACTORIZACION_MODELO.md) aplicadas (2026-09-09):**

- Vía única de adjuntos: retirados `location.photo_ids`,
  `check_list_item.response_file_ids` y `work_order_procedure_field.value_file_ids`.
  El único mecanismo es `file.owner_entity_type/id` (required). Los valores de
  `photo_ids` en producción eran nombres de fichero, no ids de entidad — sin
  pérdida estructural; quedan en el historial EAV (Regla A).
- `labor_log.hourly_rate` (decimal, sin divisa) → `hourly_rate_cents` +
  `currency` según la convención de unidad menor (tenía 0 datoms en producción).
- `iot_alert_rule.notify_groups` ahora apunta a `user_group`, misma semántica
  que `scheduled_job.target_group_id` y `reminder.target_group_id` (0 datoms).
- `provider` retirado del catálogo y de `FALLBACK_DOMAINS` de Cedar: 0 entidades
  vivas y ningún modelo lo referenciaba; `company_type=PROVIDER` lo cubre.

**Candidatos a deprecación que exigen decisión de producto/migración de datos**
(no tocar sin conciliar el dato vivo):

1. `dashboardBI` es la única entidad en camelCase (renombrar exige migración de
   entidad).
2. Dominio fantasma `"project"` en `FALLBACK_DOMAINS` de Cedar: confirmar si
   algún tenant lo usa antes de retirarlo.
