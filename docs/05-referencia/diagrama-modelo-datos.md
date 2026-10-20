# Diagrama del Modelo de Datos del Códice (`.mbf`)

> **Fuente única de verdad (SSOT):** `config/models/*.json` (58 modelos activos). La referencia detallada de atributos por modelo se encuentra en [modelos-codice.md](modelos-codice.md) (autogenerada con `python3 scripts/docs/gen_reference.py`) y la guía de autoría en [modelado de entidades del Códice](../02-guias/02-modelado-entidades-codice.md).

## Convenciones de los Diagramas

- **Flecha sólida `──1──▶`**: atributo `reference` con `cardinality: "one"` (relación `belongs_to`).
- **Flecha punteada `┈┈N┈┈▶`**: atributo `reference` con `cardinality: "many"` (arreglo de identificadores).
- **Auto-referencia**: jerarquía reflexiva dentro de la misma entidad (`asset.parent_asset_id`, `location.parent_location_id`, `work_order.parent_work_order_id`).
- 🔒 = `is_system: true` (entidades reservadas del plano de control y plataforma).
- **Almacenamiento unificado**: Todos los modelos —tanto operativos como de auditoría, series temporales IoT y libro mayor financiero— se persisten y consultan sobre el motor binario HTAP **Metri Vortex (`.mbf`)** en sus respectivas zonas físicas (Zona 1 PAX+MVCC, Zona 2 Grafo CSR ISA-95, Zona 3 Series Temporales IoT Chimp128, Zona 4 Libro Mayor `FixedDecimal128` y Zona 5 Sinopsis BI).

---

## 1. Mapa de Dominios del Catálogo

```mermaid
flowchart LR
    subgraph CMMS["Núcleo CMMS (Ejecución)"]
        WO["work_order<br/>work_order_task · work_order_task_item<br/>work_order_procedure · work_order_procedure_field<br/>labor_log · downtime_log · note"]
    end
    subgraph PLAN["Planificación y Automatización"]
        PM["preventive_maintenance<br/>preventive_maintenance_procedure<br/>scheduled_job · reminder<br/>calendar_event"]
    end
    subgraph PROT["Catálogo de Plantillas"]
        TT["procedure_template · procedure_template_field<br/>task_template · task_template_item<br/>checklist_template · checklist_template_section · checklist_template_item"]
    end
    subgraph ACT["Activos, Ubicaciones y Terceros"]
        AS["asset · location · company<br/>technical_specification"]
    end
    subgraph INV["Inventario y Contabilidad (Zona 4 .mbf)"]
        PART["part · inventory_batch<br/>inventory_movement · inventory_transfer<br/>inventory_ledger"]
    end
    subgraph IOT["IoT y Telemetría (Zona 3 .mbf)"]
        METER["meter_reading · iot_device_profile<br/>iot_subscription · iot_harvester_config<br/>iot_alert_rule · iot_device_command"]
    end
    subgraph PEOPLE["Identidad, Acceso y Turnos"]
        U["user · user_group · role · api_key<br/>shift_pattern · technician_shift"]
    end
    subgraph PLAT["Plataforma Transversal 🔒"]
        PF["tenant · domain_quota · sequence_registry<br/>file · document_chunk · electronic_signature<br/>approval_policy · approval_instance · approval_step_execution<br/>outbox_event · event_routing_rule · webhook_endpoint<br/>audit_log · domain_fault · dashboardBI · agent_metric<br/>domain_plugin · tenant_plugin"]
    end

    PLAN -->|"materializa / agenda"| CMMS
    PROT -->|"instancia en OT"| CMMS
    CMMS -->|"interviene sobre"| ACT
    CMMS -->|"consume"| INV
    CMMS -->|"asignado a"| PEOPLE
    IOT -->|"monitorea"| ACT
    INV -->|"almacenado en"| ACT
    PEOPLE -->|"gobernado por"| PLAT
```

---

## 2. Núcleo de Ejecución CMMS: `work_order` y su Órbita

```mermaid
flowchart TB
    REQ["request<br/>(solicitud de mantenimiento)"] -. "request_id" .-> WO

    WO["work_order<br/>work_order_number (secuencial por scope)<br/>status · priority · category<br/>SLA · check-in/out GPS"]
    WO -. "parent_work_order_id (jerarquía 1:N)" .-> WO

    WO -->|"asset_id"| ASSET["asset"]
    WO -->|"location_id (scope ⭑)"| LOC["location"]
    WO -. "assignees (many)" .-> U["user"]
    WO -. "assigned_group_ids (many)" .-> UG["user_group"]
    WO -->|"preventive_maintenance_id"| PM["preventive_maintenance"]
    WO -->|"scheduled_job_id (unique ⫦)"| SJ["scheduled_job 🔒"]
    WO -->|"completed_by"| U

    NOTE["note<br/>(bitácora auditable)"] -->|"work_order_id"| WO
    NOTE -->|"author_id"| U

    LL["labor_log<br/>(horas × hourly_rate_cents + currency)"] -->|"work_order_id"| WO
    LL -->|"user_id"| U

    WOT["work_order_task"] -->|"work_order_id"| WO
    WOTI["work_order_task_item"] -->|"work_order_task_id"| WOT

    WPROC["work_order_procedure<br/>(1..N por OT · procedure_order)"] -->|"work_order_id"| WO
    WPROC -->|"procedure_template_id"| PT["procedure_template"]
    WPF["work_order_procedure_field<br/>(respuesta tipada value_* + score)"] -->|"work_order_procedure_id"| WPROC
    WPF -->|"procedure_template_field_id"| PTF["procedure_template_field"]

    DT["downtime_log<br/>(parada de activo)"] -->|"work_order_id"| WO
    DT -->|"asset_id"| ASSET
```

### Invariantes Clave del Dominio `work_order`
- **Idempotencia de generación preventiva**: Constraint declarativo `unique(scheduled_job_id, asset_id)` por tenant en `work_order`, impidiendo que reintentos de planificación dupliquen órdenes de trabajo.
- **Resolución automática de secuencia (`work_order_number`)**: `location_id` actúa como proveedor de ámbito (`is_sequence_scope_provider: true`). Si la OT solo recibe `asset_id`, el generador resuelve el ámbito navegando hacia la ubicación del activo (`nearest_registered`).
- **Composición atómica (`CompositeTransact`)**: Una orden de trabajo junto con sus `work_order_procedure`, `work_order_procedure_field`, `work_order_task` y `work_order_task_item` se persiste en una única transacción ACID sobre `HOT_HEAD.mbf`.

---

## 3. Planificación Preventiva, Plantillas y Sagas Declarativas

```mermaid
flowchart LR
    subgraph DEFS["Definición de Protocolos (Estado PUBLISHED exigido por ref_state)"]
        PR["procedure_template → procedure_template_field<br/>(12 tipos de campo · puntaje score/max_score)"]
        TT["task_template → task_template_item<br/>(plantillas de tareas)"]
        CLT["checklist_template → checklist_template_section → checklist_template_item<br/>(definición de listas de verificación)"]
    end

    subgraph GEN["Planificación y Sagas (janus_router::saga)"]
        PM["preventive_maintenance<br/>(recurrencia calendario o umbral de medidor)"]
        PMP["preventive_maintenance_procedure"] -->|"preventive_maintenance_id"| PM
        PMP -->|"procedure_template_id"| PR
        PM -->|"shadow_sagas_mapping (misma TX ACID)"| SJ["scheduled_job 🔒"]
        REM["reminder"] -->|"shadow_sagas_mapping"| SJ
        SJ -->|"dispara creación idempotente"| WO2["work_order"]
        SEQ["sequence_registry 🔒"] -.->|"contador ACID por scope"| WO2
    end

    subgraph RUN["Ejecución en Orden de Trabajo"]
        WO2 --> WPR2["work_order_procedure<br/>(unique: work_order_id + procedure_template_id)"]
        WO2 --> WT2["work_order_task → work_order_task_item"]
    end
```

---

## 4. Activos, Jerarquías ISA-95, Telemetría IoT e Inventario Contable

```mermaid
flowchart TB
    subgraph JER["Jerarquías ISA-95 (Zona 2 .mbf: Grafo CSR + Euler [tin, tout])"]
        LOC["location (ISA-95)"] -. "parent_location_id" .-> LOC
        ASSET["asset"] -->|"location_id"| LOC
        ASSET -. "parent_asset_id" .-> ASSET
        TSPEC["technical_specification"] -->|"asset_id"| ASSET
        CO["company"] -. "parent_company_id" .-> CO
        ASSET -->|"manufacturer_company_id"| CO
        ASSET -->|"vendor_provider_id"| CO
    end

    subgraph TEL["Telemetría IoT (Zona 3 .mbf: Delta-of-Delta + Chimp128)"]
        SUB["iot_subscription"] -->|"device_profile_id"| PROF["iot_device_profile"]
        HARV["iot_harvester_config"] -->|"device_profile_id"| PROF
        MR["meter_reading"] -->|"iot_subscription_id"| SUB
        MR -->|"device_profile_id"| PROF
        MR -->|"asset_id"| ASSET
        MR -->|"location_id"| LOC
        AR["iot_alert_rule"] -->|"asset_id / target_asset_id"| ASSET
        CMD["iot_device_command"] -->|"asset_id"| ASSET
    end

    subgraph INV["Inventario y Libro Mayor Exacto (Zona 4 .mbf: FixedDecimal128 + Fenwick)"]
        P["part"]
        IB["inventory_batch"] -->|"part_id"| P
        IB -->|"location_id"| LOC
        IM["inventory_movement"] -->|"inventory_batch_id"| IB
        IM -->|"part_id"| P
        ITX["inventory_transfer"] -.->|"from_location_id / to_location_id"| LOC
        IL["inventory_ledger 🔒"] -->|"movement_id"| IM
    end
```

---

## 5. Identidad, Autorización ABAC, Aprobaciones y Eventos (EDA)

```mermaid
flowchart TB
    TEN["tenant 🔒"] -->|"logo_file_id"| F["file 🔒"]
    U["user 🔒"] -->|"tenant_id"| TEN
    U -->|"company_id"| CO2["company"]
    U -. "role_ids (many)" .-> R["role 🔒"]
    U -. "group_ids (many)" .-> UG["user_group"]
    UG -. "allowed_locations" .-> LOC2["location"]
    UG -. "allowed_assets" .-> AS2["asset"]
    R -. "allowed_locations" .-> LOC2
    R -. "allowed_assets" .-> AS2
    AK["api_key 🔒"] -->|"role_id"| R
    SP["shift_pattern"] -->|"user_id"| U
    SP -. "user_group_id" .-> UG
    TS["technician_shift"] -->|"shift_pattern_id"| SP

    subgraph EVID["Archivos, RAG y Firmas Electrónicas"]
        F
        DC["document_chunk 🔒"] -->|"file_id"| F
        ES["electronic_signature 🔒"] -->|"graphical_file_id"| F
    end

    subgraph APPROVALS["Flujos de Aprobación 🔒"]
        AP["approval_policy"]
        API["approval_instance"] -->|"policy_id"| AP
        ASE["approval_step_execution"] -->|"approval_instance_id"| API
        ASE -->|"electronic_signature_id"| ES
    end

    subgraph EDA["Eventos, Cuotas y Observabilidad 🔒"]
        OE["outbox_event<br/>(PENDING → PROCESSING → DELIVERED / FAILED)"]
        ERR["event_routing_rule"] -. "subscribed_rule_ids" .-> WE["webhook_endpoint"]
        AL["audit_log 🔒"]
        DF["domain_fault 🔒 (Sherlog)"]
        DQ["domain_quota 🔒"]
        AM["agent_metric 🔒"]
        DBI["dashboardBI"]
        DP["domain_plugin 🔒"] --> TP["tenant_plugin 🔒"]
    end
```

---

## 6. Asociaciones Polimórficas (`owner_entity_*` / `target_entity_*`)

Las siguientes entidades utilizan un par polimórfico `(tipo_entidad, id_entidad)` para asociarse a cualquier entidad del catálogo sin acoplar el grafo de referencias:

| Entidad Origen | Atributos Polimórficos | Entidades Destino Soportadas |
| :--- | :--- | :--- |
| `file` | `owner_entity_type` + `owner_entity_id` | Cualquier entidad del dominio (vía única canónica para adjuntos y evidencias). |
| `document_chunk` | `owner_entity_type` + `owner_entity_id` | Entidad propietaria del fragmento documental indexado para búsqueda híbrida / RAG. |
| `electronic_signature` | `signed_entity` + `signed_entity_id` | Entidad sobre la cual se estampa la firma criptográfica (`work_order`, `approval_step_execution`, etc.). |
| `approval_instance` | `target_entity_name` + `target_entity_id` | Entidad sujeta al flujo de aprobación (`work_order`, `inventory_transfer`, `request`, etc.). |
| `calendar_event` | `source_entity_type` + `source_entity_id` | `preventive_maintenance`, `reminder`, `work_order`, `technician_shift`. |
| `scheduled_job` | `parent_entity_ref` | `preventive_maintenance`, `reminder`, `user`. |
| `inventory_movement` | `reference_entity` + `reference_id` | `work_order`, `inventory_transfer` u orden externa que origina el movimiento de inventario. |
