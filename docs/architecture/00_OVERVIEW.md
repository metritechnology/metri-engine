# Metri Engine - Overview Arquitectónico

**Metri Engine** es un motor de datos (Data-First) de alto rendimiento diseñado para entornos SaaS multitenant. El sistema está optimizado para la gestión avanzada de activos, órdenes de trabajo y telemetría (IoT), combinando capacidades transaccionales (OLTP) y analíticas (OLAP). Su objetivo es proveer una plataforma unificada para inteligencia de negocios (BI) avanzada, asegurando inmutabilidad, escalabilidad y un riguroso control de acceso a nivel de atributo.

## Principios Arquitectónicos

- **Clean Architecture Funcional:** Separación estricta de las reglas de negocio (funciones puras en Clojure) de los efectos secundarios (AWS, bases de datos). El acoplamiento se gestiona mediante _Protocols_ (polimorfismo), permitiendo inyectar dependencias.
- **Diseño Basado en Esquemas (Schema-Driven):** La estructura y comportamiento de cada entidad se define dinámicamente en formato JSON, eliminando la necesidad de migraciones de código rígidas para nuevos atributos.
- **Multitenancy Estricto & Censura de Tenant Master:** Aislamiento lógico de datos por inquilino (`tenant_id`) validado obligatoriamente por Cedar ABAC. Incluso el Administrador Maestro (Tenant 0) sufre de _Zero-Trust Administrative Censorship_, siéndole restringida la visualización técnica de Dominios de Negocio (Privacidad de Datos).
- **SOLID & DIP:** Aplicación estricta de Inversión de Dependencias. El orquestador (Janus) no conoce la infraestructura de seguridad; delega la validación a un componente inyectado (Interceptor).

## Gestión de Errores Predictiva y Observabilidad Endógena (Metri Trace)

El flujo de ejecución no se basa en el lanzamiento incontrolado de excepciones.

- **Patrón Result (Zero-Exception):** Todo el dominio core de Clojure devuelve tuplas inmutables de éxito o fracaso (ej. `[:ok valor]`, `[:error contexto]`).
- **EDA Analytics (Sherlog):** Fallos críticos nivel WARNING o FATAL no se silencian. Generan el evento `DOMAIN_FAULT_DETECTED`, el cual se envía asíncronamente vía EventBridge para crear sistemas de auto-reparación o Dashboards de Inteligencia Reactiva.
- **Metri Trace (Soberanía OTel):** Abandono total de AWS X-Ray o Datadog. Metri Engine adopta el estándar OTel (`traceparent`). Los _Spans_ de medición ingresan masivamente por EventBridge, son consolidados por el Bulk Ingestion (_Hephaestus_), y caen al lago de datos bajo el esquema oficial `telemetry_span.json` para ser graficados con los propios paneles de Metri UI.

## Integración de Inteligencia Artificial (Model Context Protocol)

Metri Engine prohíbe los "Agentes Monolíticos". Toda conexión LLM es aislada mediante el protocolo oficial de la industria **MCP**.

- **Parachoques Asíncrono:** La comunicación pesada (Server-Sent Events) del LLM se mastica en el **Componente Externo 06** (AWS Serverless Node.js/TypeScript). Este Proxy le escupe llamadas gRPC directas e indoloras al motor Clojure, garantizando que el _Chain-of-Thought_ de las IA no induzca Tiempos de Bloqueo en la CMMS transaccional.
- **Zero-Prompt Setup (Data-First):** La IA descubre la forma del negocio orgánicamente al escanear los modelos JSON de Metri mediante recursos dinámicos.

## Frameworks y Tecnologías Core

El sistema descansa sobre un stack sumamente especializado:

- **Clojure & ClojureScript:** Lenguaje principal de todo el backend para aprovechar sus inmutables nativos y el REPL-Driven Development (RDD).
- **Malli:** Motor declarativo formal en Clojure para validación estricta y rápida de Data/Schemas JSON en tiempo de ejecución.
- **HoneySQL:** Librería funcional para la transformación astuta de estructuras de datos Clojure a sentencias SQL puras, previniendo SQL Injection.
- **Datalog:** Dialecto funcional nativo transaccional de Datomic.
- **Protocol Buffers (protobuf):** Formato binario universal para los contratos de comunicación (`metri.proto`), optimizando latencia.
- **AWS Cedar (Embedded PDP):** Motor matemático para la lógica de autorización expresiva (Zero-Trust), ejecutado localmente vía Java SDK para latencia zero.

## Contratos de Comunicación (El API Boundary)

Toda interacción núcleo es procesada a través del gRPC `MetriService`, y paralelamente se expone una API JSON Pública Proxy (Fase 07) para clientes externos, la cual prohíbe parámetros exploratorios analíticos protegiendo el desempeño estructural.

- `rpc Discovery(DiscoveryRequest) returns (DiscoveryResponse)`: Exposición semántica de esquemas y capacidades UI funcionales (Esencial para la IA).
- `rpc Explore(ExploreRequest) returns (ExploreResponse)`: Autocompletados, diccionarios y sugerencias dimensionales rápidas.
- `rpc Query(QueryRequest) returns (QueryResponse)`: Frontera analítica con soporte `BatchContext` y orquestación masiva multiserie.
- `rpc Transact(TransactionRequest) returns (TransactionResponse)`: Tubería transaccional de mutaciones puras hacia el motor inmutable Datomic.
- `rpc BulkIngest(BulkRequest) returns (Status)`: Tubería en binario masivo para flujos directos (sensores, auditorías o telemetría OTel de Metri Trace).

## Infraestructura (Despliegue AWS y Dualidad)

La orquestación abstracta de Clojure controla infraestructuras Cloud completamente administradas:

- **AWS ECS Fargate / AWS Lambda:** Cómputo primario multitenant del Orquestador Clojure. Ver [01.03_FASE_RUNTIME_GRPC.md](01.03_FASE_RUNTIME_GRPC.md) para la topología completa del servidor gRPC, ciclo de vida Integrant y despliegue dual.
- **Datomic Cloud (OLTP):** Almacenaje asertivo inmutable, aseguramiento de auditoría (time-travel API), soportado habitualmente en Amazon DynamoDB.
- **Amazon S3 + AWS Kinesis Firehose:** Canal particionado e infinito para decantamiento volcado de archivos columnares `.parquet`.
- **AWS Athena (OLAP / Trino):** Submotor masivo Serverless distribuido para procesamiento estadístico interanual pesado cruzando los _buckets_ Parquet.
- **Valkey (Session Store):** Clúster de alto rendimiento para el almacenamiento de sesiones binarias (Protobuf), permitiendo resolución de identidad sin saltos de red externos.
- **Amazon EventBridge:** Bus de suscripción inter-sistema para derivar eventos asincrónicos calculados previamente por triggers RAM Clojure.

## Flujos Transversales Centrales (High-Level Flows)

### A. Flujo Operacional de Ingesta (Write Path)

1. **Petición Entrante:** Cliente emite un mensaje Protobuf a `rpc Transact`.
2. **Validación de Identidad:** El Interceptor de Seguridad convalida el token contra Valkey, inyectando el `tenant_id` y `user_id` soberanos.
3. **Persistencia Trazable:** Escritura en Datomic anexando anotaciones explícitas de auditoría (`:audit/user`).
4. **Reactividad Diferencial:** Si la mutación dispara un evento, la entidad sale por Amazon EventBridge.

### B. Flujo Analítico y Mutilación (Read Path)

1. **Petición Analítica:** Cliente solicita resolución de Dashboard vía `rpc Query`.
2. **Orquestación del ATS (Janus):** Janus inicia el ciclo de vida, transformando la petición en un Raw ATS.
3. **Security Gate (PDP Local):** Janus invoca al interceptor inyectado. Este resuelve la sesión en Valkey, evalúa Cedar y devuelve un **Safe ATS** (con poda de atributos y RLS aplicado).
4. **Transformación Polimórfica:** Janus traduce el Safe ATS al dialecto ideal (HoneySQL o Datalog) e invoca las queries asíncronas.
5. **Mapeo HATEOAS (VizMeta):** Los retornos se fusionan, se inyectan adornos visuales y se emite el stream gRPC.

## 9. Contrato Maestro de Comunicación (metri.proto)

El API Boundary y los modelos DTO de Metri Engine están rigurosamente definidos en Protobuf `proto3`. El archivo fuente de verdad reside en el repositorio del motor:

👉 **[Ver Contrato Maestro: metri.proto](../../metri.proto)**

Para la configuración del servidor gRPC, compilación Protobuf, ciclo de vida Integrant y topología de despliegue, consultar:

👉 **[Fase 01.03 — Runtime & Infraestructura gRPC](01.03_FASE_RUNTIME_GRPC.md)**

## 10. Mapa de Fases Arquitectónicas

| Fase | Documento | Componente |
| :--- | :--- | :--- |
| 01 | [01_FASE_ALISTAMIENTO_ENTORNO.md](01_FASE_ALISTAMIENTO_ENTORNO.md) | Entorno — índice de subfases |
| 01.01 | [01.01_FASE_MAIN_BOOTSTRAP.md](01.01_FASE_MAIN_BOOTSTRAP.md) | `-main`, Bootstrap fail-fast, Integrant, REPL |
| 01.02 | [01.02_FASE_CLIENTES_INFRAESTRUCTURA.md](01.02_FASE_CLIENTES_INFRAESTRUCTURA.md) | 9 Clientes de Infraestructura (Infra CAPA 1) |
| 01.03 | [01.03_FASE_RUNTIME_GRPC.md](01.03_FASE_RUNTIME_GRPC.md) | Runtime gRPC — Netty, Service Impl, OTel |
| 01.04 | [01.04_FASE_LOCAL_ENV.md](01.04_FASE_LOCAL_ENV.md) | Entorno local — LocalStack, docker-compose |
| 01.TDD | [01_FASE_ALISTAMIENTO_TDD_MATRIX.md](01_FASE_ALISTAMIENTO_TDD_MATRIX.md) | Matriz TDD del alistamiento |
| 02 | [02_FASE_MOTOR_SCHEMA_DRIVEN_CORE.md](02_FASE_MOTOR_SCHEMA_DRIVEN_CORE.md) | Códice — JSON Schema SSOT |
| 03 | [03_FASE_INGESTION.md](03_FASE_INGESTION.md) | Orquestación de Ingesta (IOP + Janus) |
| 03A | [03A_FASE_IOP.md](03A_FASE_IOP.md) | Ingestion Orchestration Pipeline |
| 03B | [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md) | Janus Router — OLTP/OLAP |
| 04 | [04_FASE_MOIRA.md](04_FASE_MOIRA.md) | Moira EventEmitter — Outbox Pattern EDA |
| 05 | [05_FASE_CONSULTA.md](05_FASE_CONSULTA.md) | Motor Analítico (Aegis) — Read Path |
| 05.01 | [05.01-JANUS.md](05.01-JANUS.md) | Cerebro Janus (AST Compiler) |
| 05.02 | [05.02_FASE_JANUS_AST_IR.md](05.02_FASE_JANUS_AST_IR.md) | Janus AST IR — Contrato Data-Driven |
| 05.03 | [05.03-AEGIS.md](05.03-AEGIS.md) | Aegis — Ejecutor del Read Path |
| 05.04 | [05.04-HERMES.md](05.04-HERMES.md) | **Hermes** — Unificador Analítico Multitipo (cross-domain OLTP↔OLAP) |
| 05.05 | [05.05_FORMULA_ENGINE.md](05.05_FORMULA_ENGINE.md) | Formula Engine — métricas derivadas |
| 05.core | [05_FASE_MOTOR_ANALITICO_CORE.md](05_FASE_MOTOR_ANALITICO_CORE.md) | Motor Analítico — núcleo |
| 05.llm | [LLM_FORMULA_ENGINE_CONTEXT.md](LLM_FORMULA_ENGINE_CONTEXT.md) | Contexto LLM del Formula Engine |
| 06 | [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md) | Cedar ABAC — Zero-Trust (5 pasos) |
| 07 | [07_FASE_QUOTA_GUARD.md](07_FASE_QUOTA_GUARD.md) | QuotaGuard — Control de Recursos Por Tenant |
| 07.01 | [07.01_EXTENSION_QUOTAS_BEDROCK_NOVA.md](07.01_EXTENSION_QUOTAS_BEDROCK_NOVA.md) | Extensión de cuotas — Bedrock Nova |
| 08 | [08_FASE_METRI_Q_ASSITANT.md](08_FASE_METRI_Q_ASSITANT.md) | Metri Q Assitant — Motor de Agentes Serverless y MCP |
| 08.01-3 | [METRI_Q.md](METRI_Q.md) | Integración, Súper-Poderes y Agentic UI de Metri Q |
| 09 | [09_FASE_AUDITORIA.md](09_FASE_AUDITORIA.md) | Auditoría OLTP/OLAP + IAuditInterceptor |
| 10 | [10_FASE_GESTION_ERRORES_EDA.md](10_FASE_GESTION_ERRORES_EDA.md) | Errores, OTel, Sherlog, Railway Pattern |
| 11 | [11_FASE_BASE_DE_DATOS_VECTORIAL_SERVERLESS.md](11_FASE_BASE_DE_DATOS_VECTORIAL_SERVERLESS.md) | Metri Serverless Vector Store (AWS Bedrock Nova) |
| 11A | [11A_DISEÑO_ENTIDAD_DOCUMENT_CHUNK.md](11A_DISEÑO_ENTIDAD_DOCUMENT_CHUNK.md) | Entidad `document_chunk` |
| 12 | [12_FASE_BULK_CSV_UPLOAD_INTEGRATION.md](12_FASE_BULK_CSV_UPLOAD_INTEGRATION.md) | Bulk CSV Upload |
| 13 | [13_FASE_CSV_EXPORT_INTEGRATION.md](13_FASE_CSV_EXPORT_INTEGRATION.md) | CSV Export |
| 14 | [14_FASE_AUTH_FLOWS.md](14_FASE_AUTH_FLOWS.md) | Flujos de autenticación |
| 14.1 | [14.1_FASE_AUTH_ENGINE_INTEGRATION.md](14.1_FASE_AUTH_ENGINE_INTEGRATION.md) | Auth ↔ Engine |
| 14.2 | [14.2_FASE_AUTH_APP_INTEGRATION.md](14.2_FASE_AUTH_APP_INTEGRATION.md) | Auth ↔ App |
| 14.3 | [14.3_FASE_AUTH_NOTIFICATIONS_TEMPLATES.md](14.3_FASE_AUTH_NOTIFICATIONS_TEMPLATES.md) | Auth ↔ Notifications (plantillas) |
| Ext-01 | [COMPONENTE_EXTERNO_01_EVENT_ROUTER.md](COMPONENTE_EXTERNO_01_EVENT_ROUTER.md) | Event Router — Golang, SQS→EventBridge |
| Ext-02 | [COMPONENTE_EXTERNO_02_ECHO.md](COMPONENTE_EXTERNO_02_ECHO.md) | Echo — Retry Engine (Golang, backoff exp.) |
| Ext-02b | [COMPONENTE_EXTERNO_02_BULK_COMPACTOR.md](COMPONENTE_EXTERNO_02_BULK_COMPACTOR.md) | Bulk Compactor — compactación de ingesta masiva |
| Ext-03 | [COMPONENTE_EXTERNO_03_METRI_AUTH.md](COMPONENTE_EXTERNO_03_METRI_AUTH.md) | Metri Auth — identidad y sesiones |
| Ext-04 | [COMPONENTE_EXTERNO_04_METRI_IOT.md](COMPONENTE_EXTERNO_04_METRI_IOT.md) | Metri IoT — Harvester MQTT, reglas de alerta, IA predictiva |
| Ext-05 | [COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md](COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md) | Metri Schedulers Hub — Chronos / Kairos / Iris (Patrón Boomerang) |
| Ext-05a | [COMPONENTE_EXTERNO_05_ANEXO_IAC.md](COMPONENTE_EXTERNO_05_ANEXO_IAC.md) | Anexo IaC del Schedulers Hub — `template.yaml` + Makefile |
| Ext-05b | [COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md](COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md) | Plan de implementación del Schedulers Hub — fases, dependencias, riesgos |
| Core-LE | [PLAN_IMPLEMENTACION_LIST_ENTITIES.md](PLAN_IMPLEMENTACION_LIST_ENTITIES.md) | Plan del RPC `ListEntities` — listado de entidades por atributo indexado |
| Ext-06 | [COMPONENTE_EXTERNO_06_METRI_MCP.md](COMPONENTE_EXTERNO_06_METRI_MCP.md) | MCP Proxy — Go / AWS Lambda Streaming (AI Connector) |
| Ext-07 | [COMPONENTE_EXTERNO_07_METRI_NOTIFICATIONS.md](COMPONENTE_EXTERNO_07_METRI_NOTIFICATIONS.md) | Metri Notifications — WebSocket, Push, Email, SMS |
| **—** | **[ANEXO_ESTRUCTURA_CODIGO.md](ANEXO_ESTRUCTURA_CODIGO.md)** | **SSOT — Carpetas, ficheros y capas del proyecto** |
