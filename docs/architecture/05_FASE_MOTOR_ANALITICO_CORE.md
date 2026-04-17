# Fase 05 - Motor Analítico Core (Unificación Datahike + Athena)

Esta fase describe la integración de alto nivel entre el motor operacional transaccional (Datahike) y el motor analítico masivo en la nube (AWS Athena mediante metadatos Iceberg o Parquet). 

## 1. Arquitectura de Dos Velocidades

El motor central de consultas no está amarrado a una única tecnología subyacente. Utiliza el **Aegis Transmuter** para tomar una decisión en milisegundos sobre hacia qué infraestructura enrutar la petición.

1.  **Fast Path (Latencia ms) -> Datahike Serverless:**
    *   **Propósito:** Dashboard operacional, visualización de activos individuales, y telemetría puntual.
    *   **Ventaja:** Resuelve el 90% de las tramas (UI lists) explorando el almacenamiento local de DynamoDB a través del modelo de ramificación funcional embebido Datalog. Garantiza el Control de Acceso nativo y evita el cold start masivo.

2.  **Bulk Path (Latencia Interanual) -> AWS Athena:**
    *   **Propósito:** Consolidaciones de costos sobre años de histórico, agregaciones multi-tenant complejas.
    *   **Ventaja:** Transforma la carga RAM intensiva en una carga asíncrona delegada a AWS. Rompe el límite lógico del JVM empujando HoneySQL a un clúster *Trino* gestionado de AWS que factura por volumen escaneado ($0 en inactividad).

## 2. Diagrama de Invocación Típico

\`\`\`mermaid
graph TD
    UI[Frontend Client] -->|gRPC rpc Query| API[AWS Lambda Function URL]
    API --> JANUS[Janus Orchestrator]
    JANUS -->|Raw ATS| AUTH[Cedar Interceptor]
    AUTH -->|Safe ATS| AEGIS[Aegis Compiler]
    
    AEGIS -- "if !metrics" --> J5[Datahike Query Engine]
    AEGIS -- "if metrics && heavy" --> J6[Athena Query Engine]
    
    J5 -- "Strategy :oltp" --> Datahike[(Datahike on DynamoDB)]
    J6 -- "Strategy :olap" --> Athena[(AWS Athena / S3)]
    
    Datahike -. "Compute Pushdown" .-> J5
    Athena -. "Async Wait" .-> J6
    
    J5 --> VizMeta[HATEOAS VizMeta]
    J6 --> VizMeta
    VizMeta -->|Stream| API
\`\`\`


Metri DSL / Metri Query Language (MQL)