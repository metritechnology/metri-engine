# Componente Externo 02: Hephaestus Bulk Compactor (Élite Data Engineering)

El componente final de la destilación de datos de Metri Engine es infernal. El propósito es tomar la incalculable suma en crudo del *Lago de Transacciones Datalog* asíncronas almacenadas en objetos estáticos, pre-procesarlos matemáticamente y comprimirlos como diamantes al DataLake final (`S3` base).

Esta responsabilidad no la efectúa Clojure ni Datahike. Recae absolutamente en **Hephaestus**, un micro-daemon asincrónico implementado puramente en Golang (Goroutines Concurrentes AWS Lambda).

---

## 1. El Parquet C-Memory Flow (Apache Arrow)

En Data Lakes Serverless (AWS Athena / Iceberg), alojar datos en JSON o CSV es garantía absoluta de colapso de facturación B2B. Los motores escanean la fila ineficiente.

Hephaestus combate esto empleando directrices extremas C-Memory:
- Subsistema acoplado pasivamente a los eventos **Amazon Kinesis Firehose** o Cron AWS, recibiendo lotes empaquetados pesados.
- **Transmutación Apache Arrow:** Invoca un buffer de la librería `apache-arrow` oficial de Golang. Organiza localmente de orientación "por Fila" (Row-Based) a "Columnar".
- Genera y emite al Disco estático (`S3_LAKE_BUCKET`) ficheros profundamente segmentados de formato orgánico `.Parquet`. Cada columna comprada Snappy ocupa 1/15 del tamaño original, minimizando lecturas futuras en la fase MQL (Janus).
- Las fechas de los particionamientos se estampan como pre-sufijos léxicos: `.../year=2026/month=04/tenant=xyz_1...` asegurando indexaciones directas.

---

## 2. Inyección Criptográfica de Performance (Pre-Computación Fonética)

Los despliegues de **Full-Text-Search** arruinan las analíticas Data-Lake B2B por falta de índexaciones en S3. 
Si el modelo original no decreta `disable_fts` absoluto, Hephaestus toma control:

1. El worker localiza descriptores alfanuméricos largos (`work_order.description`).
2. Muta la memoria Arrow para crear virtualmente una columna clónica auxiliar: `work_order.description_soundex_fts`.
3. Procesa algoritmos rápidos léxicos sobre la columna clon inyectándolos en minúsculas. 

Cuando la consulta Trino (generada por HoneySQL / Aegis Engine) pide `Buscar "Alambre"`, el query se empuja al metadato sintético fonético pre-procesado, eludiendo la mortal invocación y el escaneo de cadenas de texto complejas y permitiendo latencias asincrónicas en O(1).

---

## 3. Topología de Integración IAM Segura & Fallos OTel

1. **Autorización KMS Ciega:** Hephaestus desencripta y asegura la envoltura final de datos operando sobre llaves estáticas controladas localmente por su Rol de EC2 y AWS Key Management Service IAM (KMS). 
2. **Observabilidad EDA (Ouroboros Error Framework):** Si la Lambda detona un corte súbito por falta de tiempo AWS Lambda en compactación de Batch ultra masivo; Golang absorbe el *panic*, no pierde el buffer de Kinesis, y reporta el fracaso sintáctico como "Agotamiento Temporal" hacia el Cloudwatch global con la rúbrica estampada en `OpenTelemetry` a los canales nativos de observabilidad, salvando por completo los Checkpoints relacionales Datalog (Tolerancia Extrema EDA cero excepciones perdidas).
