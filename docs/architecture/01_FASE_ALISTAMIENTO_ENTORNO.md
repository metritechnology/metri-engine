# Fase 01 - Alistamiento de Entorno e Infraestructura Core

La primera fase de **Metri Engine** establece los cimientos para un despliegue puramente Serverless, enfocado en maximizar el rendimiento de Clojure JVM bajo AWS Lambda y asegurar la paridad matemática entre los entornos locales (Mocks) y la nube real.

## 1. Topología del Motor Base (AWS Lambda Java 21)

El motor principal está escrito íntegramente en Clojure. Dado que AWS Lambda es un entorno efímero, se han implementado estrategias de mitigación severas contra los famosos "Cold Starts" (Arranques en frío de la JVM).

- **MicroVM Firecracker Snapshot:** Declaramos el atributo `SnapStart: ApplyOn: PublishedVersions` en el template interactivo de AWS SAM (`template.yaml`). Esto provoca que la infraestructura AWS inicialice el runtime estándar (`Runtime: java21`), ejecute los preinicializadores de sistema estáticos, y congele la imagen inalterable de memoria en un disco cacheado local de Firecracker. Cada invocación la resucita hiper-rápido, ofreciendo el mejor equilibrio ecosistémico para las librerías complejas subyacentes como `Datahike`.
- **Evasión de Límites API Gateway:** Para soportar streams analíticos masivos asíncronos y SSE (Server Sent Events), la función Lambda está provista con `InvokeMode: RESPONSE_STREAM` (Lambda Function URLs). Al bypassar a API Gateway, se rompe el restrictivo corte HTTP de 29 segundos.

## 2. Paridad IAM (Validación Zero-Mock)

Las pruebas locales en Metri no dependen de burdas simulaciones en memoria. Utilizan el patrón SDK de Nube Real validando directamente contra los servicios administrados mediante la Credencial AWS por Defecto de SAM:

- Al ejecutar `sam local invoke`, el motor carga el archivo `env.json` y se rutea mediante la *Default Credentials Provider Chain* hacia los servicios remotos (S3, Athena, DynamoDB), probando que las políticas verdaderas descritas en IAC carezcan de fallos 403 (Zero-Trust Permissions).
- Evadimos Wrappers Legacy: Para invocar servicios colaterales, usamos en `pom.xml` oficial `AWS Java SDK V2` y su protocolo de asincronía purificado.

## 3. Topología de Almacenaje (Variables de Entorno)

La topología principal demanda la inicialización explícita de los destinos de red:

### A. Almacenaje Transaccional OLTP (Datahike Embedded + DynamoDB)
- `DATAHIKE_STORE_URI`: Identificador base que Datahike consume para saber si escribir estocásticamente en `/tmp` (Modo Pruebas) o anclarse definitivamente al DynamoDB Serverless `!Ref MetriSchemasTable`.
- `DATAHIKE_REGION`: Región subyacente de AWS (ej. `us-east-1`) para asegurar lecturas consistentes de zona.

### B. Almacenaje Analítico OLAP (AWS S3 + Athena)
- `AWS_S3_LAKE_BUCKET`: Nombre dinámico del S3 provisionado donde se vaciarán los DataFrames en crudo Parquet.
- `ATHENA_WORKGROUP`: Workgroup pre-autorizado en AWS Athena (ej. `metri-analytics`) contra el cual Datahike delegará las conjunciones herméticas que rebasen sus límites O(1).
- `AWS_PROFILE`: En desarrollo local (`sam local / docker-compose`), permite encolar la identidad para suplantar la falta de `ExecutionRole` real.

## 4. Orquestación Local (Docker Compose)

Todas las funciones accesorias del SaaS se coordinan tras un archivo robusto contenedor:
- Levanta localmente servicios auxiliares de Redis/Valkey (Sistema de Caché para Tokens Opacos y Control de Sesión rpc).
- Permite la visualización e inyección veloz sin afectar la cuota HTTP.
