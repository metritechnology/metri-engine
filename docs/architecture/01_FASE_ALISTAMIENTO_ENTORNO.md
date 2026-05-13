# FASE 01 — Alistamiento de Entorno e Infraestructura Core

> **Documento Índice** — Rige la totalidad de los recursos AWS declarados en `template.yaml` (AWS SAM).  
> Cada sección mapea un bloque de recursos del template a su contrato arquitectónico.

La primera fase de **Metri Engine** establece los cimientos para un despliegue puramente Serverless en AWS, enfocado en tres invariantes:

1. **Arranque en frío cero** — SnapStart Firecracker sobre Java 21.
2. **Zero-Trust de red y datos** — VPC privada, KMS CMK, WAF/CloudFront + header secreto `X-Metri-Origin-Token` en el perímetro HTTP/2.
3. **Paridad local ↔ nube** — variables de entorno y credenciales AWS reales en pruebas locales (`sam local invoke`).

---

## Subfases

| Subfase   | Documento                                                                        | Responsabilidad                                                                                        |
| :-------- | :------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| **01.01** | [01.01_FASE_MAIN_BOOTSTRAP.md](01.01_FASE_MAIN_BOOTSTRAP.md)                     | Función `-main`, ciclo de vida Integrant, bootstrap fail-fast, REPL, Lambda handler                    |
| **01.02** | [01.02_FASE_CLIENTES_INFRAESTRUCTURA.md](01.02_FASE_CLIENTES_INFRAESTRUCTURA.md) | Todos los clientes de infraestructura: Datahike, Athena, Valkey, DynamoDB, SQS, OTel                   |
| **01.03** | [01.03_FASE_RUNTIME_GRPC.md](01.03_FASE_RUNTIME_GRPC.md)                         | Runtime gRPC: servidor Netty, service impl, traducción Protobuf ↔ Clojure, interceptores               |
| **01.04** | [01.04_FASE_LOCAL_ENV.md](01.04_FASE_LOCAL_ENV.md)                               | Entorno local de simulación: Docker Compose, LocalStack, Valkey, scripts init, `sam local invoke`, PoC |

---

## Parámetros del Stack (`Parameters`)

| Parámetro           | Default                   | Descripción                                                    |
| :------------------ | :------------------------ | :------------------------------------------------------------- |
| `CustomDomainName`  | `engine.metri.one`        | Dominio personalizado registrado en Route 53                   |
| `HostedZoneId`      | `Z0492322W8B6QV4SG4W4`    | Zona hospedada en Route 53 para el dominio base                |
| `AcmCertificateArn` | `arn:aws:acm:us-east-1:…` | Certificado SSL en `us-east-1` (obligatorio para CloudFront)   |
| `DatahikeTableName` | `metri-datahike-prod`     | Tabla DynamoDB del backend de persistencia Datahike Serverless |
| `AthenaWorkGroup`   | `metri-analytics`         | Workgroup operativo de AWS Athena                              |

> **Globals:** Toda función Lambda hereda `Timeout: 900s` y `MemorySize: 2048 MB`.

---

## 1. Escudo Criptográfico — KMS CMK (`MetriKmsMasterKey`)

**Propósito:** Llave Simétrica Maestra (CMK) compartida que cifra _en reposo_ todos los servicios del stack.

| Recurso SAM         | Tipo              | Detalle                                                                        |
| :------------------ | :---------------- | :----------------------------------------------------------------------------- |
| `MetriKmsMasterKey` | `AWS::KMS::Key`   | Rotación automática activada; política de acceso full al root IAM de la cuenta |
| `MetriKmsAlias`     | `AWS::KMS::Alias` | Alias legible: `alias/metri-engine-cmk`                                        |

**Servicios cifrados con esta CMK:**

- Variables de entorno de `MetriEngineFunction`
- Tablas DynamoDB (`MetriSchemasTable`)
- Colas SQS (`MetriOutboxQueue`, `MetriOutboxDLQ`)
- Bucket S3 (`MetriDataLakeBucket`)

---

## 2. Capa Perimetral — Edge Proxy HTTP/2 gRPC

### 2.1 WAF v2 (`MetriWebACL`)

`Scope: CLOUDFRONT` — el stack **debe desplegarse en `us-east-1`**.

| Regla WAF                               | Prioridad | Acción          | Propósito                                                           |
| :-------------------------------------- | :-------- | :-------------- | :------------------------------------------------------------------ |
| `MetriRateLimitRule`                    | 10        | Block           | Anti-DDoS: máx. 1 000 req / 5 s por IP                              |
| `AWSManagedRulesAmazonIpReputationList` | 20        | None (Override) | Bloqueo de botnets y proxies tóxicos vía telemetría AWS             |
| `AWSManagedRulesKnownBadInputsRuleSet`  | 30        | None (Override) | Protección JVM: corta cadenas JNDI / Log4Shell (vital para Clojure) |

### 2.2 Modelo Zero-Trust Perimetral — Header Secreto

> **Nota de diseño:** El stack **no usa CloudFront OAC (SigV4)**. En su lugar implementa un modelo Zero-Trust basado en header secreto inmutable.

La Lambda Function URL expone `AuthType: NONE` (pública por diseño gRPC). La protección perimetral se implementa en dos capas:

1. **CloudFront inyecta** el header `X-Metri-Origin-Token: ${AWS::StackId}` en cada request origen → Lambda. El valor es el `StackId` de CloudFormation, inmutable por stack.
2. **Lambda valida** el header en el interceptor de Zero-Trust. Requests sin el token correcto son rechazados con `UNAUTHENTICATED`.

| Campo                         | Valor                                                   |
| :---------------------------- | :------------------------------------------------------ |
| `MetriLambdaPublicPermission` | Permiso `lambda:InvokeFunctionUrl` con `Principal: "*"` |
| Header inyectado              | `X-Metri-Origin-Token: !Sub "${AWS::StackId}"`          |
| Variable de entorno           | `METRI_ORIGIN_TOKEN: !Ref AWS::StackId`                 |

### 2.3 CloudFront Distribution (`MetriCloudFrontDistribution`)

| Campo                   | Valor                                                                  |
| :---------------------- | :--------------------------------------------------------------------- |
| `HttpVersion`           | `http2` — soporte nativo gRPC stream                                   |
| `WebACLId`              | `!GetAtt MetriWebACL.Arn`                                              |
| `CachePolicyId`         | `4135ea2d-…` — **CachingDisabled** (esencial para streams gRPC)        |
| `OriginRequestPolicyId` | `b689b0a8-…` — **AllViewerExceptHostHeader** (evita choque TLS Lambda) |
| `ViewerProtocolPolicy`  | `redirect-to-https`                                                    |
| `AllowedMethods`        | `GET HEAD OPTIONS PUT PATCH POST DELETE`                               |
| `ViewerCertificate`     | ACM SNI-only con `AcmCertificateArn`                                   |

### 2.4 DNS — Route 53 (`MetriDnsRecord`)

Registro tipo `A` (Alias) apuntando al dominio de la distribución CloudFront.  
`HostedZoneId` del alias: `Z2FDTNDATAQYW2` (constante global para CloudFront).

---

## 3. Red Zero-Trust — VPC Privada

### 3.1 Topología VPC (`MetriVpc`)

| Recurso             | CIDR / Detalle                                        |
| :------------------ | :---------------------------------------------------- |
| `MetriVpc`          | `10.0.0.0/16` — DNS support + DNS hostnames activados |
| `PrivateSubnetA`    | `10.0.1.0/24` — AZ 0                                  |
| `PrivateSubnetB`    | `10.0.2.0/24` — AZ 1                                  |
| `PrivateRouteTable` | Tabla de rutas privada asociada a ambas subnets       |

> **No hay Internet Gateway ni NAT Gateway.** Todo el tráfico saliente de Lambda se dirige exclusivamente a través de VPC Endpoints.

### 3.2 Grupos de Seguridad

| Security Group              | Ingreso                              | Egreso                               | Propósito                                     |
| :-------------------------- | :----------------------------------- | :----------------------------------- | :-------------------------------------------- |
| `SecurityGroupLambda`       | —                                    | `0.0.0.0/0` (filtrado por Endpoints) | Egreso de Lambda hacia VPC Endpoints y Valkey |
| `SecurityGroupVPCEndpoints` | TCP 443 desde `SecurityGroupLambda`  | —                                    | Acceso HTTPS interno a Interface Endpoints    |
| `SecurityGroupValkey`       | TCP 6379 desde `SecurityGroupLambda` | —                                    | Acceso exclusivo de Lambda a Valkey           |

### 3.3 VPC Endpoints

| Endpoint           | Tipo                   | Servicio                          |
| :----------------- | :--------------------- | :-------------------------------- |
| `S3Endpoint`       | Gateway (gratuito)     | `com.amazonaws.{region}.s3`       |
| `DynamoDBEndpoint` | Gateway (gratuito)     | `com.amazonaws.{region}.dynamodb` |
| `SQSEndpoint`      | Interface (PrivateDNS) | `com.amazonaws.{region}.sqs`      |
| `AthenaEndpoint`   | Interface (PrivateDNS) | `com.amazonaws.{region}.athena`   |

Los endpoints Interface se despliegan en `PrivateSubnetA` con `SecurityGroupVPCEndpoints`.

---

## 4. Valkey Serverless — Session Store & Autorizador (`MetriValkeyCache`)

| Campo                 | Valor                                              |
| :-------------------- | :------------------------------------------------- |
| Tipo SAM              | `AWS::ElastiCache::ServerlessCache`                |
| `ServerlessCacheName` | `metri-valkey-cache`                               |
| `Engine`              | `valkey`                                           |
| Subnets               | `PrivateSubnetA` + `PrivateSubnetB`                |
| Security Group        | `SecurityGroupValkey` (solo TCP 6379 desde Lambda) |

**Variables de entorno expuestas a Lambda:**

| Variable          | Origen                                           |
| :---------------- | :----------------------------------------------- |
| `VALKEY_HOST`     | `!GetAtt MetriValkeyCache.Endpoint.Address`      |
| `VALKEY_PORT`     | `!GetAtt MetriValkeyCache.Endpoint.Port`         |
| `VALKEY_PASSWORD` | `""` (TLS gestionado por ElastiCache Serverless) |

---

## 5. Almacenamiento OLAP — Data Lake S3 (`MetriDataLakeBucket`)

| Campo          | Valor                                                              |
| :------------- | :----------------------------------------------------------------- |
| `BucketName`   | `metri-lake-{AccountId}-{Region}`                                  |
| Cifrado        | SSE-KMS con `MetriKmsMasterKey`                                    |
| Acceso público | **Bloqueado completamente** (las 4 flags `BlockPublic*` en `true`) |

**Variable de entorno:** `AWS_S3_LAKE_BUCKET`

---

## 6. Almacenamiento OLTP — Schema Store DynamoDB (`MetriSchemasTable`)

| Campo                 | Valor                           |
| :-------------------- | :------------------------------ |
| Tipo                  | `AWS::DynamoDB::Table`          |
| `BillingMode`         | `PAY_PER_REQUEST`               |
| PK                    | `PK` (String — HASH key)        |
| Cifrado               | SSE-KMS con `MetriKmsMasterKey` |
| `PointInTimeRecovery` | **Activado** (PITR)             |

**Variable de entorno:** `CEDAR_POLICIES_TABLE` → `!Ref MetriSchemasTable`

> **Nota:** Esta tabla alberga los esquemas JSON/Códice y las políticas Cedar para el motor de autorización ABAC. El nombre de variable fue actualizado de `METRI_SCHEMAS_TABLE` a `CEDAR_POLICIES_TABLE` para reflejar su rol funcional explícito.

---

## 7. Despacho EDA — Outbox SQS FIFO

### Arquitectura Dead-Letter Queue

```
MetriOutboxQueue (FIFO)
  └─ maxReceiveCount: 3
  └─ deadLetterTargetArn → MetriOutboxDLQ (FIFO)
```

| Recurso            | Nombre                  | Tipo                                                               |
| :----------------- | :---------------------- | :----------------------------------------------------------------- |
| `MetriOutboxDLQ`   | `metri-outbox-dlq.fifo` | `AWS::SQS::Queue` FIFO + ContentBasedDeduplication                 |
| `MetriOutboxQueue` | `metri-outbox.fifo`     | `AWS::SQS::Queue` FIFO + ContentBasedDeduplication + RedrivePolicy |

Ambas colas cifradas con `MetriKmsMasterKey` (`KmsDataKeyReusePeriodSeconds: 300`).

**Variable de entorno:** `OUTBOX_QUEUE_URL` → `!GetAtt MetriOutboxQueue.QueueUrl`

---

## 8. Clojure Engine — Lambda (`MetriEngineFunction`)

### 8.1 Runtime y SnapStart

| Campo              | Valor                                                              |
| :----------------- | :----------------------------------------------------------------- |
| `Runtime`          | `java21`                                                           |
| `Architectures`    | `x86_64`                                                           |
| `Handler`          | `metri.application.core::handleRequest`                            |
| `MemorySize`       | 2 048 MB (hereda de Globals)                                       |
| `Timeout`          | 900 s (hereda de Globals)                                          |
| `KmsKeyArn`        | `MetriKmsMasterKey` — cifra variables de entorno                   |
| `AutoPublishAlias` | `live`                                                             |
| `SnapStart`        | `ApplyOn: PublishedVersions` — Firecracker snapshot de memoria JVM |

### 8.2 Lambda Function URL

| Campo           | Valor                                                                                            |
| :-------------- | :----------------------------------------------------------------------------------------------- |
| `AuthType`      | `NONE` — acceso público; seguridad perimetral delegada a CloudFront WAF + `X-Metri-Origin-Token` |
| `InvokeMode`    | `BUFFERED` — compatible con SnapStart y el modelo gRPC actual                                    |
| `AllowOrigins`  | `https://engine.metri.one`, `http://localhost:3000`, `http://localhost:8080`                     |
| `ExposeHeaders` | `grpc-status`, `grpc-message`                                                                    |

> **Nota:** `BUFFERED` es el modo requerido cuando SnapStart está activo. El límite de 29 s de API Gateway no aplica aquí ya que se usa Lambda Function URL directa detrás de CloudFront.

### 8.3 VPC Config

La función se inyecta en `PrivateSubnetA` + `PrivateSubnetB` con `SecurityGroupLambda`.  
Sin acceso a internet; todo tráfico corre por los VPC Endpoints declarados en §3.3.

### 8.4 Políticas IAM (Least Privilege)

| Política               | Scope                                                                                                              |
| :--------------------- | :----------------------------------------------------------------------------------------------------------------- |
| `S3CrudPolicy`         | `MetriDataLakeBucket`                                                                                              |
| `DynamoDBCrudPolicy`   | `MetriSchemasTable`                                                                                                |
| `DynamoDBCrudPolicy`   | `DatahikeTableName` (parámetro externo)                                                                            |
| `SQSSendMessagePolicy` | `MetriOutboxQueue`                                                                                                 |
| KMS inline             | `kms:Decrypt`, `kms:Encrypt`, `kms:GenerateDataKey*` en `MetriKmsMasterKey`                                        |
| Athena inline          | `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution` en workgroup `AthenaWorkGroup` |

### 8.5 Variables de Entorno Completas

| Variable                | Origen                                      | Descripción                                                             |
| :---------------------- | :------------------------------------------ | :---------------------------------------------------------------------- |
| `ENVIRONMENT`           | `"production"`                              | Perfil de ejecución                                                     |
| `AWS_S3_LAKE_BUCKET`    | `!Ref MetriDataLakeBucket`                  | Bucket S3 Data Lake                                                     |
| `ATHENA_WORKGROUP`      | `!Ref AthenaWorkGroup`                      | Workgroup Athena OLAP                                                   |
| `DATAHIKE_DDB_TABLE`    | `!Ref DatahikeTableName`                    | Tabla DynamoDB del store transaccional Datahike                         |
| `CEDAR_POLICIES_TABLE`  | `!Ref MetriSchemasTable`                    | Tabla DynamoDB de esquemas JSON y políticas Cedar                       |
| `OUTBOX_QUEUE_URL`      | `!GetAtt MetriOutboxQueue.QueueUrl`         | URL de la cola SQS FIFO Outbox                                          |
| `KINESIS_STREAM_PREFIX` | `"metri-olap-"`                             | Prefijo de streams Kinesis para ingesta OLAP                            |
| `FAULT_BUS_NAME`        | `"metri-faults"`                            | Bus EventBridge para escalado de faults (Sherlog)                       |
| `VALKEY_HOST`           | `!GetAtt MetriValkeyCache.Endpoint.Address` | Endpoint Valkey Serverless                                              |
| `VALKEY_PORT`           | `!GetAtt MetriValkeyCache.Endpoint.Port`    | Puerto Valkey (6379)                                                    |
| `VALKEY_PASSWORD`       | `""`                                        | Contraseña Valkey (TLS gestionado por ElastiCache)                      |
| `FORCE_DEPLOY`          | `"2"`                                       | Forzar re-despliegue de SnapStart (incrementar para invalidar snapshot) |
| `METRI_ORIGIN_TOKEN`    | `!Ref AWS::StackId`                         | Token Zero-Trust inyectado por CloudFront para validación interna       |

---

## 9. Outputs del Stack

| Output                         | Descripción                                                                                      |
| :----------------------------- | :----------------------------------------------------------------------------------------------- |
| `MetriEngineFunctionPublicUrl` | URL nativa Lambda (expuesta públicamente; protegida por CloudFront WAF + `X-Metri-Origin-Token`) |
| `MetriCloudFrontDomain`        | Dominio intermedio CloudFront (`*.cloudfront.net`)                                               |
| `MetriEngineCustomDomain`      | **Endpoint final seguro:** `https://engine.metri.one`                                            |
| `MetriDataLakeArn`             | ARN del bucket S3 lago de datos                                                                  |
| `MetriValkeyEndpointInternal`  | Endpoint privado Valkey (TCP 6379 — solo accesible en la VPC)                                    |

---

## 10. Orquestación Local (Docker Compose)

El archivo `docker-compose.yml` replica localmente la topología de almacenaje:

| Servicio       | Imagen                     | Propósito                                               |
| :------------- | :------------------------- | :------------------------------------------------------ |
| Valkey 7.2     | `valkey/valkey:7.2-alpine` | Session store local (equivale a ElastiCache Serverless) |
| LocalStack 3.x | `localstack/localstack`    | Emula DynamoDB, S3, SQS, Athena, Kinesis, EventBridge   |
| Scripts init   | —                          | Crean tablas, buckets y colas al arrancar LocalStack    |

La invocación local usa `sam local invoke` con `env.json` y la _Default Credentials Provider Chain_ para validar políticas IAM reales (Zero-Mock).

Ver la configuración completa en **[01.02 — Clientes de Infraestructura §Módulo X](01.02_FASE_CLIENTES_INFRAESTRUCTURA.md#módulo-x-docker-composeyml-completo)**.

---

## 11. Diagrama de Arquitectura

```
Internet
   │  HTTPS (SNI)
   ▼
┌─────────────────────────────────────────────────┐
│  Route 53  →  CloudFront (HTTP/2)               │
│              WAF v2 (3 reglas)                  │
│              Inyecta: X-Metri-Origin-Token       │
└────────────────────────┬────────────────────────┘
                         │  HTTPS + X-Metri-Origin-Token
                         ▼
                Lambda Function URL
                (AuthType: NONE / BUFFERED)
                         │
         ┌───────────────┘
         │  MetriEngineFunction (Java 21 SnapStart)
         │  Valida: X-Metri-Origin-Token == METRI_ORIGIN_TOKEN
         │  VPC: PrivateSubnetA / PrivateSubnetB
         │
         ├──[Gateway EP]──→  DynamoDB  (Datahike + Cedar Policies)
         ├──[Gateway EP]──→  S3        (Data Lake Iceberg)
         ├──[Interface EP]─→ SQS FIFO  (Outbox + DLQ)
         ├──[Interface EP]─→ Athena    (OLAP queries)
         └──[SG TCP 6379]──→ Valkey Serverless (Session Store)
```

Todos los canales de datos en reposo cifrados con `alias/metri-engine-cmk` (KMS CMK, rotación automática).
