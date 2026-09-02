# Guía — Despliegue

> Verificada contra `Makefile` y `samconfig.toml` · 2 de septiembre de 2026

```bash
make deploy   # cargo build --release + sam build + sam deploy (perfil metri-dev, us-east-1)
```

## Qué crea el stack (`template.yaml`)

| Recurso | Uso |
|---|---|
| Lambda ARM64 `provided.al2023` (`bootstrap`) | El engine completo |
| Function URL + CloudFront + WAF + Route53 | Transporte gRPC (grpc-web) |
| KMS + Secrets Manager | Clave y secreto HMAC |
| Bucket S3 + Glue Database | Data lake Parquet / catálogo OLAP |
| 2× DynamoDB | Motor EAV + esquemas del Códice |
| 2× SQS (outbox + DLQ) | Eventos de dominio |

## Antes de desplegar a producción

- `HMAC_SECRET` fuerte en Secrets Manager — el arranque **aborta** con secreto débil o `ENVIRONMENT` desconocido (fail-closed).
- `ENVIRONMENT=production` explícito.
