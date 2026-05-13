#!/bin/bash
# Crea todas las tablas DynamoDB que Metri Engine necesita en LocalStack
# Ejecutado automáticamente por LocalStack al arrancar (ready.d/)
set -e

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
AWS_CMD="aws --endpoint-url=$ENDPOINT --region=$REGION"

echo ">>> [DynamoDB] Creando tablas..."

# 1. Tabla Datahike — backend de persistencia EAV para Datahike Serverless
$AWS_CMD dynamodb create-table \
  --table-name metri-datahike-local \
  --attribute-definitions AttributeName=Id,AttributeType=S \
  --key-schema AttributeName=Id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  2>/dev/null || echo "  [SKIP] metri-datahike-local ya existe"

echo "  [OK] metri-datahike-local"

# 2. Tabla Schemas — diccionario JSON del Códice (Schema-Driven Core)
$AWS_CMD dynamodb create-table \
  --table-name metri-schemas-local \
  --attribute-definitions AttributeName=PK,AttributeType=S \
  --key-schema AttributeName=PK,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  2>/dev/null || echo "  [SKIP] metri-schemas-local ya existe"

echo "  [OK] metri-schemas-local"

# 3. Tabla Quota — contadores atómicos por tenant (QuotaGuard)
$AWS_CMD dynamodb create-table \
  --table-name metri-quota-local \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  2>/dev/null || echo "  [SKIP] metri-quota-local ya existe"

echo "  [OK] metri-quota-local"
echo ">>> [DynamoDB] Listo."
