#!/bin/bash
# scripts/local/start.sh — Levanta el stack local completo de Metri Engine
# Stack 100% gratuito: DynamoDB Local + MinIO + ElasticMQ + Valkey
# Sin LocalStack Pro, sin auth tokens, sin costos AWS
# Uso: ./scripts/local/start.sh

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Metri Engine — Local Simulation Stack (Free)           ║"
echo "║   DynamoDB Local · MinIO · ElasticMQ · Valkey            ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Variables locales
DDB_ENDPOINT="http://localhost:8000"
S3_ENDPOINT="http://localhost:9000"
SQS_ENDPOINT="http://localhost:9324"
AWS_OPTS="--region us-east-1 --no-sign-request"

# 1. Levantar contenedores
echo "▶ [1/5] Levantando Docker Compose..."
docker compose up -d
echo "  ✓"

# 2. Esperar DynamoDB Local
echo "▶ [2/5] Esperando DynamoDB Local (puerto 8000)..."
until curl -sf "$DDB_ENDPOINT" > /dev/null 2>&1 || [ $? -eq 22 ] || [ $? -eq 52 ]; do
  printf "."
  sleep 2
done
echo " ✓"

# 3. Esperar MinIO
echo "▶ [3/5] Esperando MinIO (puerto 9000)..."
until curl -sf "$S3_ENDPOINT/minio/health/live" > /dev/null 2>&1; do
  printf "."
  sleep 2
done
echo " ✓"

# 4. Esperar ElasticMQ
echo "▶ [4/5] Esperando ElasticMQ (puerto 9324)..."
until curl -sf "http://localhost:9325" > /dev/null 2>&1; do
  printf "."
  sleep 2
done
echo " ✓"

# 5. Esperar Valkey
echo "▶ [5/5] Esperando Valkey (puerto 6379)..."
until docker exec metri-valkey-local valkey-cli ping 2>/dev/null | grep -q PONG; do
  printf "."
  sleep 1
done
echo " ✓"

# Verificar recursos
echo ""
echo "▶ Verificando recursos..."

TABLES=$(aws dynamodb list-tables \
  --endpoint-url $DDB_ENDPOINT $AWS_OPTS \
  --query 'TableNames' --output text 2>/dev/null || echo "(error)")
echo "  DynamoDB tables: $TABLES"

BUCKETS=$(aws s3api list-buckets \
  --endpoint-url $S3_ENDPOINT \
  --aws-access-key-id minioadmin \
  --aws-secret-access-key minioadmin $AWS_OPTS \
  --query 'Buckets[].Name' --output text 2>/dev/null || echo "(error)")
echo "  S3 buckets:      $BUCKETS"

echo "  SQS queues:      metri-outbox.fifo, metri-outbox-dlq.fifo (ElasticMQ)"
echo "  Valkey:          localhost:6379 PONG ✓"

echo ""
echo "✅ Stack local listo."
echo ""
echo "  DynamoDB Local: $DDB_ENDPOINT      (aws-cli: --endpoint-url $DDB_ENDPOINT)"
echo "  MinIO S3:       $S3_ENDPOINT       (consola: http://localhost:9001)"
echo "  ElasticMQ SQS:  $SQS_ENDPOINT      (web UI: http://localhost:9325)"
echo "  Valkey:         redis://localhost:6379"
echo ""
echo "  Para invocar Lambda localmente:"
echo "  → sam build && sam local invoke MetriEngineFunction \\"
echo "      -e events/poc_request.json --env-vars env.json"
echo ""
