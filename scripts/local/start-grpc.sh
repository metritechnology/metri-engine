#!/bin/bash
# scripts/local/start-grpc.sh — Levanta el servidor gRPC local completo
#
# Flujo:
#   1. Verifica prerequisitos (java, grpcurl)
#   2. Levanta infraestructura (DynamoDB Local, MinIO, ElasticMQ, Valkey)
#   3. Compila el uberjar de Clojure
#   4. Levanta el servidor gRPC Netty en localhost:9090
#   5. Verifica health con grpcurl
#
# Endpoint gRPC: localhost:9090
# Uso: ./scripts/local/start-grpc.sh

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Metri Engine — Servidor gRPC Local                     ║"
echo "║   Endpoint: grpc://localhost:9090                        ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Prerequisitos ───────────────────────────────────────────────────────────
command -v java     >/dev/null || { echo "❌ java no encontrado. Instalar: brew install --cask temurin"; exit 1; }
command -v grpcurl  >/dev/null || { echo "⚠️  grpcurl no encontrado (opcional). Instalar: brew install grpcurl"; }
command -v clj      >/dev/null || { echo "❌ clj no encontrado. Instalar: brew install clojure/tools/clojure"; exit 1; }

# ── 1. Levantar infraestructura ─────────────────────────────────────────────
echo "▶ [1/4] Levantando infraestructura (DynamoDB Local, MinIO, ElasticMQ, Valkey)..."
docker compose up -d dynamodb-local minio elasticmq valkey minio-init dynamodb-init

echo "       Esperando servicios healthy..."
until curl -sf http://localhost:8000/shell/ >/dev/null 2>&1; do printf "."; sleep 2; done
until curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do printf "."; sleep 2; done
until docker exec metri-valkey-local valkey-cli ping 2>/dev/null | grep -q PONG; do printf "."; sleep 1; done
echo " ✓"

# ── 2. Compilar uberjar ─────────────────────────────────────────────────────
echo "▶ [2/4] Compilando uberjar Clojure..."
clj -T:build uber
echo "  ✓ target/metri-engine.jar generado"

# ── 3. Preparar variables de entorno locales ────────────────────────────────
echo "▶ [3/4] Configurando variables de entorno locales..."

export ENVIRONMENT=local
export GRPC_PORT=9090
export GRPC_REFLECTION_ENABLED=true

export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# DynamoDB Local
export DATAHIKE_STORE_URI="datahike:dynamodb://us-east-1/metri-datahike-local"
export DATAHIKE_ENDPOINT="http://localhost:8000"
export METRI_SCHEMAS_TABLE="metri-schemas-local"
export DYNAMODB_QUOTA_TABLE="metri-quota-local"
export DYNAMODB_ENDPOINT="http://localhost:8000"

# MinIO S3
export AWS_S3_LAKE_BUCKET="metri-lake-local"
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="minioadmin"

# ElasticMQ SQS
export METRI_OUTBOX_QUEUE="http://localhost:9324/000000000000/metri-outbox.fifo"
export SQS_ENDPOINT="http://localhost:9324"

# Valkey
export VALKEY_HOST=localhost
export VALKEY_PORT=6379
export VALKEY_PASSWORD=""
export VALKEY_SSL=false

# OLAP stubs
export ATHENA_ENDPOINT="STUB"
export ATHENA_WORKGROUP="primary"

echo "  ✓"

# ── 4. Arrancar servidor gRPC ────────────────────────────────────────────────
echo "▶ [4/4] Arrancando servidor gRPC Netty en localhost:9090..."
echo "        (Ctrl+C para detener)"
echo ""

# Trap para cleanup al salir
trap 'echo ""; echo "Deteniendo servidor gRPC..."; exit 0' INT TERM

java \
  -Xmx1g \
  -Dfile.encoding=UTF-8 \
  -jar target/metri-engine.jar &

GRPC_PID=$!

# Esperar a que el servidor esté listo
echo "       Esperando que el servidor gRPC esté listo..."
sleep 5

for i in $(seq 1 12); do
  if command -v grpcurl >/dev/null && grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check >/dev/null 2>&1; then
    echo ""
    echo "✅ Servidor gRPC listo."
    echo ""
    echo "  Endpoint:  localhost:9090 (gRPC/HTTP2 plaintext)"
    echo ""
    echo "  ── Comandos de prueba ─────────────────────────────────────────────"
    echo "  # Listar todos los servicios (requiere GRPC_REFLECTION_ENABLED=true)"
    echo "  grpcurl -plaintext localhost:9090 list"
    echo ""
    echo "  # Describir el MetriService"
    echo "  grpcurl -plaintext localhost:9090 describe metre.MetriService"
    echo ""
    echo "  # Invocar rpc Transact"
    echo "  grpcurl -plaintext -d '{}' localhost:9090 metre.MetriService/Transact"
    echo ""
    echo "  # Health check"
    echo "  grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check"
    echo "  ────────────────────────────────────────────────────────────────────"
    echo ""
    break
  fi
  printf "."
  sleep 5
done

wait $GRPC_PID
