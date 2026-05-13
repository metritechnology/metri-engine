#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Metri Engine - Smoke Test Pipeline (FASE 2 / 3)
# ─────────────────────────────────────────────────────────────────────────────
# Ejecuta pruebas End-to-End contra el servidor gRPC (Netty) y el Lambda HTTP
# para verificar la integridad del Railway Pattern y la resolución de la Infra.
# 
# Requisitos:
# - grpcurl (brew install grpcurl)
# - jq (brew install jq)
# ─────────────────────────────────────────────────────────────────────────────

set -e

# Configuración
PROD_ENDPOINT="engine.metri.one:443"
LOCAL_ENDPOINT="localhost:9090"
PROTO_FILE="metri.proto"

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}==>${NC} $1"; }
log_warn() { echo -e "${YELLOW}==>${NC} $1"; }
log_err()  { echo -e "${RED}==>${NC} $1"; exit 1; }

# Validar dependencias
if ! command -v grpcurl &> /dev/null; then
    log_err "Dependencia faltante: grpcurl no está instalado. Ejecuta 'brew install grpcurl'"
fi

TARGET=$1
if [ -z "$TARGET" ]; then
    log_warn "Uso: ./bin/smoke_test.sh [local|prod]"
    exit 1
fi

if [ "$TARGET" = "prod" ]; then
    ENDPOINT=$PROD_ENDPOINT
    TLS_FLAG="" # usa TLS por defecto en 443
    log_info "🔥 Apuntando a PRODUCCIÓN: $ENDPOINT"
else
    ENDPOINT=$LOCAL_ENDPOINT
    TLS_FLAG="-plaintext"
    log_info "💻 Apuntando a LOCAL: $ENDPOINT"
fi

# ── 1. Prueba gRPC (HealthCheck y Reflection) ────────────────────────────────

log_info "Paso 1: Verificando gRPC Server Status..."
if grpcurl $TLS_FLAG $ENDPOINT grpc.health.v1.Health/Check > /dev/null; then
    log_info "✓ HealthCheck OK (SERVING)"
else
    log_warn "✗ Fallo en HealthCheck (¿Servidor gRPC encendido?)"
fi

# ── 2. Prueba gRPC Payload (Vía Transact) ────────────────────────────────────
# Enviamos un payload erróneo a propósito para verificar el Rich Error DTO 
# devuelto por la Infra (Ej: tabla inexistente o payload inválido).

log_info "Paso 2: Probando Endpoint 'Transact' (Verificando FASE 10 Error Handling)..."

REQUEST_PAYLOAD=$(cat <<EOF
{
  "tenant_id": "test-tenant-1",
  "entity_type": "user",
  "action": "CREATE",
  "payload": {
    "email": "test@metri.one"
  }
}
EOF
)

# Nota: Si el reflection no está habilitado en prod, usamos el .proto local
if [ "$TARGET" = "prod" ]; then
    GRPC_CMD="grpcurl -import-path . -proto $PROTO_FILE -d '$REQUEST_PAYLOAD' $ENDPOINT metri.data.grpc.MetriService/Transact"
else
    GRPC_CMD="grpcurl $TLS_FLAG -d '$REQUEST_PAYLOAD' $ENDPOINT metri.data.grpc.MetriService/Transact"
fi

echo -e "Ejecutando: \n$GRPC_CMD\n"
eval $GRPC_CMD || log_warn "Se detectó un error (Esperado según el Railway Pattern. Verifica el código devuelto)."

# ── 3. Prueba HTTP JSON (Simulación Lambda/Function URL) ────────────────────
# La interfaz Lambda parsea JSON directamente hacia el pipeline.

if [ "$TARGET" = "prod" ]; then
    log_info "Paso 3: Probando Lambda Function URL (REST Fallback)..."
    LAMBDA_URL="https://engine.metri.one/invoke" # Ajustar a la URL real del API Gateway/FURL
    
    HTTP_PAYLOAD=$(cat <<EOF
{
  "body": {
    "tenant_id": "test-tenant-1",
    "entity_type": "user",
    "action": "create",
    "payload": {
      "email": "test@metri.one"
    }
  }
}
EOF
)
    
    echo "Haciendo POST HTTP/JSON a $LAMBDA_URL..."
    curl -s -X POST "$LAMBDA_URL" \
         -H "Content-Type: application/json" \
         -d "$HTTP_PAYLOAD" | jq . || log_warn "No se pudo formatear la respuesta JSON."
fi

log_info "Smoke Tests finalizados. Verifica que las trazas de Sherlog (EventBridge) o OTel hayan sido emitidas en CloudWatch."
