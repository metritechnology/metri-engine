#!/bin/bash

# validate_proto.sh
# Herramienta en tiempo de ejecución para validar esquemas .proto

set -euo pipefail

PROTO_FILE=${1:-"../metri.proto"}

if [ ! -f "$PROTO_FILE" ]; then
    # Try looking in parent directory if it wasn't specified accurately
    if [ -f "../$PROTO_FILE" ]; then
         PROTO_FILE="../$PROTO_FILE"
    elif [ -f "./metri.proto" ]; then
         PROTO_FILE="./metri.proto"
    else
        echo "❌ Error: No se pudo encontrar el archivo $PROTO_FILE"
        exit 1
    fi
fi

INCLUDE_DIR=$(dirname "$PROTO_FILE")

echo -e "🔍 Analizando sintaxis de $PROTO_FILE..."

# Verificar si protoc está instalado
if command -v protoc &> /dev/null; then
    PROTOC_CMD="protoc"
elif python3 -c "import grpc_tools" &> /dev/null ; then
    PROTOC_CMD="python3 -m grpc_tools.protoc"
else
    echo "❌ Error: 'protoc' no fue encontrado nativamente, y tampoco 'grpcio-tools' de Python."
    echo "💡 Solución: Instala grpcio-tools usando: python3 -m pip install grpcio-tools"
    exit 1
fi

echo -e "⚙️  Ejecutando linter/compilador ($PROTOC_CMD)...\n"

# Descartar output compilado y enfocarnos solo en validación (descriptor_set_out a /dev/null)
if $PROTOC_CMD -I "$INCLUDE_DIR" --descriptor_set_out=/dev/null "$PROTO_FILE" 2>&1; then
    echo -e "\n✅ Validación Exitosa: El compilador verificó que el archivo es 100% estricto con las reglas de Proto3."
    exit 0
else
    echo -e "\n❌ Error de Validación: Se detectaron roturas de semántica/sintaxis en el archivo Proto."
    exit 1
fi
