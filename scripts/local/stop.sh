#!/bin/bash
# scripts/local/stop.sh — Detiene y limpia el stack local de Metri Engine
# Uso: ./scripts/local/stop.sh

set -e

echo "▶ Deteniendo stack local Metri Engine..."
docker compose down -v
echo "✅ Stack detenido y volúmenes eliminados."
