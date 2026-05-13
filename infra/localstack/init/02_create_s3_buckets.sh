#!/bin/bash
# Crea los buckets S3 necesarios para el Data Lake local
set -e

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
AWS_CMD="aws --endpoint-url=$ENDPOINT --region=$REGION"

echo ">>> [S3] Creando buckets..."

$AWS_CMD s3api create-bucket \
  --bucket metri-lake-local \
  --region $REGION \
  2>/dev/null || echo "  [SKIP] metri-lake-local ya existe"

echo "  [OK] metri-lake-local"

$AWS_CMD s3api create-bucket \
  --bucket metri-athena-results-local \
  --region $REGION \
  2>/dev/null || echo "  [SKIP] metri-athena-results-local ya existe"

echo "  [OK] metri-athena-results-local"
echo ">>> [S3] Listo."
