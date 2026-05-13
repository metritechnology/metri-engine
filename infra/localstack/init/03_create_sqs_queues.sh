#!/bin/bash
# Crea las colas SQS FIFO del Outbox Pattern (Moira)
set -e

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
AWS_CMD="aws --endpoint-url=$ENDPOINT --region=$REGION"

echo ">>> [SQS] Creando colas FIFO..."

# 1. DLQ primero (la cola principal la referencia en la RedrivePolicy)
$AWS_CMD sqs create-queue \
  --queue-name metri-outbox-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  2>/dev/null || echo "  [SKIP] metri-outbox-dlq.fifo ya existe"

echo "  [OK] metri-outbox-dlq.fifo"

# Obtener ARN de la DLQ
DLQ_ARN=$($AWS_CMD sqs get-queue-attributes \
  --queue-url "http://localhost:4566/000000000000/metri-outbox-dlq.fifo" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

# 2. Cola principal con RedrivePolicy apuntando a la DLQ
REDRIVE_POLICY="{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"3\"}"
$AWS_CMD sqs create-queue \
  --queue-name metri-outbox.fifo \
  --attributes \
    FifoQueue=true,ContentBasedDeduplication=true,RedrivePolicy="$REDRIVE_POLICY" \
  2>/dev/null || echo "  [SKIP] metri-outbox.fifo ya existe"

echo "  [OK] metri-outbox.fifo"
echo ">>> [SQS] Listo."
