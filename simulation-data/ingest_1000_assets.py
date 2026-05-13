#!/usr/bin/env python3
"""
ingest_1000_assets.py
─────────────────────────────────────────────────────────────────────────────
Ingesta 1 000 assets en golden-tenant reutilizando el patrón de transporte
gRPC-Web establecido en ingest_locations.py:
  - Content-Type: application/grpc-web+proto  (binario, no base64)
  - x-amz-content-sha256: sha256(framed_data)
  - Authorization: Bearer <token>
"""

import time
import struct
import hashlib
import json
import logging
import random
import requests
import google.protobuf.json_format as json_format
import google.protobuf.struct_pb2 as struct_pb2

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant"
AUTH_TOKEN   = "datalog-golden-tenant"

# ── Catalogues for realistic data ──────────────────────────────────────────────

ASSET_CATEGORIES = [
    "23-17.11.00",  # HVAC Equipment
    "23-17.31.00",  # Electrical Equipment
    "23-17.41.00",  # Plumbing Equipment
    "23-17.51.00",  # Fire Protection Equipment
    "23-17.61.00",  # Lighting Equipment
    "23-17.71.00",  # IT / Network Equipment
    "23-17.81.00",  # Security Equipment
    "23-17.91.00",  # Elevator / Transport Equipment
]

STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE", "IN_MAINTENANCE"]  # weighted

ASSET_NAMES = [
    "Compresor", "Chilller", "Bomba", "Generador", "Transformador",
    "UPS", "Panel Eléctrico", "Luminaria", "Sensor", "Cámara",
    "Ascensor", "Extractor", "Caldera", "Válvula", "Filtro",
    "Router", "Switch", "Servidor", "Rack", "Gabinete",
]

# ── gRPC-Web transport ─────────────────────────────────────────────────────────

def invoke_grpc_web(endpoint: str, proto_req):
    proto_bytes  = proto_req.SerializeToString()
    framed_data  = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    resp = requests.post(
        f"{FUNCTION_URL}{endpoint}",
        data=framed_data,
        headers={
            'Content-Type':         'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash,
            'Authorization':        f'Bearer {AUTH_TOKEN}',
        },
        stream=True,
        timeout=60,
    )

    if resp.status_code != 200:
        logging.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
        return []

    messages, offset = [], 0
    data = resp.content
    while offset < len(data):
        if offset + 5 > len(data):
            break
        flag, length = struct.unpack('!BI', data[offset:offset+5])
        offset += 5
        if flag == 0x00:
            messages.append(data[offset:offset+length])
        offset += length
    return messages


# ── Asset creator ──────────────────────────────────────────────────────────────

def create_asset(index: int) -> bool:
    name     = f"{random.choice(ASSET_NAMES)} {index:04d}"
    status   = random.choice(STATUSES)
    tag      = f"TAG-{10000 + index}"
    category = random.choice(ASSET_CATEGORIES)

    payload = {
        "name":               name,
        "status":             status,
        "tag":                tag,
        "omniclass_category": category,
    }

    req             = metri_pb2.TransactionRequest()
    req.tenant_id   = TENANT_ID
    req.entity_type = "asset"
    req.action      = metri_pb2.CREATE

    struct_payload = struct_pb2.Struct()
    struct_payload.update(payload)
    req.payload.CopyFrom(struct_payload)

    messages = invoke_grpc_web("metri.MetriService/Transact", req)
    if not messages:
        logging.warning(f"  ✗ Asset {index}: sin respuesta")
        return False

    resp = metri_pb2.TransactionResponse()
    resp.ParseFromString(messages[0])

    d = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
    if d.get("status", {}).get("success"):
        return True
    else:
        err = d.get("status", {}).get("error_message", "unknown")
        logging.warning(f"  ✗ Asset {index}: {err}")
        return False


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total   = 1000
    success = 0
    failed  = 0

    print("\n" + "═" * 60)
    print("  Metri — Ingesta masiva de 1 000 Assets")
    print(f"  Tenant: {TENANT_ID}")
    print("═" * 60 + "\n")

    for i in range(1, total + 1):
        ok = create_asset(i)
        if ok:
            success += 1
        else:
            failed += 1

        if i % 50 == 0:
            print(f"  Progreso: {i}/{total}  ✅ {success}  ✗ {failed}")

        time.sleep(0.05)  # slight throttle — avoid Lambda rate limits

    print(f"\n{'═'*60}")
    print(f"  ✅ Ingesta finalizada: {success}/{total} assets creados")
    if failed:
        print(f"  ⚠  Fallidos: {failed}")
    print(f"{'═'*60}\n")
