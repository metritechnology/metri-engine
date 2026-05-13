#!/usr/bin/env python3
"""
ingest_locations.py
───────────────────────────────────────────────────────────────────────────────
Ingesta 20 ubicaciones con relaciones padre-hijo a través del Metri Engine.

Estructura jerárquica:
  SITES (3)
    └── BUILDINGS (5) — 1-2 por site
        └── FLOORS (6) — 1-2 por building
            └── ROOMS (4) + ZONES (2) — por floor

Orden de ingesta: de afuera hacia adentro para resolver dependencias.
"""

import time
import struct
import hashlib
import json
import logging
import requests
import google.protobuf.json_format as json_format
import google.protobuf.struct_pb2 as struct_pb2

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant"
AUTH_TOKEN   = "datalog-golden-tenant"

# ─── gRPC-Web transport ───────────────────────────────────────────────────────

def invoke_grpc_web(endpoint: str, proto_req):
    proto_bytes  = proto_req.SerializeToString()
    framed_data  = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    resp = requests.post(
        f"{FUNCTION_URL}{endpoint}",
        data=framed_data,
        headers={
            'Content-Type':        'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash,
            'Authorization':       f'Bearer {AUTH_TOKEN}',
        },
        stream=True,
        timeout=60,
    )

    if resp.status_code != 200:
        logging.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
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


# ─── Transact helper ──────────────────────────────────────────────────────────

def create_location(name: str, loc_type: str, parent_id: str | None = None) -> str | None:
    """
    Creates a location entity and returns its assigned entity_id (ULID).
    Returns None on failure.
    """
    payload = {
        "name":  name,
        "type":  loc_type,
    }
    if parent_id:
        payload["parent_location_id"] = parent_id

    req     = metri_pb2.TransactionRequest()
    req.tenant_id   = TENANT_ID
    req.entity_type = "location"
    req.action      = metri_pb2.CREATE

    struct_payload = struct_pb2.Struct()
    struct_payload.update(payload)
    req.payload.CopyFrom(struct_payload)

    messages = invoke_grpc_web("metri.MetriService/Transact", req)
    if not messages:
        logging.error(f"  ✗ Sin respuesta para '{name}'")
        return None

    resp = metri_pb2.TransactionResponse()
    resp.ParseFromString(messages[0])

    d = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
    if d.get("status", {}).get("success"):
        entity_id = d.get("entity_id", "")
        logging.info(f"  ✓ {loc_type:8s} '{name}' → {entity_id}")
        return entity_id
    else:
        err = d.get("status", {}).get("error_message", "unknown error")
        logging.error(f"  ✗ {loc_type:8s} '{name}' → {err}")
        return None


# ─── Location tree definition ─────────────────────────────────────────────────

LOCATION_TREE = [
    {
        "name": "Campus Metropolitano Norte",
        "type": "SITE",
        "children": [
            {
                "name": "Torre Alfa",
                "type": "BUILDING",
                "children": [
                    {
                        "name": "Piso 1 - Torre Alfa",
                        "type": "FLOOR",
                        "children": [
                            {"name": "Sala de Reuniones A1",  "type": "ROOM"},
                            {"name": "Oficina Dirección A1",  "type": "ROOM"},
                            {"name": "Zona Común P1",        "type": "ZONE"},
                        ],
                    },
                    {
                        "name": "Piso 2 - Torre Alfa",
                        "type": "FLOOR",
                        "children": [
                            {"name": "Laboratorio TI A2",   "type": "ROOM"},
                            {"name": "Zona Servidores P2",  "type": "ZONE"},
                        ],
                    },
                ],
            },
            {
                "name": "Torre Beta",
                "type": "BUILDING",
                "children": [
                    {
                        "name": "Piso 1 - Torre Beta",
                        "type": "FLOOR",
                        "children": [
                            {"name": "Recepción Beta",     "type": "ROOM"},
                            {"name": "Sala Conferencia B", "type": "ROOM"},
                        ],
                    },
                ],
            },
        ],
    },
    {
        "name": "Planta Industrial Sur",
        "type": "SITE",
        "children": [
            {
                "name": "Nave de Producción",
                "type": "BUILDING",
                "children": [
                    {
                        "name": "Área de Manufactura",
                        "type": "FLOOR",
                        "children": [
                            {"name": "Línea Ensamblaje 1", "type": "ZONE"},
                            {"name": "Almacén de Materia Prima", "type": "ROOM"},
                        ],
                    },
                ],
            },
        ],
    },
    {
        "name": "Centro de Datos DC-01",
        "type": "SITE",
        "children": [
            {
                "name": "Módulo Principal DC",
                "type": "BUILDING",
                "children": [
                    {
                        "name": "Sala de Racks A",
                        "type": "FLOOR",
                        "children": [
                            {"name": "Rack Zone 1-8",   "type": "ZONE"},
                            {"name": "UPS Room DC",     "type": "ROOM"},
                        ],
                    },
                ],
            },
        ],
    },
]


# ─── Recursive ingest ─────────────────────────────────────────────────────────

def ingest_tree(nodes: list, parent_id: str | None = None, depth: int = 0) -> int:
    count = 0
    for node in nodes:
        indent = "  " * depth
        logging.info(f"{indent}→ Ingesting: [{node['type']}] {node['name']}")
        entity_id = create_location(node["name"], node["type"], parent_id)
        count += 1
        if entity_id and "children" in node:
            time.sleep(0.25)  # slight delay to avoid rate limiting
            count += ingest_tree(node["children"], entity_id, depth + 1)
        elif not entity_id:
            logging.warning(f"{indent}  Skipping children of '{node['name']}' (parent creation failed)")
    return count


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "═" * 60)
    print("  Metri Location Tree Ingestion")
    print("  Tenant:", TENANT_ID)
    print("═" * 60 + "\n")

    total = ingest_tree(LOCATION_TREE)

    print(f"\n{'═'*60}")
    print(f"  ✅ Ingesta completada: {total} ubicaciones creadas")
    print(f"{'═'*60}\n")
