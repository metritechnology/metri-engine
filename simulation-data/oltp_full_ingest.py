"""
oltp_full_ingest.py
===================
Ingest completo de datos de testing OLTP para Datahike.
Entidades:
  - location  : 5 ubicaciones (2 con parent → jerarquía real)
  - part       : 10 partes (referenciadas en inventory_movement)
  - inventory_movement : 300 movimientos en 90 días (tipo RECEIPT/ISSUE)

Timestamp: epoch en millisegundos (el executor OLTP normaliza >1e11 → /1000).
"""
import time
import struct
import logging
import requests
import hashlib
import random
from datetime import datetime, timedelta

import ulid as ulid_lib
import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant"
TOKEN        = "datalog-golden-tenant"


# ─── gRPC-Web helper ──────────────────────────────────────────────────────────

def invoke_grpc_web(endpoint: str, proto_req, retries=4, backoff=15):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                f"{FUNCTION_URL}{endpoint}",
                data=framed_data,
                headers={
                    'Content-Type':         'application/grpc-web+proto',
                    'X-Metri-Origin-Token': TOKEN,
                    'x-amz-content-sha256': payload_hash,
                },
                timeout=90,
            )
            if resp.status_code in (502, 503, 504):
                logging.warning(f"  [{attempt}/{retries}] HTTP {resp.status_code} (cold start). Retrying in {backoff}s...")
                time.sleep(backoff)
                continue
            if resp.status_code != 200:
                logging.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            rb = resp.content
            if len(rb) < 5:
                return None
            flag, length = struct.unpack('!BI', rb[:5])
            return rb[5:5 + length] if flag == 0x00 else None
        except requests.exceptions.Timeout:
            logging.warning(f"  [{attempt}/{retries}] Timeout. Retrying in {backoff}s...")
            time.sleep(backoff)
    logging.error("Máximo de reintentos alcanzado.")
    return None


def bulk_ingest(entity_type, columns, rows_data):
    """Envía un BulkRequest con las columnas y rows_data dados."""
    req = metri_pb2.BulkRequest()
    req.tenant_id  = TENANT_ID
    req.entity_type = entity_type
    req.action      = metri_pb2.UPSERT

    rs = req.data
    for col_key in columns:
        c = rs.columns.add()
        c.key = col_key

    for row_vals in rows_data:
        row = rs.rows_json.iter.add()
        for val in row_vals:
            v = row.values.add()
            if isinstance(val, str):
                v.string_value = val
            elif isinstance(val, (int, float)):
                v.number_value = float(val)
            # None → se omite (no se envía null_value que Datahike descarta)
            elif val is None:
                pass  # No añadir nada para campos null
            else:
                v.string_value = str(val)

    res = invoke_grpc_web("metri.MetriService/BulkIngest", req)
    if res:
        resp = metri_pb2.BulkResponse()
        resp.ParseFromString(res)
        if resp.status.success:
            logging.info(f"  ✅ {entity_type}: {resp.ingested_count} registros ingestados")
            return True
        else:
            logging.error(f"  ❌ {entity_type}: {resp.status.error_code} - {resp.status.error_message}")
    else:
        logging.error(f"  ❌ {entity_type}: sin respuesta")
    return False


# ─── Ingest ───────────────────────────────────────────────────────────────────

def ingest_all():
    now = datetime.utcnow()

    # ── 1. Locations (jerarquía real) ─────────────────────────────────────────
    logging.info("\n[1/3] Ingesting locations...")
    loc_root_1 = str(ulid_lib.new())
    loc_root_2 = str(ulid_lib.new())
    loc_child_1a = str(ulid_lib.new())
    loc_child_1b = str(ulid_lib.new())
    loc_child_2a = str(ulid_lib.new())

    # Roots: sin parent_location_id
    roots = [
        [loc_root_1,   "Almacén Central", "AC-001"],
        [loc_root_2,   "Almacén Norte",   "AN-002"],
    ]
    bulk_ingest("location", ["id", "name", "tag"], roots)

    # Children: con parent_location_id (string ULID)
    children = [
        [loc_child_1a, "Zona A",        "AC-A",  loc_root_1],
        [loc_child_1b, "Zona B",        "AC-B",  loc_root_1],
        [loc_child_2a, "Sector Norte 1","AN-S1", loc_root_2],
    ]
    bulk_ingest("location", ["id", "name", "tag", "parent_location_id"], children)
    all_loc_ids = [loc_root_1, loc_root_2, loc_child_1a, loc_child_1b, loc_child_2a]

    # ── 2. Parts ─────────────────────────────────────────────────────────────
    logging.info("\n[2/3] Ingesting parts...")
    part_ids = []
    part_rows = []
    for i in range(1, 11):
        pid = str(ulid_lib.new())
        part_ids.append(pid)
        part_rows.append([pid, f"Part-{i:03d}", f"SKU-{i:04d}"])
    bulk_ingest("part", ["id", "name", "sku"], part_rows)

    # ── 3. Inventory movements (300 registros, 90 días) ──────────────────────
    logging.info("\n[3/3] Ingesting inventory_movements...")
    TYPES    = ["RECEIPT", "ISSUE", "TRANSFER", "ADJUSTMENT"]
    TYPE_W   = [0.40,       0.35,    0.15,       0.10]        # pesos

    rows = []
    for i in range(1, 301):
        # Un movimiento por día (en promedio), distribuidos sobre los últimos 90 días
        day_offset = random.randint(0, 89)
        target_dt  = now - timedelta(days=day_offset,
                                     hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59))
        ts_ms = int(target_dt.timestamp() * 1000)   # epoch en ms

        mov_type = random.choices(TYPES, weights=TYPE_W)[0]
        quantity = round(random.uniform(10.0, 500.0), 2)
        part_id  = random.choice(part_ids)
        loc_id   = random.choice(all_loc_ids)

        rows.append([
            str(ulid_lib.new()),   # id
            ts_ms,                 # timestamp (ms epoch)
            quantity,              # quantity
            mov_type,              # type
            part_id,               # part_id
            loc_id,                # location_id
        ])

    # Enviar en batches de 100 para evitar timeout
    BATCH_SIZE = 100
    ok = True
    for b in range(0, len(rows), BATCH_SIZE):
        batch = rows[b:b + BATCH_SIZE]
        ok = bulk_ingest(
            "inventory_movement",
            ["id", "timestamp", "quantity", "type", "part_id", "location_id"],
            batch,
        ) and ok

    return ok


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.info("=== OLTP Full Ingest ===")
    logging.info(f"Target: {FUNCTION_URL}  tenant: {TENANT_ID}")
    success = ingest_all()
    if success:
        logging.info("\n✅ Ingest OLTP completo. Datahike listo para testing.")
    else:
        logging.error("\n❌ Hubo errores durante el ingest.")
