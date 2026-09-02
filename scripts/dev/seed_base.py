#!/usr/bin/env python3
import sys
import os
import json
import uuid
import random
import time
import hmac
import hashlib
import base64
from typing import Optional
from pathlib import Path

# Add proto directory to path
SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

try:
    import grpc
    import metri_pb2 as pb
    import metri_pb2_grpc as pb_grpc
    from google.protobuf import struct_pb2
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

MODELS_DIR = ENGINE_ROOT / "config" / "models"
DEFAULT_HMAC_SECRET = "local-dev-secret-do-not-use-in-prod"

# Helper for signed tokens (Zero-Trust authorization)
def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

def generate_ulid() -> str:
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    return "01" + "".join(random.choice(alphabet) for _ in range(24))

def load_schema(entity: str) -> Optional[dict]:
    path = MODELS_DIR / f"{entity}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

class SeederBase:
    def __init__(self, tenant_id: str, host: str, port: int, hmac_secret: str, dry_run: bool):
        self.tenant_id = tenant_id
        self.host = host
        self.port = port
        self.dry_run = dry_run
        self.context = {"tenant_id": tenant_id}
        self.stats = {}
        
        token = generate_signed_token(hmac_secret, tenant_id, "usr_system_bff")
        self.metadata = [('authorization', f"Bearer {token}")]
        self._stub = None

    def get_stub(self):
        if self._stub is None:
            if not GRPC_AVAILABLE:
                raise RuntimeError("gRPC libraries not imported correctly.")
            channel = grpc.insecure_channel(f"{self.host}:{self.port}")
            self._stub = pb_grpc.MetriServiceStub(channel)
        return self._stub

    def transact(self, entity: str, payload: dict) -> Optional[str]:
        if self.dry_run:
            print(f"    [DRY] Transact {entity}: {json.dumps(payload)[:80]}...")
            return f"dry-{entity[:4]}-{uuid.uuid4().hex[:8]}"

        stub = self.get_stub()
        struct_payload = struct_pb2.Struct()
        struct_payload.update(payload)
        req = pb.TransactionRequest(
            tenant_id=self.tenant_id,
            entity_type=entity,
            action=pb.CREATE,
            payload=struct_payload
        )
        try:
            resp = stub.Transact(req, timeout=10, metadata=self.metadata)
            if resp.status.success:
                return resp.entity_id
            print(f"    [ERROR] {entity}: {resp.status.error_code} - {resp.status.error_message}")
        except Exception as e:
            print(f"    [ERROR] {entity}: {e}")
        return None

    def bulk_ingest(self, entity: str, payloads: list[dict]) -> int:
        if self.dry_run:
            print(f"    [DRY] BulkIngest {entity}: {len(payloads)} items")
            return len(payloads)

        schema = load_schema(entity)
        if not schema:
            return 0
        attrs = [a["name"] for a in schema.get("attributes", []) if not a.get("auto_generate")]
        if not attrs:
            return 0
        if "id" not in attrs and any("id" in p for p in payloads):
            attrs.append("id")

        columns = [pb.ColumnSchema(key=k, label=k, type="string") for k in attrs]
        rows = []
        for p in payloads:
            values = []
            for k in attrs:
                v = p.get(k)
                if v is None:
                    values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(null_value=0))
                elif isinstance(v, bool):
                    values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(bool_value=v))
                elif isinstance(v, (int, float)):
                    values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(number_value=float(v)))
                else:
                    values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(string_value=str(v)))
            rows.append(pb.DataRow(values=values))

        rowset = pb.RowSet(columns=columns, rows_json=pb.DataRowList(iter=rows))
        req = pb.BulkRequest(
            tenant_id=self.tenant_id,
            entity_type=entity,
            action=pb.CREATE,
            data=rowset
        )
        try:
            resp = self.get_stub().BulkIngest(req, timeout=30, metadata=self.metadata)
            if resp.status.success:
                return resp.ingested_count
            print(f"    [ERROR] BulkIngest {entity}: {resp.status.error_message}")
        except Exception as e:
            print(f"    [ERROR] BulkIngest {entity}: {e}")
        return 0

    def clear_entity(self, entity: str):
        if self.dry_run:
            print(f"    [DRY] Clear entity {entity} (skipping)")
            return

        print(f"  → Limpiando tabla {entity}...")
        stub = self.get_stub()
        
        analytics_req = pb.AnalyticsRequest(
            tenant_id=self.tenant_id,
            entity=entity,
            limit=50000,
        )
        query_req = pb.QueryRequest(
            tenant_id=self.tenant_id,
            queries={"list": analytics_req}
        )
        
        try:
            responses = stub.Query(query_req, timeout=30, metadata=self.metadata)
            columns = []
            rows = []
            for resp in responses:
                if resp.status and not resp.status.success:
                    print(f"    [ERROR] Query {entity} falló: {resp.status.error_message}")
                    return
                
                batch_res = resp.batch_results.get("list") if resp.batch_results else None
                row_set = batch_res.data if batch_res else resp.data
                
                if row_set:
                    if row_set.columns:
                        columns = list(row_set.columns)
                    if row_set.rows_json and row_set.rows_json.iter:
                        rows.extend(list(row_set.rows_json.iter))
            
            if not rows:
                print(f"    ✓ No se encontraron {entity}s para limpiar.")
                return

            id_col_idx = -1
            for idx, col in enumerate(columns):
                if col.key == "id" or col.key == f"{entity}/id":
                    id_col_idx = idx
                    break
            
            if id_col_idx == -1:
                for idx, col in enumerate(columns):
                    if "id" in col.key.lower():
                        id_col_idx = idx
                        break
            
            if id_col_idx == -1:
                print(f"    [WARNING] No se pudo determinar la columna de ID para {entity}. Columnas: {[c.key for c in columns]}")
                return

            entity_ids = []
            for r in rows:
                if id_col_idx < len(r.values):
                    val = r.values[id_col_idx]
                    eid = val.string_value
                    if eid:
                        entity_ids.append(eid)

            if not entity_ids:
                print(f"    ✓ No se encontraron IDs válidos de {entity} para eliminar.")
                return

            print(f"    Eliminando {len(entity_ids)} registro(s) de {entity}...")
            deleted_count = 0
            for eid in entity_ids:
                payload = struct_pb2.Struct()
                payload.update({"id": eid})
                req = pb.TransactionRequest(
                    tenant_id=self.tenant_id,
                    entity_type=entity,
                    entity_id=eid,
                    action=pb.DELETE,
                    payload=payload
                )
                tx_resp = stub.Transact(req, timeout=10, metadata=self.metadata)
                if tx_resp.status.success:
                    deleted_count += 1
                else:
                    print(f"      [WARNING] Error eliminando {entity} {eid}: {tx_resp.status.error_message}")
            
            print(f"    ✓ Se eliminaron {deleted_count} de {len(entity_ids)} {entity}s exitosamente.")
            
        except Exception as e:
            print(f"    [ERROR] Falló la limpieza de {entity}: {e}")
