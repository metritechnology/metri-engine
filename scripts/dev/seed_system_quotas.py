#!/usr/bin/env python3
import sys
import os
import time
import json
import hmac
import hashlib
import base64
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
    from google.protobuf.json_format import MessageToDict
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

import datetime

def get_current_month_period():
    today = datetime.date.today()
    start_of_month = today.replace(day=1)
    if start_of_month.month == 12:
        start_of_next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
    else:
        start_of_next_month = start_of_month.replace(month=start_of_month.month + 1)
    return f"{start_of_month.strftime('%Y-%m-%d')}_{start_of_next_month.strftime('%Y-%m-%d')}"

PERIOD_KEY = get_current_month_period()
DEFAULT_HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

QUOTAS_MAP = {
    # Micro/Haiku (Cheap/High Limit)
    "llm:aws:nova-micro": 50000000,
    "llm:anthropic:claude-3.5-haiku": 50000000,
    "llm:anthropic:claude-haiku-4.5": 50000000,
    
    # Lite/Sonnet (Medium Limit)
    "llm:aws:nova-lite": 20000000,
    "llm:anthropic:claude-sonnet-4.6": 20000000,
    
    # Pro/Opus (Expensive/Low Limit)
    "llm:aws:nova-pro": 5000000,
    "llm:anthropic:claude-opus-4.6": 5000000,
    "llm:anthropic:claude-3-opus": 5000000,
}

def make_token(secret: str, tenant_id: str) -> str:
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": "usr_system_bff",
        "iat": now,
        "exp": now + 3600,
        "jti": f"proxy-{now}",
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode()
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b'=').decode()
    mac = hmac.new(secret.encode(), payload_bytes, hashlib.sha256)
    sig_b64 = base64.urlsafe_b64encode(mac.digest()).rstrip(b'=').decode()
    return "mk_" + payload_b64 + "." + sig_b64

def main():
    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    host = "localhost:9090"
    tenant = "system"
    secret = DEFAULT_HMAC_SECRET

    print(f"\n============================================================")
    print(f"  SEED SYSTEM QUOTAS — gRPC → {host} ({len(QUOTAS_MAP)} models)")
    print(f"============================================================")

    channel = grpc.insecure_channel(host)
    stub = pb_grpc.MetriServiceStub(channel)
    
    # Query existing quotas to prevent duplicates
    existing_map = {}
    try:
        q_req = pb.QueryRequest(tenant_id=tenant)
        ar = pb.AnalyticsRequest(
            tenant_id=tenant,
            entity="domain_quota",
            limit=100
        )
        q_req.queries["q"].CopyFrom(ar)
        token = make_token(secret, tenant)
        metadata = [("authorization", "Bearer " + token)]
        q_resp = stub.Query(q_req, timeout=10, metadata=metadata)
        
        # Parse response using MessageToDict
        dict_resp = MessageToDict(q_resp, preserving_proto_field_name=True)
        if "batch_results" in dict_resp and "q" in dict_resp["batch_results"]:
            q_data = dict_resp["batch_results"]["q"].get("data", {})
            rows_json_data = q_data.get("rows_json", {})
            if isinstance(rows_json_data, str):
                import json
                rows_json_data = json.loads(rows_json_data)
            rows = rows_json_data.get("iter", [])
            columns = q_data.get("columns", [])
            keys = [c["key"] for c in columns]
            
            for r in rows:
                details = dict(zip(keys, r["values"]))
                domain = details.get("resource_domain")
                period = details.get("period_key")
                ulid = details.get("entity/ulid") or details.get("id") or details.get("entity_id")
                if domain and period and ulid:
                    existing_map[(domain, period)] = ulid
    except Exception as e:
        print(f"  ⚠️ Warning: failed to fetch existing quotas ({e}), performing creates")

    created = 0
    updated = 0

    for domain, limit in QUOTAS_MAP.items():
        existing_id = existing_map.get((domain, PERIOD_KEY))
        
        pb_payload = struct_pb2.Struct()
        payload_data = {
            "tenant_id": tenant,
            "resource_domain": domain,
            "limit_type": "TOKEN_COUNT",
            "reset_strategy": "MONTHLY",
            "period_key": PERIOD_KEY,
            "max_limit": limit,
        }
        if existing_id:
            payload_data["entity/ulid"] = existing_id
        else:
            payload_data["current_usage"] = 0
            
        pb_payload.update(payload_data)
        
        action = pb.UPDATE if existing_id else pb.CREATE
        action_name = "UPDATE" if existing_id else "CREATE"
        print(f"  → {action_name} {domain} ({limit:,} tokens)")
        
        req = pb.TransactionRequest(
            tenant_id=tenant,
            entity_type="domain_quota",
            action=action,
            entity_id=existing_id or "",
            payload=pb_payload,
        )
        try:
            token = make_token(secret, tenant)
            metadata = [("authorization", "Bearer " + token)]
            resp = stub.Transact(req, timeout=10, metadata=metadata)
            if resp.status.success:
                print(f"     ✅ entity_id={resp.entity_id or existing_id}")
                if existing_id:
                    updated += 1
                else:
                    created += 1
            else:
                print(f"     ❌ [{resp.status.error_code}] {resp.status.error_message}")
        except grpc.RpcError as e:
            print(f"     ❌ gRPC error: {e.code()} — {e.details()}")

    print(f"\n  RESULT: Created {created}, Updated {updated} of {len(QUOTAS_MAP)} quotas successfully")
    print(f"============================================================\n")
    channel.close()

if __name__ == "__main__":
    main()
