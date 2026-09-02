#!/usr/bin/env python3
import sys
import os
import argparse
import time
import json
import hmac
import hashlib
import base64
import struct
import requests
from pathlib import Path

# Add proto directory to path
SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

try:
    import metri_pb2 as pb
    from google.protobuf import struct_pb2
    from google.protobuf.json_format import MessageToDict
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

DEFAULT_HOST = "engine.metri.one"
DEFAULT_HMAC_SECRET = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
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

MODELS_LIST = list(QUOTAS_MAP.keys())

class GrpcWebStub:
    def __init__(self, host, hmac_secret):
        self.host = host
        self.hmac_secret = hmac_secret
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"
        self.session = requests.Session()

    def _make_token(self, tenant_id):
        now = int(time.time())
        claims = {
            "exp": now + 3600,
            "iat": now,
            "jti": f"quota-prod-{now}",
            "tid": tenant_id,
            "uid": "usr_system_bff"
        }
        payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
        payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
        sig = hmac.new(self.hmac_secret.encode('utf-8'), payload_bytes, hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode('utf-8').rstrip('=')
        return f"Bearer mk_{payload_b64}.{sig_b64}"

    def _grpc_frame(self, data: bytes) -> bytes:
        return b'\x00' + struct.pack('>I', len(data)) + data

    def _parse_frames(self, body: bytes):
        idx = 0
        while idx < len(body):
            if idx + 5 > len(body):
                break
            flag = body[idx]
            length = struct.unpack('>I', body[idx+1:idx+5])[0]
            idx += 5
            payload = body[idx:idx+length]
            idx += length
            yield (flag & 0x80 != 0, payload)

    def _post(self, path: str, proto_msg) -> bytes:
        data = proto_msg.SerializeToString()
        framed = self._grpc_frame(data)
        tenant_id = getattr(proto_msg, "tenant_id", "system") or "system"
        token = self._make_token(tenant_id)
        
        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "Authorization": token,
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
            "X-Metri-Origin-Token": self.hmac_secret,
        }
        
        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.content

    def Transact(self, req: pb.TransactionRequest) -> pb.TransactionResponse:
        body = self._post("/metri.MetriService/Transact", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.TransactionResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.TransactionResponse()

def main():
    parser = argparse.ArgumentParser(description="Production Quota Seeding Operations Utility")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target engine endpoint (host:port)")
    parser.add_argument("--tenant", default="system", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--limit", type=int, default=10000, help="Max limit for the quota")
    
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python protobuf library is not available.")
        sys.exit(1)

    print("==================================================")
    print("      METRI PRODUCTION QUOTA SEED UTILITY         ")
    print("==================================================")
    print(f"Target API Host : {args.host}")
    print(f"Tenant ID       : {args.tenant}")
    print(f"Quota Limit     : {args.limit}")
    print(f"Period Key      : {PERIOD_KEY}")
    print("==================================================")

    stub = GrpcWebStub(args.host, args.secret)
    
    # Query existing quotas to prevent duplicates
    existing_map = {}
    try:
        q_req = pb.QueryRequest(tenant_id=args.tenant)
        ar = pb.AnalyticsRequest(
            tenant_id=args.tenant,
            entity="domain_quota",
            limit=100
        )
        q_req.queries["q"].CopyFrom(ar)
        q_resp = stub.Query(q_req)
        
        dict_resp = MessageToDict(q_resp, preserving_proto_field_name=True)
        if "batch_results" in dict_resp and "q" in dict_resp["batch_results"]:
            q_data = dict_resp["batch_results"]["q"].get("data", {})
            rows_json_data = q_data.get("rows_json", {})
            if isinstance(rows_json_data, str):
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
    total = len(MODELS_LIST)

    for domain in MODELS_LIST:
        limit = QUOTAS_MAP.get(domain, args.limit)
        existing_id = existing_map.get((domain, PERIOD_KEY))
        
        pb_payload = struct_pb2.Struct()
        payload_data = {
            "tenant_id": args.tenant,
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
        action_name = "Updating" if existing_id else "Creating"
        print(f"{action_name} quota for '{domain}' ({limit:,} tokens)...")
        
        req = pb.TransactionRequest(
            tenant_id=args.tenant,
            entity_type="domain_quota",
            action=action,
            entity_id=existing_id or "",
            payload=pb_payload,
        )
        
        try:
            resp = stub.Transact(req)
            if resp.status and resp.status.success:
                print(f"  ✓ Successfully completed: entity_id={resp.entity_id or existing_id}")
                if existing_id:
                    updated += 1
                else:
                    created += 1
            else:
                err_msg = resp.status.error_message if resp.status else "Unknown error"
                print(f"  ✗ Failed: {err_msg}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    print(f"\nCompleted! Created {created}, Updated {updated} of {total} quotas in production for tenant '{args.tenant}'.")

if __name__ == "__main__":
    main()
