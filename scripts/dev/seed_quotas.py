#!/usr/bin/env python3
import sys
import os
import argparse
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

MODELS_MAP = {
    "nova": [
        "llm:aws:nova-micro",
        "llm:aws:nova-lite",
        "llm:aws:nova-pro"
    ],
    "claude": [
        "llm:anthropic:claude-sonnet-4.6",
        "llm:anthropic:claude-haiku-4.5",
        "llm:anthropic:claude-opus-4.6",
        "llm:anthropic:claude-3.5-haiku",
        "llm:anthropic:claude-3-opus"
    ]
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
    parser = argparse.ArgumentParser(description="Unified Quota Seeding Script")
    parser.add_argument("--host", default="localhost:9090", help="gRPC server address (host:port)")
    parser.add_argument("--tenants", nargs="+", default=["golden-tenant-benchmark", "system", "demo"], help="List of tenants to seed")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC secret key")
    parser.add_argument("--limit", type=int, default=10000000, help="Max limit for the quota")
    parser.add_argument("--type", choices=["all", "nova", "claude"], default="all", help="Model family to seed")
    
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    # Determine models to seed
    target_domains = []
    if args.type in ("all", "nova"):
        target_domains.extend(MODELS_MAP["nova"])
    if args.type in ("all", "claude"):
        target_domains.extend(MODELS_MAP["claude"])

    print(f"\n{'═'*60}")
    print(f"  SEED QUOTAS — gRPC → {args.host} ({len(target_domains)} domains)")
    print(f"{'═'*60}")

    channel = grpc.insecure_channel(args.host)
    stub = pb_grpc.MetriServiceStub(channel)
    created = 0
    total = len(target_domains) * len(args.tenants)

    for tenant in args.tenants:
        print(f"\n=== Tenant: {tenant} ===")
        for domain in target_domains:
            print(f"  → {domain} ({args.limit:,} tokens)")
            pb_payload = struct_pb2.Struct()
            pb_payload.update({
                "tenant_id": tenant,
                "resource_domain": domain,
                "limit_type": "TOKEN_COUNT",
                "reset_strategy": "MONTHLY",
                "period_key": PERIOD_KEY,
                "max_limit": args.limit,
                "current_usage": 0,
            })
            req = pb.TransactionRequest(
                tenant_id=tenant,
                entity_type="domain_quota",
                action=pb.CREATE,
                payload=pb_payload,
            )
            try:
                token = make_token(args.secret, tenant)
                metadata = [("authorization", "Bearer " + token)]
                resp = stub.Transact(req, timeout=10, metadata=metadata)
                if resp.status.success:
                    print(f"     ✅ entity_id={resp.entity_id}")
                    created += 1
                else:
                    print(f"     ❌ [{resp.status.error_code}] {resp.status.error_message}")
            except grpc.RpcError as e:
                print(f"     ❌ gRPC error: {e.code()} — {e.details()}")

    print(f"\n  RESULTADO: {created}/{total} cuotas procesadas")
    print(f"{'═'*60}\n")
    channel.close()

if __name__ == "__main__":
    main()
