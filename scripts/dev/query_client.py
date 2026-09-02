#!/usr/bin/env python3
import sys
import os
import argparse
import json
import time
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
    from google.protobuf.json_format import MessageToJson, ParseDict
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

DEFAULT_HMAC_SECRET = "local-dev-secret-do-not-use-in-prod"

def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": f"proxy-{now}",
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

BUILTIN_QUERIES = {
    "assets-table": {
        "queries": {
            "table-assets-q": {
                "tenant_id": "system",
                "entity": "asset",
                "viz": "table",
                "limit": 10
            }
        }
    },
    "assets-line-chart": {
        "queries": {
            "chart-line-q": {
                "tenant_id": "system",
                "entity": "asset",
                "metrics": [{
                    "aggregation": "COUNT", 
                    "attribute": "id", 
                    "name": "Registros Nuevos"
                }],
                "dimensions": [{
                    "attribute": "created_at", 
                    "interval": "day", 
                    "label_template": "{{created_at}}"
                }],
                "time_frame": {
                    "type": "LAST_N_DAYS", 
                    "n_value": 30, 
                    "timezone": "America/Bogota"
                },
                "output_cast": "TIMESERIES",
                "viz": "{\"type\": \"line\", \"smooth\": true}",
                "limit": 1000
            }
        }
    }
}

def run_query(stub, metadata, tenant_id, query_dict):
    # Ensure tenant_id is set at root
    if "tenant_id" not in query_dict:
        query_dict["tenant_id"] = tenant_id

    req = pb.QueryRequest()
    try:
        ParseDict(query_dict, req)
    except Exception as e:
        print(f"✗ Failed to parse query dictionary into protobuf message: {e}")
        return

    print("Executing query on metri-engine...")
    try:
        start_time = time.time()
        response_stream = stub.Query(req, timeout=15, metadata=metadata)
        
        chunks_count = 0
        for chunk in response_stream:
            chunks_count += 1
            print(f"\n--- Chunk #{chunks_count} Received (Time elapsed: {time.time() - start_time:.4f}s) ---")
            json_str = MessageToJson(chunk, ensure_ascii=False)
            print(json_str)
            
    except grpc.RpcError as e:
        print(f"✗ gRPC Error: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"✗ Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Metri Engine gRPC Debug Query Client")
    parser.add_argument("--host", default="localhost:9090", help="gRPC address (host:port)")
    parser.add_argument("--tenant", default="system", help="Tenant ID")
    parser.add_argument("--user", default="usr_system_bff", help="User ID to authenticate as")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC secret key")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--builtin", choices=list(BUILTIN_QUERIES.keys()), help="Run a built-in pre-defined query")
    group.add_argument("--file", metavar="PATH", help="Path to a JSON file containing the QueryRequest payload")
    group.add_argument("--json", metavar="RAW_JSON", help="Raw JSON string containing the QueryRequest payload")
    
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    # Determine query payload
    if args.builtin:
        query_dict = BUILTIN_QUERIES[args.builtin]
    elif args.file:
        with open(args.file, "r") as f:
            query_dict = json.load(f)
    elif args.json:
        query_dict = json.loads(args.json)

    # Establish connection
    channel = grpc.insecure_channel(args.host)
    stub = pb_grpc.MetriServiceStub(channel)

    # Generate token
    token = generate_signed_token(args.secret, args.tenant, args.user)
    metadata = [("authorization", "Bearer " + token)]

    run_query(stub, metadata, args.tenant, query_dict)
    
    channel.close()

if __name__ == "__main__":
    main()
