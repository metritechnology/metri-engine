#!/usr/bin/env python3
import sys
import os
import time
import hmac
import hashlib
import base64
import json
import struct
import argparse
import requests
from pathlib import Path

# Add proto directory to path
SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

try:
    import metri_pb2 as pb
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

DEFAULT_HOST = "engine.metri.one"
DEFAULT_HMAC_SECRET = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"

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
            "jti": f"check-prod-{now}",
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
        
        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.content

    def Discovery(self, req: pb.DiscoveryRequest) -> pb.DiscoveryResponse:
        body = self._post("/metri.MetriService/Discovery", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.DiscoveryResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.DiscoveryResponse()

def check_tenant(stub, tenant_id):
    req = pb.DiscoveryRequest(tenant_id=tenant_id, include_attributes=False)
    try:
        resp = stub.Discovery(req)
        if resp.status and not resp.status.success:
            err_code = resp.status.error_code
            err_msg = resp.status.error_message or ""
            return False, f"[{err_code}] {err_msg}"
        
        counts = {}
        for schema in resp.schemas:
            if schema.total_count > 0:
                counts[schema.entity] = schema.total_count
        return True, counts
    except Exception as e:
        return False, str(e)

def main():
    parser = argparse.ArgumentParser(description="Check Production Data Diagnostic Tool")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target host")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC validation key")
    
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    print("==================================================")
    print("      METRI PRODUCTION DATA DIAGNOSTIC TOOL       ")
    print("==================================================")
    print(f"Target Host: {args.host}")
    print("==================================================")

    stub = GrpcWebStub(args.host, args.secret)
    
    # Common tenants to verify
    tenants = ["system", "demo", "golden-tenant-benchmark", "golden-tenant-real", "tenant-1"]
    
    for t in tenants:
        print(f"\nVerifying Tenant: '{t}'...")
        success, res = check_tenant(stub, t)
        if success:
            print(f"  ✓ Connected successfully!")
            if not res:
                print("  (No entities found)")
            for entity, count in sorted(res.items()):
                print(f"    - {entity:<20} : {count} elements")
        else:
            print(f"  ✗ Failed: {res}")

    print("\n==================================================")

if __name__ == "__main__":
    main()
