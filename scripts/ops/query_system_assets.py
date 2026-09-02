#!/usr/bin/env python3
import sys
import os
import time
import hmac
import hashlib
import base64
import json
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
            "jti": f"query-prod-{now}",
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

    def Query(self, req: pb.QueryRequest) -> pb.QueryResponse:
        body = self._post("/metri.MetriService/Query", req)
        # Query returns a stream, let's parse all responses
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.QueryResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.QueryResponse()

def main():
    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    stub = GrpcWebStub(DEFAULT_HOST, DEFAULT_HMAC_SECRET)
    
    # Construct QueryRequest for assets under tenant system
    select_tree = struct_pb2.Struct()
    select_tree.update({
        "id": True,
        "name": True,
        "status": True,
        "type": True
    })
    
    analytics_req = pb.AnalyticsRequest(
        tenant_id="system",
        entity="asset",
        limit=10,
        select_tree=select_tree
    )
    
    req = pb.QueryRequest(
        tenant_id="system",
        queries={"asset_list": analytics_req}
    )
    
    print("Sending Query request for tenant 'system' assets to production...")
    
    try:
        resp = stub.Query(req)
        if resp.status and not resp.status.success:
            print(f"✗ Failed: {resp.status.error_code} - {resp.status.error_message}")
            sys.exit(1)
            
        print("✓ Connected successfully!")
        
        # Parse batch results
        if resp.batch_results:
            for key, q_resp in resp.batch_results.items():
                print(f"\nQuery Key: '{key}'")
                if q_resp.status and not q_resp.status.success:
                    print(f"  ✗ Subquery failed: {q_resp.status.error_message}")
                    continue
                
                rows_count = 0
                if q_resp.data and q_resp.data.rows_json and q_resp.data.rows_json.iter:
                    rows = q_resp.data.rows_json.iter
                    cols = [c.key for c in q_resp.data.columns]
                    print(f"  Columns: {cols}")
                    for r in rows:
                        rows_count += 1
                        vals = []
                        for v in r.values:
                            vals.append(v.string_value or v.number_value or v.bool_value or "null")
                        print(f"    Row {rows_count}: {vals}")
                print(f"  Total rows in batch result: {rows_count}")
        else:
            print("No batch results in response.")
            
    except Exception as e:
        print(f"Error querying assets: {e}")

if __name__ == "__main__":
    main()
