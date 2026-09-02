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
            "jti": f"loc-prod-{now}",
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

    def Transact(self, req: pb.TransactionRequest) -> pb.TransactionResponse:
        body = self._post("/metri.MetriService/Transact", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.TransactionResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.TransactionResponse()

def main():
    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    stub = GrpcWebStub(DEFAULT_HOST, DEFAULT_HMAC_SECRET)
    tenant_id = "system"
    
    prefixes = ['Edificio Principal', 'Planta de Ensamblaje', 'Nave de Almacenamiento', 'Bodega Central', 'Sucursal Metropolitana', 'Centro Logístico']
    
    print("Creating locations LOC-001 through LOC-010 in production 'system' tenant...")
    
    for i in range(1, 11):
        loc_id = f"LOC-{i:03d}"
        loc_name = f"{prefixes[(i-1) % len(prefixes)]} #{i}"
        
        payload = struct_pb2.Struct()
        payload.update({
            "id": loc_id,
            "name": loc_name,
            "status": "ACTIVE",
            "type": "BUILDING",
            "description": f"Ubicación de producción {loc_id}"
        })
        
        req = pb.TransactionRequest(
            tenant_id=tenant_id,
            entity_type="location",
            entity_id=loc_id,
            action=pb.CREATE,
            payload=payload
        )
        
        try:
            resp = stub.Transact(req)
            if resp.status and resp.status.success:
                print(f"  ✓ Created location {loc_id}: {loc_name}")
            else:
                err_msg = resp.status.error_message if resp.status else "Unknown error"
                print(f"  ✗ Failed to create location {loc_id}: {err_msg}")
        except Exception as e:
            print(f"  ✗ Error creating location {loc_id}: {e}")

if __name__ == "__main__":
    main()
