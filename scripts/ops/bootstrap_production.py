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
from urllib.parse import unquote
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

    def _make_token(self, tenant_id="system"):
        now = int(time.time())
        claims = {
            "exp": now + 3600,
            "iat": now,
            "jti": f"bootstrap-prod-{now}",
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

    def Discovery(self, req: pb.DiscoveryRequest) -> pb.DiscoveryResponse:
        body = self._post("/metri.MetriService/Discovery", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.DiscoveryResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.DiscoveryResponse()

def check_tenant_exists(stub, tenant_id):
    req = pb.DiscoveryRequest(tenant_id=tenant_id, type="user", include_attributes=False)
    try:
        resp = stub.Discovery(req)
        # If we get a response and the status is success or doesn't say tenant not found, it exists
        if resp.status and not resp.status.success:
            err_msg = resp.status.error_message or ""
            if "not found" in err_msg.lower() or "not registered" in err_msg.lower():
                return False
        return True
    except Exception as e:
        err_msg = str(e)
        if "404" in err_msg or "not found" in err_msg.lower() or "not registered" in err_msg.lower():
            return False
        # If it is some other error, let's assume it might not exist or we can proceed to try creating
        return False

def create_resource(stub, tenant_id, entity_type, entity_id, payload_dict, action=pb.CREATE):
    payload = struct_pb2.Struct()
    payload.update(payload_dict)
    
    req = pb.TransactionRequest(
        tenant_id=tenant_id,
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        payload=payload
    )
    
    try:
        resp = stub.Transact(req)
        if resp.status and resp.status.success:
            print(f"  ✓ Created {entity_type} '{entity_id}' successfully.")
            return True
        else:
            err_msg = resp.status.error_message if resp.status else "Unknown error"
            print(f"  ✗ Failed to create {entity_type} '{entity_id}': {err_msg}")
            return False
    except Exception as e:
        print(f"  ✗ Error calling Transact for {entity_type} '{entity_id}': {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Metri Production Master Bootstrap Utility")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target engine endpoint (host:port)")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC validation key")
    parser.add_argument("--email", default="admin@metri.one", help="Admin email address")
    parser.add_argument("--username", default="admin", help="Admin username")
    parser.add_argument("--password-hash", default="$2a$12$OOIgpGVtZCr9H18VLoZMhufZfQ8bDWw4fuEJrBpGxmzlDiKiNWnJy", 
                        help="Bcrypt hash of the admin password")
    parser.add_argument("--force", action="store_true", help="Force creation even if tenant seems to exist")
    
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    print("==================================================")
    print("      METRI PRODUCTION SYSTEM BOOTSTRAP UTILITY   ")
    print("==================================================")
    print(f"Target API Host : {args.host}")
    print(f"HMAC Secret     : {args.secret[:6]}...{args.secret[-6:]} (Loaded)")
    print(f"Admin Email     : {args.email}")
    print(f"Admin Username  : {args.username}")
    print("==================================================")

    stub = GrpcWebStub(args.host, args.secret)
    tenant_id = "system"

    # 1. Check if tenant exists
    if not args.force:
        print(f"Checking if tenant '{tenant_id}' exists in production...")
        if check_tenant_exists(stub, tenant_id):
            print(f"\n[SKIP] Tenant '{tenant_id}' already exists in production. Bootstrap omitted.")
            print("Use --force to override.")
            sys.exit(0)
        else:
            print(f"Tenant '{tenant_id}' does not exist or pre-flight check failed. Proceeding with bootstrap...")

    # 2. Start Bootstrap
    print(f"\nStarting bootstrap for tenant '{tenant_id}'...")

    # 2.1 Create Tenant system
    tenant_payload = {
        "id": "system",
        "name": "System Master Tenant",
        "status": "ACTIVE",
        "tier": "ENTERPRISE",
        "storage_region": "us-east-1",
        "billing_admin_email": args.email
    }
    # Create tenant (using "system" as the tenant itself)
    if not create_resource(stub, "system", "tenant", "system", tenant_payload):
        print("✗ Critical: Failed to create tenant 'system'. Aborting bootstrap.")
        sys.exit(2)

    # 2.2 Create Role role_super_master
    role_super_payload = {
        "id": "role_super_master",
        "name": "super-master",
        "description": "System Super Master Administrative Role",
        "tenant_id": "system",
        "grants": [
            '{"domain":"*","actions":["VIEW","CREATE","UPDATE","DELETE","EXECUTE","EXPORT"],"scope":"ALL"}'
        ]
    }
    create_resource(stub, "system", "role", "role_super_master", role_super_payload)


    # 2.4 Create User usr_master
    user_master_payload = {
        "id": "usr_master",
        "username": args.username,
        "email": args.email,
        "password_hash": args.password_hash,
        "first_name": "Super",
        "last_name": "Administrador",
        "status": "ACTIVE",
        "user_type": "INTERNAL",
        "tenant_id": "system",
        "role_ids": ["role_super_master"]
    }
    create_resource(stub, "system", "user", "usr_master", user_master_payload)


    print("\n==================================================")
    print("      BOOTSTRAP PROCESS COMPLETED                 ")
    print("==================================================")

if __name__ == "__main__":
    main()
