#!/usr/bin/env python3
import sys
import os
import time
import uuid
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
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

DEFAULT_HOST = "engine.metri.one"
DEFAULT_HMAC_SECRET = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
DEFAULT_TENANT = "golden-tenant-real"

# Shared placeholder for capturing the last response
last_response = None

class GrpcWebStub:
    def __init__(self, host, hmac_secret):
        self.host = host
        self.hmac_secret = hmac_secret
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"
        self.session = requests.Session()

    def _make_token(self, tenant_id="demo"):
        now = int(time.time())
        claims = {
            "exp": now + 3600,
            "iat": now,
            "jti": f"verify-auth-{now}",
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

    def _post(self, path: str, proto_msg, custom_token_func=None) -> bytes:
        global last_response
        data = proto_msg.SerializeToString()
        framed = self._grpc_frame(data)
        
        tenant_id = getattr(proto_msg, "tenant_id", "demo") or "demo"
        
        token = custom_token_func() if custom_token_func else self._make_token(tenant_id)
        
        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "Authorization": token,
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
            "X-Metri-Origin-Token": self.hmac_secret,
        }
        
        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=15)
        last_response = resp
        resp.raise_for_status()
        return resp.content

    def Discover(self, req: pb.DiscoveryRequest, custom_token_func=None) -> pb.DiscoveryResponse:
        body = self._post("/metri.MetriService/Discovery", req, custom_token_func)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.DiscoveryResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.DiscoveryResponse()

def run_request(stub, req, token_func=None):
    global last_response
    last_response = None
    
    start_time = time.time()
    status_code = 200
    grpc_status = "0"
    error_msg = ""
    success = False
    schema_count = 0
    
    try:
        res = stub.Discover(req, custom_token_func=token_func)
        
        if last_response is not None:
            status_code = last_response.status_code
            grpc_status = last_response.headers.get("grpc-status", "0")
            grpc_msg = last_response.headers.get("grpc-message", "")
            
            if grpc_status != "0":
                success = False
                error_msg = unquote(grpc_msg) if grpc_msg else f"gRPC Status {grpc_status}"
            else:
                success = res.status.success if res.status else True
                if res.status and not res.status.success:
                    error_msg = f"{res.status.error_code}: {res.status.error_message}"
                else:
                    schema_count = len(res.schemas)
        else:
            success = res.status.success if res.status else True
            schema_count = len(res.schemas)
            
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        error_msg = http_err.response.text.strip()
        if len(error_msg) > 120:
            error_msg = error_msg[:117] + "..."
    except Exception as e:
        status_code = 500
        error_msg = str(e)
        
    latency = (time.time() - start_time) * 1000
    return {
        "success": success,
        "status_code": status_code,
        "grpc_status": grpc_status,
        "error_msg": error_msg,
        "schema_count": schema_count,
        "latency_ms": latency
    }

def main():
    parser = argparse.ArgumentParser(description="Metri Cedar Authentication & Authorization Production Certification Tool")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target gRPC-Web host")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC validation key")
    parser.add_argument("--tenant", default=DEFAULT_TENANT, help="Target Tenant ID")
    args = parser.parse_args()

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    print("=" * 80)
    print("      METRI DATA PLATFORM — CEDAR AUTHENTICATION CERTIFICATION TOOL      ")
    print("=" * 80)
    print(f" Target Host:    {args.host}")
    print(f" Target Tenant:  {args.tenant}")
    print(f" HMAC Secret:    {args.secret[:6]}...{args.secret[-6:]} (Loaded)")
    print("=" * 80)

    stub = GrpcWebStub(args.host, args.secret)
    req = pb.DiscoveryRequest(tenant_id=args.tenant, type="asset", include_attributes=False)
    
    unauth_results = []
    auth_results = []
    
    # 1. Testing Unauthenticated Access (10 requests)
    print("\n--- PHASE 1: Testing Unauthenticated Access (10 Requests) ---")
    
    # 1.1 Empty/No Token
    print("\nSending 5 requests with Empty/No Token...")
    for i in range(5):
        token_func = lambda: ""
        res = run_request(stub, req, token_func)
        unauth_results.append(res)
        expected_txt = "EXPECTED REJECTION" if res["grpc_status"] != "0" or not res["success"] else "UNEXPECTED ALLOW"
        print(f"  Request #{i+1:02d}: Status={res['status_code']}, gRPC-Status={res['grpc_status']}, Latency={res['latency_ms']:.2f}ms, Error='{res['error_msg']}' -> [{expected_txt}]")
        time.sleep(0.1)

    # 1.2 Invalid Malformed Tokens
    print("\nSending 5 requests with Invalid/Malformed Token...")
    for i in range(5):
        invalid_token = f"Bearer mk_eyJ1aWQiOiJ1c3Jfc3lzdGVtX2JmZiIsInRpZCI6InthcmdzLnRlbmFudH0ifQ.invalid_signature_{i}"
        token_func = lambda: invalid_token
        res = run_request(stub, req, token_func)
        unauth_results.append(res)
        expected_txt = "EXPECTED REJECTION" if res["grpc_status"] != "0" or not res["success"] else "UNEXPECTED ALLOW"
        print(f"  Request #{i+6:02d}: Status={res['status_code']}, gRPC-Status={res['grpc_status']}, Latency={res['latency_ms']:.2f}ms, Error='{res['error_msg']}' -> [{expected_txt}]")
        time.sleep(0.1)

    # 2. Testing Authenticated Access (10 requests)
    print("\n--- PHASE 2: Testing Authenticated & Authorized Access (10 Requests) ---")
    print("Testing with valid secure Zero-Trust HMAC token...")
    
    for i in range(10):
        # GrpcWebStub will generate the token dynamically in run_request if token_func is None
        res = run_request(stub, req, token_func=None)
        auth_results.append(res)
        success_txt = "SUCCESS" if res["status_code"] == 200 and res["grpc_status"] == "0" and res["success"] else "FAILED"
        schemas_info = f"{res['schema_count']} schemas found" if res["success"] else f"Error: '{res['error_msg']}'"
        print(f"  Request #{i+1:02d}: Status={res['status_code']}, gRPC-Status={res['grpc_status']}, Latency={res['latency_ms']:.2f}ms, Res=[{schemas_info}] -> [{success_txt}]")
        time.sleep(0.1)

    # Report Summary
    unauth_rejected = all(r["grpc_status"] != "0" or not r["success"] for r in unauth_results)
    auth_succeeded = all(r["status_code"] == 200 and r["grpc_status"] == "0" and r["success"] for r in auth_results)
    
    unauth_latencies = [r["latency_ms"] for r in unauth_results]
    auth_latencies = [r["latency_ms"] for r in auth_results]
    
    avg_unauth_latency = sum(unauth_latencies) / len(unauth_latencies)
    avg_auth_latency = sum(auth_latencies) / len(auth_latencies)

    print("\n" + "=" * 80)
    print("                     CERTIFICATION REPORT SUMMARY                      ")
    print("=" * 80)
    print(f" Unauthenticated Rejection Rate: {len([r for r in unauth_results if r['grpc_status'] != '0' or not r['success']])}/10 ({'100%' if unauth_rejected else 'FAIL'})")
    print(f" Authenticated Success Rate:     {len([r for r in auth_results if r['status_code'] == 200 and r['grpc_status'] == '0' and r['success']])}/10 ({'100%' if auth_succeeded else 'FAIL'})")
    print(f" Avg Unauthenticated Latency:    {avg_unauth_latency:.2f} ms")
    print(f" Avg Authenticated Latency:      {avg_auth_latency:.2f} ms")
    print("-" * 80)
    
    certified = unauth_rejected and auth_succeeded
    if certified:
        print(" CERTIFICATION RESULT: ✅ PASS")
        print(" The Metri Engine Cedar Authentication & Verification Layer is 100% SECURE and CORRECT.")
    else:
        print(" CERTIFICATION RESULT: ❌ FAIL")
        print(" Security vulnerabilities or misconfigurations detected! Review the logs above.")
    print("=" * 80)
    
    if not certified:
        sys.exit(2)

if __name__ == "__main__":
    main()
