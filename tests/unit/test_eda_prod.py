#!/usr/bin/env python3
import sys
import os
import time
import hmac
import hashlib
import base64
import json
import uuid
import requests
import struct

# Add proto paths to sys.path
PROTO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)

import metri_pb2 as pb
from google.protobuf import struct_pb2

# Configuration
ENGINE_HOST = "engine.metri.one"
HMAC_SECRET = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
BASE_URL = f"https://{ENGINE_HOST}"

def make_system_token(secret):
    now = int(time.time())
    claims = {
        "tid": "system",
        "uid": "usr_system_bff",
        "iat": now,
        "exp": now + 600,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

def grpc_frame(data: bytes) -> bytes:
    return b'\x00' + struct.pack('>I', len(data)) + data

def parse_frames(body: bytes):
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

def main():
    token = make_system_token(HMAC_SECRET)
    print("Generated system token:", token[:40] + "...")

    # Build TransactionRequest payload
    payload = struct_pb2.Struct()
    loc_id = f"loc_{uuid.uuid4().hex[:12]}"
    loc_name = f"Test EDA Location {uuid.uuid4().hex[:6].upper()}"
    
    payload.update({
        "id": loc_id,
        "name": loc_name,
        "status": "ACTIVE",
        "type": "SITE",
        "description": "Ubicación real para validar flujo de ruteo EDA y Cedar en producción"
    })

    req_pb = pb.TransactionRequest(
        tenant_id="tnt_01",
        entity_type="location",
        action=1, # CREATE
        payload=payload
    )

    framed_data = grpc_frame(req_pb.SerializeToString())
    headers = {
        "Content-Type": "application/grpc-web+proto",
        "Accept": "application/grpc-web+proto",
        "sid": token,
        "kid": "system",
        "x-tenant-id": "tnt_01",
        "x-grpc-web": "1"
    }

    url = f"{BASE_URL}/metri.MetriService/Transact"
    print(f"Sending Transact call to {url} to create location '{loc_name}'...")
    
    try:
        resp = requests.post(url, data=framed_data, headers=headers, timeout=15)
        print("Response HTTP Status:", resp.status_code)
        print("Response Headers:", dict(resp.headers))
        print("Response Content Length:", len(resp.content))
        print("Raw Content:", resp.content)
        resp.raise_for_status()
        
        # Parse Response
        success = False
        resp_msg = None
        for is_trailer, payload_bytes in parse_frames(resp.content):
            print(f"Frame - is_trailer: {is_trailer}, len: {len(payload_bytes)}")
            if not is_trailer:
                transact_resp = pb.TransactionResponse()
                transact_resp.ParseFromString(payload_bytes)
                success = transact_resp.status.success
                resp_msg = transact_resp
                break
        
        if success:
            print("✓ Success! Created entity ID:", resp_msg.entity_id)
            print("Response:", resp_msg)
        else:
            print("✗ Failed. Response:", resp_msg)
            sys.exit(1)
            
    except Exception as e:
        print("✗ Request failed with exception:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
