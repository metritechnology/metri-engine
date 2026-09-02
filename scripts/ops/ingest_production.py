#!/usr/bin/env python3
import sys
import os
import argparse
import time
import random
import uuid
import hmac
import hashlib
import base64
import json
import struct
import concurrent.futures
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

ASSET_TYPES = ["PUMP", "MOTOR", "COMPRESSOR", "VALVE", "SENSOR", "TURBINE"]
CRITICALITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
CATEGORIES = ["EQUIPMENT", "INFRASTRUCTURE", "SAFETY", "UTILITY"]

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
            "jti": f"ingest-prod-{now}",
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

    def BulkIngest(self, req: pb.BulkRequest) -> pb.BulkResponse:
        body = self._post("/metri.MetriService/BulkIngest", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.BulkResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.BulkResponse()

    def Transact(self, req: pb.TransactionRequest) -> pb.TransactionResponse:
        body = self._post("/metri.MetriService/Transact", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.TransactionResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.TransactionResponse()

def generate_asset(index):
    asset_name = f"Asset BULK {index+1} - {random.choice(ASSET_TYPES)} {uuid.uuid4().hex[:4].upper()}"
    status = "ACTIVE"
    asset_type = random.choice(ASSET_TYPES)
    health = round(random.uniform(50.0, 100.0), 2)
    location = f"LOC-{random.randint(1, 10):03d}"
    criticality = random.choice(CRITICALITIES)
    category = random.choice(CATEGORIES)
    meter_reading = round(random.uniform(100.0, 5000.0), 2)
    
    return {
        "name": asset_name,
        "status": status,
        "type": asset_type,
        "health_score": health,
        "location_id": location,
        "criticality": criticality,
        "category": category,
        "current_meter_reading": meter_reading,
        "model": "Model-Bulk"
    }

def make_bulk_request(tenant_id, rows_list, columns_keys):
    columns = [pb.ColumnSchema(key=k, label=k, type="string") for k in columns_keys]
    rows = []
    for r in rows_list:
        values = []
        for k in columns_keys:
            v = r.get(k)
            if v is None:
                values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(null_value=0))
            elif isinstance(v, bool):
                values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(bool_value=v))
            elif isinstance(v, (int, float)):
                values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(number_value=float(v)))
            else:
                values.append(pb.google_dot_protobuf_dot_struct__pb2.Value(string_value=str(v)))
        rows.append(pb.DataRow(values=values))
        
    rowset = pb.RowSet(columns=columns, rows_json=pb.DataRowList(iter=rows))
    req = pb.BulkRequest(
        tenant_id=tenant_id,
        entity_type="asset",
        action=pb.CREATE,
        data=rowset
    )
    return req

def send_batch(stub, tenant_id, batch_id, rows_list, columns_keys):
    req = make_bulk_request(tenant_id, rows_list, columns_keys)
    max_retries = 6
    backoff_factor = 2.0
    
    for attempt in range(max_retries):
        start_time = time.time()
        try:
            resp = stub.BulkIngest(req)
            duration = time.time() - start_time
            if resp.status and resp.status.success:
                return True, resp.ingested_count, duration
            else:
                err_msg = resp.status.error_message if resp.status else "Unknown error"
                is_retryable = any(x in err_msg for x in ["429", "504", "Too Many Requests", "Gateway Timeout", "RESOURCE_EXHAUSTED", "ProvisionedThroughputExceededException"])
                if is_retryable and attempt < max_retries - 1:
                    sleep_time = (backoff_factor ** attempt) + random.uniform(0.5, 1.5)
                    print(f"  [Attempt {attempt+1}/{max_retries}] Batch {batch_id:3d} throttled/timeout ({err_msg}). Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    continue
                return False, err_msg, duration
        except Exception as e:
            duration = time.time() - start_time
            err_msg = str(e)
            is_retryable = any(x in err_msg for x in ["429", "504", "Too Many Requests", "Gateway Timeout", "RESOURCE_EXHAUSTED", "ProvisionedThroughputExceededException"])
            if is_retryable and attempt < max_retries - 1:
                sleep_time = (backoff_factor ** attempt) + random.uniform(0.5, 1.5)
                print(f"  [Attempt {attempt+1}/{max_retries}] Batch {batch_id:3d} exception ({err_msg}). Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                continue
            return False, err_msg, duration
            
    return False, "Max retries exceeded", 0

def ingest_assets(stub, tenant_id, count, workers):
    batch_size = 200
    num_batches = count // batch_size
    if num_batches == 0:
        num_batches = 1
        batch_size = count

    columns_keys = [
        "name", "status", "type", "health_score", 
        "location_id", "criticality", "category", 
        "current_meter_reading", "model"
    ]
    
    print(f"Generating {count} synthetic assets in memory...")
    assets = [generate_asset(i) for i in range(count)]
    batches = [assets[i * batch_size : (i + 1) * batch_size] for i in range(num_batches)]
    
    print(f"\nStarting bulk ingestion of {num_batches} batches...")
    print(f"Concurrency level: {workers} threads\n")
    
    ok_count = 0
    fail_count = 0
    total_ingested = 0
    global_start = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(send_batch, stub, tenant_id, i + 1, batch, columns_keys): i + 1
            for i, batch in enumerate(batches)
        }
        
        for future in concurrent.futures.as_completed(futures):
            batch_id = futures[future]
            success, result, duration = future.result()
            
            if success:
                ok_count += 1
                total_ingested += result
                if ok_count % 10 == 0 or total_ingested == count:
                    print(f"  ✓ Batch {batch_id:3d} completed: Ingested {result} assets in {duration:5.2f}s | Progress: {total_ingested}/{count}")
            else:
                fail_count += 1
                print(f"  ✗ Batch {batch_id:3d} FAILED in {duration:5.2f}s: {result}")
                
    global_duration = time.time() - global_start
    throughput = total_ingested / global_duration if global_duration > 0 else 0
    
    print("\n" + "="*50)
    print(f"✓ Ingested {total_ingested}/{count} assets successfully in {global_duration:.2f}s (Throughput: {throughput:.2f}/s)")
    print("="*50)

def ingest_locations(stub, tenant_id, count):
    print(f"Ingesting {count} locations...")
    prefixes = ['Edificio', 'Planta', 'Nave', 'Bodega', 'Sucursal', 'Centro de Distribución']
    created = 0
    
    for i in range(count):
        loc_name = f"{random.choice(prefixes)} FTS-{uuid.uuid4().hex[:6].upper()} #{i+1}"
        payload = struct_pb2.Struct()
        payload.update({
            "name": loc_name,
            "status": "ACTIVE",
            "type": "BUILDING",
            "description": f"Ubicación de prueba generada {i+1}"
        })
        req = pb.TransactionRequest(
            tenant_id=tenant_id,
            entity_type="location",
            action=pb.CREATE,
            payload=payload,
        )
        try:
            resp = stub.Transact(req)
            if resp.status.success:
                created += 1
                print(f"  ✓ Created location {i+1}: {loc_name}")
            else:
                print(f"  ✗ Failed to create location {i+1}: {resp.status.error_message}")
        except Exception as e:
            print(f"  ✗ Error creating location: {e}")

    print(f"\nDone! Ingested {created}/{count} locations.")

def main():
    parser = argparse.ArgumentParser(description="Production Data Ingest Ops Utility")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target engine endpoint (host:port)")
    parser.add_argument("--tenant", default="golden-tenant-benchmark", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--type", choices=["assets", "locations"], default="assets", help="Data type to ingest")
    parser.add_argument("--count", type=int, default=1000, help="Total number of items to ingest")
    parser.add_argument("--workers", type=int, default=5, help="Number of concurrent threads for assets")
    
    args = parser.parse_args()
    
    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    print("==================================================")
    print("      METRI PRODUCTION DATA INGESTION UTILITY     ")
    print("==================================================")
    print(f"Target API Host : {args.host}")
    print(f"Tenant ID       : {args.tenant}")
    print(f"Ingest Type     : {args.type.upper()}")
    print(f"Count           : {args.count}")
    print("==================================================")

    # Establish gRPC-Web stub
    stub = GrpcWebStub(args.host, args.secret)

    if args.type == "assets":
        ingest_assets(stub, args.tenant, args.count, args.workers)
    elif args.type == "locations":
        ingest_locations(stub, args.tenant, args.count)

if __name__ == "__main__":
    main()
