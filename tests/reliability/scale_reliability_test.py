#!/usr/bin/env python3
import sys
import os
import time
import random
import uuid
import concurrent.futures
import boto3
import grpc
import hmac
import hashlib
import json
import base64

# Add path to import local client
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub
import metri_pb2 as pb
import metri_pb2_grpc as pb_grpc
from google.protobuf import struct_pb2

TENANT_ID = "golden-tenant-benchmark"
STATUSES = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
ASSET_TYPES = ["PUMP", "MOTOR", "COMPRESSOR", "VALVE", "SENSOR", "TURBINE"]
LOCATIONS = ["LOC-001", "LOC-002", "LOC-003", "LOC-004", "LOC-005"]
CRITICALITIES = ["A", "B", "C"]
CATEGORIES = ["EQUIPMENT", "INFRASTRUCTURE", "SAFETY", "UTILITY"]

# Token generation for local authentication
HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    """Generates a signed HMAC-SHA256 session token."""
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

TOKEN = generate_signed_token(HMAC_SECRET, TENANT_ID, "usr_system_bff")

def purge_database(ddb, table_name):
    print(f"Purging table {table_name}...")
    paginator = ddb.get_paginator('scan')
    deleted_count = 0
    
    for page in paginator.paginate(TableName=table_name):
        items = page.get('Items', [])
        if not items:
            continue
        
        requests = []
        for item in items:
            requests.append({
                'DeleteRequest': {
                    'Key': {
                        'PK': item['PK'],
                        'SK': item['SK']
                    }
                }
            })
            
            if len(requests) == 25:
                ddb.batch_write_item(RequestItems={table_name: requests})
                deleted_count += len(requests)
                requests = []
                
        if requests:
            ddb.batch_write_item(RequestItems={table_name: requests})
            deleted_count += len(requests)
            
    print(f"Purge complete! Deleted {deleted_count} items.")

def ingest_single_asset(stub, index):
    asset_name = f"Asset SCALE {index+1} - {random.choice(ASSET_TYPES)} {uuid.uuid4().hex[:4].upper()}"
    status = random.choice(STATUSES)
    asset_type = random.choice(ASSET_TYPES)
    health = round(random.uniform(30.0, 100.0), 2)
    location = random.choice(LOCATIONS)
    criticality = random.choice(CRITICALITIES)
    category = random.choice(CATEGORIES)
    meter_reading = round(random.uniform(100.0, 5000.0), 2)

    payload = struct_pb2.Struct()
    payload.update({
        "name":                  asset_name,
        "status":                status,
        "type":                  asset_type,
        "health_score":          health,
        "location_id":           location,
        "tags":                  f"{asset_type},{status}",
        "criticality":           criticality,
        "category":              category,
        "current_meter_reading": meter_reading,
    })

    req = pb.TransactionRequest(
        tenant_id=TENANT_ID,
        entity_type="asset",
        action=pb.CREATE,
        payload=payload,
    )
    
    try:
        metadata = [('sid', TOKEN)]
        resp = stub.Transact(req, metadata=metadata)
        if resp.status.success:
            return True, resp.entity_id
        else:
            return False, resp.status.error_message
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 60)
    print("STARTING LARGE-SCALE RELIABILITY VALIDATION (10,000 ASSETS)")
    print("=" * 60)

    ddb = boto3.client(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    table_name = 'metri-eav-local'
    
    # 1. Purge local DB
    purge_database(ddb, table_name)

    # 2. Ingest 10,000 Assets in parallel
    channel = grpc.insecure_channel('localhost:9090')
    stub = pb_grpc.MetriServiceStub(channel)
    
    total_assets = 10000
    print(f"\nIngesting {total_assets} assets in parallel via gRPC...")
    
    ok_count = 0
    fail_count = 0
    entity_ids = []
    
    start_ingest = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(ingest_single_asset, stub, i): i for i in range(total_assets)}
        for future in concurrent.futures.as_completed(futures):
            success, result = future.result()
            if success:
                ok_count += 1
                entity_ids.append(result)
            else:
                fail_count += 1
                if fail_count <= 5:
                    print(f"  Ingestion error: {result}")
            
            if (ok_count + fail_count) % 1000 == 0:
                print(f"  Progress: {ok_count + fail_count}/{total_assets} ingested...")
                
    end_ingest = time.time()
    print(f"Ingestion complete: {ok_count} OK, {fail_count} FAILED in {end_ingest - start_ingest:.2f} seconds.")

    if ok_count == 0:
        print("Error: No assets were successfully ingested. Exiting.")
        return

    # 3. Scan meta/created_at and meta/updated_at and assign random timestamps
    print("\nScanning database for time attributes...")
    paginator = ddb.get_paginator('scan')
    assets_by_pk = {}
    
    for page in paginator.paginate(TableName=table_name):
        for item in page.get('Items', []):
            pk = item['PK']['S']
            if not pk.startswith(f"T#{TENANT_ID}#E#"):
                continue
            vp = item.get('vp', {}).get('S')
            if vp in ['T#golden-tenant-benchmark#AV#meta/created_at', 'T#golden-tenant-benchmark#AV#meta/updated_at']:
                if pk not in assets_by_pk:
                    assets_by_pk[pk] = {}
                assets_by_pk[pk][vp] = item

    print(f"Found time attributes for {len(assets_by_pk)} unique assets.")

    # Assign random timestamps uniformly distributed over the last 10 days
    now_ms = int(time.time() * 1000)
    ten_days_ms = 10 * 24 * 3600 * 1000
    
    asset_timestamps = []
    update_requests = []
    
    for pk, items in assets_by_pk.items():
        random_offset = random.randint(0, ten_days_ms)
        created_at_ms = now_ms - random_offset
        asset_timestamps.append(created_at_ms)
        
        # Prepare updates
        if 'T#golden-tenant-benchmark#AV#meta/created_at' in items:
            created_item = items['T#golden-tenant-benchmark#AV#meta/created_at']
            update_requests.append((created_item['PK'], created_item['SK'], created_at_ms))
        if 'T#golden-tenant-benchmark#AV#meta/updated_at' in items:
            updated_item = items['T#golden-tenant-benchmark#AV#meta/updated_at']
            update_requests.append((updated_item['PK'], updated_item['SK'], created_at_ms))

    # Apply updates in parallel
    print(f"Applying timestamp randomizations to {len(update_requests)} DB rows...")
    
    def update_timestamp_row(pk, sk, ts_val):
        try:
            ddb.update_item(
                TableName=table_name,
                Key={'PK': pk, 'SK': sk},
                UpdateExpression="SET v = :val",
                ExpressionAttributeValues={':val': {'N': str(ts_val)}}
            )
            return True
        except Exception as e:
            return str(e)

    start_update = time.time()
    ok_updates = 0
    fail_updates = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(update_timestamp_row, r[0], r[1], r[2]): r for r in update_requests}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res is True:
                ok_updates += 1
            else:
                fail_updates += 1
                if fail_updates <= 5:
                    print(f"  Update error: {res}")
                    
    end_update = time.time()
    print(f"Timestamp updates complete: {ok_updates} OK, {fail_updates} FAILED in {end_update - start_update:.2f} seconds.")

    # 4. Define Test Cases & Precalculate expectations in Python
    now_s = int(now_ms / 1000)
    
    test_cases = [
        {
            "name": "Case A: Last 24 Hours vs Yesterday",
            "start_s": now_s - 86400,
            "end_s": now_s,
            "offset_days": 1,
            "comp_label": "yesterday"
        },
        {
            "name": "Case B: Last 3 Days vs Previous 3 Days",
            "start_s": now_s - (3 * 86400),
            "end_s": now_s,
            "offset_days": 3,
            "comp_label": "prev_3d"
        },
        {
            "name": "Case C: Last 7 Days vs Previous 7 Days",
            "start_s": now_s - (7 * 86400),
            "end_s": now_s,
            "offset_days": 7,
            "comp_label": "prev_7d"
        }
    ]

    print("\nPrecalculating expected counts in Python...")
    for case in test_cases:
        start_ms = case["start_s"] * 1000
        end_ms = case["end_s"] * 1000
        
        # Previous range shift calculation
        comp_shift_ms = case["offset_days"] * 86400 * 1000
        comp_start_ms = start_ms - comp_shift_ms
        comp_end_ms = end_ms - comp_shift_ms
        
        # In-memory filtering matches engine's inclusive ranges
        case["expected_current"] = sum(1 for ts in asset_timestamps if start_ms <= ts <= end_ms)
        case["expected_previous"] = sum(1 for ts in asset_timestamps if comp_start_ms <= ts <= comp_end_ms)
        
        print(f"  [{case['name']}]")
        print(f"    Expected Current  (Range {start_ms} - {end_ms}): {case['expected_current']}")
        print(f"    Expected Previous (Range {comp_start_ms} - {comp_end_ms}): {case['expected_previous']}")

    # 5. Run queries against engine
    stub_web = GrpcWebStub("127.0.0.1:9090")
    
    evaluations = 0
    matches = 0
    
    print("\nRunning queries against Rust Engine and comparing...")
    for case in test_cases:
        req = pb.QueryRequest()
        req.tenant_id = TENANT_ID
        
        q = req.queries["reliability-test"]
        q.tenant_id = TENANT_ID
        q.entity = "asset"
        
        q.time_frame.type = pb.TimeFrameContext.CUSTOM_RANGE
        q.time_frame.start_ts = case["start_s"] * 1000
        q.time_frame.end_ts = case["end_s"] * 1000
        q.time_frame.timezone = "UTC"
        
        metric = q.metrics.add()
        metric.entity = "asset"
        metric.attribute = "id"
        metric.aggregation = pb.COUNT
        metric.name = "total_assets"
        
        comp = q.comparisons.add()
        comp.type = pb.AnalyticalComparison.TIME_SHIFT_RELATIVE
        comp.relative_granularity = "day"
        comp.relative_amount = case["offset_days"]
        comp.label = case["comp_label"]
        
        q.output_cast = pb.KPI
        q.viz = "kpi"
        q.limit = 15000  # Scan limit above our 10,000 count to bypass page splitting
        
        try:
            results = stub_web.Query(req)
            engine_current = None
            engine_previous = None
            
            for chunk in results:
                res_val = chunk.batch_results.get("reliability-test")
                if res_val and res_val.status.success:
                    if res_val.viz_ext and res_val.viz_ext.signal:
                        engine_current = int(res_val.viz_ext.signal.value)
                        engine_previous = int(res_val.viz_ext.signal.previous_value)
            
            print(f"\n→ Result for [{case['name']}]:")
            if engine_current is not None and engine_previous is not None:
                print(f"    Engine Current  : {engine_current}")
                print(f"    Expected Current: {case['expected_current']}")
                print(f"    Engine Previous : {engine_previous}")
                print(f"    Expected Previous: {case['expected_previous']}")
                
                # Evaluate current period
                evaluations += 1
                if engine_current == case["expected_current"]:
                    matches += 1
                    print("    ✔ Current Period: MATCH")
                else:
                    print("    ❌ Current Period: MISMATCH")
                    
                # Evaluate previous period
                evaluations += 1
                if engine_previous == case["expected_previous"]:
                    matches += 1
                    print("    ✔ Previous Period: MATCH")
                else:
                    print("    ❌ Previous Period: MISMATCH")
            else:
                print("    ❌ Error: Engine returned empty/invalid response.")
                evaluations += 2
                
        except Exception as e:
            print(f"    ❌ Query failed: {e}")
            evaluations += 2

    # 6. Calculate and report Reliability Indicator
    reliability_pct = (matches / evaluations) * 100.0 if evaluations > 0 else 0.0
    print("\n" + "=" * 60)
    print(f"RELIABILITY TEST COMPLETED: {matches}/{evaluations} METRICS MATCHED")
    print(f"RELIABILITY INDICATOR: {reliability_pct:.2f}%")
    print("=" * 60)

if __name__ == "__main__":
    main()
