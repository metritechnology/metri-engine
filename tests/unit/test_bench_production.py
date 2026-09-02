#!/usr/bin/env python3
import time
import os
import sys

# Ensure protobuf modules are in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from grpc_web_client import GrpcWebStub, pb

# Set HMAC secret for production
os.environ["HMAC_SECRET"] = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
os.environ["ENGINE_HOST"] = "engine.metri.one"

def create_assets_request(tenant_id: str, offset: int = 0, limit: int = 10) -> pb.QueryRequest:
    return pb.QueryRequest(
        tenant_id=tenant_id,
        queries={
            "res": pb.AnalyticsRequest(
                tenant_id=tenant_id,
                entity="asset",
                viz="table",
                limit=limit,
                cursor=f"offset={offset}" if offset > 0 else ""
            )
        }
    )

def execute_and_time(stub, req, label):
    t0 = time.time()
    results = stub.Query(req)
    t1 = time.time()
    latency_ms = (t1 - t0) * 1000
    print(f"[{label}] Execution took: {latency_ms:.2f} ms")
    
    found = False
    for chunk in results:
        for key, res in chunk.batch_results.items():
            if res.data and res.data.rows_json and res.data.rows_json.iter:
                rows = res.data.rows_json.iter
                if not found:
                    print(f"  → Returned {len(rows)} rows (Total in DB: {res.metadata.total_count})")
                    found = True
                for r in rows[:2]: # Show first 2 as preview
                    row_dict = {}
                    for col, val in zip(res.data.columns, r.values):
                        row_dict[col.key] = val.string_value or str(val.number_value)
                    print(f"    - Preview: {row_dict.get('id', '???')} | {row_dict.get('name', 'N/A')}")
    if not found:
        print("  → Returned 0 rows")
    return latency_ms

def main():
    stub = GrpcWebStub("engine.metri.one")
    tenant_id = "golden-tenant-benchmark"

    print("\n=== RUNNING PRODUCTION EAV BENCHMARK ===")
    
    # 1. First Load (AEVT Scan Cache MISS / DynamoDB Scan on AWS)
    print("\n--- 1. First Load (Cache Miss / Database Pull) ---")
    req1 = create_assets_request(tenant_id, offset=0, limit=10)
    execute_and_time(stub, req1, "FIRST_LOAD")

    # 2. Second Load (AEVT Scan Cache HIT / Pre-sliced fast path)
    print("\n--- 2. Second Load (Cache Hit / Pre-sliced Pull) ---")
    req2 = create_assets_request(tenant_id, offset=0, limit=10)
    execute_and_time(stub, req2, "SECOND_LOAD")

    # 3. Pagination Load (AEVT Scan Cache HIT / Pre-sliced Pagination)
    print("\n--- 3. Pagination (Offset = 10, Limit = 10) ---")
    req3 = create_assets_request(tenant_id, offset=10, limit=10)
    execute_and_time(stub, req3, "PAGINATION_LOAD")

if __name__ == "__main__":
    main()
