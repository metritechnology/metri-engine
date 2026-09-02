#!/usr/bin/env python3
import json
import time
import os
import sys

# Setup paths to import the local client
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

def print_row_set(title, batch_res):
    print(f"\n--- WIDGET: {title} (Success: {batch_res.status.success}) ---")
    if not batch_res.status.success:
        print(f"  [Error]: {batch_res.status.error_code} - {batch_res.status.error_message}")
        return
    
    if batch_res.HasField("data"):
        data = batch_res.data
        if data.columns:
            print(f"  [Table Columns]: {[c.key for c in data.columns]}")
        
        strategy = data.WhichOneof("payload_strategy")
        if strategy == "rows_json":
            rows = data.rows_json.iter
            print(f"  [Rows count]: {len(rows)}")
            for i, r in enumerate(rows[:5]):
                row_vals = []
                for val in r.values:
                    if val.HasField("number_value"):
                        row_vals.append(val.number_value)
                    elif val.HasField("string_value"):
                        row_vals.append(val.string_value)
                    elif val.HasField("bool_value"):
                        row_vals.append(val.bool_value)
                    else:
                        row_vals.append(None)
                print(f"    Row {i+1}: {row_vals}")

def main():
    print("=" * 60)
    print("TESTING FILTER CRITERIA WITH REAL gRPC CASES")
    print("=" * 60)
    stub = GrpcWebStub("127.0.0.1:9090")
    tenant_id = "golden-tenant-benchmark"

    # Step 1: Discover schemas to locate attributes and their types
    print("-> Discovering schemas...")
    disc_req = pb.DiscoveryRequest(tenant_id=tenant_id, include_attributes=True)
    disc_resp = stub.Discover(disc_req)
    
    for schema in disc_resp.schemas:
        print(f"\nEntity: {schema.entity}")
        for attr in schema.attributes:
            print(f"  - {attr.name} ({attr.type})")

    # Step 2: Build dynamic filter queries
    req = pb.QueryRequest()
    req.tenant_id = tenant_id

    # 1. Test IN list_val filter (criticality IN ["CRITICAL", "HIGH"])
    q_in = req.queries["test-in-filter"]
    q_in.tenant_id = tenant_id
    q_in.entity = "asset"
    
    # Select dimensions
    dim = q_in.dimensions.add()
    dim.entity = "asset"
    dim.attribute = "criticality"
    
    # Metric (COUNT assets)
    metric = q_in.metrics.add()
    metric.entity = "asset"
    metric.attribute = "id"
    metric.aggregation = pb.COUNT
    metric.name = "Total Assets"
    
    # Filter: criticality IN ["CRITICAL", "HIGH"]
    f_node = q_in.filters.add()
    f_node.criteria.field = "criticality"
    f_node.criteria.op_ref = pb.IN
    f_node.criteria.value.list_val.values.extend(["CRITICAL", "HIGH"])
    
    q_in.output_cast = pb.TABLE
    q_in.viz = "table"

    # 2. Test BETWEEN range_values (health_score BETWEEN [50.0, 95.0])
    q_between = req.queries["test-between-filter"]
    q_between.tenant_id = tenant_id
    q_between.entity = "asset"
    
    # Metric
    metric = q_between.metrics.add()
    metric.entity = "asset"
    metric.attribute = "health_score"
    metric.aggregation = pb.AVG
    metric.name = "Avg Health Score"
    
    # Filter: health_score BETWEEN [50.0, 95.0]
    f_node = q_between.filters.add()
    f_node.criteria.field = "health_score"
    f_node.criteria.op_ref = pb.BETWEEN
    v1 = f_node.criteria.value.range_values.values.add()
    v1.number_val = 50.0
    v2 = f_node.criteria.value.range_values.values.add()
    v2.number_val = 95.0

    q_between.output_cast = pb.KPI
    q_between.viz = "kpi"

    # 3. Test timestamp_val filter (created_at GTE 1746662400000)
    q_ts = req.queries["test-timestamp-filter"]
    q_ts.tenant_id = tenant_id
    q_ts.entity = "asset"
    
    # Metric
    metric = q_ts.metrics.add()
    metric.entity = "asset"
    metric.attribute = "id"
    metric.aggregation = pb.COUNT
    metric.name = "Recent Assets count"
    
    # Filter: created_at GTE 1746662400000 (timestamp_val)
    f_node = q_ts.filters.add()
    f_node.criteria.field = "created_at"
    f_node.criteria.op_ref = pb.GTE
    f_node.criteria.value.timestamp_val = 1746662400000
    
    q_ts.output_cast = pb.KPI
    q_ts.viz = "kpi"

    print("\n-> Sending QueryRequest with dynamic filters to engine...")
    try:
        results = stub.Query(req)
        print(f"<- Received {len(results)} response chunks.")
        for res in results:
            for key, batch_res in res.batch_results.items():
                print_row_set(key, batch_res)
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
