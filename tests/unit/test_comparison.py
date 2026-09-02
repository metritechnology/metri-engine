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

def main():
    print("Iniciando prueba de gRPC - AnalyticalComparison (TIME_SHIFT_RELATIVE)")
    stub = GrpcWebStub("127.0.0.1:9090")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    
    q = req.queries["test-comparison"]
    q.tenant_id = "golden-tenant-benchmark"
    
    # 1. Scope (Entity)
    q.entity = "asset"
    
    # 2. TimeFrame (Current Period)
    now_s = int(time.time())
    q.time_frame.type = pb.TimeFrameContext.CUSTOM_RANGE
    q.time_frame.start_ts = (now_s - 86400) * 1000  # last 24h
    q.time_frame.end_ts = now_s * 1000
    q.time_frame.timezone = "UTC"
    
    # 3. Metric
    metric = q.metrics.add()
    metric.entity = "asset"
    metric.attribute = "id"
    metric.aggregation = pb.COUNT
    metric.name = "total_assets"
    
    # 4. Comparison (What we are testing)
    comp = q.comparisons.add()
    comp.type = pb.AnalyticalComparison.TIME_SHIFT_RELATIVE
    comp.relative_granularity = "day"
    comp.relative_amount = 1
    comp.label = "prev_day"

    # 5. Output Cast & Viz Hint
    q.output_cast = pb.KPI
    q.viz = "kpi"
    q.limit = 200
    
    print(f"-> Sending query request to engine")
    try:
        results = stub.Query(req)
        print(f"<- Recibidas {len(results)} respuestas (chunks)")
        for res in results:
            print("--- CHUNK ---")
            print(res)
                
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Asegurate de que el engine está corriendo en puerto 9090 (cargo run --bin bootstrap)")

if __name__ == "__main__":
    main()
