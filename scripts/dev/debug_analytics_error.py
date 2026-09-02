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

# Set HMAC secret for production
os.environ["HMAC_SECRET"] = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
os.environ["ENGINE_HOST"] = "engine.metri.one"

def main():
    stub = GrpcWebStub("engine.metri.one")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    
    # Let's request just one simple query first: kpi-total-assets-q
    q1 = req.queries["kpi-total-assets-q"]
    q1.tenant_id = "golden-tenant-benchmark"
    q1.entity = "asset"
    metric1 = q1.metrics.add()
    metric1.entity = "asset"
    metric1.attribute = "id"
    metric1.aggregation = pb.COUNT
    metric1.name = "Activos Totales"
    q1.output_cast = pb.KPI
    q1.viz = "kpi"

    print("-> Sending request...")
    try:
        body = stub._post("/metri.MetriService/Query", req)
        print(f"<- Response body length: {len(body)}")
        
        for is_trailer, payload in stub._parse_frames(body):
            if is_trailer:
                print(f"  [Trailer]: {payload.decode('utf-8', errors='ignore')}")
            else:
                msg = pb.QueryResponse()
                msg.ParseFromString(payload)
                print(f"  [Data Response] Key: {msg.batch_results.keys()}")
                for key, res in msg.batch_results.items():
                    print(f"    - Widget {key}: Success={res.status.success}, Error={res.status.error_message}")
                    if res.status.success and res.viz_ext and res.viz_ext.HasField("signal"):
                        print(f"      Signal Value: {res.viz_ext.signal.value}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
