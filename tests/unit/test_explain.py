#!/usr/bin/env python3
import sys, os
from google.protobuf.json_format import MessageToDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

def main():
    stub = GrpcWebStub("127.0.0.1:9090")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    req.explain_plan = True  # EXPLAIN PLAN!
    
    q = req.queries["debug-interval"]
    q.tenant_id = "golden-tenant-benchmark"
    q.entity = "asset"
    q.output_cast = pb.TIMESERIES
    q.viz = "line"
    q.limit = 1000
    
    d = q.dimensions.add()
    d.entity = "asset"
    d.attribute = "created_at"
    d.interval = "day"
    d.label_template = "{{created_at}}"
    
    m = q.metrics.add()
    m.entity = "asset"
    m.attribute = "id"
    m.aggregation = pb.COUNT
    m.name = "Registros"
    
    q.time_frame.type = pb.TimeFrameContext.ALL_TIME
    
    print("-> Sending explain request...")
    try:
        results = stub.Query(req)
        for res in results:
            # The explain plan is returned in status or metadata or response! Let's print the dict
            res_dict = MessageToDict(res)
            print(res_dict)
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
