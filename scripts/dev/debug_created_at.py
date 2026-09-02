#!/usr/bin/env python3
import os
import sys
import json
from google.protobuf.json_format import MessageToDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

def main():
    print("Iniciando query para assets...")
    stub = GrpcWebStub("127.0.0.1:9090")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    
    q = req.queries["asset_list"]
    q.tenant_id = "golden-tenant-benchmark"
    q.entity = "asset"
    q.viz = "table"
    q.limit = 5
    q.output_cast = pb.TABLE
    s = q.sort.add()
    s.field = "id"
    s.descending = True
    
    select_tree = {
        "id": True,
        "name": True,
        "status": True,
        "criticality": True,
        "omniclass_code": True,
        "omniclass_name": True,
        "manufacturer": True,
        "health_score": True,
        "serial_number": True,
        "created_at": True
    }
    q.select_tree.update(select_tree)
    
    try:
        results = stub.Query(req)
        print(f"<- Recibidas {len(results)} respuestas")
        for chunk in results:
            print("--- CHUNK ---")
            for key, res in chunk.batch_results.items():
                print(f"Key: {key}")
                if res.data:
                    cols = [col.key for col in res.data.columns]
                    print(f"Columns in response metadata: {cols}")
                    
                    # Convert whole response to dict to inspect it
                    res_dict = MessageToDict(res)
                    rows = res_dict.get("data", {}).get("rowsJson", {}).get("iter", [])
                    print(f"Number of rows: {len(rows)}")
                    for r in rows:
                        print("Row details:", r)
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
