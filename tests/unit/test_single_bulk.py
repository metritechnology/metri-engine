#!/usr/bin/env python3
import os
import sys
import json
from google.protobuf import struct_pb2

# Add path to import local client
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub, pb

ENGINE_HOST = "engine.metri.one"
HMAC_SECRET = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"

os.environ["ENGINE_HOST"] = ENGINE_HOST
os.environ["HMAC_SECRET"] = HMAC_SECRET

def main():
    stub = GrpcWebStub()
    
    # Generate exactly 1 asset
    asset = {
        "name":                  "Test Bulk Asset Single",
        "status":                "ACTIVE",
        "type":                  "SENSOR",
        "health_score":          95.5,
        "location_id":           "LOC-001",
        "criticality":           "MEDIUM",
        "category":              "EQUIPMENT",
        "current_meter_reading": 123.4,
        "model":                 "Model-T1"
    }
    
    columns_keys = list(asset.keys())
    
    columns = [
        pb.ColumnSchema(key=k, label=k, type="string")
        for k in columns_keys
    ]
    
    values = []
    for k in columns_keys:
        v = asset[k]
        if isinstance(v, (int, float)):
            values.append(struct_pb2.Value(number_value=float(v)))
        else:
            values.append(struct_pb2.Value(string_value=str(v)))
            
    rowset = pb.RowSet(
        columns=columns,
        rows_json=pb.DataRowList(iter=[pb.DataRow(values=values)])
    )
    
    req = pb.BulkRequest(
        tenant_id="golden-tenant-benchmark",
        entity_type="asset",
        action=pb.CREATE,
        data=rowset,
    )
    
    print("Sending single BulkIngest request...")
    try:
        resp = stub.BulkIngest(req)
        print("Response received:")
        print(f"Status success : {resp.status.success if resp.status else None}")
        print(f"Error Code     : {resp.status.error_code if resp.status else None}")
        print(f"Error Message  : {resp.status.error_message if resp.status else None}")
        print(f"Ingested Count : {resp.ingested_count}")
    except Exception as e:
        print(f"Exception raised: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"HTTP Status: {e.response.status_code}")
            print(f"Response headers: {json.dumps(dict(e.response.headers), indent=2)}")
            print(f"Response body: {e.response.text}")

if __name__ == "__main__":
    main()
