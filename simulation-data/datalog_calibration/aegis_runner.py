import os
import sys
import json
import struct
import hashlib
import requests
from google.protobuf import struct_pb2

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID = "datalog-golden-tenant"

def invoke_query(req: metri_pb2.QueryRequest):
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        },
        stream=True
    )
    
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
        
    response_bytes = resp.content
    offset = 0
    while offset < len(response_bytes):
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        if flag == 0x00:
            chunk_bytes = response_bytes[offset:offset+length]
            resp_proto = metri_pb2.QueryResponse()
            resp_proto.ParseFromString(chunk_bytes)
            if not resp_proto.status.success:
                raise Exception(f"Aegis Error: {resp_proto.status.error_code} - {resp_proto.status.error_message}")
            return resp_proto
        offset += length
    raise Exception("No valid chunk received")

def build_q001():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID
    
    q1 = req.queries["q1"]
    q1.entity = "asset"
    
    # COUNT
    m = q1.metrics.add()
    m.aggregation = metri_pb2.COUNT
    
    # GROUP BY status
    d = q1.dimensions.add()
    d.attribute = "status"
    
    return req

def build_q002():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID
    q1 = req.queries["q1"]
    q1.entity = "work_order"
    
    # SUM total_cost
    m = q1.metrics.add()
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    
    # GROUP BY priority
    d = q1.dimensions.add()
    d.attribute = "priority"
    
    return req

def build_q003():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID
    q1 = req.queries["q1"]
    q1.entity = "work_order"
    
    # AVG completion_percentage
    m = q1.metrics.add()
    m.attribute = "completion_percentage"
    m.aggregation = metri_pb2.AVG
    
    # FILTER status = IN_PROGRESS
    f = q1.filters.add()
    f.criteria.field = "status"
    f.criteria.op_ref = metri_pb2.EQ
    f.criteria.value.string_val = "IN_PROGRESS"
    
    return req

def main():
    base_dir = os.path.dirname(__file__)
    exp_path = os.path.join(base_dir, "expectations.json")
    if not os.path.exists(exp_path):
        print(f"Error: {exp_path} no existe. Ejecuta golden_set_builder.py primero.")
        return
        
    with open(exp_path, "r") as f:
        expectations = json.load(f)
        
    print("Iniciando FASE 3: Contraste cruzado (Pandas vs Aegis Datalog)...\n")
    
    tests = [
        ("Q001_count_assets_by_status", build_q001),
        ("Q002_sum_cost_by_priority", build_q002),
        ("Q003_avg_completion_in_progress", build_q003)
    ]
    
    passed = 0
    for name, builder in tests:
        print(f"Ejecutando {name}...")
        try:
            req = builder()
            resp = invoke_query(req)
            
            # TODO: Convert gRPC DataRowList back to dict for comparison
            # For now we just print the raw rows if it succeeded
            print(f"  ✅ Aegis retornó datos para {name}")
            print(f"  Rows recibidas: {len(resp.results['q1'].rows_json.iter)}")
            # We would parse the rows into a dict mapping dimension -> metric and compare
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            
    print(f"\nFinalizado.")

if __name__ == "__main__":
    main()
