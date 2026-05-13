import time
import struct
import logging
import requests
import hashlib
import json
import google.protobuf.json_format as json_format
from google.protobuf.struct_pb2 import Struct

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
FUNCTION_URL = "https://engine.metri.one/"

def invoke_grpc_web(endpoint: str, proto_req):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    resp = requests.post(
        f"{FUNCTION_URL}{endpoint}", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        },
        stream=True,
        timeout=60
    )
    
    if resp.status_code != 200:
        logging.error(f"Error {resp.status_code}: {resp.text}")
        return []

    response_bytes = resp.content
    messages = []
    offset = 0
    while offset < len(response_bytes):
        if offset + 5 > len(response_bytes):
            break
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        if flag == 0x00:
            messages.append(response_bytes[offset:offset+length])
        offset += length
    return messages

def run_tests():
    logging.info("Ejecutando Consulta RAW (SELECT reading_value) sobre meter_reading...")
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    q_name = "Raw_Select_Reading_Value"
    
    ar = metri_pb2.AnalyticsRequest(
        tenant_id="golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.TABLE, 
        limit=5
    )
    
    s = Struct()
    s.update({"fields": ["reading_value"]})
    ar.select_tree.CopyFrom(s)
    
    req.queries[q_name].CopyFrom(ar)
    
    res_messages = invoke_grpc_web("metri.MetriService/Query", req)
    
    results_map = {}
    if res_messages:
        for res_bytes in res_messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results_map[k] = v

    print(f"\nResultados RAW obtenidos:")
    print(json.dumps(results_map, indent=2))
    
    with open("meter_reading_reading_value.json", "w") as f:
        json.dump(results_map, f, indent=2)
    print("\nReporte JSON generado exitosamente en meter_reading_reading_value.json")
    
if __name__ == "__main__":
    run_tests()
