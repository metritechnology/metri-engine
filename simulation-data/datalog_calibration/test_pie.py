import time
import struct
import logging
import requests
import hashlib
import json
import os
import sys
import google.protobuf.json_format as json_format

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
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

def run_pie_test():
    logging.info("Ejecutando prueba VIZ PIE sobre OLTP...")
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
    q = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.PIE
    )
    m = q.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM

    d = q.dimensions.add()
    d.entity = "work_order"
    d.attribute = "status"

    req.queries["pie_test_1"].CopyFrom(q)
    
    # Test OLAP PIE as well
    q_olap = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.PIE
    )
    m2 = q_olap.metrics.add()
    m2.entity = "meter_reading"
    m2.attribute = "reading_value"
    m2.aggregation = metri_pb2.SUM

    d2 = q_olap.dimensions.add()
    d2.entity = "meter_reading"
    d2.attribute = "unit_of_measure"

    req.queries["pie_test_2"].CopyFrom(q_olap)

    logging.info("Enviando consulta gRPC a engine.metri.one...")
    messages = invoke_grpc_web("metri.MetriService/Query", req)
    
    results = {}
    if messages:
        for res_bytes in messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results[k] = v

    print(json.dumps(results, indent=2))
    
if __name__ == "__main__":
    run_pie_test()
