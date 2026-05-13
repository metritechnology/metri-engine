import sys
import json
import logging
from google.protobuf.json_format import MessageToDict
import urllib.request
sys.path.append('.')
sys.path.append('src/metri/grpc')
import struct
import hashlib
import requests
import google.protobuf.json_format as json_format
import metri_pb2

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
        stream=True
    )
    response_bytes = resp.content
    messages = []
    offset = 0
    while offset < len(response_bytes):
        if offset + 5 > len(response_bytes): break
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        if flag == 0x00: messages.append(response_bytes[offset:offset+length])
        offset += length
    return messages

def run_test():
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
    q = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.KPI
    )
    
    m = q.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    
    req.queries["kpi"].CopyFrom(q)
    
    messages = invoke_grpc_web("metri.MetriService/Query", req)
    if messages:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(messages[0])
        print(json_format.MessageToJson(resp, preserving_proto_field_name=True))

if __name__ == "__main__":
    run_test()
