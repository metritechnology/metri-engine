import grpc
import json
import os
import struct
import hashlib
import requests
import time
from google.protobuf.json_format import MessageToDict
import sys
sys.path.append(os.path.abspath('src/metri/grpc/python'))
import metri_pb2

FUNCTION_URL = "https://engine.metri.one/"

def run():
    cmp_query = metri_pb2.AnalyticsRequest()
    cmp_query.entity = "meter_reading"
    cmp_query.output_cast = metri_pb2.KPI
    cmp_query.viz = "indicator"
    m4 = cmp_query.metrics.add()
    m4.attribute = "reading_value"
    m4.aggregation = metri_pb2.AVG
    cmp1 = cmp_query.comparisons.add()
    cmp1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp1.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    cmp_query.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    cmp_query.time_frame.n_value = 30
    cmp_query.tenant_id = "golden-tenant"
    
    req = metri_pb2.QueryRequest(
        tenant_id="golden-tenant",
        explain_plan=True
    )
    req.queries["q_kpi_cmp_tf"].CopyFrom(cmp_query)
    
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        }
    )
    
    response_bytes = resp.content
    offset = 0
    while offset < len(response_bytes):
        if offset + 5 > len(response_bytes): break
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        if flag == 0x00:
            chunk_data = response_bytes[offset:offset+length]
            qr = metri_pb2.QueryResponse()
            qr.ParseFromString(chunk_data)
            print(json.dumps(MessageToDict(qr, preserving_proto_field_name=True), indent=2))
        offset += length

if __name__ == "__main__":
    run()
