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
    
    try:
        resp = requests.post(
            f"{FUNCTION_URL}{endpoint}", 
            data=framed_data, 
            headers={
                'Content-Type': 'application/grpc-web+proto',
                'x-amz-content-sha256': payload_hash
            },
            stream=True,
            timeout=30
        )
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return None

    if resp.status_code != 200:
        logging.error(f"Error {resp.status_code}: {resp.text}")
        return None

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

def run_query(name, q):
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    req.queries[name].CopyFrom(q)
    
    logging.info(f"Testing {name}...")
    msgs = invoke_grpc_web("metri.MetriService/Query", req)
    if msgs is None:
        logging.error(f"{name} FAILED with HTTP error.")
        return
    
    if len(msgs) == 0:
        logging.error(f"{name} FAILED with no messages.")
        return
        
    for res_bytes in msgs:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(res_bytes)
        json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
        print("RAW JSON:", json.dumps(json_dict, indent=2))
        if "batch_results" in json_dict and name in json_dict["batch_results"]:
            status = json_dict["batch_results"][name].get("status", {})
            if status.get("success"):
                logging.info(f"{name} SUCCESS.")
            else:
                logging.error(f"{name} FAILED in batch_results: {status}")
        else:
            logging.error(f"{name} NOT FOUND in batch_results.")

def run_tests():
    # 1. TIMESERIES (Line/Bar)
    q1 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.TIMESERIES
    )
    m1 = q1.metrics.add()
    m1.entity = "meter_reading"
    m1.attribute = "reading_value"
    m1.aggregation = metri_pb2.SUM
    d1 = q1.dimensions.add()
    d1.entity = "meter_reading"
    d1.attribute = "timestamp"
    d1.interval = "day"
    q1.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q1.time_frame.n_value = 6
    q1.time_frame.timezone = "America/Bogota"
    q1.viz = "line"
    run_query("olap_timeseries_line", q1)

    # 5. PIE
    q5 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.PIE
    )
    m5 = q5.metrics.add()
    m5.entity = "meter_reading"
    m5.attribute = "reading_value"
    m5.aggregation = metri_pb2.SUM
    d5 = q5.dimensions.add()
    d5.entity = "meter_reading"
    d5.attribute = "unit_of_measure"
    q5.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q5.time_frame.n_value = 6
    q5.viz = "pie"
    run_query("olap_pie_chart", q5)

if __name__ == "__main__":
    run_tests()
