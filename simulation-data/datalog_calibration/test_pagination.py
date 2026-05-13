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

    headers = {
        "Content-Type": "application/grpc-web+proto",
        "x-grpc-web": "1",
        "x-metri-origin-token": "antigravity-dev-test",
        "x-metri-payload-hash": payload_hash
    }

    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query",
        data=framed_data,
        headers=headers,
        stream=True
    )

    if resp.status_code != 200:
        logging.error(f"HTTP {resp.status_code}: {resp.text}")
        return None

    buffer = b""
    responses = []
    for chunk in resp.iter_content(chunk_size=4096):
        buffer += chunk
        while len(buffer) >= 5:
            flags, length = struct.unpack('!BI', buffer[:5])
            if len(buffer) < 5 + length:
                break
            msg_bytes = buffer[5:5+length]
            buffer = buffer[5+length:]
            
            # Flags: 0 is regular message, 128 is trailers
            if flags == 0:
                proto_resp = metri_pb2.QueryResponse()
                proto_resp.ParseFromString(msg_bytes)
                responses.append(proto_resp)

    # Return the last one with batch_results, or just merge them
    for r in reversed(responses):
        if r.batch_results:
            return r
    return responses[-1] if responses else None

def run_pagination_test():
    logging.info("Iniciando prueba de Paginación en Metri Engine...")
    
    # OLAP Limit 2
    logging.info("--- PRUEBA OLAP CON LIMIT = 2 ---")
    req_olap = metri_pb2.QueryRequest()
    req_olap.tenant_id = "datalog-golden-tenant"
    
    q_olap = metri_pb2.AnalyticsRequest()
    q_olap.tenant_id = "datalog-golden-tenant"
    q_olap.entity = "meter_reading"
    
    m_olap = q_olap.metrics.add()
    m_olap.entity = "meter_reading"
    m_olap.attribute = "reading_value"
    m_olap.aggregation = metri_pb2.MAX
    
    d_olap = q_olap.dimensions.add()
    d_olap.entity = "meter_reading"
    d_olap.attribute = "timestamp"
    
    q_olap.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q_olap.time_frame.n_value = 6
    q_olap.time_frame.timezone = "America/Bogota"

    q_olap.limit = 2
    req_olap.queries["olap_paginated_query"].CopyFrom(q_olap)
    
    resp_olap = invoke_grpc_web(FUNCTION_URL, req_olap)
    if resp_olap:
        json_olap = json_format.MessageToDict(resp_olap)
        logging.info("Resultado OLAP:")
        logging.info(json.dumps(json_olap, indent=2))
    
    # OLTP Limit 1
    logging.info("--- PRUEBA OLTP CON LIMIT = 1 ---")
    req_oltp = metri_pb2.QueryRequest()
    req_oltp.tenant_id = "datalog-golden-tenant"
    
    q_oltp = metri_pb2.AnalyticsRequest()
    q_oltp.tenant_id = "datalog-golden-tenant"
    q_oltp.entity = "asset"
    q_oltp.limit = 1
    req_oltp.queries["oltp_paginated_query"].CopyFrom(q_oltp)
    
    resp_oltp = invoke_grpc_web(FUNCTION_URL, req_oltp)
    if resp_oltp:
        json_oltp = json_format.MessageToDict(resp_oltp)
        logging.info("Resultado OLTP:")
        logging.info(json.dumps(json_oltp, indent=2))

if __name__ == "__main__":
    run_pagination_test()
