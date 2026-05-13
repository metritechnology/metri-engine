import time
import struct
import logging
import requests
import hashlib
from datetime import datetime
import google.protobuf.json_format as json_format
import json

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
        stream=True
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

def generate_report():
    logging.info("Generando informe Complejo Analítico (COUNT, SUM, AVG)...")
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    
    # Q1: COMPLEX - Count, Sum, Avg with Filter
    q_complex = metri_pb2.AnalyticsRequest()
    q_complex.tenant_id = "golden-tenant"
    q_complex.entity = "meter_reading"
    q_complex.output_cast = metri_pb2.TABLE
    
    # COUNT
    m_count = q_complex.metrics.add()
    m_count.entity = "meter_reading"
    m_count.attribute = "reading_value"
    m_count.aggregation = metri_pb2.COUNT
    
    # SUM
    m_sum = q_complex.metrics.add()
    m_sum.entity = "meter_reading"
    m_sum.attribute = "reading_value"
    m_sum.aggregation = metri_pb2.SUM
    
    # AVG
    m_avg = q_complex.metrics.add()
    m_avg.entity = "meter_reading"
    m_avg.attribute = "reading_value"
    m_avg.aggregation = metri_pb2.AVG
    
    # DIMENSION
    d_unit = q_complex.dimensions.add()
    d_unit.entity = "meter_reading"
    d_unit.attribute = "unit_of_measure"
    
    # FILTER: reading_value > 50
    f_node = q_complex.filters.add()
    f_node.criteria.field = "reading_value"
    f_node.criteria.op_ref = metri_pb2.GT
    f_node.criteria.value.number_val = 50.0
    
    req.queries["complex_analysis"].CopyFrom(q_complex)
    
    logging.info("Enviando QueryRequest a Janus...")
    res_messages = invoke_grpc_web("metri.MetriService/Query", req)
    
    if res_messages:
        for i, res_bytes in enumerate(res_messages):
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_str = json_format.MessageToJson(resp, preserving_proto_field_name=True)
            print(f"--- Chunk {i+1} JSON ---")
            print(json.dumps(json.loads(json_str), indent=2))
    else:
        logging.error("No se recibió respuesta válida.")

if __name__ == "__main__":
    generate_report()
