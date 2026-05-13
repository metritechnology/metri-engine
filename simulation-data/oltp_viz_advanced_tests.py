import time
import struct
import logging
import requests
import hashlib
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
            'X-Metri-Origin-Token': 'datalog-golden-tenant',
            'x-amz-content-sha256': payload_hash
        }
    )
    
    if resp.status_code != 200:
        logging.error(f"Error HTTP {resp.status_code}: {resp.text}")
        return None

    response_bytes = resp.content
    if len(response_bytes) < 5:
        return None
        
    flag, length = struct.unpack('!BI', response_bytes[:5])
    if flag == 0x00:
        return response_bytes[5:5+length]
    return None

def test_kpi_comparison():
    logging.info("Corriendo prueba: OLTP KPI Comp+TF Test")
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    
    # With comparison
    query_cmp = metri_pb2.AnalyticsRequest()
    query_cmp.tenant_id = "golden-tenant"
    query_cmp.entity = "inventory_movement"
    query_cmp.output_cast = metri_pb2.KPI
    query_cmp.viz = "indicator"
    m_cmp = query_cmp.metrics.add()
    m_cmp.aggregation = metri_pb2.AVG
    m_cmp.attribute = "quantity"
    query_cmp.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    query_cmp.time_frame.n_value = 30
    cmp = query_cmp.comparisons.add()
    cmp.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    req.queries["q_cmp"].CopyFrom(query_cmp)


    res_bytes = invoke_grpc_web("metri.MetriService/Query", req)
    
    if res_bytes:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(res_bytes)
        
        if resp.status.success:
            from google.protobuf.json_format import MessageToDict
            chunk_dict = MessageToDict(resp, preserving_proto_field_name=True)
            print(json.dumps(chunk_dict, indent=2))
        else:
            logging.error(f"Error en consulta: {resp.status.error_message}")
    else:
        logging.error("No se recibió respuesta válida")

if __name__ == "__main__":
    test_kpi_comparison()
