import time
import struct
import logging
import requests
import hashlib
import json
import os
import sys
import google.protobuf.json_format as json_format

# Añadir simulation-data al path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
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
    logging.info("Ejecutando pruebas sobre OLTP para TimeFrames y Comparisons...")
    
    # 1. Preparar casos de TimeFrames (100)
    tf_req = metri_pb2.QueryRequest()
    tf_req.tenant_id = "datalog-golden-tenant"
    
    for i in range(1, 21):
        q_name = f"timeframe_q{i}"
        tf_analytics = metri_pb2.AnalyticsRequest(
            tenant_id="datalog-golden-tenant", 
            entity="meter_reading", 
            output_cast=metri_pb2.TABLE, 
            limit=100
        )
        tf_analytics.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        tf_analytics.time_frame.n_value = i
        tf_analytics.time_frame.timezone = "America/Bogota"
        tf_req.queries[q_name].CopyFrom(tf_analytics)
        
    # 2. Preparar casos de Comparisons (100)
    comp_req = metri_pb2.QueryRequest()
    comp_req.tenant_id = "datalog-golden-tenant"
    
    for i in range(1, 21):
        q_name = f"comparison_q{i}"
        comp_analytics = metri_pb2.AnalyticsRequest(
            tenant_id="datalog-golden-tenant", 
            entity="meter_reading", 
            output_cast=metri_pb2.TABLE, 
            limit=100
        )
        # La comparación requiere un timeframe base para tener sentido en OLAP, en OLTP es opcional
        comp_analytics.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        comp_analytics.time_frame.n_value = 30
        comp_analytics.time_frame.timezone = "America/Bogota"
        
        comp = metri_pb2.AnalyticalComparison()
        comp.relative_granularity = "day"
        comp.relative_amount = -i
        
        comp_analytics.comparisons.append(comp)
        comp_req.queries[q_name].CopyFrom(comp_analytics)

    # 3. Invocar TimeFrames
    logging.info("Enviando batch de 100 consultas de TimeFrames...")
    tf_messages = invoke_grpc_web("metri.MetriService/Query", tf_req)
    
    tf_results = {}
    if tf_messages:
        for res_bytes in tf_messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    tf_results[k] = v

    # 4. Invocar Comparisons
    logging.info("Enviando batch de 100 consultas de Comparisons...")
    comp_messages = invoke_grpc_web("metri.MetriService/Query", comp_req)
    
    comp_results = {}
    if comp_messages:
        for res_bytes in comp_messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    comp_results[k] = v

    report = {
        "time_frames": tf_results,
        "comparisons": comp_results
    }
    
    with open("oltp_reports.json", "w") as f:
        json.dump(report, f, indent=2)
    
    logging.info("Reporte JSON generado exitosamente en oltp_reports.json con 100 casos por categoría.")

if __name__ == "__main__":
    run_tests()
