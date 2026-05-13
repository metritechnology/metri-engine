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

def run_tests():
    logging.info("Ejecutando pruebas Avanzadas (Aggregates, TimeFrames, Comparisons) sobre OLTP...")
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
    # Q1: Aggregate simple (KPI) sin TimeFrame
    q_kpi = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.KPI
    )
    m = q_kpi.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    req.queries["kpi_total_cost"].CopyFrom(q_kpi)

    # Q2: Aggregate (PIE) por Status
    q_pie = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.PIE
    )
    m = q_pie.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    d = q_pie.dimensions.add()
    d.entity = "work_order"
    d.attribute = "status"
    req.queries["pie_cost_by_status"].CopyFrom(q_pie)

    # Q3: TimeFrames (LAST_200_DAYS)
    q_tf = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.KPI
    )
    m = q_tf.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    
    q_tf.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q_tf.time_frame.n_value = 200
    q_tf.time_frame.timezone = "America/Bogota"
    req.queries["timeframe_last_200_days"].CopyFrom(q_tf)

    # Q4: Comparisons (LAST_200_DAYS vs LAST_200_DAYS desplazado -1)
    q_comp = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.KPI
    )
    m = q_comp.metrics.add()
    m.entity = "work_order"
    m.attribute = "total_cost"
    m.aggregation = metri_pb2.SUM
    
    q_comp.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q_comp.time_frame.n_value = 200
    q_comp.time_frame.timezone = "America/Bogota"
    
    comp = q_comp.comparisons.add()
    comp.relative_granularity = "day"
    comp.relative_amount = -200
    req.queries["comparison_vs_previous_200_days"].CopyFrom(q_comp)

    logging.info("Enviando consultas gRPC (OLTP)...")
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

    with open("oltp_advanced_analytics.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logging.info("Reporte JSON con agregados y timeframes guardado en oltp_advanced_analytics.json")

if __name__ == "__main__":
    run_tests()
