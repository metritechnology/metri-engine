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
            timeout=120
        )
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return []

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
    logging.info("Ejecutando pruebas VIZ AVANZADAS (Timeseries, Scatter, Table, KPI Comparison)...")
    results = {}
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
    # 1. TIMESERIES (Line Chart default, but we request "bar" via viz_hint)
    q1 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.TIMESERIES
    )
    m1 = q1.metrics.add()
    m1.entity = "work_order"
    m1.attribute = "total_cost"
    m1.aggregation = metri_pb2.SUM
    
    d1 = q1.dimensions.add()
    d1.entity = "work_order"
    d1.attribute = "status"
    
    q1.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q1.time_frame.n_value = 30
    q1.time_frame.timezone = "America/Bogota"
    
    # NEW: Request "bar" dynamically
    q1.viz = "bar"
    
    req.queries["test_timeseries_bar"].CopyFrom(q1)

    # 2. BUBBLE (Scatter Chart)
    q3 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.BUBBLE
    )
    m3 = q3.metrics.add()
    m3.entity = "work_order"
    m3.attribute = "total_cost"
    m3.aggregation = metri_pb2.AVG
    
    d3 = q3.dimensions.add()
    d3.entity = "work_order"
    d3.attribute = "status"
    
    q3.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q3.time_frame.n_value = 6
    req.queries["test_scatter_bubble"].CopyFrom(q3)

    # 3. TABLE
    q4 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", # Testing OLAP Table
        output_cast=metri_pb2.TABLE
    )
    m4 = q4.metrics.add()
    m4.entity = "meter_reading"
    m4.attribute = "reading_value"
    m4.aggregation = metri_pb2.MAX
    
    d4 = q4.dimensions.add()
    d4.entity = "meter_reading"
    d4.attribute = "unit_of_measure"
    
    req.queries["test_table_olap"].CopyFrom(q4)

    # 4. KPI with comparison (Indicator) explicitly requesting "indicator"
    q5 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="work_order", 
        output_cast=metri_pb2.KPI
    )
    m5 = q5.metrics.add()
    m5.entity = "work_order"
    m5.attribute = "total_cost"
    m5.aggregation = metri_pb2.SUM
    
    q5.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q5.time_frame.n_value = 7
    q5.time_frame.timezone = "America/Bogota"
    
    c5 = q5.comparisons.add()
    c5.relative_granularity = "week"
    c5.relative_amount = -1
    
    # NEW: Request "indicator" dynamically
    q5.viz = "indicator"
    
    req.queries["test_kpi_indicator"].CopyFrom(q5)

    logging.info(f"Enviando {len(req.queries)} consultas gRPC avanzadas...")
    messages = invoke_grpc_web("metri.MetriService/Query", req)
    
    if messages:
        for res_bytes in messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results[k] = v

    with open("viz_advanced_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Recibidos {len(results)} resultados.")
    logging.info("Reporte JSON guardado en viz_advanced_results.json")

if __name__ == "__main__":
    run_tests()
