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
    logging.info("Ejecutando TODAS las pruebas VIZ en OLAP (meter_reading)...")
    results = {}
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
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
    req.queries["olap_timeseries_line"].CopyFrom(q1)

    # 2. SCATTER / BUBBLE
    q2 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.BUBBLE
    )
    m2 = q2.metrics.add()
    m2.entity = "meter_reading"
    m2.attribute = "reading_value"
    m2.aggregation = metri_pb2.AVG
    
    d2 = q2.dimensions.add()
    d2.entity = "meter_reading"
    d2.attribute = "timestamp"
    
    q2.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q2.time_frame.n_value = 6
    q2.viz = "scatter"
    req.queries["olap_scatter_bubble"].CopyFrom(q2)

    # 3. TABLE
    q3 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.TABLE
    )
    m3 = q3.metrics.add()
    m3.entity = "meter_reading"
    m3.attribute = "reading_value"
    m3.aggregation = metri_pb2.MAX
    
    d3 = q3.dimensions.add()
    d3.entity = "meter_reading"
    d3.attribute = "timestamp"
    
    q3.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q3.time_frame.n_value = 6
    q3.viz = "table"
    req.queries["olap_table"].CopyFrom(q3)

    # 4. KPI con Comparativa (Indicator)
    q4 = metri_pb2.AnalyticsRequest(
        tenant_id="datalog-golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.KPI
    )
    m4 = q4.metrics.add()
    m4.entity = "meter_reading"
    m4.attribute = "reading_value"
    m4.aggregation = metri_pb2.SUM
    
    q4.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q4.time_frame.n_value = 1
    q4.time_frame.timezone = "America/Bogota"
    
    c4 = q4.comparisons.add()
    c4.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    c4.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    
    q4.viz = "indicator"
    req.queries["olap_kpi_indicator"].CopyFrom(q4)

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
    req.queries["olap_pie_chart"].CopyFrom(q5)

    logging.info(f"Enviando {len(req.queries)} consultas OLAP avanzadas al servidor...")
    messages = invoke_grpc_web("metri.MetriService/Query", req)
    
    if messages:
        for res_bytes in messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results[k] = v

    output_file = "olap_viz_all_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Recibidos {len(results)} resultados.")
    logging.info(f"Reporte JSON guardado en {output_file}")

if __name__ == "__main__":
    run_tests()
