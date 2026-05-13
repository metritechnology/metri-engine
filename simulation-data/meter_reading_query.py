import time
import struct
import logging
import requests
import hashlib
from datetime import datetime

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
    import google.protobuf.json_format as json_format
    import json
    
    logging.info("Generando informe Analítico (KPI, PIE, AREA)...")
    req = metri_pb2.QueryRequest()
    req.tenant_id = "golden-tenant"
    
    # Q1: KPI - Suma total de consumo
    q_kpi = metri_pb2.AnalyticsRequest()
    q_kpi.tenant_id = "golden-tenant"
    q_kpi.entity = "meter_reading"
    q_kpi.output_cast = metri_pb2.KPI
    m_kpi = q_kpi.metrics.add()
    m_kpi.entity = "meter_reading"
    m_kpi.attribute = "reading_value"
    m_kpi.aggregation = metri_pb2.SUM
    req.queries["kpi_total"].CopyFrom(q_kpi)
    
    # Q2: PIE - Consumo por unidad de medida
    q_pie = metri_pb2.AnalyticsRequest()
    q_pie.tenant_id = "golden-tenant"
    q_pie.entity = "meter_reading"
    q_pie.output_cast = metri_pb2.PIE
    m_pie = q_pie.metrics.add()
    m_pie.entity = "meter_reading"
    m_pie.attribute = "reading_value"
    m_pie.aggregation = metri_pb2.SUM
    d_pie = q_pie.dimensions.add()
    d_pie.entity = "meter_reading"
    d_pie.attribute = "unit_of_measure"
    req.queries["pie_by_unit"].CopyFrom(q_pie)
    
    # Q3: AREA (TIMESERIES) - Consumo en el tiempo (Agrupado por día)
    q_area = metri_pb2.AnalyticsRequest()
    q_area.tenant_id = "golden-tenant"
    q_area.entity = "meter_reading"
    q_area.output_cast = metri_pb2.TIMESERIES
    m_area = q_area.metrics.add()
    m_area.entity = "meter_reading"
    m_area.attribute = "reading_value"
    m_area.aggregation = metri_pb2.SUM
    d_area = q_area.dimensions.add()
    d_area.entity = "meter_reading"
    d_area.attribute = "timestamp"
    d_area.interval = "DAY"
    
    # Configuramos el viz para renderizar como AREA
    q_area.viz = "AREA" # Custom hint opcional en adición a TIMESERIES
    
    req.queries["area_timeseries"].CopyFrom(q_area)
    
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
