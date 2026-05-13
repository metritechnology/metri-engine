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
    logging.info("Ejecutando 50 pruebas VIZ KPI sobre OLTP y OLAP...")
    
    results = {}
    
    # Send in batches of 10 to avoid CloudFront 504 Timeout
    for batch_idx in range(5):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "datalog-golden-tenant"
        
        start_idx = batch_idx * 10 + 1
        end_idx = start_idx + 10
        
        for i in range(start_idx, end_idx):
            if i <= 25:
                # OLTP
                q = metri_pb2.AnalyticsRequest(
                    tenant_id="datalog-golden-tenant", 
                    entity="work_order", 
                    output_cast=metri_pb2.KPI
                )
                m = q.metrics.add()
                m.entity = "work_order"
                m.attribute = "total_cost"
                m.aggregation = metri_pb2.SUM if i % 2 == 0 else metri_pb2.AVG

                if i % 3 == 0:
                    q.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
                    q.time_frame.n_value = i * 10
                    q.time_frame.timezone = "America/Bogota"
                
                if i % 4 == 0:
                    comp = q.comparisons.add()
                    comp.relative_granularity = "day"
                    comp.relative_amount = -i * 10
                
                if i % 5 == 0:
                    q.output_cast = metri_pb2.PIE
                    d = q.dimensions.add()
                    d.entity = "work_order"
                    d.attribute = "status"

                req.queries[f"test_oltp_{i}"].CopyFrom(q)
            else:
                # OLAP
                q = metri_pb2.AnalyticsRequest(
                    tenant_id="datalog-golden-tenant", 
                    entity="meter_reading", 
                    output_cast=metri_pb2.KPI
                )
                m = q.metrics.add()
                m.entity = "meter_reading"
                m.attribute = "reading_value"
                m.aggregation = metri_pb2.SUM if i % 2 == 0 else metri_pb2.AVG

                if i % 3 == 0:
                    q.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
                    q.time_frame.n_value = (i-25) * 10
                    q.time_frame.timezone = "America/Bogota"
                
                if i % 4 == 0:
                    comp = q.comparisons.add()
                    comp.relative_granularity = "day"
                    comp.relative_amount = -(i-25) * 10
                
                if i % 5 == 0:
                    q.output_cast = metri_pb2.PIE
                    d = q.dimensions.add()
                    d.entity = "meter_reading"
                    d.attribute = "unit_of_measure"

                req.queries[f"test_olap_{i}"].CopyFrom(q)

        logging.info(f"Enviando Batch {batch_idx+1}/5 con {len(req.queries)} consultas gRPC...")
        messages = invoke_grpc_web("metri.MetriService/Query", req)
        
        if messages:
            for res_bytes in messages:
                resp = metri_pb2.QueryResponse()
                resp.ParseFromString(res_bytes)
                json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
                if "batch_results" in json_dict:
                    for k, v in json_dict["batch_results"].items():
                        results[k] = v

    with open("viz_kpi_50_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Recibidos {len(results)} resultados.")
    logging.info("Reporte JSON guardado en viz_kpi_50_results.json")

if __name__ == "__main__":
    run_tests()
