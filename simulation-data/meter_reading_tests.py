import json
import logging
import struct
import hashlib
import requests
import metri_pb2
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"

def invoke_grpc_query(req: metri_pb2.QueryRequest):
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    start_time = time.time()
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        }
    )
    duration_ms = int((time.time() - start_time) * 1000)
    
    if resp.status_code != 200:
        logging.error(f"Error HTTP {resp.status_code}: {resp.text}")
        return None, duration_ms

    response_bytes = resp.content
    results = []
    
    offset = 0
    while offset < len(response_bytes):
        if offset + 5 > len(response_bytes):
            break
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        if flag == 0x00:
            chunk_data = response_bytes[offset:offset+length]
            qr = metri_pb2.QueryResponse()
            qr.ParseFromString(chunk_data)
            results.append(qr)
        offset += length
        
    return results, duration_ms

def message_to_dict(msg):
    from google.protobuf.json_format import MessageToDict
    return MessageToDict(msg, preserving_proto_field_name=True)

def run_tests():
    report = {"tests": []}
    
    def add_test_result(name, queries, description):
        logging.info(f"Corriendo prueba: {name}")
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        
        for k, v in queries.items():
            v.tenant_id = "golden-tenant"
            req.queries[k].CopyFrom(v)
            
        results, duration = invoke_grpc_query(req)
        
        test_report = {
            "name": name,
            "description": description,
            "duration_ms": duration,
            "results": {}
        }
        
        if results:
            for i, res in enumerate(results):
                res_dict = message_to_dict(res)
                # Keep only relevant info to avoid huge json
                if "batch_results" in res_dict:
                    for bk, bv in res_dict["batch_results"].items():
                        test_report["results"][bk] = bv
                else:
                    test_report["results"][f"chunk_{i}"] = res_dict
        else:
            test_report["error"] = "No response"
            
        report["tests"].append(test_report)

    # 1. KPI Query (Total Reading Value)
    kpi_query = metri_pb2.AnalyticsRequest()
    kpi_query.entity = "meter_reading"
    kpi_query.output_cast = metri_pb2.KPI
    kpi_query.viz = "kpi"
    m1 = kpi_query.metrics.add()
    m1.attribute = "reading_value"
    m1.aggregation = metri_pb2.SUM
    m1.name = "total_reading"
    
    add_test_result("KPI Test", {"q_kpi": kpi_query}, "KPI simple calculando el SUM de reading_value.")

    # 2. PIE Query (Reading by Unit)
    pie_query = metri_pb2.AnalyticsRequest()
    pie_query.entity = "meter_reading"
    pie_query.output_cast = metri_pb2.PIE
    pie_query.viz = "pie"
    m2 = pie_query.metrics.add()
    m2.attribute = "reading_value"
    m2.aggregation = metri_pb2.SUM
    d1 = pie_query.dimensions.add()
    d1.attribute = "unit_of_measure"
    
    add_test_result("PIE Test", {"q_pie": pie_query}, "Desglose PIE de reading_value por unit_of_measure.")

    # 3. TIMESERIES (Line/Area) Query
    ts_query = metri_pb2.AnalyticsRequest()
    ts_query.entity = "meter_reading"
    ts_query.output_cast = metri_pb2.TIMESERIES
    ts_query.viz = "area"
    m3 = ts_query.metrics.add()
    m3.attribute = "reading_value"
    m3.aggregation = metri_pb2.AVG
    d2 = ts_query.dimensions.add()
    d2.attribute = "timestamp"
    d2.interval = "day"
    
    add_test_result("TIMESERIES Area Test", {"q_ts": ts_query}, "Serie de tiempo AREA del AVG de reading_value agrupado por día.")

    # 4. KPI Comparison
    cmp_query = metri_pb2.AnalyticsRequest()
    cmp_query.entity = "meter_reading"
    cmp_query.output_cast = metri_pb2.KPI
    cmp_query.viz = "indicator"
    m4 = cmp_query.metrics.add()
    m4.attribute = "reading_value"
    m4.aggregation = metri_pb2.AVG
    cmp1 = cmp_query.comparisons.add()
    cmp1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp1.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    
    add_test_result("KPI Comparison", {"q_cmp": cmp_query}, "KPI con comparación contra el periodo anterior.")

    # 5. Timeframe Query
    tf_query = metri_pb2.AnalyticsRequest()
    tf_query.entity = "meter_reading"
    tf_query.output_cast = metri_pb2.TABLE
    tf_query.viz = "table"
    tf_query.limit = 10
    tf_query.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    tf_query.time_frame.n_value = 30
    
    add_test_result("Timeframe Test", {"q_tf": tf_query}, "Tabla filtrada por los últimos 30 días con límite 10.")


    # Escribir reporte
    with open("/Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/olap_viz_test_report.json", "w") as f:
        json.dump(report, f, indent=2)
    logging.info("Reporte generado en: /Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/olap_viz_test_report.json")


if __name__ == "__main__":
    run_tests()
