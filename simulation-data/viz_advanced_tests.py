import json
import logging
import struct
import hashlib
import requests
import metri_pb2
import time
import os

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
    return MessageToDict(msg, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)

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
                if "batch_results" in res_dict:
                    for bk, bv in res_dict["batch_results"].items():
                        test_report["results"][bk] = bv
                else:
                    test_report["results"][f"chunk_{i}"] = res_dict
        else:
            test_report["error"] = "No response"
            
        report["tests"].append(test_report)

    # 1. KPI con comparación y timeframe
    kpi_cmp_tf = metri_pb2.AnalyticsRequest()
    kpi_cmp_tf.entity = "meter_reading"
    kpi_cmp_tf.output_cast = metri_pb2.KPI
    kpi_cmp_tf.viz = "indicator"
    m1 = kpi_cmp_tf.metrics.add()
    m1.attribute = "reading_value"
    m1.aggregation = metri_pb2.AVG
    cmp1 = kpi_cmp_tf.comparisons.add()
    cmp1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp1.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    kpi_cmp_tf.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    kpi_cmp_tf.time_frame.n_value = 30
    
    add_test_result("KPI Comp+TF Test", {"q_kpi_cmp_tf": kpi_cmp_tf}, "KPI con comparacion contra periodo anterior filtrado por LAST_N_DAYS = 30.")

    # 2. Line (Timeseries)
    line_query = metri_pb2.AnalyticsRequest()
    line_query.entity = "meter_reading"
    line_query.output_cast = metri_pb2.TIMESERIES
    line_query.viz = "line"
    m2 = line_query.metrics.add()
    m2.attribute = "reading_value"
    m2.aggregation = metri_pb2.MAX
    d2 = line_query.dimensions.add()
    d2.attribute = "timestamp"
    d2.interval = "week"
    
    add_test_result("Line Test", {"q_line": line_query}, "Visualizacion de Line chart para maximas lecturas semanales.")

    # 3. Pie (Breakdown)
    pie_query = metri_pb2.AnalyticsRequest()
    pie_query.entity = "meter_reading"
    pie_query.output_cast = metri_pb2.PIE
    pie_query.viz = "pie"
    m3 = pie_query.metrics.add()
    m3.attribute = "reading_value"
    m3.aggregation = metri_pb2.SUM
    d3 = pie_query.dimensions.add()
    d3.attribute = "unit_of_measure"
    
    add_test_result("Pie Test", {"q_pie": pie_query}, "Desglose (Pie) de suma de readings por unidad de medida.")

    # 4. Scatter (Dos metricas, 1 dimension)
    # Scatter requests in ECharts typically map multiple dimensions or metrics.
    # We will test two aggregations grouped by asset_id
    scatter_query = metri_pb2.AnalyticsRequest()
    scatter_query.entity = "meter_reading"
    scatter_query.output_cast = metri_pb2.TABLE # Scatter typically needs raw table data to map X and Y
    scatter_query.viz = "scatter"
    
    m4_1 = scatter_query.metrics.add()
    m4_1.attribute = "reading_value"
    m4_1.aggregation = metri_pb2.AVG
    m4_1.name = "avg_reading"
    
    m4_2 = scatter_query.metrics.add()
    m4_2.attribute = "reading_value"
    m4_2.aggregation = metri_pb2.MAX
    m4_2.name = "max_reading"
    
    d4 = scatter_query.dimensions.add()
    d4.attribute = "asset_id"
    
    add_test_result("Scatter Test", {"q_scatter": scatter_query}, "Scatter chart correlacionando avg_reading vs max_reading por asset.")

    # Escribir reporte
    artifact_path = "/Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/viz_advanced_results.json"
    os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
    with open(artifact_path, "w") as f:
        json.dump(report, f, indent=2)
    logging.info(f"Reporte generado en: {artifact_path}")

if __name__ == "__main__":
    run_tests()
