import time
import struct
import logging
import requests
import hashlib
import json
import google.protobuf.json_format as json_format

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

def send_query(req, q_name, results_map):
    res_messages = invoke_grpc_web("metri.MetriService/Query", req)
    if res_messages:
        for res_bytes in res_messages:
            resp = metri_pb2.QueryResponse()
            resp.ParseFromString(res_bytes)
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results_map[k] = v

def run_tests():
    logging.info("Construyendo Suite de 50 Tests OLAP (Batching)...")
    
    aggregations = [
        ("COUNT", metri_pb2.COUNT), ("SUM", metri_pb2.SUM), ("AVG", metri_pb2.AVG),
        ("MIN", metri_pb2.MIN), ("MAX", metri_pb2.MAX), ("MEDIAN", metri_pb2.MEDIAN),
        ("STD_DEV", metri_pb2.STD_DEV), ("VARIANCE", metri_pb2.VARIANCE),
        ("PERCENTILE_90", metri_pb2.PERCENTILE_90), ("PERCENTILE_95", metri_pb2.PERCENTILE_95),
        ("PERCENTILE_99", metri_pb2.PERCENTILE_99)
    ]
    bivariate = [("CORRELATION", metri_pb2.CORRELATION), ("LINEAR_REGRESSION", metri_pb2.LINEAR_REGRESSION)]
    
    results_map = {}
    test_id = 1
    
    # 1. Pruebas Globales
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Global"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        send_query(req, q_name, results_map)
        test_id += 1

    # 2. Bivariadas
    for name, agg_enum in bivariate:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Bivariate"
        req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.secondary_attribute = "timestamp"
        m.aggregation = agg_enum
        send_query(req, q_name, results_map)
        test_id += 1
        
    # 3. Filtradas
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Filtered"
        req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        f = req.queries[q_name].filters.add()
        f.criteria.field = "reading_value"
        f.criteria.op_ref = metri_pb2.GT
        f.criteria.value.number_val = 80.0
        send_query(req, q_name, results_map)
        test_id += 1
        
    # 4. Agrupadas
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Grouped"
        req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        d = req.queries[q_name].dimensions.add()
        d.attribute = "unit_of_measure"
        send_query(req, q_name, results_map)
        test_id += 1
        
    # 5. Múltiples métricas
    for i in range(15):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Combined_MultiMetric"
        req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        req.queries[q_name].metrics.add(attribute="reading_value", aggregation=metri_pb2.COUNT, name="conteo")
        req.queries[q_name].metrics.add(attribute="reading_value", aggregation=metri_pb2.AVG, name="promedio")
        req.queries[q_name].metrics.add(attribute="reading_value", aggregation=metri_pb2.PERCENTILE_99, name="p99")
        req.queries[q_name].metrics.add(attribute="reading_value", secondary_attribute="timestamp", aggregation=metri_pb2.CORRELATION, name="corr")
        f = req.queries[q_name].filters.add()
        f.criteria.field = "unit_of_measure"
        f.criteria.op_ref = metri_pb2.EQ
        f.criteria.value.string_val = "kWh"
        send_query(req, q_name, results_map)
        test_id += 1

    print(f"# Reporte de Suite de Pruebas: 50 Agregaciones OLAP\n")
    print(f"Total de tests ejecutados: {len(results_map)}\n")
    success_count = sum(1 for v in results_map.values() if v.get("status", {}).get("success") == True)
    print(f"**Éxitos:** {success_count} / {test_id - 1}")
    print(f"**Fallos:** {test_id - 1 - success_count}\n")
    
    print("## Detalles por Test")
    for k in sorted(results_map.keys()):
        status = "✅ OK" if results_map[k].get("status", {}).get("success") else "❌ ERROR"
        error = "" if status == "✅ OK" else str(results_map[k].get("status"))
        
        rows = []
        if status == "✅ OK" and "data" in results_map[k] and "rows_json" in results_map[k]["data"]:
            rows = results_map[k]["data"]["rows_json"].get("iter", [])
        
        print(f"- **{k}**: {status} {error}")
        if len(rows) > 0:
            print(f"  - Columnas: {[c.get('name') for c in results_map[k]['data']['columns']]}")
            print(f"  - Muestra (1 fila): {rows[0]['values']}")
    
    with open("olap_50_tests_results.json", "w") as f:
        json.dump(results_map, f, indent=2)
    print("Reporte JSON generado exitosamente en olap_50_tests_results.json")
    
if __name__ == "__main__":
    run_tests()
