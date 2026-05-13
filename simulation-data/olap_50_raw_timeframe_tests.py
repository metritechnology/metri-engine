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
    logging.info("Construyendo Suite de 50 Tests OLAP RAW (Sin Aggregations)...")
    
    results_map = {}
    test_id = 1
    
    # 5 categorías, cada una con 10 tests = 50 tests
    
    # Categoria 1: TimeFrame LAST_N_DAYS
    for i in range(10):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Raw_Last_{i+1}_Days"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE, limit=2))
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        tf.n_value = i + 1
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1

    # Categoria 2: TimeFrame THIS_MONTH
    for i in range(10):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Raw_ThisMonth_Limit{i+1}"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE, limit=i+1))
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.THIS_MONTH
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1

    # Categoria 3: TimeFrame CUSTOM_RANGE
    for i in range(10):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Raw_CustomRange_Offset{i}"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE, limit=2))
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.CUSTOM_RANGE
        tf.start_ts = 1714275330000 - 86400000 * (i + 1)
        tf.end_ts = 1714275330000 + 86400000 * (i + 1)
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1

    # Categoria 4: Comparativa BENCHMARK
    for i in range(10):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Raw_Benchmark_{i}"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE, limit=2))
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        tf.n_value = 30
        tf.timezone = "America/Bogota"
        
        comp = req.queries[q_name].comparisons.add()
        comp.type = metri_pb2.AnalyticalComparison.BENCHMARK
        comp.label = f"Threshold {i * 10}"
        comp.benchmark_value = float(i * 10)
        
        send_query(req, q_name, results_map)
        test_id += 1

    # Categoria 5: Comparativa TIME_SHIFT_SHORTCUT
    for i in range(10):
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_Raw_TimeShift_{i}"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE, limit=2))
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.THIS_MONTH
        tf.timezone = "America/Bogota"
        
        comp = req.queries[q_name].comparisons.add()
        comp.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
        comp.label = "vs Same Period Last Year"
        comp.shortcut = metri_pb2.AnalyticalComparison.SAME_PERIOD_LAST_YEAR
        
        send_query(req, q_name, results_map)
        test_id += 1

    print(f"# Reporte de Suite RAW: 50 TimeFrames & Comparativas\n")
    print(f"Total de tests ejecutados: {len(results_map)}\n")
    success_count = sum(1 for v in results_map.values() if v.get("status", {}).get("success") == True)
    print(f"**Éxitos:** {success_count} / {test_id - 1}")
    print(f"**Fallos:** {test_id - 1 - success_count}\n")
    
    with open("olap_50_raw_results.json", "w") as f:
        json.dump(results_map, f, indent=2)
    print("Reporte JSON generado exitosamente en olap_50_raw_results.json")
    
if __name__ == "__main__":
    run_tests()
