import sys
import json
import logging
import google.protobuf.json_format as json_format
import grpc
import os
import requests
import struct

sys.path.append('.')
sys.path.append('src/metri/grpc')

import metri_pb2
import metri_pb2_grpc

logging.basicConfig(level=logging.INFO)

def run_test():
    is_prod = os.environ.get("ENVIRONMENT") == "production"
    
    if is_prod:
        print("🌍 Conectando a Producción (engine.metri.one:443 via gRPC-Web)...")
        channel = None
        stub = None
    else:
        print("🖥️  Conectando a Local (localhost:9090 via grpcio)...")
        channel = grpc.insecure_channel('localhost:9090')
        stub = metri_pb2_grpc.MetriServiceStub(channel)
        
    req = metri_pb2.QueryRequest()
    # Using 'golden-tenant' which is commonly used in meter_reading ingestion scripts
    req.tenant_id = "golden-tenant"
    
    q = metri_pb2.AnalyticsRequest(
        tenant_id="golden-tenant", 
        entity="meter_reading", 
        output_cast=metri_pb2.KPI
    )
    
    # METRIC: SUM of reading_value
    m = q.metrics.add()
    m.entity = "meter_reading"
    m.attribute = "reading_value"
    m.aggregation = metri_pb2.SUM
    
    # TIMEFRAME: LAST_N_MONTHS (1 month)
    q.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q.time_frame.n_value = 1
    q.time_frame.timezone = "America/Bogota"
    
    # COMPARISON: PREVIOUS_PERIOD
    c = q.comparisons.add()
    c.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    c.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    
    # VIZ SETTING
    q.viz = "indicator"
    
    req.queries["olap_kpi_comparison"].CopyFrom(q)
    
    logging.info("Enviando consulta KPI OLAP...")
    results = {}
    try:
        if is_prod:
            proto_bytes = req.SerializeToString()
            frame = struct.pack('!B', 0) + struct.pack('!I', len(proto_bytes)) + proto_bytes
            headers = {
                "Content-Type": "application/grpc-web+proto",
                "Accept": "application/grpc-web+proto",
                "X-Grpc-Web": "1"
            }
            r = requests.post("https://engine.metri.one/metri.MetriService/Query", data=frame, headers=headers)
            if r.status_code == 200:
                resp_bytes = r.content
                if len(resp_bytes) > 5:
                    flag, length = struct.unpack('!BI', resp_bytes[0:5])
                    data = resp_bytes[5:5+length]
                    resp = metri_pb2.QueryResponse()
                    resp.ParseFromString(data)
                    json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
                    if "batch_results" in json_dict:
                        for k, v in json_dict["batch_results"].items():
                            results[k] = v
            else:
                logging.error(f"HTTP Error: {r.status_code}")
                return
        else:
            responses = stub.Query(req)
            for resp in responses:
                json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
                if "batch_results" in json_dict:
                    for k, v in json_dict["batch_results"].items():
                        results[k] = v
    except Exception as e:
        logging.error(f"Error: {e}")
        return
    
    output_file = "olap_kpi_viz_output.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Reporte guardado en {output_file}")

if __name__ == "__main__":
    run_test()
