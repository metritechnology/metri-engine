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
    logging.info("Ejecutando pruebas sobre OLTP para validar gRPC Query con retornos de filas...")
    
    req = metri_pb2.QueryRequest()
    req.tenant_id = "datalog-golden-tenant"
    
    statuses = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
    
    # Vamos a generar 100 queries de filtrado de assets para asegurar datos
    for i in range(1, 101):
        q_name = f"oltp_query_{i}"
        analytics = metri_pb2.AnalyticsRequest(
            tenant_id="datalog-golden-tenant", 
            entity="asset", 
            output_cast=metri_pb2.TABLE, 
            limit=5
        )
        
        # Sin filtro adicional
        req.queries[q_name].CopyFrom(analytics)
        
        req.queries[q_name].CopyFrom(analytics)
        
    logging.info("Enviando batch de 100 consultas a gRPC Query (OLTP)...")
    
    # Partimos en batches de 20 para evitar Timeout de CloudFront 504
    keys = list(req.queries.keys())
    batch_size = 20
    all_results = {}
    
    for i in range(0, len(keys), batch_size):
        batch_keys = keys[i:i+batch_size]
        batch_req = metri_pb2.QueryRequest()
        batch_req.tenant_id = "datalog-golden-tenant"
        for k in batch_keys:
            batch_req.queries[k].CopyFrom(req.queries[k])
            
        logging.info(f"Enviando batch de {len(batch_keys)} consultas ({i+1} a {i+len(batch_keys)})...")
        messages = invoke_grpc_web("metri.MetriService/Query", batch_req)
        
        if messages:
            for res_bytes in messages:
                resp = metri_pb2.QueryResponse()
                resp.ParseFromString(res_bytes)
                json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
                if "batch_results" in json_dict:
                    for k, v in json_dict["batch_results"].items():
                        all_results[k] = v

    with open("oltp_grpc_report.json", "w") as f:
        json.dump(all_results, f, indent=2)
    
    logging.info("Reporte JSON con resultados reales generado en oltp_grpc_report.json")

if __name__ == "__main__":
    run_tests()
