import json
import logging
import struct
import boto3
import hashlib
import requests
import sys

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Configuraciones base
FUNCTION_URL = "https://engine.metri.one/"

def make_query_request():
    req = metri_pb2.QueryRequest()
    req.tenant_id = "test-tenant-1"
    
    # Enable explain_plan flag
    req.explain_plan = True
    
    # Query 1
    metric = metri_pb2.MetricDefinition()
    metric.entity = "meter_reading"
    metric.attribute = "value"
    metric.aggregation = metri_pb2.SUM
    
    analytics_req = metri_pb2.AnalyticsRequest()
    analytics_req.entity = "meter_reading"
    analytics_req.metrics.append(metric)
    
    req.queries["q1"].CopyFrom(analytics_req)
    
    return req

def invoke_query(req: metri_pb2.QueryRequest):
    logging.info("--- Petición gRPC-Web (Explain Plan) a Function URL ---")

    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    try:
        resp = requests.post(
            f"{FUNCTION_URL}metri.MetriService/Query", 
            data=framed_data, 
            headers={
                'Content-Type': 'application/grpc-web+proto',
                'x-amz-content-sha256': payload_hash
            },
            stream=True
        )
        
        if resp.status_code != 200:
            logging.error(f"❌ FAULT: Status {resp.status_code}")
            logging.error(resp.text)
            return

        response_bytes = resp.content
        if len(response_bytes) < 5:
            logging.error("Respuesta demasiado corta")
            return
            
        offset = 0
        while offset < len(response_bytes):
            flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
            offset += 5
            
            if flag == 0x00: # Data frame
                chunk_bytes = response_bytes[offset:offset+length]
                proto_resp = metri_pb2.QueryResponse()
                proto_resp.ParseFromString(chunk_bytes)
                
                if "q1" in proto_resp.batch_results:
                    q1_res = proto_resp.batch_results["q1"]
                    if q1_res.status.success == True:
                        logging.info("✅ AST IR recibido correctamente:")
                        # AST is serialized in the first row, in a string column 'ast_ir'
                        rows = q1_res.data.rows_json.iter
                        cols = [c.key for c in q1_res.data.columns]
                        logging.info(f"Cols: {cols}")
                        logging.info(f"Rows count: {len(rows)}")
                        if len(rows) > 0 and "ast_ir" in cols:
                            ast_idx = cols.index("ast_ir")
                            ast_val = rows[0].values[ast_idx]
                            if ast_val and ast_val.HasField("string_value"):
                                print(ast_val.string_value)
                            else:
                                logging.warning("No se encontró 'ast_ir' en los valores")
                        else:
                            logging.warning("No se encontró 'ast_ir' en las columnas o no hay filas")
                    else:
                        logging.info(f"Received status success: {q1_res.status.success}")
                        logging.info(f"Metadata: {q1_res.metadata}")
                        rows = q1_res.data.rows_json.iter
                        logging.info(f"Data rows length: {len(rows)}")
                        if len(rows) > 0:
                            print("Valores de la primera fila:")
                            for k, v in rows[0].values.items():
                                print(f"  {k}: {v.string_value}")
            elif flag == 0x80: # Trailing headers
                pass
            
            offset += length

    except Exception as e:
        import traceback
        logging.error(f"Error de red: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    req = make_query_request()
    invoke_query(req)
