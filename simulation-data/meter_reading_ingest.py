import time
import struct
import logging
import requests
import hashlib
from datetime import datetime, timedelta
import random
import json

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
        }
    )
    
    if resp.status_code != 200:
        logging.error(f"Error HTTP {resp.status_code}: {resp.text}")
        return None

    response_bytes = resp.content
    if len(response_bytes) < 5:
        return None
        
    flag, length = struct.unpack('!BI', response_bytes[:5])
    if flag == 0x00:
        return response_bytes[5:5+length]
    return None

def ingest_data():
    logging.info("Generando 1000 eventos de meter_reading...")
    
    req = metri_pb2.BulkRequest()
    req.tenant_id = "golden-tenant"
    req.entity_type = "meter_reading"
    req.action = metri_pb2.UPSERT
    
    rs = req.data
    
    c1 = rs.columns.add()
    c1.key = "id"
    c2 = rs.columns.add()
    c2.key = "timestamp"
    c3 = rs.columns.add()
    c3.key = "reading_value"
    c4 = rs.columns.add()
    c4.key = "unit_of_measure"
    c5 = rs.columns.add()
    c5.key = "asset_id"
    c6 = rs.columns.add()
    c6.key = "metadata"
    
    base_time = int(datetime.now().timestamp())
    
    # Generate 1000 records spread over the last 60 days
    # 60 days = 5184000 seconds
    for i in range(1000):
        row = rs.rows_json.iter.add()
        
        # id
        v_id = row.values.add()
        v_id.string_value = f"mr-{base_time}-{i}"
        
        # timestamp: randomly distributed across the last 60 days
        random_offset_seconds = random.randint(0, 60 * 24 * 3600)
        
        v_ts = row.values.add()
        v_ts.number_value = base_time - random_offset_seconds
        
        # reading_value
        v_rv = row.values.add()
        v_rv.number_value = random.uniform(10.0, 100.0)
        
        # unit_of_measure
        v_um = row.values.add()
        v_um.string_value = random.choice(["KWH", "CEL", "LTR"])
        
        # asset_id
        v_mid = row.values.add()
        v_mid.string_value = f"asset-{random.randint(1, 10)}"
        
        # metadata
        v_meta = row.values.add()
        v_meta.string_value = json.dumps({"sensor_type": random.choice(["A", "B", "C"]), "firmware": "v1.2"})
        
    logging.info("Enviando BulkRequest a Metri Engine...")
    res_bytes = invoke_grpc_web("metri.MetriService/BulkIngest", req)
    
    if res_bytes:
        resp = metri_pb2.BulkResponse()
        resp.ParseFromString(res_bytes)
        if resp.status.success:
            logging.info(f"Ingesta exitosa! Entidades procesadas: {resp.ingested_count}")
        else:
            logging.error(f"Fallo en la ingesta: {resp.status.error_code} - {resp.status.error_message}")
    else:
        logging.error("No se recibió respuesta válida.")

if __name__ == "__main__":
    ingest_data()
