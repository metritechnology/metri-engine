import time
import struct
import logging
import requests
import hashlib
from datetime import datetime, timedelta
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
    logging.info("Generando datos controlados para el escenario KPI...")
    
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
    
    # Let's generate exactly 90 records:
    # 30 records for Far Past (Day -90 to Day -61): avg 50
    # 30 records for Previous Period (Day -60 to Day -31): avg 100
    # 30 records for Current Period (Day -30 to Day -1): avg 150
    
    now = datetime.utcnow()
    
    for i in range(1, 91):
        target_date = now - timedelta(days=i)
        
        row = rs.rows_json.iter.add()
        
        # id
        v_id = row.values.add()
        v_id.string_value = f"mr-comp-alpha-v2-{i}"
        
        # timestamp
        v_ts = row.values.add()
        v_ts.number_value = int(target_date.timestamp())  # epoch seconds (no ms)
        
        # reading_value
        v_rv = row.values.add()
        if 1 <= i <= 30:
            v_rv.number_value = 150.0
        elif 31 <= i <= 60:
            v_rv.number_value = 100.0
        else:
            v_rv.number_value = 50.0
            
        # unit_of_measure
        v_um = row.values.add()
        v_um.string_value = "CEL"
        
        # asset_id
        v_mid = row.values.add()
        v_mid.string_value = "COMP-ALPHA-01"
        
    logging.info(f"Enviando {len(rs.rows_json.iter)} registros a Metri Engine...")
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
