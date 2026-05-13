import time
import struct
import logging
import requests
import hashlib
from datetime import datetime, timedelta

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
            'X-Metri-Origin-Token': 'datalog-golden-tenant',
            'x-amz-content-sha256': payload_hash
        },
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

import ulid

def create_dependency(entity_type, ulid_val, name_val):
    logging.info(f"Creando {entity_type} con id {ulid_val}...")
    req = metri_pb2.BulkRequest()
    req.tenant_id = "golden-tenant"
    req.entity_type = entity_type
    req.action = metri_pb2.UPSERT
    
    rs = req.data
    c1 = rs.columns.add()
    c1.key = "id"
    c2 = rs.columns.add()
    c2.key = "name"
    
    row = rs.rows_json.iter.add()
    v1 = row.values.add()
    v1.string_value = ulid_val
    v2 = row.values.add()
    v2.string_value = name_val
    
    res = invoke_grpc_web("metri.MetriService/BulkIngest", req)
    if res:
        resp = metri_pb2.BulkResponse()
        resp.ParseFromString(res)
        if not resp.status.success:
            logging.error(f"Fallo al crear {entity_type}: {resp.status.error_message}")
            return False
    return True

def ingest_data():
    part_ulid = str(ulid.new())
    loc_ulid = str(ulid.new())
    
    if not create_dependency("part", part_ulid, "Test Part Alpha"):
        return
    if not create_dependency("location", loc_ulid, "Test Loc Alpha"):
        return

    logging.info("Generando datos controlados OLTP para el escenario KPI...")
    req = metri_pb2.BulkRequest()
    req.tenant_id = "golden-tenant"
    req.entity_type = "inventory_movement"
    req.action = metri_pb2.UPSERT
    
    rs = req.data
    c1 = rs.columns.add()
    c1.key = "id"
    c2 = rs.columns.add()
    c2.key = "timestamp"
    c3 = rs.columns.add()
    c3.key = "quantity"
    c4 = rs.columns.add()
    c4.key = "type"
    c5 = rs.columns.add()
    c5.key = "part_id"
    c6 = rs.columns.add()
    c6.key = "location_id"
    
    now = datetime.utcnow()
    
    for i in range(1, 91):
        target_date = now - timedelta(days=i)
        row = rs.rows_json.iter.add()
        
        v_id = row.values.add()
        v_id.string_value = str(ulid.new())
        
        v_ts = row.values.add()
        v_ts.number_value = int(target_date.timestamp())  # epoch seconds (no ms)

        v_rv = row.values.add()
        if 1 <= i <= 30:
            v_rv.number_value = 150.0
        elif 31 <= i <= 60:
            v_rv.number_value = 100.0
        else:
            v_rv.number_value = 50.0
            
        v_typ = row.values.add()
        v_typ.string_value = "RECEIPT"
        
        v_pid = row.values.add()
        v_pid.string_value = part_ulid
        
        v_lid = row.values.add()
        v_lid.string_value = loc_ulid
        
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
