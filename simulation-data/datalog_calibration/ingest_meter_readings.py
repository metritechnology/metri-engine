import time
import struct
import hashlib
import requests
import sys
import os
import random
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2

FUNCTION_URL = "https://engine.metri.one/"

def ingest_entities(tenant_id, entity_type, entities):
    if not entities:
        return True
    
    batch_size = 50
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i + batch_size]
        print(f"Ingestando lote de {len(batch)} {entity_type}s ({i+1} a {i+len(batch)}) en Datahike...")
        
        req = metri_pb2.BulkRequest()
        req.tenant_id = tenant_id
        req.entity_type = entity_type
        req.action = metri_pb2.CREATE
        
        keys = list(batch[0].keys())
        for k in keys:
            col = req.data.columns.add()
            col.key = k
            col.type = "string"
            
        data_row_list = metri_pb2.DataRowList()
        for ent in batch:
            row = data_row_list.iter.add()
            for k in keys:
                val = ent.get(k)
                v = row.values.add()
                if val is None:
                    v.null_value = 0
                elif isinstance(val, bool):
                    v.bool_value = val
                elif isinstance(val, (int, float)):
                    v.number_value = float(val)
                else:
                    v.string_value = str(val)
                    
        req.data.rows_json.CopyFrom(data_row_list)
        
        proto_bytes = req.SerializeToString()
        framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
        payload_hash = hashlib.sha256(framed_data).hexdigest()
        
        resp = requests.post(
            f"{FUNCTION_URL}metri.MetriService/BulkIngest", 
            data=framed_data, 
            headers={
                'Content-Type': 'application/grpc-web+proto',
                'x-amz-content-sha256': payload_hash
            },
            stream=True
        )
        
        if resp.status_code != 200:
            print(f"❌ Error HTTP {resp.status_code}: {resp.text}")
            return False
            
        response_bytes = resp.content
        offset = 0
        success = False
        while offset < len(response_bytes):
            flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
            offset += 5
            if flag == 0x00:
                chunk_bytes = response_bytes[offset:offset+length]
                resp_proto = metri_pb2.BulkResponse()
                resp_proto.ParseFromString(chunk_bytes)
                if not resp_proto.status.success:
                    print(f"❌ Error BulkIngest: {resp_proto.status.error_code} - {resp_proto.status.error_message}")
                    return False
                else:
                    success = True
            offset += length
    print(f"✅ Ingesta exitosa para {entity_type} (total {len(entities)}).")
    return True

def generate_readings(count=200):
    readings = []
    base_time = int(datetime.now().timestamp())
    for i in range(1, count + 1):
        ts = base_time - (i * 86400 / 2) # Every 12 hours back
        readings.append({
            "id": f"01KQBPBDXVXVBKSA00RD{i:04d}",
            "asset_id": "01KQBPBDXVXVBKSA00AST00004",
            "reading_value": float(100 + (i % 10)),
            "unit_of_measure": "kWh",
            "reading_type": "electricity",
            "timestamp": ts
        })
    return readings

if __name__ == "__main__":
    readings = generate_readings(200)
    ingest_entities("datalog-golden-tenant", "meter_reading", readings)
