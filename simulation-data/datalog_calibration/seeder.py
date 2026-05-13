import json
import uuid
import random
import sys
import os
from datetime import datetime, timedelta
import grpc

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src/metri/grpc"))

import metri_pb2
import metri_pb2_grpc

def get_deterministic_ulid(prefix, index):
    # Generates a deterministic 26-character ULID string
    hex_str = f"{prefix.upper()}{index:05d}".rjust(10, '0')
    return f"01KQBPBDXVXVBKSA{hex_str}"

def generate_locations(count=50):
    locations = []
    types = ["SITE", "BUILDING", "FLOOR", "ROOM", "ZONE"]
    for i in range(1, count + 1):
        loc = {
            "id": get_deterministic_ulid("loc", i),
            "name": f"Location {i}",
            "tag": f"L-000{i:03d}",
            "type": types[i % len(types)],
            "area_value": 100.0 + (i * 10.5),
            "area_unit": "m2"
        }
        # Every 5th location is a child of the 1st
        if i > 5 and i % 5 == 0:
            loc["parent_location_id"] = get_deterministic_ulid("loc", 1)
        locations.append(loc)
    return locations

def generate_assets(locations, count=1000):
    assets = []
    statuses = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
    for i in range(1, count + 1):
        # Deterministic but pseudo-random distribution
        loc = locations[i % len(locations)]
        status = statuses[i % len(statuses)]
        asset = {
            "id": get_deterministic_ulid("ast", i),
            "name": f"Asset {i}",
            "serial_number": f"SN-{10000+i}",
            "tag": f"A-000{i:04d}",
            "status": status,
            "location_id": loc["id"],
            "omniclass_category": f"21-0{i%9}"
        }
        assets.append(asset)
    return assets

def generate_work_orders(assets, count=5000):
    work_orders = []
    statuses = ["OPEN", "IN_PROGRESS", "CLOSED", "CANCELLED"]
    priorities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    
    base_time = int(datetime(2026, 1, 1).timestamp())
    
    for i in range(1, count + 1):
        asset = assets[i % len(assets)]
        status = statuses[i % len(statuses)]
        priority = priorities[i % len(priorities)]
        
        # Distribute due_date across the year (roughly 14 per day)
        due_date = base_time + ((i * 86400) // 14)
        
        wo = {
            "id": get_deterministic_ulid("wrk", i),
            "work_order_number": f"WO-{20000+i}",
            "title": f"Mantenimiento preventivo {i}",
            "description": f"Revisión de rutina para {asset['name']}",
            "asset_id": asset["id"],
            "location_id": asset["location_id"],
            "status": status,
            "priority": priority,
            "due_date": due_date,
            "total_cost": float(50.0 + (i % 500) * 1.5),
            "completion_percentage": float((i * 10) % 100) if status != "CLOSED" else 100.0
        }
        work_orders.append(wo)
    return work_orders

def ingest_entities(tenant_id, entity_type, entities):
    if not entities:
        return True
    
    import os
    import requests
    import struct
    
    is_prod = os.environ.get("ENVIRONMENT") == "production"
    
    if is_prod:
        print("🌍 Conectando a Producción (engine.metri.one:443 via gRPC-Web)...")
        channel = None
        stub = None
    else:
        channel = grpc.insecure_channel('localhost:9090')
        stub = metri_pb2_grpc.MetriServiceStub(channel)
        
    batch_size = 10
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i + batch_size]
        print(f"Ingestando lote de {len(batch)} {entity_type}s ({i+1} a {i+len(batch)}) en Datahike...")
        
        req = metri_pb2.BulkRequest()
        req.tenant_id = tenant_id
        req.entity_type = entity_type
        req.action = metri_pb2.CREATE
        
        for ent in batch:
            ent["tenant_id"] = tenant_id

        keys = list(batch[0].keys())
        
        # Define columns
        for k in keys:
            col = req.data.columns.add()
            col.key = k
            col.type = "string" # generic for ingestion
            
        data_row_list = metri_pb2.DataRowList()
        
        for ent in batch:
            row = data_row_list.iter.add()
            for k in keys:
                val = ent.get(k)
                # map to google.protobuf.Value
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
        
        try:
            if is_prod:
                proto_bytes = req.SerializeToString()
                frame = struct.pack('!B', 0) + struct.pack('!I', len(proto_bytes)) + proto_bytes
                headers = {
                    "Content-Type": "application/grpc-web+proto",
                    "Accept": "application/grpc-web+proto",
                    "X-Grpc-Web": "1"
                }
                print("Enviando request a Producción...")
                r = requests.post("https://engine.metri.one/metri.MetriService/BulkIngest", data=frame, headers=headers)
                
                if r.status_code == 200:
                    resp_bytes = r.content
                    if len(resp_bytes) > 5:
                        flag, length = struct.unpack('!BI', resp_bytes[0:5])
                        data = resp_bytes[5:5+length]
                        resp = metri_pb2.BulkResponse()
                        resp.ParseFromString(data)
                        if not resp.status.success:
                            print(f"❌ Error BulkIngest: {resp.status.error_code} - {resp.status.error_message}")
                            return False
                    else:
                        print("❌ Respuesta vacía o inválida")
                        return False
                else:
                    print(f"❌ HTTP Error: {r.status_code} - {r.text}")
                    return False
            else:
                print("Enviando request a Local...")
                resp = stub.BulkIngest(req)
                if not resp.status.success:
                    print(f"❌ Error BulkIngest: {resp.status.error_code} - {resp.status.error_message}")
                    return False
                    
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
            
    print(f"✅ Ingesta exitosa para {entity_type} (total {len(entities)}).")
    return True

def main():
    print("Generando Golden Set determinista...")
    locations = generate_locations(50)
    assets = generate_assets(locations, 1000)
    work_orders = generate_work_orders(assets, 200)
    
    tenant = "datalog-golden-tenant"
    wo_success = ingest_entities(tenant, "work_order", work_orders)
    
    if wo_success:
        print("\n🏆 FASE 1 COMPLETADA: Datos inyectados a Aegis/Datahike con éxito.")

if __name__ == "__main__":
    main()
