import grpc
import sys
import struct
import argparse
import requests
import time
import random

import metri_pb2
import metri_pb2_grpc

def send_grpc_web(url, proto_req, method_name, retries=5):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    
    import base64
    b64_framed_data = base64.b64encode(framed_data)
    
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{url}{method_name}",
                data=b64_framed_data,
                headers={
                    'Content-Type': 'application/grpc-web-text'
                },
                timeout=45 # Wait up to 45s
            )
            
            if resp.status_code in [429, 502, 503, 504]:
                print(f"  [Attempt {attempt+1}] Status {resp.status_code} (Lambda warm-up/Rate Limit). Retrying in 20s...")
                time.sleep(20)
                continue
                
            if resp.status_code != 200:
                print(f"Error {resp.status_code}: {resp.text[:100]}")
                return None
                
            grpc_status = resp.headers.get('grpc-status', '0')
            if grpc_status != '0':
                print(f"gRPC Status {grpc_status}: {resp.headers.get('grpc-message')}")
            
            response_data = resp.content
            if len(response_data) < 5:
                print("Invalid response length")
                return None
                
            compressed_flag, length = struct.unpack('!BI', response_data[:5])
            return response_data[5:5+length]
            
        except requests.exceptions.Timeout:
            print(f"  [Attempt {attempt+1}] Timeout local. Retrying in 20s...")
            time.sleep(20)
            
    return None

def main():
    parser = argparse.ArgumentParser(description="Mass Simulation OLTP/OLAP")
    parser.add_argument("--mode", choices=["local", "prod"], default="local")
    args = parser.parse_args()
    
    stub = None
    if args.mode == "local":
        channel = grpc.insecure_channel('localhost:9090')
        stub = metri_pb2_grpc.MetriServiceStub(channel)
        print("Conectando a localhost:9090 (Netty)")
    else:
        url = "https://engine.metri.one/"
        print(f"Conectando a {url} (API Gateway / Function URL)")

    tenant_id = "test-tenant-1"
    
    print("\n[1] Creando 5 Assets (OLTP)...")
    asset_ids = []
    
    for i in range(5):
        req = metri_pb2.TransactionRequest()
        req.tenant_id = tenant_id
        req.entity_type = "asset"
        req.action = metri_pb2.OperationAction.CREATE
        req.payload.update({
            "name": f"Sensor Asset {i}",
            "status": "ACTIVE"
        })
        
        try:
            if args.mode == "local":
                resp = stub.Transact(req)
                asset_ids.append(resp.entity_id)
            else:
                resp_bytes = send_grpc_web(url, req, "metri.MetriService/Transact")
                if resp_bytes:
                    resp = metri_pb2.TransactionResponse.FromString(resp_bytes)
                    if resp.status.success:
                        asset_ids.append(resp.entity_id)
                    else:
                        print(f"Asset {i} falló: {resp.status.error_code} - {resp.status.error_message}")
        except Exception as e:
            print(f"Excepción en Asset {i}:", e)
            
    print(f"✅ Se crearon {len(asset_ids)} assets exitosamente.")
    
    if not asset_ids:
        print("No se pudieron crear assets. Abortando simulación OLAP.")
        return

    # --- 100 METER READINGS (OLAP) ---
    print("\n[2] Enviando 100 Meter Readings (OLAP) via BulkIngest...")
    bulk_req_mr = metri_pb2.BulkRequest()
    bulk_req_mr.tenant_id = tenant_id
    bulk_req_mr.entity_type = "meter_reading"
    bulk_req_mr.action = metri_pb2.OperationAction.CREATE

    col_keys_mr = ["asset_id", "reading_value", "unit_of_measure", "timestamp_epoch"]
    for key in col_keys_mr:
        col = bulk_req_mr.data.columns.add()
        col.key = key

    for _ in range(100):
        asset_id = random.choice(asset_ids)
        data_row = bulk_req_mr.data.rows_json.iter.add()
        for val_str in [str(asset_id), str(random.uniform(10.0, 50.0)), "CEL", str(int(time.time() * 1000))]:
            v = data_row.values.add()
            v.string_value = val_str

    try:
        if args.mode == "local":
            resp_mr = stub.BulkIngest(bulk_req_mr)
            print(f"✅ Meter Reading local completado. Insertados: {resp_mr.ingested_count}")
        else:
            resp_bytes = send_grpc_web(url, bulk_req_mr, "metri.MetriService/BulkIngest")
            if resp_bytes:
                resp_mr = metri_pb2.BulkResponse.FromString(resp_bytes)
                if resp_mr.status.success:
                    print(f"✅ Meter Reading prod completado. Insertados: {resp_mr.ingested_count}")
                else:
                    print(f"Meter Reading falló: {resp_mr.status.error_code}")
    except Exception as e:
        print("Excepción en BulkIngest MR:", e)

    # --- 100 AUDIT LOGS (OLAP) ---
    print("\n[3] Enviando 100 Audit Logs (OLAP) via BulkIngest...")
    bulk_req_al = metri_pb2.BulkRequest()
    bulk_req_al.tenant_id = tenant_id
    bulk_req_al.entity_type = "audit_log"
    bulk_req_al.action = metri_pb2.OperationAction.CREATE

    col_keys_al = ["user_id", "action_type", "resource_domain", "resource_id", "client_ip", "execution_time_ms"]
    for key in col_keys_al:
        col = bulk_req_al.data.columns.add()
        col.key = key

    for _ in range(100):
        data_row = bulk_req_al.data.rows_json.iter.add()
        for val_str in [
            "usr-test-999", 
            random.choice(["READ", "WRITE", "DELETE", "ACCESS_DENIED"]), 
            "asset", 
            str(random.choice(asset_ids)), 
            "192.168.1.100", 
            str(random.randint(10, 500))
        ]:
            v = data_row.values.add()
            v.string_value = val_str

    try:
        if args.mode == "local":
            resp_al = stub.BulkIngest(bulk_req_al)
            print(f"✅ Audit Log local completado. Insertados: {resp_al.ingested_count}")
        else:
            resp_bytes = send_grpc_web(url, bulk_req_al, "metri.MetriService/BulkIngest")
            if resp_bytes:
                resp_al = metri_pb2.BulkResponse.FromString(resp_bytes)
                if resp_al.status.success:
                    print(f"✅ Audit Log prod completado. Insertados: {resp_al.ingested_count}")
                else:
                    print(f"Audit Log falló: {resp_al.status.error_code}")
    except Exception as e:
        print("Excepción en BulkIngest AL:", e)

if __name__ == '__main__':
    main()
