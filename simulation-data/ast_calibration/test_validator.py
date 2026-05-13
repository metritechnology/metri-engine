import os
import sys
import struct
import hashlib
import requests
import json

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2

FUNCTION_URL = "https://engine.metri.one/"

def test_invalid_request():
    print("Enviando un request sin tenant_id para probar la Defensa Inquebrantable FASE 10...")
    
    # Para que pase el validate_tenant! pero falle en Malli, pongamos un campo con algo inválido.
    # Por ejemplo, en protobuf los enums pueden tener valor 0 (UNSPECIFIED).
    # Malli `[:enum :COUNT :SUM ...]` no tiene 0 (UNSPECIFIED)
    req = metri_pb2.QueryRequest()
    req.tenant_id = "test-tenant-validator"
    req.queries["q1"].entity = "asset"
    # Set aggregation to 0 (which might be unspecified or invalid according to our Malli schema)
    # The Malli schema requires one of :COUNT, :SUM, etc. 0 will map to :UNRECOGNIZED or something that Malli rejects.
    req.queries["q1"].metrics.add(attribute="temperature", aggregation=999)
    
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        },
        stream=True
    )
    
    print(f"HTTP Status: {resp.status_code}")
    
    # Parseamos la respuesta (chunk gRPC o Railway chunk)
    response_bytes = resp.content
    offset = 0
    while offset < len(response_bytes):
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        
        if flag == 0x00:
            chunk_bytes = response_bytes[offset:offset+length]
            resp_proto = metri_pb2.QueryResponse()
            resp_proto.ParseFromString(chunk_bytes)
            print("\n[Respuesta del Motor]")
            print(f"Status Success: {resp_proto.status.success}")
            print(f"Error Code: {resp_proto.status.error_code}")
            print(f"Error Message: {resp_proto.status.error_message}")
            if resp_proto.status.error_code == "JANUS_VAL_001":
                print("\n✅ EXCELENTE: La validación de Malli interceptó correctamente el payload y devolvió JANUS_VAL_001.")
        offset += length

if __name__ == "__main__":
    test_invalid_request()
