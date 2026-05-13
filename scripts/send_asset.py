import sys
import base64
import requests
import struct
from google.protobuf.struct_pb2 import Struct

import metri_pb2

def encode_grpc_web(proto_msg):
    payload = proto_msg.SerializeToString()
    # gRPC-Web format: 1 byte flag (0 for data), 4 bytes length, then payload
    header = struct.pack('>B I', 0, len(payload))
    return header + payload

def decode_grpc_web(response_bytes):
    if len(response_bytes) < 5:
        return None
    flag, length = struct.unpack('>B I', response_bytes[:5])
    return response_bytes[5:5+length]

def main():
    # Construct the struct payload
    payload_struct = Struct()
    payload_struct.update({
        "name": "Python Asset",
        "serial_number": "SN-PY-001",
        "status": "ACTIVE"
    })

    req = metri_pb2.TransactionRequest(
        tenant_id="golden-tenant-123",
        entity_type="asset",
        action=metri_pb2.CREATE,
        payload=payload_struct
    )

    data = encode_grpc_web(req)
    b64_data = base64.b64encode(data).decode('utf-8')

    url = "https://d21ik83yjpr5g6.cloudfront.net/metri.MetriService/Transact"
    headers = {
        "Content-Type": "application/grpc-web-text",
        "Accept": "application/grpc-web-text"
    }

    print(f"Sending gRPC to {url}")
    response = requests.post(url, data=b64_data, headers=headers)
    
    if response.status_code == 200:
        b64_resp = response.text
        if b64_resp:
            print("Raw response:", response.text)
            try:
                resp_bytes = base64.b64decode(b64_resp)
                proto_bytes = decode_grpc_web(resp_bytes)
                if proto_bytes:
                    resp = metri_pb2.TransactionResponse()
                    resp.ParseFromString(proto_bytes)
                    print("Success!")
                    print(resp)
                else:
                    print("Empty response payload")
            except Exception as e:
                print("Error parsing response:", e)
    else:
        print(f"Failed: {response.status_code}")
        print(response.text)

if __name__ == '__main__':
    main()
