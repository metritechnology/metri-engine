#!/usr/bin/env python3
import sys
import json
import struct
import hashlib
import requests
import google.protobuf.json_format as json_format
import metri_pb2

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant"
AUTH_TOKEN   = "datalog-golden-tenant"

def discover_entities():
    req = metri_pb2.DiscoveryRequest()
    req.tenant_id = TENANT_ID
    req.include_attributes = True

    proto_bytes  = req.SerializeToString()
    framed_data  = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Discovery",
        data=framed_data,
        headers={
            'Content-Type':         'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash,
            'Authorization':        f'Bearer {AUTH_TOKEN}',
        },
        stream=True,
        timeout=60,
    )

    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return

    data = resp.content
    offset = 0
    messages = []
    while offset < len(data):
        if offset + 5 > len(data):
            break
        flag, length = struct.unpack('!BI', data[offset:offset+5])
        offset += 5
        if flag == 0x00:
            messages.append(data[offset:offset+length])
        offset += length

    if not messages:
        print("No response messages")
        return

    response = metri_pb2.DiscoveryResponse()
    response.ParseFromString(messages[0])
    print(json.dumps(json.loads(json_format.MessageToJson(response, preserving_proto_field_name=True)), indent=2))

if __name__ == "__main__":
    discover_entities()
