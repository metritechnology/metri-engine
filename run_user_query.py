import sys
import os

# Add the path to metri_pb2
sys.path.append(os.path.join(os.path.dirname(__file__), 'src/metri/grpc/python'))

import requests
import struct
import hashlib
import json
import metri_pb2
from google.protobuf.json_format import MessageToDict

TENANT_ID = "golden-tenant"

req = metri_pb2.QueryRequest()
req.tenant_id = TENANT_ID

q = metri_pb2.AnalyticsRequest()
q.tenant_id = TENANT_ID
q.entity = "asset"
q.limit = 50
# The other fields are empty lists/strings/false, which corresponds to default values in proto3
# _viz, _outputCast (0 = OUTPUT_CAST_UNSPECIFIED), _search, _explainPlan

req.queries["user_query"].CopyFrom(q)

proto_bytes = req.SerializeToString()
framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
payload_hash = hashlib.sha256(framed_data).hexdigest()

print(f"Sending query for entity 'asset' with limit 50...")

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    data=framed_data,
    headers={
        'Content-Type': 'application/grpc-web+proto',
        'X-Metri-Origin-Token': 'datalog-golden-tenant',
        'x-amz-content-sha256': payload_hash,
    }
)

if resp.status_code != 200:
    print(f"Error {resp.status_code}: {resp.text}")
    sys.exit(1)

frames = []
idx = 0
response_bytes = resp.content
while idx + 5 <= len(response_bytes):
    flag, length = struct.unpack('!BI', response_bytes[idx:idx + 5])
    idx += 5
    if flag == 0x00:
        frames.append(response_bytes[idx:idx + length])
    idx += length

print(f"Received {len(frames)} frames. Parsing...")
for f in frames:
    resp_msg = metri_pb2.QueryResponse()
    resp_msg.ParseFromString(f)
    print(json.dumps(MessageToDict(resp_msg, preserving_proto_field_name=True), indent=2))
