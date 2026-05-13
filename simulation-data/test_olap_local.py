import requests
import struct
import hashlib
import json
import uuid
import metri_pb2
from google.protobuf.json_format import MessageToDict

TENANT_ID = "golden-tenant-dc14c542-6f38-4c5e-8462-5eb342ca84a1"

req = metri_pb2.QueryRequest()
req.tenant_id = TENANT_ID

q = metri_pb2.AnalyticsRequest()
q.tenant_id = TENANT_ID
q.entity = "meter_reading"
q.output_cast = metri_pb2.TABLE
q.viz = "table"
q.limit = 5
req.queries["olap_table"].CopyFrom(q)

proto_bytes = req.SerializeToString()
framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    data=framed_data,
    headers={
        'Content-Type': 'application/grpc-web+proto',
    }
)

frames = []
idx = 0
response_bytes = resp.content
while idx + 5 <= len(response_bytes):
    flag, length = struct.unpack('!BI', response_bytes[idx:idx + 5])
    idx += 5
    if flag == 0x00:
        frames.append(response_bytes[idx:idx + length])
    idx += length

for f in frames:
    resp = metri_pb2.QueryResponse()
    resp.ParseFromString(f)
    print(MessageToDict(resp, preserving_proto_field_name=True))

