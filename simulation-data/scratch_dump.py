import struct
import requests
import hashlib
import json
import metri_pb2

TENANT_ID = "golden-tenant-1234"

req = metri_pb2.QueryRequest()
req.tenant_id = TENANT_ID
q = metri_pb2.AnalyticsRequest()
q.tenant_id = TENANT_ID
q.entity = "inventory_movement"
q.output_cast = metri_pb2.TABLE
q.viz = "table"
q.limit = 5
req.queries["oltp_table"].CopyFrom(q)

proto_bytes = req.SerializeToString()
framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
payload_hash = hashlib.sha256(framed_data).hexdigest()

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    data=framed_data,
    headers={
        'Content-Type': 'application/grpc-web+proto',
        'X-Metri-Origin-Token': "datalog-golden-tenant",
        'x-amz-content-sha256': payload_hash,
    }
)
with open('resp.bin', 'wb') as f:
    f.write(resp.content)
print("Saved resp.bin, length:", len(resp.content))
