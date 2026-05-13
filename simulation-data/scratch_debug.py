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
print("Length:", len(proto_bytes))
print("Bytes:", proto_bytes.hex())
