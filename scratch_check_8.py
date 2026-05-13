import requests, sys, struct, json
import metri_pb2
from metri_pb2 import QueryResponse, AnalyticsRequest

req = metri_pb2.QueryRequest()
req.tenant_id = "golden-tenant"

q_table = metri_pb2.AnalyticsRequest()
q_table.tenant_id = "golden-tenant"
q_table.entity = "asset"
q_table.output_cast = metri_pb2.TABLE
q_table.viz = "table"
q_table.limit = 10

req.queries["asset_table"].CopyFrom(q_table)

proto_bytes = req.SerializeToString()
framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    data=framed_data,
    headers={
        'Content-Type': 'application/grpc-web+proto',
        'X-Grpc-Web': '1',
        'X-Metri-Origin-Token': 'golden-tenant'
    }
)

body = resp.content
print("Response length:", len(body))
offset = 0
while offset < len(body):
    flag, length = struct.unpack_from('!BI', body, offset)
    offset += 5
    payload = body[offset:offset+length]
    offset += length
    if flag == 0:
        qr = QueryResponse()
        qr.ParseFromString(payload)
        rows = qr.batch_results["asset_table"].data.rows_json.iter
        print("TABLE ROWS NO TF:", len(rows))
        for row in rows:
             print("Row values:", row.values)
