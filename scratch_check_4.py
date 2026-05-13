import requests, sys, struct, json
import metri_pb2
from metri_pb2 import QueryResponse, AnalyticsRequest
from google.protobuf.json_format import MessageToDict

req = metri_pb2.QueryRequest()

q_table = metri_pb2.AnalyticsRequest()
q_table.tenant_id = "datalog-golden-tenant"
q_table.entity = "asset"
q_table.output_cast = metri_pb2.TABLE
q_table.viz = "table"
q_table.select_tree.update({"status": True, "name": True})

# NO TIME FRAME!

req.queries["asset_table"].CopyFrom(q_table)

proto_bytes = req.SerializeToString()
framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes

resp = requests.post(
    "https://engine.metri.one/metri.MetriService/Query",
    data=framed_data,
    headers={
        'Content-Type': 'application/grpc-web+proto',
        'X-Grpc-Web': '1',
        'X-Metri-Origin-Token': 'datalog-golden-tenant'
    }
)

body = resp.content
offset = 0
while offset < len(body):
    flag, length = struct.unpack_from('!BI', body, offset)
    offset += 5
    payload = body[offset:offset+length]
    offset += length
    if flag == 0:
        qr = QueryResponse()
        qr.ParseFromString(payload)
        print("TABLE ROWS NO TF:", len(qr.batch_results["asset_table"].data.rows_json.iter))

