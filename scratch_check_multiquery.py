import requests, sys, struct, json
import metri_pb2
from metri_pb2 import QueryRequest, AnalyticsRequest

req = QueryRequest()
req.tenant_id = "golden-tenant"

# Query 1: kpi_avg_health
q1 = AnalyticsRequest()
q1.tenant_id = "golden-tenant"
q1.entity = "asset"
q1.output_cast = metri_pb2.KPI
q1.viz = "kpi"
m1 = q1.metrics.add()
m1.aggregation = metri_pb2.AVG
m1.attribute = "health_score"
req.queries["kpi_avg_health"].CopyFrom(q1)

# Query 2: table_assets
q2 = AnalyticsRequest()
q2.tenant_id = "golden-tenant"
q2.entity = "asset"
q2.output_cast = metri_pb2.TABLE
q2.viz = "table"
q2.limit = 5
d1 = q2.dimensions.add()
d1.attribute = "name"
m2 = q2.metrics.add()
m2.aggregation = metri_pb2.COUNT
m2.attribute = "id"
req.queries["table_assets"].CopyFrom(q2)

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
offset = 0
while offset < len(body):
    flag, length = struct.unpack_from('!BI', body, offset)
    offset += 5
    payload = body[offset:offset+length]
    offset += length
    if flag == 0:
        qr = metri_pb2.QueryResponse()
        qr.ParseFromString(payload)
        from google.protobuf.json_format import MessageToDict
        d = MessageToDict(qr)
        print("KEYS:", d.get("batchResults", {}).keys())
        print(json.dumps(d, indent=2))
