import requests, sys, struct, json
import metri_pb2
from metri_pb2 import QueryRequest, AnalyticsRequest

req = QueryRequest()
req.tenant_id = "golden-tenant"

q = AnalyticsRequest()
q.tenant_id = "golden-tenant"
q.entity = "asset"
q.output_cast = metri_pb2.KPI
q.viz = "kpi"

metric = q.metrics.add()
metric.aggregation = metri_pb2.AVG
metric.field = "health_score"

req.queries["kpi_avg_health"].CopyFrom(q)

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
        res = qr.batch_results["kpi_avg_health"]
        print("Success:", res.status.success)
        print("Error:", res.status.error_message)
        print("KPI Value:", res.data.kpi.value)
