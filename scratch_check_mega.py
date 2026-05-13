import requests, sys, struct, json
import metri_pb2
from metri_pb2 import QueryRequest, AnalyticsRequest

req = QueryRequest()
req.tenant_id = "golden-tenant"

# Query 1: kpi-total-assets
q1 = AnalyticsRequest()
q1.tenant_id = "golden-tenant"
q1.entity = "asset"
q1.output_cast = metri_pb2.KPI
q1.viz = "kpi"
m1 = q1.metrics.add()
m1.aggregation = metri_pb2.COUNT
m1.attribute = "id"
c1 = q1.comparisons.add()
c1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_RELATIVE
c1.relative_amount = 1
c1.relative_granularity = "month"
req.queries["kpi-total-assets::kpi-total-assets-q"].CopyFrom(q1)

# Query 2: table-assets
q2 = AnalyticsRequest()
q2.tenant_id = "golden-tenant"
q2.entity = "asset"
q2.output_cast = metri_pb2.TABLE
q2.viz = "table"
d2a = q2.dimensions.add()
d2a.attribute = "name"
d2b = q2.dimensions.add()
d2b.attribute = "status"
m2 = q2.metrics.add()
m2.aggregation = metri_pb2.COUNT
m2.attribute = "id"
req.queries["table-assets::table-assets-q"].CopyFrom(q2)

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
        print(f"Overall status: {qr.status.success}")
        print("\nBatch Results keys:", list(qr.batch_results.keys()))
        for key, res in qr.batch_results.items():
            print(f"\n[{key}] success: {res.status.success}")
            if not res.status.success:
                print(f"[{key}] error: {res.status.error_code} - {res.status.error_message}")
            else:
                if res.data.HasField("kpi"):
                    print(f"[{key}] KPI value: {res.data.kpi.value}")
                elif res.data.HasField("rows_json"):
                    print(f"[{key}] Table rows: {len(res.data.rows_json.iter)}")
