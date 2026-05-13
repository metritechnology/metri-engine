import requests, sys, struct, json
from metri_pb2 import QueryResponse, AnalyticsRequest
from google.protobuf.json_format import MessageToDict
import benchmark_assets

req = benchmark_assets.build_query_request()
# OVERRIDE TO USE A TENANT WE KNOW HAS DATA
req.queries["asset_pie"].tenant_id = "datalog-golden-tenant"
req.queries["asset_kpi"].tenant_id = "datalog-golden-tenant"
req.queries["asset_table"].tenant_id = "datalog-golden-tenant"

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
        print("PIE BREAKDOWN KEYS:", list(qr.batch_results["asset_pie"].viz_ext.breakdown.signals.keys()))
        print("KPI VALUE:", qr.batch_results["asset_kpi"].viz_ext.signal.value)
        
