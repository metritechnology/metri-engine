"""
generate_oltp_report.py
=======================
Genera un JSON de inspección manual para las 11 estrategias OLTP.
Equivalente a generate_json_report.py pero exclusivo para el motor OLTP.
"""
import struct
import logging
import requests
import hashlib
import json
import os

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL  = "https://engine.metri.one/"
TENANT_ID     = "golden-tenant"
TOKEN         = "datalog-golden-tenant"
ARTIFACT_PATH = "/Users/macuser/.gemini/antigravity/brain/686d10dd-3bf6-4597-a21c-e1372c00ad58/artifacts/viz_oltp_manual_verification.json"


def invoke_grpc_web(endpoint: str, proto_req):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    resp = requests.post(
        f"{FUNCTION_URL}{endpoint}",
        data=framed_data,
        headers={
            'Content-Type':         'application/grpc-web+proto',
            'X-Metri-Origin-Token': TOKEN,
            'x-amz-content-sha256': payload_hash,
        },
        timeout=60,
    )
    if resp.status_code != 200:
        return None
    rb = resp.content
    frames = []
    idx = 0
    while idx + 5 <= len(rb):
        flag, length = struct.unpack('!BI', rb[idx:idx+5])
        idx += 5
        if flag == 0x00:
            frames.append(rb[idx:idx+length])
        idx += length
    return frames


def build_request():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID

    def add_table(key, entity, limit=10):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.TABLE; q.viz = "table"; q.limit = limit
        req.queries[key].CopyFrom(q)

    def add_ts(key, entity, agg, attr, interval):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.TIMESERIES; q.viz = "line"
        m = q.metrics.add(); m.aggregation = agg; m.attribute = attr
        d = q.dimensions.add(); d.attribute = "timestamp"; d.interval = interval
        req.queries[key].CopyFrom(q)

    def add_bubble(key, entity, metrics_spec, dim_attr):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.BUBBLE; q.viz = "scatter"
        for agg, attr in metrics_spec:
            m = q.metrics.add(); m.aggregation = agg; m.attribute = attr
        d = q.dimensions.add(); d.attribute = dim_attr
        req.queries[key].CopyFrom(q)

    def add_pie(key, entity, agg, attr, dim_attr):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.PIE; q.viz = "pie"
        m = q.metrics.add(); m.aggregation = agg; m.attribute = attr
        d = q.dimensions.add(); d.attribute = dim_attr
        req.queries[key].CopyFrom(q)

    def add_kpi(key, entity, metrics_spec):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.KPI; q.viz = "kpi"
        for agg, attr in metrics_spec:
            m = q.metrics.add(); m.aggregation = agg; m.attribute = attr
        req.queries[key].CopyFrom(q)

    def add_tree(key, entity, parent_field):
        q = metri_pb2.AnalyticsRequest()
        q.tenant_id = TENANT_ID; q.entity = entity
        q.output_cast = metri_pb2.TABLE; q.viz = "tree"
        q.hierarchy.inject_has_children = True
        q.hierarchy.parent_field = parent_field
        req.queries[key].CopyFrom(q)

    # inventory_movement
    add_table("oltp_table",         "inventory_movement", limit=10)
    add_ts(   "oltp_line_day",      "inventory_movement", metri_pb2.SUM, "quantity", "day")
    add_ts(   "oltp_line_week",     "inventory_movement", metri_pb2.SUM, "quantity", "week")
    add_ts(   "oltp_line_month",    "inventory_movement", metri_pb2.AVG, "quantity", "month")
    add_bubble("oltp_scatter_avg",  "inventory_movement", [(metri_pb2.AVG, "quantity")], "type")
    add_bubble("oltp_scatter_minmax","inventory_movement",
               [(metri_pb2.MIN, "quantity"), (metri_pb2.MAX, "quantity")], "type")
    add_pie(  "oltp_pie_count",     "inventory_movement", metri_pb2.COUNT, "quantity", "type")
    add_pie(  "oltp_pie_sum",       "inventory_movement", metri_pb2.SUM,   "quantity", "type")
    add_kpi(  "oltp_kpi",           "inventory_movement",
              [(metri_pb2.SUM, "quantity"), (metri_pb2.COUNT, "quantity"), (metri_pb2.AVG, "quantity")])
    # location
    add_tree( "oltp_tree",          "location", "parent_location_id")
    add_table("oltp_location_table","location", limit=10)

    return req


def run():
    logging.info("Generando reporte OLTP para inspección manual...")
    req = build_request()
    frames = invoke_grpc_web("metri.MetriService/Query", req)

    if not frames:
        logging.error("Sin respuesta del engine."); return

    from google.protobuf.json_format import MessageToDict
    results = {}
    for f in frames:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(f)
        if resp.status.success:
            chunk = MessageToDict(resp, preserving_proto_field_name=True)
            if "batch_results" in chunk:
                results.update(chunk["batch_results"])

    os.makedirs(os.path.dirname(ARTIFACT_PATH), exist_ok=True)
    with open(ARTIFACT_PATH, "w") as fp:
        json.dump(results, fp, indent=2, ensure_ascii=False)

    logging.info(f"✅ JSON guardado en: {ARTIFACT_PATH}")
    for key in results:
        rows = results[key].get("data", {}).get("rows_json", {}).get("iter", [])
        meta = results[key].get("metadata", {})
        print(f"  [{key}] rows={len(rows)} total={meta.get('total_count','?')} engine={meta.get('engine','?')}")


if __name__ == "__main__":
    run()
