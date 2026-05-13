"""
viz_oltp_strategies.py
======================
Suite completa de prueba manual para las 5 estrategias de visualización OLTP.
Equivalente al viz_all_strategies.py pero orientado exclusivamente al motor OLTP (Datahike).

Estrategias evaluadas:
  1. TABLE       → inventory_movement (proyección directa, paginado)
  2. TIMESERIES  → inventory_movement (SUM quantity por día)
  3. BUBBLE      → inventory_movement (AVG quantity agrupado por type)
  4. PIE         → inventory_movement (COUNT quantity por type)
  5. TREE        → location (jerarquía parent_location_id)

Genera output de consola por estrategia + JSON de reporte.
"""
import struct
import logging
import requests
import hashlib
import json
import sys

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant"
TOKEN        = "datalog-golden-tenant"


# ─── gRPC-Web helper ──────────────────────────────────────────────────────────

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
        logging.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
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


# ─── Suite de queries ─────────────────────────────────────────────────────────

def build_query_request():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID

    # ── 1. TABLE: inventory_movement (limit 10) ──────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.TABLE
    q.viz        = "table"
    q.limit      = 10
    req.queries["oltp_table"].CopyFrom(q)

    # ── 2. TIMESERIES: SUM(quantity) por día ─────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.TIMESERIES
    q.viz        = "line"
    m = q.metrics.add()
    m.aggregation = metri_pb2.SUM
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "timestamp"
    d.interval  = "day"
    req.queries["oltp_line"].CopyFrom(q)

    # ── 3. TIMESERIES: SUM(quantity) por semana ───────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.TIMESERIES
    q.viz        = "line"
    m = q.metrics.add()
    m.aggregation = metri_pb2.SUM
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "timestamp"
    d.interval  = "week"
    req.queries["oltp_line_weekly"].CopyFrom(q)

    # ── 4. TIMESERIES: AVG(quantity) por mes ─────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.TIMESERIES
    q.viz        = "line"
    m = q.metrics.add()
    m.aggregation = metri_pb2.AVG
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "timestamp"
    d.interval  = "month"
    req.queries["oltp_line_monthly"].CopyFrom(q)

    # ── 5. BUBBLE: AVG(quantity) por type ────────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.BUBBLE
    q.viz        = "scatter"
    m = q.metrics.add()
    m.aggregation = metri_pb2.AVG
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "type"
    req.queries["oltp_scatter"].CopyFrom(q)

    # ── 6. BUBBLE: MIN + MAX quantity por type ────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.BUBBLE
    q.viz        = "scatter"
    m1 = q.metrics.add()
    m1.aggregation = metri_pb2.MIN
    m1.attribute   = "quantity"
    m2 = q.metrics.add()
    m2.aggregation = metri_pb2.MAX
    m2.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "type"
    req.queries["oltp_scatter_minmax"].CopyFrom(q)

    # ── 7. PIE: COUNT por type ────────────────────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.PIE
    q.viz        = "pie"
    m = q.metrics.add()
    m.aggregation = metri_pb2.COUNT
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "type"
    req.queries["oltp_pie"].CopyFrom(q)

    # ── 8. PIE: SUM(quantity) por type ────────────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.PIE
    q.viz        = "pie"
    m = q.metrics.add()
    m.aggregation = metri_pb2.SUM
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute = "type"
    req.queries["oltp_pie_sum"].CopyFrom(q)

    # ── 9. KPI: SUM global ────────────────────────────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "inventory_movement"
    q.output_cast = metri_pb2.KPI
    q.viz        = "kpi"
    m1 = q.metrics.add()
    m1.aggregation = metri_pb2.SUM
    m1.attribute   = "quantity"
    m2 = q.metrics.add()
    m2.aggregation = metri_pb2.COUNT
    m2.attribute   = "quantity"
    m3 = q.metrics.add()
    m3.aggregation = metri_pb2.AVG
    m3.attribute   = "quantity"
    req.queries["oltp_kpi"].CopyFrom(q)

    # ── 10. TREE: location con jerarquía parent_location_id ──────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "location"
    q.output_cast = metri_pb2.TABLE
    q.viz        = "tree"
    q.hierarchy.inject_has_children = True
    q.hierarchy.parent_field        = "parent_location_id"
    req.queries["oltp_tree"].CopyFrom(q)

    # ── 11. TABLE: location sin jerarquía (raw) ──────────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id  = TENANT_ID
    q.entity     = "location"
    q.output_cast = metri_pb2.TABLE
    q.viz        = "table"
    req.queries["oltp_location_table"].CopyFrom(q)

    return req


# ─── Ejecución ────────────────────────────────────────────────────────────────

STRATEGY_ORDER = [
    "oltp_table",
    "oltp_line",
    "oltp_line_weekly",
    "oltp_line_monthly",
    "oltp_scatter",
    "oltp_scatter_minmax",
    "oltp_pie",
    "oltp_pie_sum",
    "oltp_kpi",
    "oltp_tree",
    "oltp_location_table",
]

def run():
    logging.info("=== Suite OLTP: 11 Estrategias de Visualización ===")
    req = build_query_request()
    frames = invoke_grpc_web("metri.MetriService/Query", req)

    if not frames:
        logging.error("No se recibió respuesta válida del engine.")
        sys.exit(1)

    from google.protobuf.json_format import MessageToDict
    results = {}
    for f in frames:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(f)
        if resp.status.success:
            chunk = MessageToDict(resp, preserving_proto_field_name=True)
            if "batch_results" in chunk:
                results.update(chunk["batch_results"])
        else:
            logging.error(f"Error en chunk: {resp.status.error_message}")

    print("\n" + "=" * 60)
    print("      REPORTE VIZ ESTRATEGIAS OLTP (Datahike)")
    print("=" * 60)

    all_ok = True
    for key in STRATEGY_ORDER:
        if key in results:
            print(f"\n[{key.upper()}] Status: OK")
            print(json.dumps(results[key], indent=2))
        else:
            print(f"\n[{key.upper()}] ❌ ERROR: Missing from batch_results")
            all_ok = False

    print("\n" + "=" * 60)
    if all_ok:
        print("✅ TODAS las estrategias retornaron resultados.")
    else:
        print("❌ Algunas estrategias fallaron.")

    return results


if __name__ == "__main__":
    run()
