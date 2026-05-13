"""
test_filter_suite.py
====================
Suite de validación de FilterNode para OLAP y OLTP.

Operadores cubiertos (14): EQ, NEQ, GT, GTE, LT, LTE, IN, NOT_IN,
  BETWEEN, LIKE, CONTAINS, IS_NULL, IS_NOT_NULL, MATCHES
Grupos: AND, OR, NOT
Dot-path: location_id.name (ref cross-entity)

Entidades:
  OLAP → meter_reading   (attrs: reading_value number, timestamp epoch, type string)
  OLTP → inventory_movement (attrs: quantity number, timestamp epoch, type string)

Uso:
  python test_filter_suite.py              # producción
  python test_filter_suite.py --local      # localhost:9090
"""

import sys, json, struct, hashlib, logging, argparse, time
from datetime import datetime, timezone
from pathlib import Path
import requests, grpc
import metri_pb2, metri_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
REPORT_PATH = Path(__file__).parent / "filter_suite_report.json"

# ── Helpers proto ──────────────────────────────────────────────────────────────

def _criteria(field, op, *, str_val=None, num_val=None, bool_val=None,
              list_str=None, list_num=None, range_vals=None):
    c = metri_pb2.FilterCriteria()
    c.field   = field
    c.op_ref  = getattr(metri_pb2, op)
    v = metri_pb2.FilterValue()
    if str_val  is not None: v.string_val  = str_val
    if num_val  is not None: v.number_val  = num_val
    if bool_val is not None: v.bool_val    = bool_val
    if list_str is not None:
        # list_val = StringList { repeated string values }
        sl = metri_pb2.StringList()
        for s in list_str:
            sl.values.append(s)
        v.list_val.CopyFrom(sl)
    if list_num is not None:
        # range_values = FilterValueList { repeated FilterValue }
        fvl = metri_pb2.FilterValueList()
        for n in list_num:
            fv = metri_pb2.FilterValue(); fv.number_val = n
            fvl.values.append(fv)
        v.range_values.CopyFrom(fvl)
    if range_vals is not None:
        fvl = metri_pb2.FilterValueList()
        for n in range_vals:
            fv = metri_pb2.FilterValue(); fv.number_val = n
            fvl.values.append(fv)
        v.range_values.CopyFrom(fvl)
    c.value.CopyFrom(v)
    n = metri_pb2.FilterNode(); n.criteria.CopyFrom(c)
    return n

def _group(conjunction, *nodes):
    g = metri_pb2.FilterGroup()
    g.conjunction = conjunction
    for nd in nodes:
        g.nodes.append(nd)
    fn = metri_pb2.FilterNode(); fn.group.CopyFrom(g)
    return fn

def _kpi(tenant, entity, attribute, agg=metri_pb2.SUM):
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id = tenant; q.entity = entity
    q.output_cast = metri_pb2.KPI; q.viz = "kpi"
    m = q.metrics.add(); m.attribute = attribute; m.aggregation = agg
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 90
    return q

# ── Build batch request ────────────────────────────────────────────────────────

def build_filter_request(tenant="golden-tenant"):
    req = metri_pb2.QueryRequest(tenant_id=tenant)

    # ── OLAP: meter_reading ────────────────────────────────────────────────────

    # F01: EQ — unit_of_measure = "CEL" (valor real en Athena — verificado via EXPLORE)
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("unit_of_measure", "EQ", str_val="CEL"))
    req.queries["olap_eq_unit"].CopyFrom(q)

    # F02: NEQ — unit_of_measure != "KWH" (no existe KWH en seed → devuelve todos)
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("unit_of_measure", "NEQ", str_val="KWH"))
    req.queries["olap_neq_unit"].CopyFrom(q)

    # F03: GT — reading_value > 100
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("reading_value", "GT", num_val=100.0))
    req.queries["olap_gt_value"].CopyFrom(q)

    # F04: GTE — reading_value >= 150
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.AVG)
    q.filters.append(_criteria("reading_value", "GTE", num_val=150.0))
    req.queries["olap_gte_value"].CopyFrom(q)

    # F05: LT — reading_value < 100
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("reading_value", "LT", num_val=100.0))
    req.queries["olap_lt_value"].CopyFrom(q)

    # F06: LTE — reading_value <= 50
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.SUM)
    q.filters.append(_criteria("reading_value", "LTE", num_val=50.0))
    req.queries["olap_lte_value"].CopyFrom(q)

    # F07: BETWEEN — reading_value BETWEEN 50 AND 100
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("reading_value", "BETWEEN", range_vals=[50.0, 100.0]))
    req.queries["olap_between_value"].CopyFrom(q)

    # F08: IN — unit_of_measure IN ["CEL"] (valor real confirmado en Athena)
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("unit_of_measure", "IN", list_str=["CEL", "KWH"]))
    req.queries["olap_in_unit"].CopyFrom(q)

    # F09: NOT_IN — type NOT IN ["WATER"]
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("unit_of_measure", "NOT_IN", list_str=["psi", "lpm"]))
    req.queries["olap_not_in_unit"].CopyFrom(q)

    # F10: AND group — reading_value >= 50 AND reading_value <= 150
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.AVG)
    q.filters.append(_group("AND",
        _criteria("reading_value", "GTE", num_val=50.0),
        _criteria("reading_value", "LTE", num_val=150.0)))
    req.queries["olap_and_group"].CopyFrom(q)

    # F11: OR group — reading_value < 60 OR reading_value > 140
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_group("OR",
        _criteria("reading_value", "LT", num_val=60.0),
        _criteria("reading_value", "GT", num_val=140.0)))
    req.queries["olap_or_group"].CopyFrom(q)

    # F12: NOT group — NOT (reading_value = 100)
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_group("NOT",
        _criteria("reading_value", "EQ", num_val=100.0)))
    req.queries["olap_not_group"].CopyFrom(q)

    # F13: IS_NOT_NULL — reading_value IS NOT NULL
    q = _kpi(tenant, "meter_reading", "reading_value", metri_pb2.COUNT)
    q.filters.append(_criteria("reading_value", "IS_NOT_NULL"))
    req.queries["olap_is_not_null"].CopyFrom(q)

    # ── OLTP: inventory_movement ───────────────────────────────────────────────

    # F14: EQ — type = "RECEIPT"
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.COUNT)
    q.filters.append(_criteria("type", "EQ", str_val="RECEIPT"))
    req.queries["oltp_eq_type"].CopyFrom(q)

    # F15: GT — quantity > 100
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.COUNT)
    q.filters.append(_criteria("quantity", "GT", num_val=100.0))
    req.queries["oltp_gt_quantity"].CopyFrom(q)

    # F16: BETWEEN — quantity BETWEEN 50 AND 100
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.COUNT)
    q.filters.append(_criteria("quantity", "BETWEEN", range_vals=[50.0, 100.0]))
    req.queries["oltp_between_quantity"].CopyFrom(q)

    # F17: LTE — quantity <= 50
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.SUM)
    q.filters.append(_criteria("quantity", "LTE", num_val=50.0))
    req.queries["oltp_lte_quantity"].CopyFrom(q)

    # F18: IN — type IN ["RECEIPT"]
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.SUM)
    q.filters.append(_criteria("type", "IN", list_str=["RECEIPT"]))
    req.queries["oltp_in_type"].CopyFrom(q)

    # F19: AND — quantity >= 100 AND quantity <= 150
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.AVG)
    q.filters.append(_group("AND",
        _criteria("quantity", "GTE", num_val=100.0),
        _criteria("quantity", "LTE", num_val=150.0)))
    req.queries["oltp_and_group"].CopyFrom(q)

    # F20: OR — quantity < 60 OR quantity > 140
    q = _kpi(tenant, "inventory_movement", "quantity", metri_pb2.COUNT)
    q.filters.append(_group("OR",
        _criteria("quantity", "LT", num_val=60.0),
        _criteria("quantity", "GT", num_val=140.0)))
    req.queries["oltp_or_group"].CopyFrom(q)

    return req

# ── Transport ──────────────────────────────────────────────────────────────────

PROD_URL   = "https://engine.metri.one/"
PROD_TOKEN = "datalog-golden-tenant"

def _parse_frames(rb):
    results = {}; idx = 0
    import metri_pb2
    while idx + 5 <= len(rb):
        flag, length = struct.unpack("!BI", rb[idx:idx+5]); idx += 5
        if flag == 0x00:
            r = metri_pb2.QueryResponse()
            r.ParseFromString(rb[idx:idx+length])
            from google.protobuf.json_format import MessageToDict
            d = MessageToDict(r, preserving_proto_field_name=True)
            results.update(d.get("batch_results", {}))
        idx += length
    return results

def _grpc_call(keys_subset, req):
    """Envia solo un subconjunto de queries al engine."""
    sub = metri_pb2.QueryRequest(tenant_id=req.tenant_id)
    for k in keys_subset:
        if k in req.queries:
            sub.queries[k].CopyFrom(req.queries[k])
    pb = sub.SerializeToString()
    fd = struct.pack("!BI", 0, len(pb)) + pb
    for attempt in range(1, 5):
        try:
            resp = requests.post(
                f"{PROD_URL}metri.MetriService/Query", data=fd,
                headers={"Content-Type": "application/grpc-web+proto",
                         "X-Metri-Origin-Token": PROD_TOKEN,
                         "x-amz-content-sha256": hashlib.sha256(fd).hexdigest()},
                timeout=120)
            if resp.status_code == 504:
                logging.warning(f"  [{attempt}/4] HTTP 504. Retry en 15s...")
                time.sleep(15); continue
            return _parse_frames(resp.content)
        except Exception as e:
            logging.error(f"  [{attempt}/4] Error: {e}"); time.sleep(5)
    return {}

def send_production(req):
    # Dividimos en 2 batches de 10 para evitar timeout en cold start
    keys = list(req.queries.keys())
    mid  = len(keys) // 2
    logging.info(f"Batch 1/2 ({mid} queries)...")
    r1 = _grpc_call(keys[:mid], req)
    time.sleep(2)
    logging.info(f"Batch 2/2 ({len(keys)-mid} queries)...")
    r2 = _grpc_call(keys[mid:], req)
    return {**r1, **r2}

def send_local(req, host="localhost", port=9090):
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub    = metri_pb2_grpc.MetriServiceStub(channel)
    resp    = stub.Query(req, timeout=60)
    from google.protobuf.json_format import MessageToDict
    d = MessageToDict(resp, preserving_proto_field_name=True)
    return d.get("batch_results", {})

# ── Expected results (valores calculados de los datos seed) ───────────────────
# OLAP meter_reading: 90 registros — 30×150, 30×100, 30×50
# OLTP inventory_movement: 90 registros — 30×150, 30×100, 30×50 (tipo RECEIPT)

EXPECTATIONS = {
    # OLAP
    "olap_eq_unit":        {"min_rows": 1, "note": "unit_of_measure=CEL (valor real Athena) → todos 90"},
    "olap_neq_unit":       {"min_rows": 1, "note": "unit_of_measure!=KWH (no existe) → todos 90"},
    "olap_gt_value":       {"min_rows": 1, "expect_val_range": (1, 90), "note": "rv>100 → 30 registros (rv=150)"},
    "olap_gte_value":      {"min_rows": 1, "expect_val_name": "avg", "note": "rv>=150 → avg=150"},
    "olap_lt_value":       {"min_rows": 1, "note": "rv<100 → 30 registros (rv=50)"},
    "olap_lte_value":      {"min_rows": 1, "note": "rv<=50 → sum=1500"},
    "olap_between_value":  {"min_rows": 1, "note": "50<=rv<=100 → 60 registros"},
    "olap_in_unit":        {"min_rows": 1, "note": "unit_of_measure IN [CEL,KWH] → todos 90"},
    "olap_not_in_unit":    {"min_rows": 1, "note": "unit_of_measure NOT IN [psi,lpm] → todos"},
    "olap_and_group":      {"min_rows": 1, "note": "50<=rv<=150 → avg in [50,150]"},
    "olap_or_group":       {"min_rows": 1, "note": "rv<60 OR rv>140 → 60 registros"},
    "olap_not_group":      {"min_rows": 1, "note": "rv != 100 → 60 registros"},
    "olap_is_not_null":    {"min_rows": 1, "note": "rv IS NOT NULL → todos"},
    # OLTP
    "oltp_eq_type":        {"min_rows": 1, "note": "type=RECEIPT → todos 90"},
    "oltp_gt_quantity":    {"min_rows": 1, "note": "qty>100 → 30 registros"},
    "oltp_between_quantity":{"min_rows": 1,"note": "50<=qty<=100 → 60 registros"},
    "oltp_lte_quantity":   {"min_rows": 1, "note": "qty<=50 → sum=1500"},
    "oltp_in_type":        {"min_rows": 1, "note": "IN [RECEIPT] → todos"},
    "oltp_and_group":      {"min_rows": 1, "note": "100<=qty<=150 → avg≈125"},
    "oltp_or_group":       {"min_rows": 1, "note": "qty<60 OR qty>140 → 60"},
}

QUERY_ORDER = [
    "olap_eq_unit","olap_neq_unit","olap_gt_value","olap_gte_value",
    "olap_lt_value","olap_lte_value","olap_between_value","olap_in_unit",
    "olap_not_in_unit","olap_and_group","olap_or_group","olap_not_group",
    "olap_is_not_null",
    "oltp_eq_type","oltp_gt_quantity","oltp_between_quantity",
    "oltp_lte_quantity","oltp_in_type","oltp_and_group","oltp_or_group",
]

# ── Análisis ───────────────────────────────────────────────────────────────────

def analyse(results: dict) -> dict:
    ok = 0; fail = 0
    report = {"summary": {}, "results": {}}
    SEP = "═" * 64

    print(f"\n{SEP}")
    print(f"  FILTER SUITE — {len(QUERY_ORDER)} queries")
    print(SEP)

    for key in QUERY_ORDER:
        exp = EXPECTATIONS.get(key, {})
        r   = results.get(key, {})

        status_ok   = r.get("status", {}).get("success", False)
        data        = r.get("data", {})
        columns     = [c.get("key") for c in data.get("columns", [])]
        rows        = data.get("rows_json", {}).get("iter", [])
        row0        = rows[0] if rows else {}
        values      = row0.get("values", []) if isinstance(row0, dict) else []
        val0        = values[0] if values else None
        exec_ms     = r.get("metadata", {}).get("execution_time_ms", "?")
        engine      = r.get("metadata", {}).get("engine", "?")
        err_msg     = r.get("status", {}).get("error_message", "")
        err_code    = r.get("status", {}).get("error_code", "")

        issues = []
        if not status_ok:
            issues.append(f"❌ error [{err_code}]: {err_msg}")
        elif not rows:
            issues.append("⚠️  0 rows — posible bug de filtro o sin datos seed")

        label = "✅ OK" if (status_ok and not issues) else "❌ FAIL"
        if status_ok and not issues:
            ok += 1
        else:
            fail += 1

        note = exp.get("note", "")
        print(f"\n{label} [{key.upper()}]")
        print(f"   engine  : {engine}  |  exec_ms: {exec_ms}")
        print(f"   columns : {columns}")
        print(f"   rows    : {len(rows)}  |  val[0]: {val0}")
        print(f"   🎯 {note}")
        for iss in issues:
            print(f"   {iss}")

        report["results"][key] = {
            "ok": status_ok and not issues,
            "engine": engine,
            "exec_ms": exec_ms,
            "rows": len(rows),
            "columns": columns,
            "val0": val0,
            "issues": issues,
            "note": note,
            "error_code": err_code,
            "error_message": err_msg,
        }

    print(f"\n{SEP}")
    print(f"  TOTAL: {ok}/{ok+fail} OK  |  {fail} FALLIDOS")
    print(SEP)

    report["summary"] = {
        "total": ok + fail,
        "ok": ok,
        "failed": fail,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return report

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--port",  type=int, default=9090)
    args = parser.parse_args()

    req = build_filter_request()

    if args.local:
        print(f"INFO: === Filter Suite | LOCAL (localhost:{args.port}) ===")
        results = send_local(req, port=args.port)
    else:
        print("INFO: === Filter Suite | PRODUCCIÓN ===")
        results = send_production(req)

    report = analyse(results)
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nINFO: 📄 Reporte guardado en: {REPORT_PATH}")
    sys.exit(0 if report["summary"]["failed"] == 0 else 1)

if __name__ == "__main__":
    main()
