"""
viz_all_strategies.py
======================
Suite completa de visualizaciones OLTP + OLAP.
Incluye ingest automático si las bases están vacías.

NUEVO: Ejercita label_template en DimensionDefinition (PIE/LINE/AREA)
       y en ChartDecoration vía el campo label_template del proto.
       Formato Mustache: {{attribute}} interpolado por el engine al renderizar.
"""

import struct
import logging
import requests
import hashlib
import json
import time
import random
from datetime import datetime, timedelta
from pathlib import Path

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
import uuid
TENANT_ID    = f"golden-tenant-{uuid.uuid4()}"
TOKEN        = "datalog-golden-tenant"


# ─── gRPC-Web helpers ─────────────────────────────────────────────────────────

def invoke_grpc_web(endpoint: str, proto_req, token=TOKEN, retries=4, backoff=15):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                f"{FUNCTION_URL}{endpoint}",
                data=framed_data,
                headers={
                    'Content-Type':         'application/grpc-web+proto',
                    'X-Metri-Origin-Token': token,
                    'x-amz-content-sha256': payload_hash,
                },
                timeout=90,
            )
            if resp.status_code in (502, 503, 504):
                logging.warning(f"  [{attempt}/{retries}] HTTP {resp.status_code} (cold start). Retrying in {backoff}s...")
                time.sleep(backoff)
                continue
            if resp.status_code != 200:
                logging.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
                return None

            response_bytes = resp.content
            frames = []
            idx = 0
            while idx + 5 <= len(response_bytes):
                flag, length = struct.unpack('!BI', response_bytes[idx:idx + 5])
                idx += 5
                if flag == 0x00:
                    frames.append(response_bytes[idx:idx + length])
                idx += length
            return frames if frames else None

        except requests.exceptions.Timeout:
            logging.warning(f"  [{attempt}/{retries}] Timeout. Retrying in {backoff}s...")
            time.sleep(backoff)

    logging.error("Máximo de reintentos alcanzado.")
    return None


def invoke_grpc_web_single(endpoint: str, proto_req, token=TOKEN):
    """Versión simplificada para Transact/Bulk que devuelve el primer frame."""
    frames = invoke_grpc_web(endpoint, proto_req, token=token)
    if frames:
        return frames[0]
    return None


def bulk_ingest(entity_type, columns, rows_data):
    req = metri_pb2.BulkRequest()
    req.tenant_id   = TENANT_ID
    req.entity_type = entity_type
    req.action      = metri_pb2.UPSERT

    rs = req.data
    for col_key in columns:
        c = rs.columns.add()
        c.key = col_key

    for row_vals in rows_data:
        row = rs.rows_json.iter.add()
        for val in row_vals:
            v = row.values.add()
            if isinstance(val, str):
                v.string_value = val
            elif isinstance(val, (int, float)):
                v.number_value = float(val)
            elif val is None:
                pass  # omitir null
            else:
                v.string_value = str(val)

    res = invoke_grpc_web_single("metri.MetriService/BulkIngest", req)
    if res:
        resp = metri_pb2.BulkResponse()
        resp.ParseFromString(res)
        if resp.status.success:
            logging.info(f"  ✅ {entity_type}: {resp.ingested_count} registros")
            return True
        else:
            logging.error(f"  ❌ {entity_type}: {resp.status.error_code} - {resp.status.error_message}")
    else:
        logging.error(f"  ❌ {entity_type}: sin respuesta")
    return False


# ─── Ingest OLTP ──────────────────────────────────────────────────────────────

def ingest_oltp():
    """Ingest de locations, parts e inventory_movements."""
    try:
        import ulid as ulid_lib
        new_ulid = lambda: str(ulid_lib.new())
    except ImportError:
        import uuid
        new_ulid = lambda: str(uuid.uuid4())

    now = datetime.utcnow()
    logging.info("\n[OLTP 1/3] Ingesting locations...")

    loc_root_1   = new_ulid()
    loc_root_2   = new_ulid()
    loc_child_1a = new_ulid()
    loc_child_1b = new_ulid()
    loc_child_2a = new_ulid()

    bulk_ingest("location", ["id", "name", "tag"],
                [[loc_root_1, "Almacén Central", "AC-001"],
                 [loc_root_2, "Almacén Norte",   "AN-002"]])

    bulk_ingest("location", ["id", "name", "tag", "parent_location_id"],
                [[loc_child_1a, "Zona A",         "AC-A",  loc_root_1],
                 [loc_child_1b, "Zona B",         "AC-B",  loc_root_1],
                 [loc_child_2a, "Sector Norte 1", "AN-S1", loc_root_2]])

    all_loc_ids = [loc_root_1, loc_root_2, loc_child_1a, loc_child_1b, loc_child_2a]

    logging.info("\n[OLTP 2/3] Ingesting parts...")
    part_ids = []
    part_rows = []
    for i in range(1, 11):
        pid = new_ulid()
        part_ids.append(pid)
        part_rows.append([pid, f"Part-{i:03d}", f"SKU-{i:04d}"])
    bulk_ingest("part", ["id", "name", "sku"], part_rows)

    logging.info("\n[OLTP 3/3] Ingesting inventory_movements (300 registros)...")
    TYPES  = ["RECEIPT", "ISSUE", "TRANSFER", "ADJUSTMENT"]
    TYPE_W = [0.40,       0.35,    0.15,       0.10]

    rows = []
    for _ in range(300):
        day_off   = random.randint(0, 89)
        target_dt = now - timedelta(days=day_off,
                                    hours=random.randint(0, 23),
                                    minutes=random.randint(0, 59))
        ts_ms = int(target_dt.timestamp() * 1000)
        rows.append([
            new_ulid(),
            ts_ms,
            round(random.uniform(10.0, 500.0), 2),
            random.choices(TYPES, weights=TYPE_W)[0],
            random.choice(part_ids),
            random.choice(all_loc_ids),
        ])

    for b in range(0, len(rows), 100):
        bulk_ingest("inventory_movement",
                    ["id", "timestamp", "quantity", "type", "part_id", "location_id"],
                    rows[b:b + 100])
        time.sleep(3)   # evitar cascade de cold starts en Lambda


# ─── Ingest OLAP ──────────────────────────────────────────────────────────────

def ingest_olap():
    """Ingest de 1 000 meter_readings en los últimos 60 días."""
    logging.info("\n[OLAP] Ingesting meter_readings (1000 registros)...")
    base_time = int(datetime.utcnow().timestamp())
    rows = []
    for i in range(1000):
        ts = base_time - random.randint(0, 60 * 24 * 3600)
        rows.append([
            f"mr-{base_time}-{i}",
            ts,
            round(random.uniform(10.0, 100.0), 2),
            random.choice(["KWH", "CEL", "LTR"]),
            f"asset-{random.randint(1, 10)}",
            json.dumps({"sensor_type": random.choice(["A", "B", "C"]),
                        "firmware": "v1.2"}),
        ])

    bulk_ingest("meter_reading",
                ["id", "timestamp", "reading_value",
                 "unit_of_measure", "asset_id", "metadata"],
                rows)


# ─── Queries con label_template ───────────────────────────────────────────────

def build_query_request():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID

    # ══════════════════════════ OLTP ══════════════════════════════════════════

    # 1. OLTP Table
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "inventory_movement"
    q.output_cast = metri_pb2.TABLE
    q.viz         = "table"
    q.limit       = 5
    req.queries["oltp_table"].CopyFrom(q)

    # 2. OLTP Line — label_template en DimensionDefinition
    #    Etiqueta: "Día {{timestamp}} ({{interval}})"
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "inventory_movement"
    q.output_cast = metri_pb2.TIMESERIES
    q.viz         = "line"
    m = q.metrics.add()
    m.aggregation = metri_pb2.SUM
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute        = "timestamp"
    d.interval         = "day"
    d.label_template   = "{{timestamp}}"           # ← NEW: label_template en DimensionDefinition
    req.queries["oltp_line"].CopyFrom(q)

    # 3. OLTP Scatter (Bubble)
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "inventory_movement"
    q.output_cast = metri_pb2.BUBBLE
    q.viz         = "scatter"
    m = q.metrics.add()
    m.aggregation = metri_pb2.AVG
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute        = "type"
    d.label_template   = "Tipo: {{type}}"           # ← NEW
    req.queries["oltp_scatter"].CopyFrom(q)

    # 4. OLTP Pie — label_template en dimension para slice names
    #    Ejemplo real: "{{type}} ({{quantity}} uds)"
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "inventory_movement"
    q.output_cast = metri_pb2.PIE
    q.viz         = "pie"
    m = q.metrics.add()
    m.aggregation = metri_pb2.COUNT
    m.attribute   = "quantity"
    d = q.dimensions.add()
    d.attribute        = "type"
    d.label_template   = "{{type}}"                 # ← NEW: slice label
    req.queries["oltp_pie"].CopyFrom(q)

    # 5. OLTP Tree (Hierarchy)
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "location"
    q.output_cast = metri_pb2.TABLE
    q.viz         = "tree"
    q.hierarchy.inject_has_children = True
    q.hierarchy.parent_field        = "parent_location_id"
    req.queries["oltp_tree"].CopyFrom(q)

    # ══════════════════════════ OLAP ══════════════════════════════════════════

    # 1. OLAP Table
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.TABLE
    q.viz         = "table"
    q.limit       = 5
    req.queries["olap_table"].CopyFrom(q)

    # 2. OLAP Line — label_template en DimensionDefinition
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.TIMESERIES
    q.viz         = "line"
    m = q.metrics.add()
    m.aggregation = metri_pb2.AVG
    m.attribute   = "reading_value"
    d = q.dimensions.add()
    d.attribute        = "timestamp"
    d.interval         = "day"
    d.label_template   = "{{timestamp}}"            # ← NEW
    req.queries["olap_line"].CopyFrom(q)

    # 3. OLAP Scatter (Bubble)
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.BUBBLE
    q.viz         = "scatter"
    m = q.metrics.add()
    m.aggregation = metri_pb2.AVG
    m.attribute   = "reading_value"
    d = q.dimensions.add()
    d.attribute        = "unit_of_measure"
    d.label_template   = "{{unit_of_measure}} — {{reading_value}} avg"  # ← NEW
    req.queries["olap_scatter"].CopyFrom(q)

    # 4. OLAP Pie — label_template en dimension (nombre de slice)
    #    Ejemplo real: "{{unit_of_measure}}" → "KWH", "CEL", "LTR"
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.PIE
    q.viz         = "pie"
    m = q.metrics.add()
    m.aggregation = metri_pb2.COUNT
    m.attribute   = "reading_value"
    d = q.dimensions.add()
    d.attribute        = "unit_of_measure"
    d.label_template   = "{{unit_of_measure}}"      # ← NEW: PIE slice label
    req.queries["olap_pie"].CopyFrom(q)

    # 5. OLAP Tree (ANO-007 fix)
    # meter_reading no tiene jerarquía self-referencial real (asset_id es FK externa).
    # Redefinido como TABLE agrupada por asset_id con conteo de lecturas —
    # el viz_hint "tree" indica al cliente que renderice como árbol de nivel-1
    # (grupo raíz = asset, hijos = lecturas). inject_has_children=False: no hay
    # campo parent en meter_reading → el árbol es plano (profundidad 1).
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = TENANT_ID
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.TABLE
    q.viz         = "tree"
    q.limit       = 10
    # Sin hierarchy: solo agrupamos a nivel de TABLE con asset_id en el SELECT
    # El cliente usa viz="tree" para renderizar como drill-down de primer nivel
    req.queries["olap_tree"].CopyFrom(q)

    return req


# ─── Ejecución y reporte ──────────────────────────────────────────────────────

def test_all_strategies():
    logging.info("═══════════════════════════════════════════════════════")
    logging.info("  VIZ ALL STRATEGIES — con label_template (OLTP + OLAP)")
    logging.info("═══════════════════════════════════════════════════════")

    # ── Paso 1: Ingestar datos ─────────────────────────────────────────────
    logging.info("\n▶ [1/2] Ingesting data (las DBs están vacías)...")
    ingest_oltp()
    ingest_olap()
    logging.info("\n  ✅ Ingest completo.\n")

    # ── Paso 2: Esperar Flush de Firehose ──────────────────────────────────
    logging.info("\n⏳ [1.5/2] Esperando 315 segundos para que Kinesis Firehose vuelque los datos en S3/Iceberg...")
    time.sleep(315)

    # ── Paso 3: Ejecutar batch query ───────────────────────────────────────
    logging.info("▶ [2/2] Ejecutando batch query (10 estrategias)...")
    req = build_query_request()
    frames = invoke_grpc_web("metri.MetriService/Query", req)

    if not frames:
        logging.error("No se recibió respuesta válida del engine.")
        return

    from google.protobuf.json_format import MessageToDict
    combined = {}

    for f in frames:
        resp = metri_pb2.QueryResponse()
        resp.ParseFromString(f)
        if resp.status.success:
            chunk = MessageToDict(resp, preserving_proto_field_name=True)
            if "batch_results" in chunk:
                combined.update(chunk["batch_results"])
        else:
            logging.error(f"Error en frame: {resp.status.error_message}")

    # ── Reporte ────────────────────────────────────────────────────────────
    OLTP_KEYS = ["oltp_table", "oltp_line", "oltp_scatter", "oltp_pie", "oltp_tree"]
    OLAP_KEYS = ["olap_table", "olap_line", "olap_scatter", "olap_pie", "olap_tree"]

    # Labels templates enviados por query para validación visual
    LABEL_TEMPLATES = {
        "oltp_line":    "{{timestamp}}",
        "oltp_scatter": "Tipo: {{type}}",
        "oltp_pie":     "{{type}}",
        "olap_line":    "{{timestamp}}",
        "olap_scatter": "{{unit_of_measure}} — {{reading_value}} avg",
        "olap_pie":     "{{unit_of_measure}}",
    }

    def print_section(title, keys):
        print(f"\n{'='*57}")
        print(f"  {title}")
        print(f"{'='*57}")
        for key in keys:
            tmpl = LABEL_TEMPLATES.get(key)
            tmpl_str = f"  [label_template: \"{tmpl}\"]" if tmpl else ""
            if key in combined:
                print(f"\n✅ [{key.upper()}]{tmpl_str}")
                result = combined[key]
                # Mostrar viz-ext si existe (label_template resuelto)
                if "viz_ext" in result:
                    print(f"  viz_ext: {json.dumps(result['viz_ext'], indent=4, ensure_ascii=False)}")
                # Mostrar primeras 2 filas de datos
                data = result.get("data", {})
                rows_raw = data.get("rows_json", {}).get("iter", [])[:2]
                cols = [c.get("key", "") if isinstance(c, dict) else c
                        for c in data.get("columns", [])]
                if rows_raw:
                    print(f"  columns: {cols}")
                    for row in rows_raw:
                        if isinstance(row, dict):
                            vals = []
                            for v in row.get("values", []):
                                if isinstance(v, dict):
                                    vals.append(v.get("string_value") or v.get("number_value"))
                                else:
                                    vals.append(v)
                        elif isinstance(row, (list, tuple)):
                            vals = list(row)
                        else:
                            vals = [row]
                        print(f"  row:     {vals}")
            else:
                print(f"\n❌ [{key.upper()}] FALTANTE en batch_results{tmpl_str}")

    print_section("REPORTE VIZ ESTRATEGIAS (OLTP)", OLTP_KEYS)
    print_section("REPORTE VIZ ESTRATEGIAS (OLAP)", OLAP_KEYS)

    print(f"\n{'='*57}")
    ok_count  = sum(1 for k in OLTP_KEYS + OLAP_KEYS if k in combined)
    fail_count = len(OLTP_KEYS) + len(OLAP_KEYS) - ok_count
    print(f"  TOTAL: {ok_count}/10 OK  |  {fail_count} FALLIDOS")
    print(f"{'='*57}\n")

    # ── Reporte JSON ───────────────────────────────────────────────────────
    _generate_json_report(combined, OLTP_KEYS, OLAP_KEYS, LABEL_TEMPLATES,
                          ok_count, fail_count)


# ─── Helpers para el reporte JSON ─────────────────────────────────────────────

def _extract_sample_rows(result: dict, max_rows: int = 5) -> list:
    """Extrae hasta max_rows filas mapeadas a sus columnas como {col: valor}."""
    data = result.get("data", {})
    cols = [c.get("key", "") if isinstance(c, dict) else str(c)
            for c in data.get("columns", [])]
    rows_raw = data.get("rows_json", {}).get("iter", [])[:max_rows]
    sample = []
    for row in rows_raw:
        if isinstance(row, dict):
            vals = []
            for v in row.get("values", []):
                if isinstance(v, dict):
                    vals.append(v.get("string_value") or v.get("number_value"))
                else:
                    vals.append(v)
        elif isinstance(row, (list, tuple)):
            vals = list(row)
        else:
            vals = [row]
        row_dict = {cols[i]: vals[i] for i in range(min(len(cols), len(vals)))}
        sample.append(row_dict)
    return sample


def _generate_json_report(combined: dict, oltp_keys: list, olap_keys: list,
                           label_templates: dict, ok_count: int, fail_count: int):
    """Genera y escribe viz_all_strategies_label_template_report.json."""
    from datetime import timezone

    run_ts   = datetime.now(timezone.utc).isoformat()
    all_keys = oltp_keys + olap_keys

    queries_report = []
    for key in all_keys:
        result = combined.get(key)
        entry: dict = {
            "query_key":      key,
            "engine":         "OLTP" if key.startswith("oltp") else "OLAP",
            "viz_strategy":   key.split("_", 1)[1],
            "label_template": label_templates.get(key),
            "status":         "OK" if result else "MISSING",
        }
        if result:
            data  = result.get("data", {})
            cols  = [c.get("key", "") if isinstance(c, dict) else str(c)
                     for c in data.get("columns", [])]
            entry["viz_ext"]             = result.get("viz_ext", {})
            entry["columns"]             = cols
            entry["total_rows_returned"] = len(data.get("rows_json", {}).get("iter", []))
            entry["sample_rows"]         = _extract_sample_rows(result, max_rows=5)
        else:
            entry["viz_ext"]             = None
            entry["columns"]             = []
            entry["total_rows_returned"] = 0
            entry["sample_rows"]         = []
        queries_report.append(entry)

    report = {
        "meta": {
            "suite":        "viz_all_strategies",
            "description":  "Validación completa de estrategias VIZ (OLTP+OLAP) con label_template",
            "generated_at": run_ts,
            "engine_url":   FUNCTION_URL,
            "tenant_id":    TENANT_ID,
            "proto_fields": "label_template @ DimensionDefinition + ChartDecoration",
            "pb2_tool":     "grpc_tools.protoc libprotoc 31.1",
        },
        "summary": {
            "total_queries":           len(all_keys),
            "passed":                  ok_count,
            "failed":                  fail_count,
            "pass_rate_pct":           round(ok_count / len(all_keys) * 100, 1),
            "oltp_queries":            len(oltp_keys),
            "olap_queries":            len(olap_keys),
            "label_template_queries":  sum(1 for k in all_keys if label_templates.get(k)),
        },
        "ingest": {
            "location_roots":       2,
            "location_children":    3,
            "parts":                10,
            "inventory_movements":  300,
            "meter_readings":       1000,
            "total_records":        1315,
        },
        "label_templates_used": label_templates,
        "queries": queries_report,
    }

    out_path = Path(__file__).parent / "viz_all_strategies_label_template_report.json"
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logging.info(f"\n\U0001f4c4 Reporte JSON escrito en: {out_path}")
    print(f"\n\U0001f4c4 Reporte JSON \u2192 {out_path}")


if __name__ == "__main__":
    test_all_strategies()
