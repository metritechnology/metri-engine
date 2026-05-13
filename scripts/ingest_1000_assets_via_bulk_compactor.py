#!/usr/bin/env python3
"""
ingest_1000_assets_via_bulk_compactor.py
=========================================
Ingesta masiva de 1000 Assets CMMS a través del Metri Bulk Compactor Lambda.

ARQUITECTURA:
  Este script → invoca BulkCompactorFunction Lambda (Kinesis Firehose event)
                       ↓
               BulkCompactor.CompactorHandler()
                       ↓
               schema.ResolveEngine("asset") → EngineOLTP
                       ↓
               oltp.WriteToOLTP() — semáforo 5 concurrentes, retry x4
                       ↓
               MetriEngine.Transact (gRPC-Web → CloudFront → WAF)
                       ↓
               Datahike OLTP → metri-datahike-prod-v3 (DynamoDB)

BATCHING:
  Lambda Firehose tiene límite de 6MB por invocación.
  Assets CMMS pesan ~500 bytes JSON → 100 assets ≈ 50KB → seguro.
  1000 assets = 10 invocaciones × 100 assets.

SCHEMA (asset.json):
  tenant_id, entity_type son campos de envelope (routing).
  El resto son atributos CMMS: name, status, type, category, criticality,
  manufacturer, model, serial_number, omniclass_*, uniclass_code,
  health_score, current_meter_reading, telemetry_config.

Uso:
  cd metri-engine   (para tener acceso a los scripts de aws)
  python3 scripts/ingest_1000_assets_via_bulk_compactor.py [opciones]

  Opciones:
    --count N        Número de assets (default: 1000)
    --batch-size N   Assets por invocación Lambda (default: 100)
    --tenant ID      Tenant destino (default: golden-tenant-123)
    --dry-run        Solo genera el payload, no invoca Lambda
"""

import argparse
import base64
import json
import random
import sys
import time
import uuid
from typing import Iterator

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ─── Configuración ─────────────────────────────────────────────────────────────
FUNCTION_NAME = "metri-bulk-compactor-BulkCompactorFunction-lOktlOC3XwFf"
AWS_REGION    = "us-east-1"
AWS_PROFILE   = "metri-dev"

# ─── Taxonomía CMMS (alineada con asset.json) ─────────────────────────────────
OMNICLASS = [
    ("23-17 11 11", "Air Handling Units"),
    ("23-17 11 13", "Fan Coil Units"),
    ("23-17 13 11", "Chillers"),
    ("23-17 17 11", "Cooling Towers"),
    ("23-33 29 11", "Electric Motors"),
    ("23-33 31 11", "Variable Frequency Drives"),
    ("23-33 33 11", "Transformers"),
    ("23-35 31 11", "Fire Pumps"),
    ("23-35 33 11", "Jockey Pumps"),
    ("23-39 11 11", "Elevators"),
    ("23-39 13 11", "Escalators"),
    ("23-41 17 11", "Boilers"),
    ("23-41 19 11", "Heat Exchangers"),
    ("23-45 21 11", "Emergency Generators"),
    ("23-45 23 11", "UPS Systems"),
]

UNICLASS = [
    "Ss_65_10_15", "Ss_65_10_30", "Ss_65_10_45",
    "Ss_60_10_10", "Ss_60_10_75", "Ss_60_10_55",
    "Ss_75_10_55", "Ss_35_10_30", "Ss_35_10_40",
    "Ss_65_30_70", "Ss_65_10_60", "Ss_60_55_20",
]

MANUFACTURERS = [
    "Carrier", "Trane", "York", "Daikin", "Mitsubishi Electric",
    "Siemens", "Honeywell", "Schneider Electric", "ABB", "Emerson",
    "Parker Hannifin", "Grundfos", "Xylem", "Caterpillar", "Cummins",
    "Eaton", "General Electric", "Rockwell Automation", "Danfoss",
]

ASSET_TYPES      = ["HVAC", "Electrical", "Plumbing", "Mechanical",
                    "Fire Safety", "Vertical Transport", "Utilities"]
ASSET_CATEGORIES = ["Primary Equipment", "Secondary Equipment", "Auxiliary",
                    "Support Systems", "Safety Critical", "Mission Critical"]
STATUSES         = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
STATUS_WEIGHTS   = [0.60, 0.20, 0.20]
CRITICALITIES    = ["A", "B", "C"]
CRIT_WEIGHTS     = [0.25, 0.40, 0.35]
IOT_TEMPLATES    = [
    '{{"topic": "meters/hvac/{sn}/temp", "unit": "celsius", "interval_s": 60}}',
    '{{"topic": "meters/elec/{sn}/kwh",  "unit": "kWh",     "interval_s": 300}}',
    '{{"topic": "meters/pump/{sn}/bar",  "unit": "bar",      "interval_s": 30}}',
    None, None, None,  # 50% sin telemetría
]


# ─── Generator ────────────────────────────────────────────────────────────────
def make_asset_record(index: int, tenant_id: str) -> dict:
    """
    Genera un registro de Asset CMMS listo para el envelope de Kinesis Firehose.
    Los campos `tenant_id` y `entity_type` son leídos por groupByTenantAndEntity()
    en el CompactorHandler para el routing y el multitenant isolation.
    """
    omni_code, omni_name = random.choice(OMNICLASS)
    manufacturer         = random.choice(MANUFACTURERS)
    model_name           = f"{omni_name.split()[0]}-{random.randint(100, 9999)}"
    serial_number        = f"SN-{uuid.uuid4().hex[:8].upper()}"
    status               = random.choices(STATUSES, STATUS_WEIGHTS)[0]
    criticality          = random.choices(CRITICALITIES, CRIT_WEIGHTS)[0]
    health_score         = round(
        random.uniform(60.0, 100.0) if status == "active"
        else random.uniform(0.0, 75.0), 2
    )
    telemetry_tpl = random.choice(IOT_TEMPLATES)

    now_ms = int(time.time() * 1000)
    twelve_months_ms = 365 * 24 * 60 * 60 * 1000
    random_timestamp = now_ms - random.randint(0, twelve_months_ms)

    record = {
        # ── Envelope (leído por el CompactorHandler para routing) ──
        "tenant_id":   tenant_id,
        "entity_type": "asset",

        # ── Atributos CMMS (asset.json) ────────────────────────────
        "name":                   f"{omni_name} #{index:04d}",
        "status":                 status,
        "type":                   random.choice(ASSET_TYPES),
        "category":               random.choice(ASSET_CATEGORIES),
        "criticality":            criticality,
        "manufacturer":           manufacturer,
        "model":                  model_name,
        "serial_number":          serial_number,
        "omniclass_code":         omni_code,
        "omniclass_name":         omni_name,
        "uniclass_code":          random.choice(UNICLASS),
        "health_score":           health_score,
        "current_meter_reading":  round(random.uniform(0.0, 50_000.0), 2),
        "timestamp":              random_timestamp,
    }

    if telemetry_tpl:
        record["telemetry_config"] = telemetry_tpl.format(sn=serial_number)

    return record


def make_firehose_record(row: dict) -> dict:
    """
    Envuelve un row JSON en el formato KinesisFirehoseEventRecord que espera
    el `events.KinesisFirehoseEvent` del aws-lambda-go SDK:
      { recordId, approximateArrivalTimestamp, data: <base64> }
    """
    return {
        "recordId":                     str(uuid.uuid4()).replace("-", ""),
        "approximateArrivalTimestamp":  int(time.time() * 1000),
        "data":                         base64.b64encode(
                                            json.dumps(row).encode()
                                        ).decode(),
    }


def make_firehose_event(records: list[dict]) -> dict:
    """
    Construye el evento KinesisFirehoseEvent completo que recibe el Lambda:
    https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
    """
    return {
        "invocationId":      str(uuid.uuid4()),
        "deliveryStreamArn": f"arn:aws:firehose:{AWS_REGION}:982592308819:deliverystream/metri-olap-stream-asset",
        "region":            AWS_REGION,
        "records":           [make_firehose_record(r) for r in records],
    }


def chunked(lst: list, size: int) -> Iterator[list]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ─── Lambda Invoker ───────────────────────────────────────────────────────────
def invoke_batch(lambda_client, event: dict, batch_num: int, total_batches: int) -> dict:
    """Invoca el BulkCompactorFunction con un evento Firehose y retorna el resultado."""
    payload_bytes = json.dumps(event).encode()
    size_kb       = len(payload_bytes) / 1024

    print(f"  Batch {batch_num:02d}/{total_batches} "
          f"({len(event['records'])} records, {size_kb:.1f} KB) → invocando Lambda...",
          end=" ", flush=True)

    t0 = time.monotonic()
    try:
        response = lambda_client.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="RequestResponse",  # síncrono — esperamos el resultado
            Payload=payload_bytes,
        )
    except (BotoCoreError, ClientError) as e:
        elapsed = (time.monotonic() - t0) * 1000
        print(f"❌ ERROR AWS: {e}")
        return {"batch": batch_num, "success": False, "error": str(e),
                "elapsed_ms": elapsed}

    elapsed_ms = (time.monotonic() - t0) * 1000
    body_raw   = response["Payload"].read()

    try:
        body = json.loads(body_raw)
    except json.JSONDecodeError:
        body = {"raw": body_raw.decode(errors="replace")}

    status_code   = response.get("StatusCode", 0)
    function_err  = response.get("FunctionError", "")
    records_resp  = body.get("records", [])

    ok_count  = sum(1 for r in records_resp if r.get("result") == "Ok")
    fail_count= sum(1 for r in records_resp if r.get("result") != "Ok")

    if function_err:
        print(f"❌ FunctionError={function_err} | {elapsed_ms:.0f}ms")
        return {"batch": batch_num, "success": False,
                "error": function_err, "body": body, "elapsed_ms": elapsed_ms}

    print(f"✅ HTTP {status_code} | ✓{ok_count} ✗{fail_count} | {elapsed_ms:.0f}ms")
    return {"batch": batch_num, "success": True,
            "ok": ok_count, "fail": fail_count, "elapsed_ms": elapsed_ms}


# ─── CLI ──────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingesta masiva de assets CMMS vía Metri Bulk Compactor Lambda"
    )
    parser.add_argument("--count",      type=int, default=1000,
                        help="Total de assets a ingestar (default: 1000)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Assets por invocación Lambda (default: 100)")
    parser.add_argument("--tenant",     type=str, default="golden-tenant",
                        help="Tenant ID (default: golden-tenant)")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Genera payload pero NO invoca Lambda")
    return parser.parse_args()


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    total_batches = (args.count + args.batch_size - 1) // args.batch_size

    print("\n" + "═" * 65)
    print("  🏭  Ingesta Masiva Assets CMMS — vía Metri Bulk Compactor")
    print("═" * 65)
    print(f"  Schema:     asset.json  (engine=oltp → Datahike OLTP)")
    print(f"  Tenant:     {args.tenant}")
    print(f"  Total:      {args.count} assets")
    print(f"  Batch size: {args.batch_size} assets/invocación")
    print(f"  Batches:    {total_batches} invocaciones Lambda")
    print(f"  Lambda:     {FUNCTION_NAME}")
    print(f"  Dry-run:    {args.dry_run}")
    print("═" * 65 + "\n")

    # Generar todos los assets
    print(f"  Generando {args.count} registros CMMS...", end=" ", flush=True)
    all_rows = [make_asset_record(i + 1, args.tenant) for i in range(args.count)]
    print(f"OK ({len(all_rows)} registros)\n")

    if args.dry_run:
        sample = all_rows[0]
        print("  [DRY-RUN] Muestra del primer registro:")
        print(json.dumps(sample, indent=4))
        print("\n  [DRY-RUN] Ejemplo de evento Firehose (1 record):")
        event_sample = make_firehose_event([sample])
        print(json.dumps(event_sample, indent=2)[:800] + "\n  ...")
        sys.exit(0)

    # Crear cliente Lambda
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    lambda_client = session.client("lambda")

    # Invocar en batches
    results     = []
    total_ok    = 0
    total_fail  = 0
    t_start     = time.monotonic()

    batches = list(chunked(all_rows, args.batch_size))
    for i, batch_rows in enumerate(batches, start=1):
        event  = make_firehose_event(batch_rows)
        result = invoke_batch(lambda_client, event, i, len(batches))
        results.append(result)
        total_ok   += result.get("ok",   0)
        total_fail += result.get("fail", 0)

        # Pausa entre batches para no saturar el Engine (que tiene retry interno)
        if i < len(batches):
            time.sleep(1.0)

    # Resumen final
    elapsed = time.monotonic() - t_start
    latencies = [r["elapsed_ms"] for r in results]
    avg_ms    = sum(latencies) / len(latencies)
    failed_batches = [r for r in results if not r["success"]]

    print("\n" + "═" * 65)
    print(f"  ✅  Resumen de Ingesta")
    print("═" * 65)
    print(f"  Tenant:            {args.tenant}")
    print(f"  Assets enviados:   {args.count}")
    print(f"  Batches exitosos:  {len(results) - len(failed_batches)}/{len(results)}")
    print(f"  Records ✓ OK:      {total_ok}")
    print(f"  Records ✗ Fail:    {total_fail}")
    print(f"  Tiempo total:      {elapsed:.1f}s")
    print(f"  Lat. Lambda avg:   {avg_ms:.0f}ms/batch")
    if failed_batches:
        print(f"\n  ⚠️  Batches fallados:")
        for r in failed_batches:
            print(f"    Batch #{r['batch']:02d}: {r.get('error','?')}")
    print("═" * 65 + "\n")

    sys.exit(1 if failed_batches else 0)


if __name__ == "__main__":
    main()
