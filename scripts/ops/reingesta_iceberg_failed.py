#!/usr/bin/env python3
"""Reingesta de objetos `iceberg-failed` de Firehose — Fase 3 de PLAN_CORRECCIONES_PENDIENTES.md.

Los objetos en `errors/firehose/iceberg-failed/` del lake contienen, en `rawData`
(base64), los records que el destino Iceberg rechazó (causa histórica:
WarehouseLocation inexistente, ver MEDICION_COSTO_OLAP.md §5a). Cada línea de
`rawData` es UN JSON válido — exactamente el contrato que Firehose exige, así
que la reingesta es re-emitirlos por el stream de origen, deducible del nombre
del objeto: `metri-olap-stream-audit-log-1-2026-…` → `metri-olap-stream-audit-log`.

La idempotencia es gratis: `UniqueKeys: ["id"]` hace que el destino Iceberg
haga merge — reingestar dos veces no duplica filas.

Uso:
    python3 scripts/ops/reingesta_iceberg_failed.py            # dry-run: solo conteos
    python3 scripts/ops/reingesta_iceberg_failed.py --apply    # reemite y archiva
    python3 scripts/ops/reingesta_iceberg_failed.py --apply --no-archive

Requiere: boto3, AWS_PROFILE con acceso a S3 + Firehose (perfil metri-dev).
"""

import argparse
import base64
import json
import re
import sys
from collections import defaultdict

import boto3

BUCKET = "metri-lake-982592308819-us-east-1"
ERROR_PREFIX = "errors/firehose/iceberg-failed/"
ARCHIVE_PREFIX = "errors/firehose/reingested/"
BATCH_LIMIT = 450  # PutRecordBatch admite 500; margen de seguridad


def stream_from_key(key: str) -> str:
    """metri-olap-stream-audit-log-2-2026-07-30-… → metri-olap-stream-audit-log

    El sufijo del nombre es `-<intento>-<YYYY-MM-DD>-<HH-MM-SS>-<uuid>`.
    """
    name = key.rsplit("/", 1)[-1]
    stripped = re.split(r"-\d+-\d{4}-\d{2}-\d{2}-", name)[0]
    if stripped == name:
        raise ValueError(f"no puedo deducir el stream de {key}")
    return stripped


def decode_records(body: bytes):
    """Devuelve (payloads, ids): rawData base64 → una línea, un JSON válido."""
    payloads, ids = [], []
    for line in body.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        raw = entry.get("rawData")
        if not raw:
            continue
        payloads.append(base64.b64decode(raw))
        try:
            ids.append(json.loads(payloads[-1]).get("id", "?"))
        except json.JSONDecodeError:
            ids.append("<no-json>")
    return payloads, ids


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="emite los records (sin esto, dry-run)")
    ap.add_argument("--no-archive", action="store_true", help="con --apply: no mover los objetos a reingested/")
    args = ap.parse_args()

    s3 = boto3.client("s3")
    fh = boto3.client("firehose")

    keys, token = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": ERROR_PREFIX}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]

    if not keys:
        print("No hay objetos iceberg-failed — nada que hacer.")
        return 0

    # ── Dry-run: inventario por stream, sin tocar nada ──────────────────────
    per_stream = defaultdict(list)  # stream -> [(key, payloads, ids)]
    stats = defaultdict(lambda: {"objetos": 0, "records": 0, "ids": set()})
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        payloads, ids = decode_records(body)
        stream = stream_from_key(key)
        per_stream[stream].append((key, payloads, ids))
        st = stats[stream]
        st["objetos"] += 1
        st["records"] += len(payloads)
        st["ids"].update(ids)

    print(f"{'stream':40} {'objetos':>8} {'records':>8} {'ids distintos':>14}")
    for stream, st in sorted(stats.items()):
        print(f"{stream:40} {st['objetos']:>8} {st['records']:>8} {len(st['ids']):>14}")
    total_ids = sum(len(st["ids"]) for st in stats.values())
    print(f"\nEsperado en tablas tras el merge por id: {total_ids} filas nuevas como máximo.")
    if not args.apply:
        print("\nDRY-RUN — no se emitió nada. Repite con --apply para ejecutar.")
        return 0

    # ── Apply: PutRecordBatch por stream y archivo de los objetos ───────────
    emitted = failed = 0
    for stream, items in sorted(per_stream.items()):
        batch = [(f"reingesta-{i}", p) for (_, payloads, _) in items for i, p in enumerate(payloads)]
        for i in range(0, len(batch), BATCH_LIMIT):
            chunk = batch[i : i + BATCH_LIMIT]
            resp = fh.put_record_batch(
                DeliveryStreamName=stream,
                Records=[{"Data": d} for _, d in chunk],
            )
            emitted += len(chunk) - resp["FailedPutCount"]
            failed += resp["FailedPutCount"]
            if resp["FailedPutCount"]:
                for r in resp["RequestResponses"]:
                    if r.get("ErrorCode"):
                        print(f"  FALLO {stream}: {r['ErrorCode']} {r.get('ErrorMessage', '')[:120]}", file=sys.stderr)
        if args.no_archive:
            continue
        for key, _, _ in items:
            target = ARCHIVE_PREFIX + key[len(ERROR_PREFIX):]
            s3.copy_object(Bucket=BUCKET, Key=target, CopySource={"Bucket": BUCKET, "Key": key})
            s3.delete_object(Bucket=BUCKET, Key=key)

    print(f"\nEmitidos: {emitted} | Fallidos: {failed} | Archivados: {0 if args.no_archive else sum(len(v) for v in per_stream.values())} objetos")
    if failed:
        print("HAY FALLOS — los records fallidos NO se archivaron si el lote falló parcialmente;", file=sys.stderr)
        print("revisa la alarma metri-olap-*-iceberg-failed-rows y reingesta de nuevo (es idempotente).", file=sys.stderr)
        return 1
    print("La entrega a Iceberg tardará ~5 min (buffer). Concilia con una query por id contra cada tabla.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
