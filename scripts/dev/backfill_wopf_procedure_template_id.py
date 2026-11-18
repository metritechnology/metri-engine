#!/usr/bin/env python3.14
"""Backfill: work_order_procedure_field.procedure_template_id.

El Códice declara `work_order_procedure_field.procedure_template_id`
(referencia indexada y dimensión hacia `procedure_template`) para permitir
filtrar y agrupar respuestas de pasos por procedimiento maestro a través de
múltiples órdenes de trabajo.

Las filas nuevas creadas por `metri-cmms-plugin` (`wo-composer` y
`procedure-score-finalizer`) y los snapshots del frontend ya lo diligencian.
Este script rellena de forma idempotente las filas históricas recorriendo las
instancias `work_order_procedure` del tenant y sellando `procedure_template_id`
con `suppress_events=True` en cada `work_order_procedure_field` que aún no lo
tenga.

Uso:
    python3.14 scripts/dev/backfill_wopf_procedure_template_id.py [--tenant T] [--host H] [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3

SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))
sys.path.append(str(ENGINE_ROOT / "scripts" / "ops"))

import metri_pb2 as pb  # noqa: E402
from google.protobuf import struct_pb2  # noqa: E402
from bootstrap_production import (  # noqa: E402
    DEFAULT_HOST,
    DEFAULT_HMAC_SECRET_NAME,
    DEFAULT_PROFILE,
    DEFAULT_REGION,
    GrpcWebStub,
    resolve_hmac_secret_from_aws,
)

DEFAULT_TENANT = "01M642NFTHSE99K1VJFFDYFN31"


def query_rows(
    stub: GrpcWebStub,
    tenant_id: str,
    entity: str,
    eq_filters: dict[str, str] | None = None,
    limit: int = 1000,
) -> list[dict[str, object]]:
    filters = []
    for field, val in (eq_filters or {}).items():
        filters.append(
            pb.FilterNode(
                criteria=pb.FilterCriteria(
                    field=field,
                    value=pb.FilterValue(string_val=val),
                    op_ref=pb.EQ,
                )
            )
        )
    req = pb.QueryRequest(
        tenant_id=tenant_id,
        queries={
            "q": pb.AnalyticsRequest(
                tenant_id=tenant_id,
                entity=entity,
                filters=filters,
                limit=limit,
            )
        },
    )
    body = stub._post("/metri.MetriService/Query", req)
    for is_trailer, frame in stub._parse_frames(body):
        if is_trailer:
            continue
        resp = pb.QueryResponse()
        resp.ParseFromString(frame)
        effective = resp
        if not resp.HasField("data") and "q" in resp.batch_results:
            effective = resp.batch_results["q"]
        if not effective.status.success:
            print(
                f"    [ERROR] Query {entity}: "
                f"{effective.status.error_code} - {effective.status.error_message}"
            )
            return []
        cols = [c.key for c in effective.data.columns]
        rows: list[dict[str, object]] = []
        for row in effective.data.rows_json.iter:
            item: dict[str, object] = {}
            for idx, key in enumerate(cols):
                if idx >= len(row.values):
                    continue
                v = row.values[idx]
                kind = v.WhichOneof("kind")
                if kind == "string_value":
                    item[key] = v.string_value
                elif kind == "number_value":
                    item[key] = v.number_value
                elif kind == "bool_value":
                    item[key] = v.bool_value
            rows.append(item)
        return rows
    return []


def row_id(row: dict[str, object]) -> str:
    val = row.get("id") or row.get("entity/ulid") or ""
    return str(val).strip()


def update_field_template_id(
    stub: GrpcWebStub,
    tenant_id: str,
    field_id: str,
    procedure_template_id: str,
) -> bool:
    payload = struct_pb2.Struct()
    payload.update({"id": field_id, "procedure_template_id": procedure_template_id})
    req = pb.TransactionRequest(
        tenant_id=tenant_id,
        entity_type="work_order_procedure_field",
        entity_id=field_id,
        action=pb.UPDATE,
        payload=payload,
        suppress_events=True,
    )
    body = stub._post("/metri.MetriService/Transact", req)
    for is_trailer, frame in stub._parse_frames(body):
        if is_trailer:
            continue
        resp = pb.TransactionResponse()
        resp.ParseFromString(frame)
        if resp.status.success:
            return True
        print(
            f"    [ERROR] UPDATE work_order_procedure_field/{field_id}: "
            f"{resp.status.error_code} - {resp.status.error_message}"
        )
        return False
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill work_order_procedure_field.procedure_template_id"
    )
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    session = boto3.Session(profile_name=DEFAULT_PROFILE, region_name=DEFAULT_REGION)
    hmac_secret = resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    stub = GrpcWebStub(args.host, hmac_secret)

    print(f"Tenant: {args.tenant} · Host: {args.host} · Dry-run: {args.dry_run}")
    procedures = query_rows(stub, args.tenant, "work_order_procedure")
    print(f"  instancias work_order_procedure encontradas: {len(procedures)}")

    tpl_field_cache: dict[str, str] = {}
    updated = already_ok = missing_tpl = 0

    for proc in procedures:
        proc_id = row_id(proc)
        if not proc_id:
            continue
        tpl_id = str(proc.get("procedure_template_id") or "").strip()
        fields = query_rows(
            stub,
            args.tenant,
            "work_order_procedure_field",
            {"work_order_procedure_id": proc_id},
        )
        for fld in fields:
            fid = row_id(fld)
            if not fid:
                continue
            current_tpl = str(fld.get("procedure_template_id") or "").strip()
            if current_tpl:
                already_ok += 1
                continue

            resolved_tpl = tpl_id
            if not resolved_tpl:
                tpl_field_id = str(fld.get("procedure_template_field_id") or "").strip()
                if tpl_field_id:
                    if tpl_field_id not in tpl_field_cache:
                        tf_rows = query_rows(
                            stub,
                            args.tenant,
                            "procedure_template_field",
                            {"entity/ulid": tpl_field_id},
                        )
                        tpl_field_cache[tpl_field_id] = (
                            str(tf_rows[0].get("procedure_template_id") or "").strip()
                            if tf_rows
                            else ""
                        )
                    resolved_tpl = tpl_field_cache[tpl_field_id]

            if not resolved_tpl:
                missing_tpl += 1
                continue

            if args.dry_run:
                print(f"  [DRY] {fid}.procedure_template_id = {resolved_tpl} (proc={proc_id})")
                updated += 1
                continue

            if update_field_template_id(stub, args.tenant, fid, resolved_tpl):
                updated += 1
                print(f"  ✓ {fid}.procedure_template_id = {resolved_tpl}")
            time.sleep(0.05)

    print(
        f"--- actualizados: {updated}, ya diligenciados: {already_ok}, "
        f"sin plantilla origen: {missing_tpl}"
    )


if __name__ == "__main__":
    main()
