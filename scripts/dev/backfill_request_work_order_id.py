#!/usr/bin/env python3.14
"""Backfill: request.work_order_id (back-pointer 1:1 solicitud -> OT).

El Códice declara `request.work_order_id` (referencia a work_order) como
back-pointer desnormalizado: la fuente de verdad del vínculo sigue siendo
`work_order.request_id`, y las solicitudes convertidas DESPUÉS de la
introducción del campo lo reciben en la misma escritura que las marca
CONVERTED (hook onAfterSave del formulario de OT en metri-app). Las
convertidas ANTES de ese deploy no lo tienen: este backfill lo rellena
recorriendo las solicitudes CONVERTED y buscando su OT por `request_id`.

Idempotente: reescribir el mismo valor no rompe nada; se puede reejecutar.

Uso:
    python3.14 scripts/dev/backfill_request_work_order_id.py [--tenant T] [--dry-run]
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


def list_ids(stub: GrpcWebStub, tenant_id: str, entity: str, filters: dict[str, str]) -> list[str]:
    req = pb.ListEntitiesRequest(tenant_id=tenant_id, entity_type=entity, limit=500)
    for k, v in filters.items():
        req.filters[k] = v
    body = stub._post("/metri.MetriService/ListEntities", req)
    for is_trailer, frame in stub._parse_frames(body):
        if not is_trailer:
            resp = pb.ListEntitiesResponse()
            resp.ParseFromString(frame)
            if resp.status.success:
                if resp.truncated:
                    print("    [WARN] resultado truncado en 500 — reejecutar hasta 0 cambios")
                return list(resp.entity_ids)
            print(f"    [ERROR] ListEntities {entity}: {resp.status.error_code} - {resp.status.error_message}")
            return []
    return []


def set_work_order_id(stub: GrpcWebStub, tenant_id: str, request_id: str, work_order_id: str) -> bool:
    struct_payload = struct_pb2.Struct()
    struct_payload.update({"id": request_id, "work_order_id": work_order_id})
    req = pb.TransactionRequest(
        tenant_id=tenant_id,
        entity_type="request",
        action=pb.UPDATE,
        payload=struct_payload,
    )
    body = stub._post("/metri.MetriService/Transact", req)
    for is_trailer, frame in stub._parse_frames(body):
        if not is_trailer:
            resp = pb.TransactionResponse()
            resp.ParseFromString(frame)
            if resp.status.success:
                return True
            print(f"    [ERROR] UPDATE {request_id}: {resp.status.error_code} - {resp.status.error_message}")
            return False
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill request.work_order_id en solicitudes CONVERTED")
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    session = boto3.Session(profile_name=DEFAULT_PROFILE, region_name=DEFAULT_REGION)
    hmac_secret = resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    stub = GrpcWebStub(args.host, hmac_secret)

    print(f"Tenant: {args.tenant} · Host: {args.host}")
    converted = list_ids(stub, args.tenant, "request", {"status": "CONVERTED"})
    print(f"  solicitudes CONVERTED: {len(converted)}")

    linked = skipped = 0
    for rid in converted:
        wos = list_ids(stub, args.tenant, "work_order", {"request_id": rid})
        if not wos:
            skipped += 1
            print(f"  · {rid} CONVERTED sin OT por request_id — dato inconsistente, se omite")
            continue
        wo_id = wos[0]
        if len(wos) > 1:
            print(f"  [WARN] {rid} tiene {len(wos)} OTs por request_id; se enlaza la primera ({wo_id})")
        if args.dry_run:
            print(f"  [DRY] {rid}.work_order_id = {wo_id}")
            linked += 1
            continue
        if set_work_order_id(stub, args.tenant, rid, wo_id):
            linked += 1
            print(f"  ✓ {rid}.work_order_id = {wo_id}")
        time.sleep(0.2)

    print(f"--- {linked} enlazadas, {skipped} omitidas")


if __name__ == "__main__":
    main()
