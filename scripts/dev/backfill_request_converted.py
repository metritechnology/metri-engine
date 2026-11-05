#!/usr/bin/env python3.14
"""Backfill: solicitudes APPROVED con OT vinculada -> CONVERTED.

Máquina de seis estados del Códice (request.json): CONVERTED es el terminal
feliz — la solicitud aprobada cuya work_order ya existe. Las solicitudes
históricas quedaron en APPROVED cuando ese valor cubría ambos significados;
este backfill las mueve a CONVERTED solo si existe una work_order con
`request_id` apuntando a ellas. Las APPROVED sin OT se quedan como están
(aprobada, conversión pendiente — estado válido de la nueva máquina).

Uso:
    python3.14 scripts/dev/backfill_request_converted.py [--tenant T] [--dry-run]
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


def update_status(stub: GrpcWebStub, tenant_id: str, request_id: str, status: str) -> bool:
    struct_payload = struct_pb2.Struct()
    struct_payload.update({"id": request_id, "status": status})
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
    parser = argparse.ArgumentParser(description="Backfill APPROVED->CONVERTED en solicitudes con OT vinculada")
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    session = boto3.Session(profile_name=DEFAULT_PROFILE, region_name=DEFAULT_REGION)
    hmac_secret = resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    stub = GrpcWebStub(args.host, hmac_secret)

    print(f"Tenant: {args.tenant} · Host: {args.host}")
    approved = list_ids(stub, args.tenant, "request", {"status": "APPROVED"})
    print(f"  solicitudes APPROVED: {len(approved)}")

    moved = skipped = 0
    for rid in approved:
        has_wo = bool(list_ids(stub, args.tenant, "work_order", {"request_id": rid}))
        if not has_wo:
            skipped += 1
            print(f"  · {rid} sin OT vinculada — se queda APPROVED")
            continue
        if args.dry_run:
            print(f"  [DRY] {rid} -> CONVERTED")
            moved += 1
            continue
        if update_status(stub, args.tenant, rid, "CONVERTED"):
            moved += 1
            print(f"  ✓ {rid} -> CONVERTED")
        time.sleep(0.2)

    print(f"--- {moved} convertidas, {skipped} se quedan APPROVED (sin OT)")


if __name__ == "__main__":
    main()
