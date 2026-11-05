#!/usr/bin/env python3
"""Backfill de `tags` en las 10 solicitudes sembradas (post-deploy del modelo).

Las solicitudes se crearon antes de que el modelo `request` en producción
declarara el atributo `tags` (el engine lo descartó fail-open). Una vez
desplegado el modelo, este script las actualiza (UPDATE) con sus etiquetas.

Uso:
    python3.14 scripts/dev/backfill_request_tags_prod.py [--tenant T]
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

# id de la solicitud sembrada → etiquetas (mismo orden que seed_requests_prod.py)
TAGS_BY_ID: dict[str, list[str]] = {
    "01M6KT8QFVTPBXJVDN8QPBTCB1": ["fuga", "hidráulica", "seguridad"],
    "01M6KT8TFHJ10PQNYH2A7PSKEX": ["ruido", "vibración"],
    "01M6KT8WX9QP2MR5SBK20440H9": ["eléctrico", "iluminación"],
    "01M6KT8Z9KZ1H1MHRQX1H8VWDN": ["vibración", "parada", "energía"],
    "01M6KT91XQJQPTDTWWA8QQEF7H": ["calidad", "infraestructura"],
    "01M6KT93SQ8M1Z4473WEXB6YF6": ["instrumentación", "caldera"],
    "01M6KT96Y18BW18TC2YK2TP0YC": ["parada", "planificada"],
    "01M6KT99GBTS85QKEX9T9CWQV7": ["seguridad", "falso-positivo"],
    "01M6KT9C4BD019DSD81PVQDK3Q": ["fuga", "calidad", "infraestructura"],
    "01M6KT9J8CC7JKJBHDW042Q4FF": ["seguridad", "señalización"],
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill de tags en solicitudes sembradas")
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    args = parser.parse_args()

    session = boto3.Session(profile_name=DEFAULT_PROFILE, region_name=DEFAULT_REGION)
    hmac_secret = resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    stub = GrpcWebStub(args.host, hmac_secret)

    updated = 0
    for entity_id, tags in TAGS_BY_ID.items():
        struct_payload = struct_pb2.Struct()
        struct_payload.update({"id": entity_id, "tags": tags})
        req = pb.TransactionRequest(
            tenant_id=args.tenant,
            entity_type="request",
            entity_id=entity_id,
            action=pb.UPDATE,
            payload=struct_payload,
        )
        body = stub._post("/metri.MetriService/Transact", req)
        ok = False
        for is_trailer, frame in stub._parse_frames(body):
            if not is_trailer:
                resp = pb.TransactionResponse()
                resp.ParseFromString(frame)
                ok = bool(resp.status.success)
                if not ok:
                    print(f"  [ERROR] {entity_id}: {resp.status.error_code} - {resp.status.error_message}")
        if ok:
            updated += 1
            print(f"  ✓ {entity_id} tags={tags}")
        time.sleep(0.2)

    print(f"--- {updated}/{len(TAGS_BY_ID)} solicitudes actualizadas")


if __name__ == "__main__":
    main()
