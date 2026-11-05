#!/usr/bin/env python3
"""Seed de solicitudes (request) en PRODUCCIÓN para verificación UI del plugin.

Crea 10 solicitudes variadas en el tenant real (estados, prioridades,
solicitantes usuario/invitado, activos, ubicaciones y etiquetas) para
ejercitar la bandeja, los filtros y la ficha de detalle.

Uso:
    python3.14 scripts/dev/seed_requests_prod.py [--tenant T] [--dry-run]
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


def transact(stub: GrpcWebStub, tenant_id: str, payload: dict) -> str | None:
    struct_payload = struct_pb2.Struct()
    struct_payload.update(payload)
    req = pb.TransactionRequest(
        tenant_id=tenant_id,
        entity_type="request",
        action=pb.CREATE,
        payload=struct_payload,
    )
    body = stub._post("/metri.MetriService/Transact", req)
    for is_trailer, frame in stub._parse_frames(body):
        if not is_trailer:
            resp = pb.TransactionResponse()
            resp.ParseFromString(frame)
            if resp.status.success:
                return resp.entity_id
            print(f"    [ERROR] {resp.status.error_code} - {resp.status.error_message}")
            return None
    return None


def explore_ids(stub: GrpcWebStub, tenant_id: str, entity: str, limit: int = 5) -> list[str]:
    req = pb.ExploreRequest(tenant_id=tenant_id, entity=entity, attribute="id", limit=limit)
    try:
        resp = stub.Explore(req)
        if resp.status and resp.status.success:
            return list(resp.values)
    except Exception as e:
        print(f"    [WARN] Explore {entity}: {e}")
    return []


def build_samples(users: list[str], assets: list[str], locations: list[str]) -> list[dict]:
    """10 solicitudes representativas del dominio CMMS (contenido en español)."""
    u = users[0] if users else None
    a1, a2 = (assets + [None, None])[:2]
    l1, l2 = (locations + [None, None])[:2]

    samples = [
        {
            "title": "Fuga de aceite en bomba hidráulica principal",
            "description": "Se observa charco de aceite bajo la bomba desde el turno de la mañana. La presión se mantiene, pero la fuga va a más.",
            "priority": "HIGH",
            "status": "PENDING",
            "tags": ["fuga", "hidráulica", "seguridad"],
            "asset_id": a1,
            "location_id": l1,
            "requested_by_user": u,
        },
        {
            "title": "Ruido anómalo en motor del transportador B",
            "description": "Zumbido metálico intermitente al arrancar en frío. Desaparece a los 10 minutos de operación.",
            "priority": "MEDIUM",
            "status": "PENDING",
            "tags": ["ruido", "vibración"],
            "asset_id": a2,
            "location_id": l1,
            "requested_by_user": u,
        },
        {
            "title": "Iluminación fundida en pasillo de almacén norte",
            "description": "Tres luminarias LED sin funcionar; zona con tránsito de montacargas.",
            "priority": "LOW",
            "status": "PENDING",
            "tags": ["eléctrico", "iluminación"],
            "location_id": l2,
            "requested_by_email": "operaciones.turno.noche@metri.one",
        },
        {
            "title": "Vibración excesiva en compresor de aire 2",
            "description": "La vibración supera lo habitual al tacto y el consumo eléctrico ha subido un 8% esta semana.",
            "priority": "CRITICAL",
            "status": "PENDING",
            "tags": ["vibración", "parada", "energía"],
            "asset_id": a1,
            "requested_by_user": u,
        },
        {
            "title": "Puerta de muelle de carga no cierra hermética",
            "description": "Entra aire y polvo; afecta a la cámara de producto refrigerado contigua.",
            "priority": "MEDIUM",
            "status": "APPROVED",
            "tags": ["calidad", "infraestructura"],
            "location_id": l2,
            "requested_by_email": "calidad.planta@metri.one",
        },
        {
            "title": "Sensor de temperatura de caldera da lecturas erráticas",
            "description": "Saltos de ±15 °C sin cambio real de proceso. Sospecha de cableado o sonda degradada.",
            "priority": "HIGH",
            "status": "APPROVED",
            "tags": ["instrumentación", "caldera"],
            "asset_id": a2,
            "requested_by_user": u,
        },
        {
            "title": "Solicitud de parada programada para cambio de correas",
            "description": "Ventana solicitada para el sábado 06:00-10:00. Correas ya en almacén.",
            "priority": "MEDIUM",
            "status": "APPROVED",
            "tags": ["parada", "planificada"],
            "asset_id": a1,
            "location_id": l1,
            "requested_by_user": u,
        },
        {
            "title": "Falso aviso de humos en panel del sector 3",
            "description": "Se repitió dos veces sin presencia de humo; probable detector sucio.",
            "priority": "LOW",
            "status": "DECLINED",
            "tags": ["seguridad", "falso-positivo"],
            "location_id": l1,
            "requested_by_email": "seguridad.patrimonial@metri.one",
        },
        {
            "title": "Goteo en tejado sobre línea de empaquetado",
            "description": "Aparece solo con lluvia intensa. Riesgo de contaminación de producto.",
            "priority": "HIGH",
            "status": "PENDING",
            "tags": ["fuga", "calidad", "infraestructura"],
            "location_id": l2,
            "requested_by_user": u,
        },
        {
            "title": "Actualizar señalización de rutas de evacuación",
            "description": "Tras la redistribución de racks, dos rutas señalizadas quedaron obstruidas.",
            "priority": "MEDIUM",
            "status": "PENDING",
            "tags": ["seguridad", "señalización"],
            "requested_by_email": "hse@metri.one",
        },
    ]
    # Descartar claves sin valor (p. ej. referencias no encontradas)
    return [{k: v for k, v in s.items() if v is not None} for s in samples]


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed de 10 solicitudes de ejemplo en producción")
    parser.add_argument("--tenant", default=DEFAULT_TENANT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    session = boto3.Session(profile_name=DEFAULT_PROFILE, region_name=DEFAULT_REGION)
    hmac_secret = resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    stub = GrpcWebStub(args.host, hmac_secret)

    print(f"Tenant: {args.tenant} · Host: {args.host}")
    users = explore_ids(stub, args.tenant, "user")
    assets = explore_ids(stub, args.tenant, "asset")
    locations = explore_ids(stub, args.tenant, "location")
    print(f"  refs: users={len(users)} assets={len(assets)} locations={len(locations)}")

    samples = build_samples(users, assets, locations)
    include_tags = True
    created = 0
    for s in samples:
        if args.dry_run:
            print(f"  [DRY] {s['title']} · {s['status']}/{s['priority']} · tags={s.get('tags')}")
            created += 1
            continue
        payload = dict(s)
        if not include_tags:
            payload.pop("tags", None)
        entity_id = transact(stub, args.tenant, payload)
        if entity_id is None and include_tags and "tags" in payload:
            # El modelo de producción aún puede no tener `tags` (deploy en
            # gate): reintentar sin etiquetas y seguir sin ellas.
            print("    [WARN] reintentando sin tags (modelo prod aún sin el atributo)")
            include_tags = False
            payload.pop("tags", None)
            entity_id = transact(stub, args.tenant, payload)
        if entity_id:
            created += 1
            print(f"  ✓ {s['title'][:52]:<52} {s['status']:<15} id={entity_id}")
        time.sleep(0.2)

    print(f"--- {created}/{len(samples)} solicitudes creadas (tags={'sí' if include_tags else 'no'})")


if __name__ == "__main__":
    main()
