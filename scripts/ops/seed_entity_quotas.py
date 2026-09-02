#!/usr/bin/env python3
"""
seed_entity_quotas.py — la cuota de escritura de un tenant, la que faltaba.

QUÉ ARREGLA
───────────
El `QuotaGuard` es fail-closed: sin fila `domain_quota` vigente para
`(tenant, entidad, WRITE_COUNT)` no se puede crear NADA de esa entidad. Y el
dominio que consulta es el tipo de entidad tal cual —`asset`, `form_template`—,
sin traducción.

El panel llevaba tiempo escribiendo dominios que no son entidades
(`cmms:assets`, `iot:messages`), así que esas filas existían y no gobernaban
nada, y las entidades de verdad no tenían ninguna. Resultado en producción:

    Quota001: No quota configured for tenant=... resource_domain=form_template
    limit_type=WRITE_COUNT

Este script deja al tenant en un estado sano:

  · La CUOTA POR DEFECTO (`resource_domain = "*"`), que el motor usa cuando no
    hay fila propia, con un contador independiente por dominio. Con ella, una
    entidad que nadie tarifó deja de nacer bloqueada.
  · Con `--all-entities`, además una fila por cada modelo del catálogo, para
    poder ajustar límites entidad a entidad desde el panel.

Es idempotente: lo que ya está vigente no se toca.

USO
───
    python3 scripts/ops/seed_entity_quotas.py --tenant 01M12AGKPCR3YDYW9HG9ZQYXS6
    python3 scripts/ops/seed_entity_quotas.py --tenant TNT --all-entities --dry-run
"""
import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))
sys.path.append(str(SCRIPT_DIR))

from seed_quotas_production import (  # noqa: E402
    DEFAULT_HMAC_SECRET,
    DEFAULT_HOST,
    GrpcWebStub,
)

try:
    import metri_pb2 as pb
    from google.protobuf import struct_pb2
    from google.protobuf.json_format import MessageToDict
    PROTO_AVAILABLE = True
except ImportError:
    PROTO_AVAILABLE = False

MODELS_DIR = ENGINE_ROOT / "config" / "models"

# El dominio comodín, tal como lo entiende `src/quota/resolver.rs`.
DEFAULT_QUOTA_DOMAIN = "*"

# Entidades que el guard ya exime: darles cuota no haría nada.
# Ver `cedar/authorizer.rs::is_quota_exempt`.
EXEMPT = {"tenant", "domain_quota", "quota", "domain_plugin", "tenant_plugin"}

# Entidades que el motor escribe por su cuenta —auditoría, outbox, trabajos—.
# Ponerles techo sería que el sistema se bloquee a sí mismo.
INTERNAL = {"audit_log", "outbox_event", "scheduled_job", "domain_fault", "sequence_registry"}

# `LIFETIME` a propósito: una cuota que renueva necesita que alguien abra el
# ciclo siguiente, y este script no es un proceso que corra cada mes. El motor
# sabe renovar la de un ciclo cerrado (`quota/resolver.rs::renew_latest`), pero
# lo sano para la cuota de respaldo es que no caduque nunca.
PERIOD_KEY = "LIFETIME"
RESET_STRATEGY = "FIXED"


def catalogo() -> list:
    """Los tipos de entidad del motor, según sus modelos."""
    entidades = []
    for ruta in sorted(MODELS_DIR.glob("*.json")):
        try:
            modelo = json.loads(ruta.read_text())
        except (OSError, json.JSONDecodeError) as e:
            print(f"  ⚠️  {ruta.name}: no se pudo leer ({e})")
            continue

        nombre = modelo.get("entity")
        if not nombre or nombre in EXEMPT or nombre in INTERNAL:
            continue
        entidades.append(nombre)

    return entidades


def cuotas_existentes(stub, tenant: str) -> dict:
    """`(dominio, tipo) → period_key` de lo que ya hay. Para no duplicar."""
    existentes = {}
    try:
        q_req = pb.QueryRequest(tenant_id=tenant)
        ar = pb.AnalyticsRequest(tenant_id=tenant, entity="domain_quota", limit=500)
        q_req.queries["q"].CopyFrom(ar)
        resp = MessageToDict(stub.Query(q_req), preserving_proto_field_name=True)

        datos = resp.get("batch_results", {}).get("q", {}).get("data", {})
        filas = datos.get("rows_json", {})
        if isinstance(filas, str):
            filas = json.loads(filas)
        claves = [c["key"] for c in datos.get("columns", [])]

        for fila in filas.get("iter", []):
            detalle = dict(zip(claves, fila["values"]))
            dominio = detalle.get("resource_domain")
            tipo = detalle.get("limit_type")
            if dominio and tipo:
                existentes[(dominio, tipo)] = detalle.get("period_key")
    except Exception as e:
        print(f"  ⚠️  No se pudieron leer las cuotas existentes ({e}); se intentará crear todo")

    return existentes


def crear(stub, tenant: str, dominio: str, limite: int, dry_run: bool) -> bool:
    payload = struct_pb2.Struct()
    payload.update({
        "tenant_id": tenant,
        "resource_domain": dominio,
        "limit_type": "WRITE_COUNT",
        "reset_strategy": RESET_STRATEGY,
        "period_key": PERIOD_KEY,
        "max_limit": limite,
        "current_usage": 0,
    })

    if dry_run:
        print(f"  · (dry-run) crearía {dominio} → {limite:,}")
        return True

    resp = stub.Transact(pb.TransactionRequest(
        tenant_id=tenant,
        entity_type="domain_quota",
        action=pb.CREATE,
        payload=payload,
    ))

    if resp.status and resp.status.success:
        print(f"  ✓ {dominio} → {limite:,}")
        return True

    print(f"  ✗ {dominio}: {resp.status.error_message if resp.status else 'error desconocido'}")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Cuotas de escritura por entidad y cuota por defecto")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--tenant", required=True, help="Tenant al que se le aprovisiona")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET)
    parser.add_argument("--default-limit", type=int, default=100000,
                        help="Techo de la cuota por defecto (por dominio, no en total)")
    parser.add_argument("--entity-limit", type=int, default=10000,
                        help="Techo de cada fila por entidad con --all-entities")
    parser.add_argument("--all-entities", action="store_true",
                        help="Además de la cuota por defecto, una fila por entidad del catálogo")
    parser.add_argument("--dry-run", action="store_true", help="Enseña qué haría y no escribe")

    args = parser.parse_args()

    if not PROTO_AVAILABLE:
        print("✗ Falta la librería protobuf de Python.")
        sys.exit(1)

    print("=" * 60)
    print("  CUOTAS DE ESCRITURA — APROVISIONAMIENTO")
    print("=" * 60)
    print(f"Host    : {args.host}")
    print(f"Tenant  : {args.tenant}")
    print(f"Modo    : {'catálogo completo' if args.all_entities else 'solo cuota por defecto'}")
    print("=" * 60)

    stub = GrpcWebStub(args.host, args.secret)
    existentes = cuotas_existentes(stub, args.tenant)

    objetivos = [(DEFAULT_QUOTA_DOMAIN, args.default_limit)]
    if args.all_entities:
        objetivos += [(e, args.entity_limit) for e in catalogo()]

    creadas = 0
    saltadas = 0

    for dominio, limite in objetivos:
        if (dominio, "WRITE_COUNT") in existentes:
            saltadas += 1
            continue
        if crear(stub, args.tenant, dominio, limite, args.dry_run):
            creadas += 1

    print()
    print(f"  Creadas: {creadas} · Ya existían: {saltadas} · Objetivos: {len(objetivos)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
