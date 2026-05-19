#!/usr/bin/env python3
"""
seed_local_data.py — Inyector de Entidades de Prueba (Fase 2)
=============================================================
Inyecta entidades locales usando los schemas en config/models/ (50 modelos).

Para cada entidad requerida por los tests, usa rpc Transact o rpc BulkIngest
para crear registros que ejerciten:
  - Jerarquías (parent_location_id, parent_asset_id)
  - Relaciones entre entidades
  - Todos los tipos de output_cast (KPI, TABLE, PIE, TIMESERIES, TREE)

Garantiza la regla Anti-Empty antes de cualquier test de lectura.

Uso:
    python seed_local_data.py                           # modo dry-run (imprime payloads)
    python seed_local_data.py --host localhost --port 50051 --tenant T1
    python seed_local_data.py --entity location         # solo una entidad
    python seed_local_data.py --bulk                    # usar BulkIngest en vez de Transact uno a uno
"""

import sys
import json
import uuid
import random
import argparse
import os
from typing import Optional
from pathlib import Path

# ── Intentar importar gRPC ──────────────────────────────────────────────────
try:
    import grpc
    import metri_pb2
    import metri_pb2_grpc
    from google.protobuf import struct_pb2
    from google.protobuf.json_format import ParseDict
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

# ═════════════════════════════════════════════════════════════════════════════
# CARGA DE SCHEMAS
# ═════════════════════════════════════════════════════════════════════════════

MODELS_DIR = Path(__file__).parent / "config" / "models"

def load_schema(entity: str) -> Optional[dict]:
    path = MODELS_DIR / f"{entity}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

def list_entities() -> list[str]:
    return sorted([p.stem for p in MODELS_DIR.glob("*.json")])

# ═════════════════════════════════════════════════════════════════════════════
# GENERADOR DE DATOS SINTÉTICOS
# ═════════════════════════════════════════════════════════════════════════════

FAKE_NAMES = {
    "location": [
        "Planta Norte", "Planta Sur", "Edificio Central", "Almacén A",
        "Sala de Control", "Subestación 1", "Torre de Enfriamiento",
        "Cuarto de Máquinas", "Zona de Producción", "Área Administrativa"
    ],
    "asset": [
        "Compresor Atlas", "Bomba Centrífuga B1", "Motor Eléctrico M3",
        "Generador Diesel", "Chiller Industrial", "Transformador T5",
        "Banda Transportadora", "Extrusor E2", "Turbina de Vapor",
        "Panel de Control PLC"
    ],
    "work_order": [
        "Mantenimiento preventivo mensual", "Inspección de rodamientos",
        "Cambio de aceite hidráulico", "Calibración de sensores",
        "Reparación de fuga hidráulica", "Reemplazo de correa de transmisión",
        "Limpieza de filtros de aire", "Revisión eléctrica general",
        "Lubricación de engranajes", "Actualización de firmware PLC"
    ],
    "user": [
        "Carlos Rodríguez", "María González", "Juan Pérez", "Ana Martínez",
        "Pedro López", "Laura Hernández", "Diego García", "Sofía Torres"
    ],
    "part": [
        "Rodamiento SKF 6205", "Sello hidráulico 50mm", "Correa Gates B75",
        "Filtro de aceite 90L", "Fusible 30A", "Sensor de temperatura PT100",
        "Válvula solenoide 1/2\"", "Cable THHN 12AWG"
    ],
    "provider": [
        "TechServ Industrial", "Mantenimiento Pro S.A.S.", "Electro Soluciones",
        "Hidráulica del Norte", "Calibración Técnica Ltda."
    ],
}

STATUS_OPTIONS = {
    "location":   ["ACTIVE", "INACTIVE"],
    "asset":      ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"],
    "work_order": ["OPEN", "IN_PROGRESS", "COMPLETED", "CANCELLED"],
    "part":       ["AVAILABLE", "LOW_STOCK", "OUT_OF_STOCK"],
    "user":       ["ACTIVE", "INACTIVE"],
}

def fake_name(entity: str, idx: int) -> str:
    names = FAKE_NAMES.get(entity, [])
    if names:
        return names[idx % len(names)]
    return f"{entity.title()} #{idx + 1}"

def fake_email(name: str) -> str:
    clean = name.lower().replace(" ", ".").replace("á","a").replace("é","e")
    return f"{clean}@metri.local"

def generate_payload(schema: dict, idx: int, context: dict) -> dict:
    """
    Genera un payload sintético para la entidad basado en su schema.
    context puede contener: location_ids, asset_ids, user_ids, etc.
    """
    entity = schema.get("entity", "unknown")
    attrs = schema.get("attributes", [])

    payload = {}
    for attr in attrs:
        name = attr["name"]
        atype = attr.get("type", "string")
        required = attr.get("required", False)
        auto_generate = attr.get("auto_generate")

        # Saltar auto-generados
        if auto_generate:
            continue

        # Campos especiales por nombre
        if name == "name":
            payload[name] = fake_name(entity, idx)
            continue
        if name == "email":
            payload[name] = fake_email(fake_name("user", idx))
            continue
        if name == "description":
            payload[name] = f"Registro de prueba #{idx + 1} para {entity}"
            continue

        # Parent FK (jerarquías)
        if name == "parent_location_id" and context.get("location_ids"):
            # 50% chance de ser raíz
            if idx % 2 == 0:
                payload[name] = None
            else:
                payload[name] = random.choice(context["location_ids"])
            continue

        if name == "parent_asset_id" and context.get("asset_ids"):
            if idx % 3 == 0:
                payload[name] = None
            else:
                payload[name] = random.choice(context["asset_ids"])
            continue

        # FK references a otras entidades
        if name == "location_id" and context.get("location_ids"):
            payload[name] = random.choice(context["location_ids"])
            continue
        if name == "asset_id" and context.get("asset_ids"):
            payload[name] = random.choice(context["asset_ids"])
            continue
        if name in ("assigned_to", "requester_id", "created_by") and context.get("user_ids"):
            payload[name] = random.choice(context["user_ids"])
            continue
        if name == "provider_id" and context.get("provider_ids"):
            payload[name] = random.choice(context["provider_ids"])
            continue

        # Por tipo
        if atype == "string":
            if not required:
                continue
            payload[name] = f"{name}_{idx}"

        elif atype == "enum":
            options = attr.get("options", STATUS_OPTIONS.get(entity, ["ACTIVE"]))
            payload[name] = options[idx % len(options)]

        elif atype == "number":
            if not required:
                continue
            payload[name] = round(random.uniform(10, 5000), 2)

        elif atype == "boolean":
            payload[name] = idx % 2 == 0

        elif atype in ("date", "datetime"):
            payload[name] = "2024-01-15T10:00:00Z"

        elif atype == "uuid":
            # Solo si requerido y no hay contexto
            if required:
                payload[name] = str(uuid.uuid4())

    return payload


# ═════════════════════════════════════════════════════════════════════════════
# PLAN DE SIEMBRA
# ═════════════════════════════════════════════════════════════════════════════

# Orden de creación respetando dependencias FK
SEED_PLAN = [
    # Nivel 0: sin dependencias
    {"entity": "user",         "count": 5,  "deps": []},
    {"entity": "company",      "count": 2,  "deps": []},
    {"entity": "role",         "count": 3,  "deps": []},
    {"entity": "provider",     "count": 5,  "deps": []},

    # Nivel 1: location jerárquica (raíces primero)
    {"entity": "location",     "count": 10, "deps": [],
     "note": "5 raíces + 5 hijos con parent_location_id"},

    # Nivel 2: asset con location
    {"entity": "asset",        "count": 15, "deps": ["location"],
     "note": "assets con location_id y parent_asset_id jerárquico"},

    # Nivel 3: part (sin deps complejas)
    {"entity": "part",         "count": 10, "deps": []},

    # Nivel 4: work_order con location + asset + user
    {"entity": "work_order",   "count": 20, "deps": ["location", "asset", "user"],
     "note": "work_orders distribuidos entre status: OPEN, IN_PROGRESS, COMPLETED"},

    # Nivel 5: tablas auxiliares de work_order
    {"entity": "work_order_task",      "count": 10, "deps": ["work_order"]},
    {"entity": "check_list",           "count": 5,  "deps": ["work_order"]},
    {"entity": "downtime_log",         "count": 8,  "deps": ["asset", "work_order"]},
    {"entity": "labor_log",            "count": 10, "deps": ["work_order", "user"]},

    # Mantenimiento preventivo
    {"entity": "preventive_maintenance","count": 5, "deps": ["asset", "location"]},

    # Inventario
    {"entity": "inventory_movement",   "count": 10, "deps": ["part", "location"]},

    # Audit + Meter readings
    {"entity": "meter_reading",        "count": 10, "deps": ["asset"]},
    {"entity": "note",                 "count": 5,  "deps": []},
]


class Seeder:
    def __init__(self, tenant_id: str, dry_run: bool = True,
                 host: str = "localhost", port: int = 50051):
        self.tenant_id = tenant_id
        self.dry_run = dry_run
        self.host = host
        self.port = port
        self.context: dict[str, list[str]] = {}  # entity -> [ids creados]
        self.stats: dict[str, int] = {}
        self._stub = None

    def _get_stub(self):
        if self._stub is None:
            if not GRPC_AVAILABLE:
                raise RuntimeError("grpcio no instalado. pip install grpcio")
            channel = grpc.insecure_channel(f"{self.host}:{self.port}")
            self._stub = metri_pb2_grpc.MetriServiceStub(channel)
        return self._stub

    def _transact(self, entity: str, payload: dict) -> Optional[str]:
        """Llama rpc Transact CREATE y retorna el entity_id creado."""
        if self.dry_run:
            fake_id = f"dry-{entity[:4]}-{uuid.uuid4().hex[:8]}"
            print(f"    [DRY-RUN] Transact {entity}: {json.dumps(payload, ensure_ascii=False)[:80]}...")
            return fake_id

        stub = self._get_stub()
        pb_payload = struct_pb2.Struct()
        pb_payload.update(payload)
        req = metri_pb2.TransactionRequest(
            tenant_id=self.tenant_id,
            entity_type=entity,
            action=metri_pb2.CREATE,
            payload=pb_payload,
        )
        try:
            resp = stub.Transact(req, timeout=10)
            if resp.status.success:
                return resp.entity_id
            else:
                print(f"    [ERROR] {entity}: {resp.status.error_code} — {resp.status.error_message}")
                return None
        except Exception as e:
            print(f"    [ERROR] {entity}: {e}")
            return None

    def _bulk_ingest(self, entity: str, payloads: list[dict]) -> int:
        """Llama rpc BulkIngest y retorna cuántos se ingirieron."""
        if self.dry_run:
            print(f"    [DRY-RUN] BulkIngest {entity}: {len(payloads)} registros")
            return len(payloads)

        stub = self._get_stub()
        schema = load_schema(entity)
        if not schema:
            return 0

        attrs = [a["name"] for a in schema.get("attributes", []) if not a.get("auto_generate")]
        if not attrs:
            return 0

        columns = [
            metri_pb2.ColumnSchema(key=k, label=k, type="string")
            for k in attrs
        ]

        rows = []
        for p in payloads:
            values = []
            for k in attrs:
                v = p.get(k)
                if v is None:
                    values.append(metri_pb2.google_dot_protobuf_dot_struct__pb2.Value(
                        null_value=0))
                elif isinstance(v, bool):
                    values.append(metri_pb2.google_dot_protobuf_dot_struct__pb2.Value(bool_value=v))
                elif isinstance(v, (int, float)):
                    values.append(metri_pb2.google_dot_protobuf_dot_struct__pb2.Value(number_value=float(v)))
                else:
                    values.append(metri_pb2.google_dot_protobuf_dot_struct__pb2.Value(string_value=str(v)))
            rows.append(metri_pb2.DataRow(values=values))

        rowset = metri_pb2.RowSet(
            columns=columns,
            rows_json=metri_pb2.DataRowList(iter=rows)
        )
        req = metri_pb2.BulkRequest(
            tenant_id=self.tenant_id,
            entity_type=entity,
            action=metri_pb2.CREATE,
            data=rowset,
        )
        try:
            resp = stub.BulkIngest(req, timeout=30)
            if resp.status.success:
                return resp.ingested_count
            else:
                print(f"    [ERROR] BulkIngest {entity}: {resp.status.error_message}")
                return 0
        except Exception as e:
            print(f"    [ERROR] BulkIngest {entity}: {e}")
            return 0

    def seed_entity(self, entity: str, count: int, use_bulk: bool = False):
        schema = load_schema(entity)
        if not schema:
            print(f"  ⚠  Schema no encontrado para '{entity}' — saltando")
            return

        print(f"\n  → Sembrando {count} registros de '{entity}'...")

        payloads = []
        for i in range(count):
            payload = generate_payload(schema, i, self.context)
            payloads.append(payload)

        if use_bulk:
            ingested = self._bulk_ingest(entity, payloads)
            self.stats[entity] = ingested
            print(f"     ✓ BulkIngest: {ingested}/{count} ingresados")
        else:
            created_ids = []
            for i, payload in enumerate(payloads):
                eid = self._transact(entity, payload)
                if eid:
                    created_ids.append(eid)
            self.context[f"{entity}_ids"] = created_ids
            self.stats[entity] = len(created_ids)
            print(f"     ✓ Transact: {len(created_ids)}/{count} creados")

    def run_full_seed(self, target_entities: Optional[list[str]] = None, use_bulk: bool = False):
        print(f"\n{'═'*60}")
        print(f"  SEED LOCAL DATA — tenant={self.tenant_id} dry_run={self.dry_run}")
        print(f"{'═'*60}")

        for step in SEED_PLAN:
            entity = step["entity"]
            count = step["count"]
            note = step.get("note", "")

            if target_entities and entity not in target_entities:
                continue

            # Verificar schema existe
            if not (MODELS_DIR / f"{entity}.json").exists():
                print(f"  ⚠  {entity}.json no encontrado — saltando")
                continue

            if note:
                print(f"\n  [{entity}] {note}")

            self.seed_entity(entity, count, use_bulk=use_bulk)

        # Reporte final
        print(f"\n{'─'*60}")
        print(f"  RESUMEN DE SIEMBRA")
        print(f"{'─'*60}")
        total = sum(self.stats.values())
        for entity, count in sorted(self.stats.items()):
            print(f"  ✓ {entity:<35} {count:>4} registros")
        print(f"{'─'*60}")
        print(f"  TOTAL: {total} registros creados")
        print(f"{'═'*60}\n")

    def verify_data_exists(self) -> dict[str, bool]:
        """
        Verifica que existen datos para las entidades clave antes de ejecutar tests.
        Usa rpc Query con output_cast=TABLE y limit=1.
        """
        if self.dry_run:
            print("\n  [DRY-RUN] Verificación Anti-Empty simulada")
            return {entity: True for step in SEED_PLAN
                    for entity in [step["entity"]]}

        stub = self._get_stub()
        results = {}
        key_entities = ["location", "asset", "work_order", "user", "part"]

        print(f"\n  Verificando Anti-Empty para {len(key_entities)} entidades...")
        for entity in key_entities:
            req = metri_pb2.QueryRequest(
                tenant_id=self.tenant_id,
                queries={
                    f"check_{entity}": metri_pb2.AnalyticsRequest(
                        tenant_id=self.tenant_id,
                        entity=entity,
                        limit=1,
                    )
                }
            )
            try:
                has_data = False
                for chunk in stub.Query(req, timeout=5):
                    if chunk.data and chunk.data.rows_json.iter:
                        has_data = True
                    break
                results[entity] = has_data
                icon = "✓" if has_data else "✗"
                print(f"  {icon} {entity}: {'datos presentes' if has_data else 'VACÍO — ejecutar seed'}")
            except Exception as e:
                results[entity] = False
                print(f"  ✗ {entity}: ERROR — {e}")

        return results


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Metri Local Data Seeder")
    parser.add_argument("--host",   default="localhost")
    parser.add_argument("--port",   type=int, default=50051)
    parser.add_argument("--tenant", default="demo")
    parser.add_argument("--dry-run",action="store_true", default=True,
                        help="Solo imprimir payloads sin enviar (default)")
    parser.add_argument("--execute", action="store_true",
                        help="Enviar requests reales al servidor gRPC")
    parser.add_argument("--bulk",   action="store_true",
                        help="Usar BulkIngest en vez de Transact uno a uno")
    parser.add_argument("--entity", nargs="+",
                        help="Sembrar solo estas entidades")
    parser.add_argument("--list",   action="store_true",
                        help="Listar entidades disponibles")
    parser.add_argument("--verify", action="store_true",
                        help="Verificar que existen datos antes de proceder")
    args = parser.parse_args()

    if args.list:
        entities = list_entities()
        print(f"\nEntidades disponibles en config/models/ ({len(entities)}):")
        for e in entities:
            print(f"  • {e}")
        return

    dry_run = not args.execute
    seeder = Seeder(
        tenant_id=args.tenant,
        dry_run=dry_run,
        host=args.host,
        port=args.port,
    )

    if args.verify:
        results = seeder.verify_data_exists()
        empty = [e for e, ok in results.items() if not ok]
        if empty:
            print(f"\n  Entidades vacías detectadas: {empty}")
            print("  Ejecutar: python seed_local_data.py --execute")
        return

    seeder.run_full_seed(
        target_entities=args.entity,
        use_bulk=args.bulk,
    )

    if not dry_run:
        print("\nVerificando datos post-seed...")
        seeder.verify_data_exists()


if __name__ == "__main__":
    main()
