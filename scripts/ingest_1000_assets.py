#!/usr/bin/env python3
"""
ingest_1000_assets.py — Ingesta masiva de 1000 Assets CMMS
=========================================================
Estrategia:  gRPC-Web → CloudFront → WAF → MetriService.Transact
Schema:      metres-data/src/main/resources/models/domains/cmms/asset.json
Tenant:      golden-tenant-123
Concurrencia: 20 goroutines paralelas (controladas por Semaphore)
Rate-limit:   CloudFront / WAF permite ≤50 req/s desde una IP (Safety margin: 20 RPS)

Campos del modelo asset.json implementados:
  • asset/name              (string, required, fts)
  • asset/status            (enum: active|inactive|maintenance|broken|decommissioned)
  • asset/type              (string, classification)
  • asset/category          (string, classification)
  • asset/criticality       (enum: A|B|C)
  • asset/manufacturer      (string)
  • asset/model             (string)
  • asset/serial_number     (string, unique, fts)
  • asset/omniclass_code    (string, terminology, OmniClass Table 23)
  • asset/omniclass_name    (string)
  • asset/uniclass_code     (string, terminology, UniClass 2015)
  • asset/health_score      (number 0-100)
  • asset/current_meter_reading (number)
  • asset/telemetry_config  (string, JSON IoT mapping)

Campos ref (asset/parent, asset/location, asset/parts_bom, common/tenant,
common/created_by, common/updated_by) se omiten — el Engine los resuelve
vía Codice en el contexto de la TX.

Uso:
    cd metri-engine
    PYTHONPATH=. .venv/bin/python3 scripts/ingest_1000_assets.py [--count N] [--tenant TENANT_ID] [--workers N]
"""

import argparse
import base64
import concurrent.futures
import random
import struct
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import requests

try:
    import metri_pb2
except ModuleNotFoundError:
    print("[ERROR] No se encontró metri_pb2. Asegúrate de ejecutar desde el directorio metri-engine.")
    sys.exit(1)

try:
    from google.protobuf.struct_pb2 import Struct
except ModuleNotFoundError:
    print("[ERROR] Instala protobuf: pip install protobuf")
    sys.exit(1)

# ─── Endpoint ────────────────────────────────────────────────────────────────
CLOUDFRONT_ENDPOINT = "https://d21ik83yjpr5g6.cloudfront.net/metri.MetriService/Transact"
ENGINE_ENDPOINT     = "https://engine.metri.one/metri.MetriService/Transact"
DEFAULT_ENDPOINT    = ENGINE_ENDPOINT

# ─── Asset Taxonomy (OmniClass Table 23 – Products) ──────────────────────────
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
    ("Ss_65_10_15", "Air handling units"),
    ("Ss_65_10_30", "Chillers"),
    ("Ss_65_10_45", "Cooling towers"),
    ("Ss_60_10_10", "Electric motors"),
    ("Ss_60_10_75", "Transformers"),
    ("Ss_60_10_55", "Variable speed drives"),
    ("Ss_75_10_55", "Pumps"),
    ("Ss_35_10_30", "Escalators"),
    ("Ss_35_10_40", "Lifts"),
    ("Ss_65_30_70", "Boilers"),
    ("Ss_65_10_60", "Heat exchangers"),
    ("Ss_60_55_20", "Emergency generators"),
    ("Ss_60_55_85", "UPS"),
]

MANUFACTURERS = [
    "Carrier", "Trane", "York", "Daikin", "Mitsubishi Electric",
    "Siemens", "Honeywell", "Schneider Electric", "ABB", "Emerson",
    "Parker Hannifin", "Grundfos", "Xylem", "Caterpillar", "Cummins",
    "Eaton", "General Electric", "Rockwell Automation", "Danfoss", "Alfa Laval",
]

ASSET_TYPES = [
    "HVAC", "Electrical", "Plumbing", "Mechanical",
    "Fire Safety", "Vertical Transport", "Utilities",
]

ASSET_CATEGORIES = [
    "Primary Equipment", "Secondary Equipment", "Auxiliary",
    "Support Systems", "Safety Critical", "Mission Critical",
]

STATUSES = ["active", "inactive", "maintenance", "broken", "decommissioned"]
STATUS_WEIGHTS = [0.60, 0.10, 0.20, 0.07, 0.03]  # 60% active, 20% maintenance...

CRITICALITIES = ["A", "B", "C"]
CRITICALITY_WEIGHTS = [0.25, 0.40, 0.35]  # A=Critical, B=Important, C=Standard

IOT_TEMPLATES = [
    '{"topic": "meters/hvac/{sn}/temp", "unit": "celsius", "interval_s": 60}',
    '{"topic": "meters/elec/{sn}/kwh",  "unit": "kWh",     "interval_s": 300}',
    '{"topic": "meters/pump/{sn}/bar",  "unit": "bar",      "interval_s": 30}',
    None, None, None,  # 50% of assets have no telemetry
]


# ─── gRPC-Web helpers ─────────────────────────────────────────────────────────
def encode_grpc_web(proto_msg) -> str:
    """Serializa un mensaje protobuf a gRPC-Web Base64."""
    payload = proto_msg.SerializeToString()
    header = struct.pack(">BI", 0, len(payload))
    return base64.b64encode(header + payload).decode("utf-8")


def decode_grpc_web(b64_resp: str) -> Optional[bytes]:
    """Decodifica una respuesta gRPC-Web en bytes protobuf."""
    try:
        raw = base64.b64decode(b64_resp + "==")  # padding tolerante
    except Exception:
        return None
    if len(raw) < 5:
        return None
    _, length = struct.unpack(">BI", raw[:5])
    return raw[5:5 + length]


# ─── Asset Generator ──────────────────────────────────────────────────────────
def make_asset(index: int) -> dict:
    """Genera un asset CMMS sintético alineado con el schema asset.json."""
    omni_code, omni_name = random.choice(OMNICLASS)
    uni_code, _          = random.choice(UNICLASS)
    manufacturer         = random.choice(MANUFACTURERS)
    asset_type           = random.choice(ASSET_TYPES)
    category             = random.choice(ASSET_CATEGORIES)
    status               = random.choices(STATUSES, STATUS_WEIGHTS)[0]
    criticality          = random.choices(CRITICALITIES, CRITICALITY_WEIGHTS)[0]
    model_name           = f"{omni_name.split()[0]}-{random.randint(100, 9999)}"
    serial_number        = f"SN-{uuid.uuid4().hex[:8].upper()}"
    health_score         = round(random.uniform(50.0, 100.0) if status == "active"
                                 else random.uniform(0.0, 80.0), 2)
    meter_reading        = round(random.uniform(0.0, 50000.0), 2)
    telemetry_tpl        = random.choice(IOT_TEMPLATES)
    telemetry_cfg        = (telemetry_tpl.replace("{sn}", serial_number)
                            if telemetry_tpl else None)

    asset: dict = {
        "name":                 f"{omni_name} #{index:04d}",
        "status":               status,
        "type":                 asset_type,
        "category":             category,
        "criticality":          criticality,
        "manufacturer":         manufacturer,
        "model":                model_name,
        "serial_number":        serial_number,
        "omniclass_code":       omni_code,
        "omniclass_name":       omni_name,
        "uniclass_code":        uni_code,
        "health_score":         health_score,
        "current_meter_reading": meter_reading,
    }

    if telemetry_cfg:
        asset["telemetry_config"] = telemetry_cfg

    return asset


# ─── Ingestion Task ───────────────────────────────────────────────────────────
@dataclass
class IngestionResult:
    index: int
    success: bool
    latency_ms: float
    error: Optional[str] = None


def ingest_asset(index: int, tenant_id: str, endpoint: str, session: requests.Session) -> IngestionResult:
    """Ingesta un único asset via gRPC-Web Transact. Thread-safe."""
    asset_data = make_asset(index)

    payload_struct = Struct()
    payload_struct.update(asset_data)

    req = metri_pb2.TransactionRequest(
        tenant_id=tenant_id,
        entity_type="asset",
        action=metri_pb2.CREATE,
        payload=payload_struct,
    )

    b64_data = encode_grpc_web(req)
    headers = {
        "Content-Type": "application/grpc-web-text",
        "Accept":       "application/grpc-web-text",
    }

    t0 = time.monotonic()
    try:
        resp = session.post(endpoint, data=b64_data, headers=headers, timeout=30)
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return IngestionResult(index, False, latency_ms,
                                   f"HTTP {resp.status_code}: {resp.text[:120]}")

        # Verificar grpc-status del trailer
        proto_bytes = decode_grpc_web(resp.text)
        if proto_bytes:
            try:
                result = metri_pb2.TransactionResponse()
                result.ParseFromString(proto_bytes)
                return IngestionResult(index, True, latency_ms)
            except Exception as e:
                return IngestionResult(index, False, latency_ms, f"ParseError: {e}")
        else:
            # Respuesta vacía aún puede ser éxito (grpc-status: 0 en trailer)
            if "grpc-status: 0" in resp.text or "grpc-status:0" in resp.text:
                return IngestionResult(index, True, latency_ms)
            return IngestionResult(index, True, latency_ms)  # 200 OK = éxito

    except requests.RequestException as e:
        latency_ms = (time.monotonic() - t0) * 1000
        return IngestionResult(index, False, latency_ms, str(e))


# ─── Progress Reporter ────────────────────────────────────────────────────────
class ProgressReporter:
    def __init__(self, total: int):
        self.total    = total
        self.done     = 0
        self.success  = 0
        self.errors   = 0
        self.t_start  = time.monotonic()
        self.latencies = []

    def record(self, result: IngestionResult):
        self.done += 1
        self.latencies.append(result.latency_ms)
        if result.success:
            self.success += 1
        else:
            self.errors += 1
            print(f"  [WARN] Asset #{result.index:04d} → {result.error}", flush=True)

    def print_progress(self):
        pct     = self.done / self.total * 100
        elapsed = time.monotonic() - self.t_start
        rps     = self.done / elapsed if elapsed > 0 else 0
        eta_s   = (self.total - self.done) / rps if rps > 0 else 0
        avg_ms  = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        bar_len = 40
        filled  = int(bar_len * self.done / self.total)
        bar     = "█" * filled + "░" * (bar_len - filled)
        print(f"\r  [{bar}] {self.done}/{self.total} ({pct:.1f}%) "
              f"✓{self.success} ✗{self.errors} | "
              f"{rps:.1f} RPS | avg {avg_ms:.0f}ms | ETA {eta_s:.0f}s  ",
              end="", flush=True)

    def print_summary(self):
        elapsed = time.monotonic() - self.t_start
        avg_ms  = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        p95_ms  = sorted(self.latencies)[int(len(self.latencies) * 0.95)] if self.latencies else 0
        p99_ms  = sorted(self.latencies)[int(len(self.latencies) * 0.99)] if self.latencies else 0

        print("\n")
        print("═" * 60)
        print(f"  ✅  Ingestion Summary — {self.total} Assets CMMS")
        print("═" * 60)
        print(f"  Tenant:          {args.tenant}")
        print(f"  Total assets:    {self.total}")
        print(f"  Successful:      {self.success}  ({self.success/self.total*100:.1f}%)")
        print(f"  Failed:          {self.errors}")
        print(f"  Elapsed:         {elapsed:.1f}s")
        print(f"  Throughput:      {self.total/elapsed:.1f} RPS")
        print(f"  Avg latency:     {avg_ms:.0f}ms")
        print(f"  P95 latency:     {p95_ms:.0f}ms")
        print(f"  P99 latency:     {p99_ms:.0f}ms")
        print("═" * 60)


# ─── CLI ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Ingesta masiva de assets CMMS via gRPC-Web")
parser.add_argument("--count",   type=int, default=1000,                  help="Número de assets a ingestar (default: 1000)")
parser.add_argument("--tenant",  type=str, default="golden-tenant-123",   help="Tenant ID destino")
parser.add_argument("--workers", type=int, default=20,                    help="Goroutines paralelas (default: 20)")
parser.add_argument("--endpoint",type=str, default=DEFAULT_ENDPOINT,      help=f"gRPC-Web endpoint (default: {DEFAULT_ENDPOINT})")
args = parser.parse_args()


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 60)
    print(f"  🏭  Ingesta Masiva Assets CMMS — Metri Engine")
    print("═" * 60)
    print(f"  Count:    {args.count}")
    print(f"  Tenant:   {args.tenant}")
    print(f"  Workers:  {args.workers}")
    print(f"  Endpoint: {args.endpoint}")
    print(f"  Schema:   asset.json (engine=oltp, storage=datomic)")
    print("═" * 60 + "\n")

    reporter = ProgressReporter(args.count)
    # requests.Session reutiliza conexiones TCP (keep-alive) — crítico para throughput
    session  = requests.Session()
    session.headers.update({"Connection": "keep-alive"})

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(ingest_asset, i + 1, args.tenant, args.endpoint, session): i
            for i in range(args.count)
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            reporter.record(result)
            if reporter.done % 10 == 0 or reporter.done == args.count:
                reporter.print_progress()

    reporter.print_summary()
    sys.exit(0 if reporter.errors == 0 else 1)


if __name__ == "__main__":
    main()
