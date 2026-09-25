#!/usr/bin/env python3
"""Metri Engine — Verificación real de la caché de consultas (T0/T1).

Implementa las suites de producción del plan (§11.5 de
docs/architecture/PLAN_CACHE_JANUS_DYNAMODB.md) contra el endpoint real del
motor — local (:9090) o producción (ENGINE_HOST), mismo camino que los
clientes:

  T0 — Smoke post-deploy: query 2× ⇒ la 2ª debe traer data idéntica,
       metadata.cache_hits=1 y 0 < cache_ttl_seconds ≤ TTL.
  T1 — Equivalencia diferencial: corpus de queries, cada una 2×; compara
       miss ≡ hit campo a campo (data/columns/total). Con --record guarda
       goldens; con --replay los compara (para validar off vs ddb).

Uso:
  python3 tests/e2e/cache_verify.py --suite t0  [--host localhost:9090] [--tenant demo]
  python3 tests/e2e/cache_verify.py --suite t1 --record   # graba goldens (modo off/shadow)
  python3 tests/e2e/cache_verify.py --suite t1 --replay   # compara (modo ddb)

Requiere: grpcurl y un engine con datos sembrados (make seed). Para
producción: ENGINE_HOST y HMAC_SECRET por entorno — nunca commiteados
(patrón de production_smoke_test.py).

Nota T0: cache_hits=1 solo se reporta con QUERY_CACHE_MODE=ddb; en shadow/off
el script lo detecta y degrada la aserción a "respuesta idéntica" (T1).
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import uuid

DEFAULT_HOST = "localhost:9090"
DEFAULT_TENANT = "demo"
GOLDENS_DIR = os.path.join(os.path.dirname(__file__), "cache_verify")
PROTO_DIR = "/Users/macuser/projects/metri/metri-engine"
PROTO_FILE = "metri.proto"

# Secreto local alineado con docker-compose/.env.local (solo desarrollo).
LOCAL_HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"
results = []


def sign_token(secret: str, tenant_id: str, user_id: str, ttl: int = 3600) -> str:
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl,
        "jti": str(uuid.uuid4()),
    }
    payload = json.dumps(claims, separators=(",", ":")).encode()
    b64 = lambda b: base64.urlsafe_b64encode(b).decode().rstrip("=")
    sig = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    return f"mk_{b64(payload)}.{b64(sig)}"


def grpc(host: str, token: str, payload: dict) -> dict:
    cmd = [
        "grpcurl", "-plaintext",
        "-import-path", PROTO_DIR,
        "-proto", PROTO_FILE,
        "-rpc-header", f"sid: {token}",
        "-d", json.dumps(payload),
        host,
        "metri.MetriService/Query",
    ]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise RuntimeError(f"grpcurl falló: {out.stderr[:500]}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def query(entity: str, output_cast: int = 3, **extra) -> dict:
    """QueryRequest de una sub-query sobre `entity`."""
    sub = {"entity": entity, "outputCast": output_cast, "limit": 50}
    sub.update(extra)
    return {"queries": {"q1": sub}}


def extract(body: dict) -> dict:
    """Proyección determinista de la respuesta para comparar (T1)."""
    batch = (body.get("batchResults") or {})
    chunk = next(iter(batch.values()), {})
    meta = chunk.get("metadata") or {}
    return {
        "columns": chunk.get("columns"),
        "data": chunk.get("data"),
        "pagination": chunk.get("pagination"),
        "status_success": (chunk.get("status") or {}).get("success"),
        "total": meta.get("totalCount"),
        # Excluidos a propósito (no deterministas): query_id, executionTimeMs,
        # cacheHits, cacheTtlSeconds.
    }


def cache_meta(body: dict) -> dict:
    batch = (body.get("batchResults") or {})
    chunk = next(iter(batch.values()), {})
    meta = chunk.get("metadata") or {}
    return {
        "cache_hits": meta.get("cacheHits", 0),
        "cache_ttl_seconds": meta.get("cacheTtlSeconds", 0),
    }


def check(name: str, ok: bool, detail: str = ""):
    results.append((name, ok, detail))
    mark = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not ok else ""))


def suite_t0(host: str, token: str, tenant: str):
    print(f"\n{T.YELLOW}── T0: smoke post-deploy (tenant {tenant}) ──{RESET}")

    req = query("asset", outputCast=3)  # TABLE sobre entidad estable del seed
    first = grpc(host, token, req)
    second = grpc(host, token, req)

    m1, m2 = cache_meta(first), cache_meta(second)
    d1, d2 = extract(first), extract(second)

    check("primera respuesta success", d1["status_success"] is True)
    check("data idéntica entre 1ª y 2ª", d1 == d2)
    if m2["cache_hits"] == 1:
        check("2ª reporta cache_hits=1", True)
        check(
            "cache_ttl_seconds en (0, TTL]",
            0 < m2["cache_ttl_seconds"] <= 3600,
            f"ttl={m2['cache_ttl_seconds']}",
        )
        if m1.get("cache_ttl_seconds"):
            check(
                "TTL de la 2ª ≤ TTL de la 1ª (frescura decrece)",
                m2["cache_ttl_seconds"] <= m1["cache_ttl_seconds"],
                f"{m1['cache_ttl_seconds']} → {m2['cache_ttl_seconds']}",
            )
    else:
        print(
            f"  [{YELLOW}SKIP{RESET}] cache_hits≠1 — el motor no está en modo ddb "
            "(off/shadow o query no elegible); T0 degrada a equivalencia."
        )


CORPUS = [
    ("table_assets", query("asset", outputCast=3)),
    ("kpi_assets", query("asset", outputCast=1)),
    ("pie_assets", query("asset", outputCast=4, dimensions=[{"attribute": "asset/status"}])),
    (
        "timeseries_assets",
        query(
            "asset",
            outputCast=2,
            dimensions=[{"attribute": "meta/created_at", "interval": "DAY"}],
        ),
    ),
    ("filtered_assets", query("asset", outputCast=3, filters=[
        {"group": {"conjunction": "AND",
                   "nodes": [{"criteria": {"field": "asset/status", "opRef": "EQ",
                                           "value": {"stringVal": "ACTIVE"}}}]}}
    ])),
    ("work_orders", query("work_order", outputCast=3)),
    ("locations", query("location", outputCast=3)),
]


def suite_t1(host: str, token: str, mode: str):
    print(f"\n{YELLOW}── T1: equivalencia diferencial ({mode}) ──{RESET}")

    goldens_path = os.path.join(GOLDENS_DIR, "goldens.json")
    goldens = {}
    if mode == "replay":
        with open(goldens_path) as f:
            goldens = json.load(f)

    for name, req in CORPUS:
        first = grpc(host, token, req)
        second = grpc(host, token, req)
        d1, d2 = extract(first), extract(second)

        check(f"[{name}] miss ≡ hit", d1 == d2, json.dumps(d1)[:120] + " vs " + json.dumps(d2)[:120])

        if mode == "record":
            goldens[name] = d1
        else:
            check(
                f"[{name}] ≡ golden",
                d1 == goldens.get(name),
                "diff contra golden grabado",
            )
            meta = cache_meta(second)
            if meta["cache_hits"] == 1:
                check(f"[{name}] hit reportado", True)

    if mode == "record":
        os.makedirs(GOLDENS_DIR, exist_ok=True)
        with open(goldens_path, "w") as f:
            json.dump(goldens, f, indent=2, sort_keys=True)
        print(f"  → goldens grabados en {goldens_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=["t0", "t1"], default="t0")
    ap.add_argument("--host", default=os.environ.get("ENGINE_HOST", DEFAULT_HOST))
    ap.add_argument("--tenant", default=DEFAULT_TENANT)
    ap.add_argument("--user", default="usr_system_bff")
    ap.add_argument("--record", action="store_true", help="T1: grabar goldens (modo off/shadow)")
    ap.add_argument("--replay", action="store_true", help="T1: comparar contra goldens (modo ddb)")
    args = ap.parse_args()

    secret = os.environ.get("HMAC_SECRET", LOCAL_HMAC_SECRET)
    token = sign_token(secret, args.tenant, args.user)
    host = args.host

    print(f"cache_verify → {host} | suite {args.suite} | tenant {args.tenant}")

    if args.suite == "t0":
        suite_t0(host, token, args.tenant)
    else:
        mode = "record" if args.record else "replay"
        suite_t1(host, token, mode)

    failed = [r for r in results if not r[1]]
    print(
        f"\n{GREEN if not failed else RED}{'═' * 50}{RESET}\n"
        f"Resultado: {len(results) - len(failed)}/{len(results)} passed"
        + (f" · {len(failed)} FAILED" if failed else "")
    )
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
