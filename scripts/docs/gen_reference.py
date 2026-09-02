#!/usr/bin/env python3
"""Genera docs/reference/ desde las fuentes únicas del motor.

La documentación de referencia NUNCA se escribe a mano (Fase 5B, regla 5):
- api-grpc.md        ← proto/metri.proto
- codigos-error.md   ← config/errors/error_catalog.toml
- modelos-codice.md  ← config/models/*.json

El CI falla si el generado está desactualizado: `python3 scripts/docs/gen_reference.py --check`.
"""
from __future__ import annotations
import json, re, sys, tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REF = ROOT / "docs" / "reference"

def gen_api_grpc() -> str:
    proto = (ROOT / "proto" / "metri.proto").read_text()
    lines = ["# Referencia gRPC", "",
             "> Generado desde `proto/metri.proto` — la fuente única. No editar a mano.",
             "> Regenerar: `python3 scripts/docs/gen_reference.py`", ""]
    current_service = None
    for raw in proto.splitlines():
        l = raw.strip()
        m = re.match(r"service\s+(\w+)\s*\{", l)
        if m:
            current_service = m.group(1)
            lines += [f"## Servicio `{current_service}`", ""]
            continue
        m = re.match(r"rpc\s+(\w+)\s*\(", l)
        if m and current_service:
            lines.append(f"- `{current_service}.{m.group(1)}`")
            continue
        m = re.match(r"message\s+(\w+)", l)
        if m:
            lines.append(f"")
            lines.append(f"### Mensaje `{m.group(1)}`")
            continue
        m = re.match(r"enum\s+(\w+)", l)
        if m:
            lines.append(f"")
            lines.append(f"### Enum `{m.group(1)}`")
    # deduplicar RPCs
    seen, body = set(), []
    for l in lines:
        body.append(l)
    return "\n".join(body) + "\n"

def gen_errores() -> str:
    data = tomllib.loads((ROOT / "config/errors/error_catalog.toml").read_text())
    out = ["# Códigos de error", "",
           "> Generado desde `config/errors/error_catalog.toml` (catálogo canónico).",
           "> Regenerar: `python3 scripts/docs/gen_reference.py`", "",
           f"Versión del catálogo: **{data.get('version')}**", ""]
    by_family: dict[str, list] = {}
    for e in data.get("errors", []):
        by_family.setdefault(e.get("family", "?"), []).append(e)
    for fam in sorted(by_family):
        out += [f"## Familia `{fam}` ({len(by_family[fam])} códigos)", "",
                "| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |",
                "|---|---|---|---|---|---|"]
        for e in sorted(by_family[fam], key=lambda x: x["code"]):
            out.append(f"| `{e['code']}` | {e.get('stage','')} | {e.get('http_status','')} | "
                       f"{e.get('grpc_status','')} | {'sí' if e.get('retryable') else 'no'} | {e.get('description','')} |")
        out.append("")
    return "\n".join(out)

def gen_modelos() -> str:
    models_dir = ROOT / "config" / "models"
    out = ["# Modelos del Códice", "",
           "> Generado desde `config/models/*.json` — el registro SSOT de esquemas.",
           "> Regenerar: `python3 scripts/docs/gen_reference.py`", "",
           f"Total de modelos: **{len(list(models_dir.glob('*.json')))}**", "",
           "| Entidad | Motor | Atributos |", "|---|---|---|"]
    for f in sorted(models_dir.glob("*.json")):
        m = json.loads(f.read_text())
        attrs = m.get("attributes", [])
        names = ", ".join(
            f"`{a['name']}`" if isinstance(a, dict) else f"`{a}`" for a in attrs
        )
        out.append(f"| `{m.get('entity', f.stem)}` | {m.get('engine','oltp')} | {names} |")
    out.append("")
    return "\n".join(out)

def write(name: str, content: str, check: bool) -> bool:
    path = REF / name
    if check:
        current = path.read_text() if path.exists() else ""
        if current != content:
            print(f"DESACTUALIZADO: docs/reference/{name} — regenéralo", file=sys.stderr)
            return False
        return True
    path.write_text(content)
    print(f"generado docs/reference/{name}")
    return True

def main() -> int:
    check = "--check" in sys.argv
    ok = all([
        write("api-grpc.md", gen_api_grpc(), check),
        write("codigos-error.md", gen_errores(), check),
        write("modelos-codice.md", gen_modelos(), check),
    ])
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
