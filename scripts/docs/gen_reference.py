#!/usr/bin/env python3
"""Genera docs/05-referencia/ desde las fuentes únicas de verdad (SSOT) del motor.

La documentación de referencia se autogenera de forma determinista:
- api-grpc.md        ← proto/metri.proto + proto/eda.proto
- codigos-error.md   ← config/errors/error_catalog.toml
- modelos-codice.md  ← config/models/*.json

El CI falla si el contenido generado está desactualizado:
`python3 scripts/docs/gen_reference.py --check`.
"""
from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REF = ROOT / "docs" / "05-referencia"


def _parse_proto_file(proto_rel: str) -> list[str]:
    proto = (ROOT / proto_rel).read_text(encoding="utf-8")
    lines: list[str] = [f"## Archivo `{proto_rel}`", ""]
    current_service = None
    for raw in proto.splitlines():
        l = raw.strip()
        m = re.match(r"service\s+(\w+)\s*\{", l)
        if m:
            current_service = m.group(1)
            lines += [f"### Servicio `{current_service}`", ""]
            continue
        m = re.match(r"rpc\s+(\w+)\s*\(\s*([^)]+)\s*\)\s*returns\s*\(\s*([^)]+)\s*\)", l)
        if m and current_service:
            lines.append(
                f"- `{current_service}.{m.group(1)}({m.group(2).strip()}) -> {m.group(3).strip()}`"
            )
            continue
        m = re.match(r"rpc\s+(\w+)\s*\(", l)
        if m and current_service:
            lines.append(f"- `{current_service}.{m.group(1)}`")
            continue
        m = re.match(r"message\s+(\w+)", l)
        if m:
            lines.append("")
            lines.append(f"#### Mensaje `{m.group(1)}`")
            continue
        m = re.match(r"enum\s+(\w+)", l)
        if m:
            lines.append("")
            lines.append(f"#### Enum `{m.group(1)}`")
    lines.append("")
    return lines


def gen_api_grpc() -> str:
    lines = [
        "# Referencia de Contratos gRPC (`proto/`)",
        "",
        "> Generado automáticamente desde `proto/metri.proto` y `proto/eda.proto` — fuente única de verdad (SSOT).",
        "> Regenerar: `python3 scripts/docs/gen_reference.py`",
        "",
    ]
    for proto_rel in ("proto/metri.proto", "proto/eda.proto"):
        lines.extend(_parse_proto_file(proto_rel))
    return "\n".join(lines).rstrip() + "\n"


def gen_errores() -> str:
    data = tomllib.loads((ROOT / "config/errors/error_catalog.toml").read_text(encoding="utf-8"))
    out = [
        "# Catálogo Canónico de Códigos de Error",
        "",
        "> Generado automáticamente desde `config/errors/error_catalog.toml` (catálogo canónico 1:1 con `ErrorCode`).",
        "> Regenerar: `python3 scripts/docs/gen_reference.py`",
        "",
        f"Versión del catálogo: **{data.get('version')}** · Total de entradas: **{len(data.get('errors', []))}**",
        "",
    ]
    by_family: dict[str, list] = {}
    for e in data.get("errors", []):
        by_family.setdefault(e.get("family", "?"), []).append(e)
    for fam in sorted(by_family):
        out += [
            f"## Familia `{fam}` ({len(by_family[fam])} códigos)",
            "",
            "| Código | Etapa | HTTP | gRPC | Reintentable | Descripción |",
            "|---|---|---|---|---|---|",
        ]
        for e in sorted(by_family[fam], key=lambda x: x["code"]):
            flag = ""
            if e.get("deprecated"):
                flag = " ⚠ **deprecado**"
            elif e.get("reserved"):
                flag = " *(reservado)*"
            out.append(
                f"| `{e['code']}` | {e.get('stage','')} | {e.get('http_status','')} | "
                f"{e.get('grpc_status','')} | {'sí' if e.get('retryable') else 'no'} | "
                f"{e.get('description','')}{flag} |"
            )
        out.append("")
    return "\n".join(out)


def gen_modelos() -> str:
    models_dir = ROOT / "config" / "models"
    files = sorted(models_dir.glob("*.json"))
    out = [
        "# Catálogo de Modelos del Códice (`config/models/`)",
        "",
        "> Generado automáticamente desde `config/models/*.json` — registro SSOT de esquemas.",
        "> Regenerar: `python3 scripts/docs/gen_reference.py`",
        "",
        f"Total de modelos registrados: **{len(files)}**",
        "",
        "| Entidad | Motor | Sistema | Historial | Constraints | Reglas EDA | Atributos |",
        "|---|---|:---:|:---:|:---:|:---:|---|",
    ]
    for f in files:
        m = json.loads(f.read_text(encoding="utf-8"))
        attrs = m.get("attributes", [])
        names = ", ".join(
            f"`{a['name']}`" if isinstance(a, dict) else f"`{a}`" for a in attrs
        )
        is_sys = "🔒" if m.get("is_system") else "—"
        hist = "✓" if m.get("track_history") else "—"
        n_constraints = len(m.get("constraints", []))
        n_rules = len(m.get("event_rules", []))
        out.append(
            f"| `{m.get('entity', f.stem)}` | `{m.get('engine','mbf')}` | {is_sys} | {hist} | "
            f"{n_constraints} | {n_rules} | {names} |"
        )
    out.append("")
    return "\n".join(out)


def write(name: str, content: str, check: bool) -> bool:
    REF.mkdir(parents=True, exist_ok=True)
    path = REF / name
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current != content:
            print(
                f"DESACTUALIZADO: docs/05-referencia/{name} — ejecuta `python3 scripts/docs/gen_reference.py`",
                file=sys.stderr,
            )
            return False
        return True
    path.write_text(content, encoding="utf-8")
    print(f"generado docs/05-referencia/{name}")
    return True


def main() -> int:
    check = "--check" in sys.argv
    ok = all(
        [
            write("api-grpc.md", gen_api_grpc(), check),
            write("codigos-error.md", gen_errores(), check),
            write("modelos-codice.md", gen_modelos(), check),
        ]
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
