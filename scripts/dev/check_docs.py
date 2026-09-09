#!/usr/bin/env python3
"""Auditor de documentación de metri-engine (PLAN_DOCUMENTACION.md §2.7).

Verifica, con ratchet, que cada archivo .rs de src/ abra con su cabecera
`//!` con título en inglés. El código generado (`src/janus/fbs.rs`) queda
exento; el resto de archivos pendientes vive en el allowlist, que solo
puede encoger (un archivo corregido sale; un archivo nuevo sin cabecera
falla aunque el allowlist tenga otros).

Modos:
  (default)          ratchet: falla si un .rs sin cabecera no está en el allowlist
  --strict           además falla si el allowlist no está vacío (meta de la Fase 5)
  --update-baseline  regenera scripts/dev/docs_header_allowlist.json con el estado actual
  --report           imprime el resumen y sale sin fallar
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
ALLOWLIST_PATH = ROOT / "scripts" / "dev" / "docs_header_allowlist.json"

# Código generado: exento de cabecera por diseño (PLAN_DOCUMENTACION §2.7).
GENERATED = {"src/janus/fbs.rs"}

# Una cabecera //! en cualquiera de las primeras N líneas cuenta como presente.
HEADER_WINDOW = 15

# Heurística suave (solo warning, no falla): el título debe estar en inglés.
SPANISH_TITLE_MARKERS = re.compile(
    r"\b(el|la|los|las|un|una|unos|unas|de|del|que|qué|para|porque|con|como|"
    r"según|este|esta|ese|esa|genera|código|cada|versión|módulo|función)\b",
    re.IGNORECASE,
)

ALLOWLIST_COMMENT = (
    "Archivos .rs pendientes de cabecera //! (PLAN_DOCUMENTACION §2.7). "
    "El ratchet solo admite eliminaciones: un archivo corregido sale de esta "
    "lista; un archivo nuevo sin cabecera falla aunque esta lista tenga otros."
)


def header_status(path: Path) -> tuple[bool, str | None]:
    """Devuelve (tiene_cabecera, warning_idioma)."""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        print(f"WARN: no se pudo leer {path}: {exc}")
        return False, None
    window = lines[:HEADER_WINDOW]
    doc_lines = [line.strip() for line in window if line.strip().startswith("//!")]
    if not doc_lines:
        return False, None
    title = doc_lines[0][3:].strip()
    if title and SPANISH_TITLE_MARKERS.search(title):
        return True, f"el título parece estar en español: {title!r}"
    if not title:
        return True, "la cabecera //! no abre con una línea de título"
    return True, None


def scan() -> tuple[list[str], list[str]]:
    missing: list[str] = []
    warned: list[str] = []
    for path in sorted(SRC.rglob("*.rs")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in GENERATED:
            continue
        ok, warning = header_status(path)
        if not ok:
            missing.append(rel)
        elif warning:
            warned.append(f"{rel}: {warning}")
    return missing, warned


def load_allowlist() -> list[str]:
    if not ALLOWLIST_PATH.exists():
        return []
    data = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
    return list(data.get("missing_header", []))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="falla si el allowlist no está vacío")
    parser.add_argument("--update-baseline", action="store_true", help="regenera el allowlist")
    parser.add_argument("--report", action="store_true", help="solo reporta, nunca falla")
    args = parser.parse_args()

    missing, warned = scan()

    if args.update_baseline:
        ALLOWLIST_PATH.write_text(
            json.dumps({"_comment": ALLOWLIST_COMMENT, "missing_header": missing}, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"allowlist regenerado: {len(missing)} archivos sin cabecera, {len(warned)} con warning de idioma")
        return 0

    allow = load_allowlist()
    allow_set = set(allow)
    new_missing = [f for f in missing if f not in allow_set]
    fixed = [f for f in allow if f not in set(missing)]

    print(f"archivos .rs escaneados       : {len(list(SRC.rglob('*.rs')))}")
    print(f"sin cabecera //!              : {len(missing)} (allowlist: {len(allow)}, corregidos desde el baseline: {len(fixed)})")
    print(f"warnings de título (no bloquea): {len(warned)}")

    if args.report:
        return 0

    exit_code = 0
    if new_missing:
        exit_code = 1
        print(f"\nFALLO (ratchet): {len(new_missing)} archivos nuevos sin cabecera //! (no están en el allowlist):")
        for rel in new_missing[:20]:
            print(f"  - {rel}")
        print("Añade la cabecera //! o, si es código generado, decláralo en GENERATED.")
    if args.strict and missing:
        exit_code = 1
        print(f"\nFALLO (--strict): quedan {len(missing)} archivos sin cabecera en el allowlist.")
    if fixed:
        print("sugerencia: ejecuta `--update-baseline` para consolidar los archivos corregidos")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
