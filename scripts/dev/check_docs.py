#!/usr/bin/env python3
"""Auditor integral de cobertura documental 100% de metri-engine.

Verifica 4 invariantes de documentación unificada:
1. 100% de archivos `.rs` en `src/` abren con cabecera de módulo `//!` no vacía.
2. 100% de los 16 módulos de `src/` tienen su ficha detallada en `docs/04-componentes/`
   y cada ficha documenta todos los archivos `.rs` productivos de su módulo.
3. 0 documentos `.md` huérfanos en `docs/` (todos alcanzables desde `docs/README.md`).
4. 0 enlaces relativos rotos en `README.md` y `docs/**/*.md`.

Modos:
  (default)          ratchet + validación estructural de docs/
  --strict           exige 0 archivos sin cabecera, 100% módulos/archivos cubiertos, 0 huérfanos y 0 links rotos
  --update-baseline  regenera scripts/dev/docs_header_allowlist.json
  --report           imprime el resumen de cobertura y sale con código 0
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
DOCS = ROOT / "docs"
ALLOWLIST_PATH = ROOT / "scripts" / "dev" / "docs_header_allowlist.json"

GENERATED = {"src/janus/fbs.rs"}
HEADER_WINDOW = 15

MODULE_TO_COMPONENT_DOC = {
    "mbf": "01-mbf.md",
    "eav": "02-eav.md",
    "codice": "03-codice.md",
    "domain": "04-domain.md",
    "application": "05-application.md",
    "janus": "06-janus.md",
    "janus_router": "07-janus-router.md",
    "aegis": "08-aegis.md",
    "cedar": "09-cedar.md",
    "quota": "10-quota.md",
    "iop": "11-iop.md",
    "eda": "12-eda.md",
    "temporal": "13-temporal.md",
    "grpc": "14-grpc.md",
    "infrastructure": "15-infrastructure.md",
    "otel": "16-otel.md",
}

ALLOWLIST_COMMENT = (
    "Archivos .rs pendientes de cabecera //!. "
    "El ratchet solo admite eliminaciones: meta = 0 archivos pendientes."
)


def is_test_rs(rel_from_mod: Path) -> bool:
    if any(p in ("tests", "testing") for p in rel_from_mod.parts):
        return True
    name = rel_from_mod.name
    return (
        name == "test_support.rs"
        or name.startswith("test_")
        or name.endswith("_tests.rs")
        or name.endswith("_test.rs")
    )


def header_status(path: Path) -> tuple[bool, str | None]:
    """Devuelve (tiene_cabecera, warning)."""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return False, f"no se pudo leer {path}: {exc}"
    window = lines[:HEADER_WINDOW]
    doc_lines = [line.strip() for line in window if line.strip().startswith("//!")]
    if not doc_lines:
        return False, None
    title = doc_lines[0][3:].strip()
    if not title:
        return True, "la cabecera //! no abre con una línea descriptiva"
    return True, None


def scan_rs_headers() -> tuple[list[str], list[str]]:
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


def check_component_coverage() -> list[str]:
    """Verifica que los 16 módulos de src/ y todos sus archivos productivos estén en docs/04-componentes/."""
    issues: list[str] = []
    comp_dir = DOCS / "04-componentes"
    src_mods = sorted([p.name for p in SRC.iterdir() if p.is_dir()])
    for mod in src_mods:
        doc_name = MODULE_TO_COMPONENT_DOC.get(mod)
        if not doc_name:
            issues.append(f"módulo `src/{mod}` no está mapeado en MODULE_TO_COMPONENT_DOC")
            continue
        doc_path = comp_dir / doc_name
        if not doc_path.exists():
            issues.append(f"falta ficha de componente `docs/04-componentes/{doc_name}` para `src/{mod}`")
            continue
        content = doc_path.read_text(encoding="utf-8", errors="replace")
        mod_dir = SRC / mod
        for rs_file in sorted(mod_dir.rglob("*.rs")):
            rel_mod = rs_file.relative_to(mod_dir)
            if is_test_rs(rel_mod):
                continue
            rel_str = rel_mod.as_posix()
            fname = rel_mod.name
            if rel_str not in content and fname not in content:
                issues.append(
                    f"`docs/04-componentes/{doc_name}` no menciona el archivo productivo `src/{mod}/{rel_str}`"
                )
    return issues


def check_orphan_and_broken_links() -> tuple[list[str], list[str]]:
    """Verifica 0 documentos .md huérfanos y 0 enlaces relativos rotos."""
    orphans: list[str] = []
    broken_links: list[str] = []

    docs_index = DOCS / "README.md"
    index_text = docs_index.read_text(encoding="utf-8", errors="replace") if docs_index.exists() else ""

    all_md_files = [ROOT / "README.md"] + sorted(DOCS.rglob("*.md"))
    for md in sorted(DOCS.rglob("*.md")):
        if md == docs_index:
            continue
        rel_from_docs = md.relative_to(DOCS).as_posix()
        if rel_from_docs not in index_text:
            orphans.append(f"docs/{rel_from_docs}")

    link_pat = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
    fence_pat = re.compile(r"^\s*```")
    for md in all_md_files:
        if not md.exists():
            broken_links.append(f"archivo requerido no existe: {md.relative_to(ROOT).as_posix()}")
            continue
        in_fence = False
        for line_no, line in enumerate(md.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if fence_pat.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for m in link_pat.finditer(line):
                raw_target = m.group(1).strip()
                if (
                    not raw_target
                    or raw_target.startswith(("#", "http://", "https://", "mailto:", "file://", "conversation://"))
                ):
                    continue
                target_path_str = raw_target.split("#", 1)[0].strip()
                if not target_path_str:
                    continue
                resolved = (md.parent / target_path_str).resolve()
                if not resolved.exists():
                    broken_links.append(
                        f"{md.relative_to(ROOT).as_posix()}:{line_no} -> enlace roto `{raw_target}`"
                    )
    return orphans, broken_links


def load_allowlist() -> list[str]:
    if not ALLOWLIST_PATH.exists():
        return []
    data = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
    return list(data.get("missing_header", []))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="falla ante cualquier brecha en src/ o docs/")
    parser.add_argument("--update-baseline", action="store_true", help="regenera el allowlist")
    parser.add_argument("--report", action="store_true", help="solo reporta, nunca falla")
    args = parser.parse_args()

    missing, warned = scan_rs_headers()

    if args.update_baseline:
        ALLOWLIST_PATH.write_text(
            json.dumps({"_comment": ALLOWLIST_COMMENT, "missing_header": missing}, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"allowlist regenerado: {len(missing)} archivos sin cabecera")
        return 0

    allow = load_allowlist()
    allow_set = set(allow)
    new_missing = [f for f in missing if f not in allow_set]
    fixed = [f for f in allow if f not in set(missing)]

    comp_issues = check_component_coverage()
    orphans, broken_links = check_orphan_and_broken_links()

    total_rs = len(list(SRC.rglob("*.rs")))
    total_md = len(list(DOCS.rglob("*.md"))) + (1 if (ROOT / "README.md").exists() else 0)
    print(f"archivos .rs escaneados        : {total_rs}")
    print(f"sin cabecera //!               : {len(missing)} (allowlist: {len(allow)}, corregidos: {len(fixed)})")
    print(f"warnings de cabecera           : {len(warned)}")
    print(f"archivos .md unificados        : {total_md}")
    print(f"brechas en docs/04-componentes : {len(comp_issues)}")
    print(f"documentos .md huérfanos       : {len(orphans)}")
    print(f"enlaces Markdown rotos         : {len(broken_links)}")

    if args.report:
        for item in comp_issues + orphans + broken_links:
            print(f"  - {item}")
        return 0

    exit_code = 0
    if new_missing:
        exit_code = 1
        print(f"\nFALLO (ratchet): {len(new_missing)} archivos nuevos sin cabecera //!:")
        for rel in new_missing[:20]:
            print(f"  - {rel}")
    if args.strict and (missing or comp_issues or orphans or broken_links):
        exit_code = 1
        for label, items in (
            ("archivos .rs sin cabecera //!", missing),
            ("brechas de cobertura en docs/04-componentes/", comp_issues),
            ("documentos .md huérfanos (no enlazados en docs/README.md)", orphans),
            ("enlaces Markdown rotos", broken_links),
        ):
            if items:
                print(f"\nFALLO (--strict) — {label} ({len(items)}):")
                for it in items[:30]:
                    print(f"  - {it}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
