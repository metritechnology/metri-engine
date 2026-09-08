#!/usr/bin/env python3
"""Auditor del patrón Result — garantía del 100% de adopción (PLAN_PATRON_RESULT.md).

Instrumento de garantía del plan. Tres trabajos:

  1. Escanea `src/` excluyendo tests (convención del repo: dirs `tests/` y
     `testing/`, ficheros `*tests*.rs`, `test_support.rs`, `fbs.rs` generado,
     y bloques `#[cfg(test)]` inline) y cuenta violaciones por módulo:
       unwrap        → `.unwrap()`
       expect        → `.expect(`
       panic_macro   → `panic!` / `todo!` / `unimplemented!` / `unreachable!`
       assert_macro  → `assert!` / `assert_eq!` / `assert_ne!` en código no-test
       string_result → `Result<_, String | Box<dyn Error> | anyhow::Error>`
       anyhow        → cualquier uso de la crate `anyhow`
  2. Verifica la paridad del catálogo de errores (ADR-005):
     `pub enum ErrorCode` (src/domain/errors.rs) ↔ `config/errors/error_catalog.toml`
       - toda variante de `ALL` tiene mapeo en `canonical_code()`
       - todo código canónico existe como entrada del TOML
       - los códigos canónicos son únicos (1 variante = 1 código)
       - el fallback estático de `is_retryable()` coincide con el TOML
       - no hay entradas TOML duplicadas ni códigos muertos
  3. Aplica un gate de no-regresión (ratchet) sobre
     `scripts/dev/result_pattern_baseline.json`: las métricas solo pueden bajar.

Modos:
  (sin flags)        gate ratchet para CI: falla si algo crece sobre la línea base
  --strict           exige cero violaciones (salvo la allowlist de invariantes
                     permanentes de scripts/dev/result_pattern_allowlist.json)
  --only MOD         restringe el escaneo de código al módulo MOD (el catálogo
                     siempre se verifica); útil para el gate por fase
  --update-baseline  reescribe la línea base con lo medido hoy
  --verbose          lista cada violación (fichero:línea)
  --json             salida máquina para CI

Códigos de salida: 0 = gate en verde · 1 = gate en rojo · 2 = error interno.
"""
from __future__ import annotations

import json
import re
import sys
import tomllib
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
ERRORS_RS = SRC / "domain" / "errors.rs"
CATALOG_TOML = ROOT / "config" / "errors" / "error_catalog.toml"
BASELINE_JSON = Path(__file__).resolve().parent / "result_pattern_baseline.json"
ALLOWLIST_JSON = Path(__file__).resolve().parent / "result_pattern_allowlist.json"

CHECKS = ("unwrap", "expect", "panic_macro", "assert_macro", "string_result", "anyhow")

# Ficheros que la convención del repo declara no-código (tests o generados).
EXCLUDED_NAMES = {"test_support.rs", "fbs.rs"}


# ────────────────────────── limpieza de líneas ──────────────────────────

def sanitize(line: str) -> str:
    """Quita comentarios `//` y el contenido de literales de cadena y chars.

    Evita falsos positivos (`.unwrap()` dentro de un mensaje, llaves de
    `format!` rompiendo el conteo de llaves). Limitación conocida: raw strings
    `r#"..."#` se tratan como cadenas normales; no aparecen en el hot path.
    """
    out: list[str] = []
    i, n = 0, len(line)
    in_str = False
    while i < n:
        c = line[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            out.append('""')
            in_str = True
            i += 1
            continue
        if c == "'":
            j = i + 2
            if j < n and line[j - 1] == "\\":
                j += 1
            if j < n and line[j] == "'":
                i = j + 1
                continue
        if c == "/" and i + 1 < n and line[i + 1] == "/":
            break
        out.append(c)
        i += 1
    return "".join(out)


def find_test_regions(lines: list[str]) -> set[int]:
    """Devuelve los índices (0-based) de los bloques `#[cfg(test)] mod x { … }` inline."""
    regions: set[int] = set()
    i = 0
    while i < len(lines):
        if re.match(r"\s*#\[cfg\(test\)\]", lines[i]):
            j = i + 1
            while j < min(i + 4, len(lines)) and not re.match(r"\s*(pub(\([^)]*\))?\s+)?mod\s+\w+", lines[j]):
                j += 1
            if j < len(lines):
                m = re.match(r"\s*(pub(\([^)]*\))?\s+)?mod\s+\w+", lines[j])
                if m and "{" in sanitize(lines[j]):
                    depth = 0
                    started = False
                    k = j
                    while k < len(lines):
                        s = sanitize(lines[k])
                        depth += s.count("{") - s.count("}")
                        if "{" in s:
                            started = True
                        if started and depth <= 0:
                            break
                        k += 1
                    regions.update(range(i, min(k + 1, len(lines))))
        i += 1
    return regions


# ────────────────────────── escaneo de código ──────────────────────────

RE_UNWRAP = re.compile(r"\.unwrap\(\)")
RE_EXPECT = re.compile(r"\.expect\(")
RE_PANIC = re.compile(r"\b(?:panic|todo|unimplemented|unreachable)!")
RE_ASSERT = re.compile(r"\bassert(?:_eq|_ne)?!")
RE_ANYHOW = re.compile(r"\banyhow\b")

BANNED_RESULT_ERR = (
    lambda e: e == "String"
    or e.startswith("Box<dyn")
    or "anyhow::Error" in e
    or re.search(r"\bdyn\s+(std::error::)?Error\b", e) is not None
)


def find_string_result_errs(text: str) -> list[tuple[int, str]]:
    """Encuentra `Result<_, E>` con E prohibido, balanceando ángulos (multilínea)."""
    out: list[tuple[int, str]] = []
    for m in re.finditer(r"\bResult\s*<", text):
        i = m.end()
        depth = 1
        while i < len(text) and depth > 0:
            c = text[i]
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            elif c == ";":
                break
            i += 1
        if depth != 0:
            continue
        inner = text[m.end(): i - 1]
        split, d = None, 0
        for idx, ch in enumerate(inner):
            if ch in "<([":
                d += 1
            elif ch in ">)":
                d -= 1
            elif ch == "]":
                d -= 1
            elif ch == "," and d == 0:
                split = idx
                break
        if split is None:
            continue
        err = inner[split + 1:].strip()
        if BANNED_RESULT_ERR(err):
            out.append((text.count("\n", 0, m.start()) + 1, err))
    return out


def module_of(rel: Path) -> str:
    parts = rel.parts
    return parts[0] if len(parts) > 1 else "(raíz)"


def is_test_path(rel: Path) -> bool:
    if any(p in ("tests", "testing") for p in rel.parts):
        return True
    # test_support.rs · golden_test.rs · *_tests.rs · test_*.rs
    return re.search(r"(^test_|_tests?\.)", rel.name) is not None or rel.name in EXCLUDED_NAMES


def scan_code(only: str | None) -> tuple[dict, list[dict]]:
    """Devuelve ({check: {módulo: [(file, line, detalle)]}}) y hallazgos planos."""
    hits: dict[str, dict[str, list]] = {c: defaultdict(list) for c in CHECKS}
    details: list[dict] = []
    for rs in sorted(SRC.rglob("*.rs")):
        rel = rs.relative_to(SRC)
        if is_test_path(rel):
            continue
        mod = module_of(rel)
        if only and mod != only:
            continue
        try:
            raw = rs.read_text().splitlines()
        except UnicodeDecodeError:
            continue
        clean = [sanitize(l) for l in raw]
        test_lines = find_test_regions(raw)
        masked = "\n".join("" if idx in test_lines else clean[idx] for idx in range(len(clean)))
        rules = [
            ("unwrap", RE_UNWRAP, lambda l: ""),
            ("expect", RE_EXPECT, lambda l: ""),
            ("panic_macro", RE_PANIC, lambda l: ""),
            ("assert_macro", RE_ASSERT, lambda l: ""),
            ("anyhow", RE_ANYHOW, lambda l: ""),
        ]
        for name, rx, _det in rules:
            for idx, line in enumerate(clean):
                if idx in test_lines:
                    continue
                m = rx.search(line)
                if m:
                    hits[name][mod].append((str(rel), idx + 1, m.group(0)))
                    details.append({"check": name, "module": mod, "file": str(rel), "line": idx + 1})
        for line_no, err in find_string_result_errs(masked):
            hits["string_result"][mod].append((str(rel), line_no, err))
            details.append({"check": "string_result", "module": mod, "file": str(rel), "line": line_no, "detail": err})
    return {c: {m: v for m, v in mods.items()} for c, mods in hits.items()}, details


# ────────────────────────── paridad del catálogo (ADR-005) ──────────────────────────

def parse_catalog() -> tuple[list[str], dict, dict]:
    data = tomllib.loads(CATALOG_TOML.read_text())
    codes = [e["code"] for e in data.get("errors", [])]
    retryable = {e["code"]: bool(e.get("retryable", False)) for e in data.get("errors", [])}
    # Regla C5: entradas reserved/deprecated pueden no tener variante que las use.
    inactive = {
        e["code"]: bool(e.get("reserved", False) or e.get("deprecated", False))
        for e in data.get("errors", [])
    }
    return codes, retryable, inactive


def parse_errors_rs() -> dict:
    text = ERRORS_RS.read_text()
    # variantes declaradas en el enum (entre `pub enum ErrorCode {` y su cierre a col 0)
    enum_m = re.search(r"pub enum ErrorCode \{(.*?)\n\}", text, re.S)
    enum_variants = re.findall(r"^    ([A-Z]\w*),?\s*(?://.*)?$", enum_m.group(1), re.M) if enum_m else []
    # lista `pub const ALL` (zero-drop)
    all_m = re.search(r"pub const ALL[^\[]*\[(.*?)\];", text, re.S)
    all_variants = re.findall(r"ErrorCode::(\w+)", all_m.group(1)) if all_m else []
    # cada método se acota a su propio cuerpo: los brazos de stage() usan el
    # mismo patrón `ErrorCode::X => "..."` que canonical_code() y no son códigos.
    canon_region = _method_region(text, "pub fn canonical_code", "pub fn is_retryable")
    canon = dict(re.findall(r"ErrorCode::(\w+)\s*=>\s*\"([^\"]+)\"", canon_region))
    retry_region = _method_region(text, "pub fn is_retryable", "pub fn stage")
    fb_m = re.search(r"matches!\(\s*self,\s*(.*?)\)", retry_region, re.S)
    fallback = re.findall(r"ErrorCode::(\w+)", fb_m.group(1)) if fb_m else []
    return {"enum": enum_variants, "all": all_variants, "canon": canon, "fallback": set(fallback)}


def _method_region(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start) if end_marker in text[start:] else len(text)
    return text[start:end]


def check_catalog() -> dict:
    toml_codes, toml_retryable, inactive = parse_catalog()
    rs = parse_errors_rs()
    canon = rs["canon"]
    issues: dict[str, list[str]] = {
        "enum_vs_all": [],           # variante en enum sin entrada en ALL (o viceversa)
        "variant_without_mapping": [],  # en ALL sin brazo en canonical_code()
        "missing_in_toml": [],       # código canónico sin entrada TOML
        "duplicate_targets": [],     # N variantes → 1 código (viola 1:1)
        "unused_toml": [],           # entrada TOML que ninguna variante usa
        "toml_duplicates": [],       # código repetido dentro del TOML
        "retryable_fallback_mismatch": [],
    }
    seen, dup = set(), set()
    for c in toml_codes:
        (dup if c in seen else seen).add(c)
    issues["toml_duplicates"] = sorted(dup)

    for v in rs["enum"]:
        if v not in rs["all"]:
            issues["enum_vs_all"].append(v)
    for v in rs["all"]:
        if v not in rs["enum"]:
            issues["enum_vs_all"].append(f"ALL::{v}")
        if v not in canon:
            issues["variant_without_mapping"].append(v)
    for v, code in sorted(canon.items()):
        if code not in seen:
            issues["missing_in_toml"].append(f"{v} → {code}")
    targets: dict[str, list[str]] = defaultdict(list)
    for v, code in canon.items():
        targets[code].append(v)
    for code, vs in sorted(targets.items()):
        if len(vs) > 1:
            issues["duplicate_targets"].append(f"{code} ← {', '.join(vs)}")
    used = set(canon.values())
    issues["unused_toml"] = [c for c in toml_codes if c not in used and not inactive.get(c, False)]
    for v in sorted(rs["all"]):
        code = canon.get(v)
        if not code or code not in toml_retryable:
            continue  # ya se reporta en missing_in_toml / variant_without_mapping
        expected = v in rs["fallback"]
        if toml_retryable[code] is not expected:
            issues["retryable_fallback_mismatch"].append(v)

    return {
        "enum_variants": len(rs["enum"]),
        "all_variants": len(rs["all"]),
        "canonical_mappings": len(canon),
        "toml_entries": len(toml_codes),
        "issues": {k: v for k, v in issues.items()},
    }


# ────────────────────────── allowlist y ratchet ──────────────────────────

def load_allowlist() -> list[dict]:
    if not ALLOWLIST_JSON.exists():
        return []
    return json.loads(ALLOWLIST_JSON.read_text()).get("entries", [])


def apply_allowlist(code_hits: dict, allowlist: list[dict]) -> tuple[dict, list[dict], list[str]]:
    """Descuenta las invariantes permanentes (por fichero exacto); devuelve conteos netos."""
    net: dict[str, dict[str, list]] = {c: {m: list(v) for m, v in mods.items()} for c, mods in code_hits.items()}
    used: list[dict] = []
    stale: list[str] = []
    for entry in allowlist:
        check, f = entry["check"], entry["path"].removeprefix("src/")
        quota, orig = int(entry.get("max", 0)), int(entry.get("max", 0))
        for mod, items in net.get(check, {}).items():
            kept = []
            for it in items:
                if quota > 0 and it[0] == f:
                    quota -= 1  # hallazgo cubierto por la invariante declarada
                    continue
                kept.append(it)
            net[check][mod] = kept
        (used if quota < orig else stale).append(entry if quota < orig else entry["path"])
    return net, used, stale


def counts_of(net: dict) -> dict:
    return {
        check: {mod: len(items) for mod, items in sorted(mods.items()) if items}
        for check, mods in net.items()
    }


def catalog_issue_counts(cat: dict) -> dict:
    return {k: len(v) for k, v in cat["issues"].items()}


def ratchet_failures(current: dict, baseline: dict) -> list[str]:
    fails = []
    for check, mods in current.items():
        base_mods = baseline.get(check, {})
        for mod, n in mods.items():
            if n > base_mods.get(mod, 0):
                fails.append(f"{check}/{mod}: {n} > línea base {base_mods.get(mod, 0)}")
    return fails


# ────────────────────────── presentación ──────────────────────────

def print_report(net: dict, cat: dict, modules: list[str], gate: str, verbose: bool, allow_used: list[dict], allow_stale: list[str]) -> None:
    closed = 0
    header = f"{'módulo':<16}" + "".join(f"{c:>13}" for c in CHECKS) + f"{'estado':>10}"
    print("═══ Patrón Result — auditoría (violaciones en código no-test) ═══")
    print(header)
    print("-" * len(header))
    total = {c: 0 for c in CHECKS}
    for mod in modules:
        row_total = sum(len(net[c].get(mod, [])) for c in CHECKS)
        closed += row_total == 0
        for c in CHECKS:
            total[c] += len(net[c].get(mod, []))
        print(f"{mod:<16}" + "".join(f"{len(net[c].get(mod, [])):>13}" for c in CHECKS)
              + f"{'✓ OK' if row_total == 0 else '✗ deuda':>10}")
    print("-" * len(header))
    print(f"{'TOTAL':<16}" + "".join(f"{total[c]:>13}" for c in CHECKS))
    pct = 100.0 * closed / len(modules) if modules else 100.0
    print(f"\nCobertura de módulos cerrados: {closed}/{len(modules)} ({pct:.0f}%)")

    print("\n═══ Catálogo de errores (ADR-005) ═══")
    print(f"enum ErrorCode: {cat['enum_variants']} variantes · ALL: {cat['all_variants']} · "
          f"mapeos canónicos: {cat['canonical_mappings']} · entradas TOML: {cat['toml_entries']}")
    labels = {
        "enum_vs_all": "enum ↔ ALL desincronizados",
        "variant_without_mapping": "variantes sin mapeo canónico",
        "missing_in_toml": "códigos canónicos sin entrada TOML",
        "duplicate_targets": "códigos compartidos por N variantes (debe ser 1:1)",
        "unused_toml": "entradas TOML sin variante",
        "toml_duplicates": "códigos duplicados en el TOML",
        "retryable_fallback_mismatch": "fallback is_retryable() ≠ TOML",
    }
    cat_ok = True
    for key, label in labels.items():
        items = cat["issues"].get(key, [])
        cat_ok &= not items
        mark = "✓" if not items else f"✗ ({len(items)})"
        print(f"  {mark} {label}")
        for it in items[:10]:
            print(f"        · {it}")
        if len(items) > 10:
            print(f"        … y {len(items) - 10} más")

    if allow_used:
        print(f"\nAllowlist de invariantes permanentes aplicada: {len(allow_used)} entrada(s)")
    for path in allow_stale:
        print(f"⚠ allowlist obsoleta (ya no produce hallazgos, elimínala): {path}")
    print(f"\nGate: {gate}")
    if verbose:
        print("\n── Detalle de violaciones ──")
        for check, mods in net.items():
            for mod, items in mods.items():
                for f, line, det in items:
                    print(f"  [{check}] src/{f}:{line}  {det}")


def main() -> int:
    args = sys.argv[1:]
    strict = "--strict" in args
    update = "--update-baseline" in args
    as_json = "--json" in args
    verbose = "--verbose" in args
    only = None
    if "--only" in args:
        only = args[args.index("--only") + 1]

    code_hits, _details = scan_code(only)
    allowlist = load_allowlist()
    net, allow_used, allow_stale = apply_allowlist(code_hits, allowlist)
    cat = check_catalog()

    current = {"code": counts_of(net), "catalog": catalog_issue_counts(cat)}

    modules = sorted(
        {module_of(p.relative_to(SRC)) for p in SRC.rglob("*.rs") if not is_test_path(p.relative_to(SRC))}
        | set().union(*[{m for m in net[c]} for c in CHECKS])
    )

    fails: list[str] = []
    if update:
        BASELINE_JSON.write_text(json.dumps({
            "generated_at": date.today().isoformat(),
            "plan": "docs/architecture/PLAN_PATRON_RESULT.md",
            "code": current["code"],
            "catalog": current["catalog"],
        }, indent=2, ensure_ascii=False) + "\n")
        gate = "línea base actualizada → " + str(BASELINE_JSON.relative_to(ROOT))
    elif strict:
        for check, mods in net.items():
            for mod, items in mods.items():
                if items:
                    fails.append(f"{check}/{mod}: {len(items)} (modo estricto: se exige 0)")
        if not only:  # el catálogo es global: se exige solo en el gate global (Fase 1)
            fails += [f"catálogo/{k}: {len(v)}" for k, v in cat["issues"].items() if v]
        gate = "ESTRICTO"
    else:
        baseline = json.loads(BASELINE_JSON.read_text()) if BASELINE_JSON.exists() else {"code": {}, "catalog": {}}
        fails = ratchet_failures(current["code"], baseline.get("code", {}))
        for key, n in current["catalog"].items():
            if n > baseline.get("catalog", {}).get(key, 0):
                fails.append(f"catálogo/{key}: {n} > línea base {baseline.get('catalog', {}).get(key, 0)}")
        gate = "RATCHET (no-regresión)"

    if as_json:
        print(json.dumps({"gate": gate, "ok": not fails, "failures": fails,
                          "code": current["code"], "catalog": cat}, indent=2, ensure_ascii=False))
    else:
        print_report(net, cat, modules, gate, verbose, allow_used, allow_stale)

    return 1 if fails else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 — el auditor nunca debe colgar el CI en silencio
        print(f"error interno del auditor: {exc}", file=sys.stderr)
        sys.exit(2)
