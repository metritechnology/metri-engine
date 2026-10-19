#!/usr/bin/env python3
"""Auditor de SOLID, DRY y Clean Code por archivo — metri-engine.

Evalúa cada archivo de `src/` (Rust) y `scripts/` + `tests/` (Python):
  SOLID      : srp_file_lines (<=500L), dip_layer_violation, isp_fat_trait (<=5 métodos)
  DRY        : dry_duplicate_block (>=10L), dry_scattered_env, dry_python_infra
  Clean Code : cc_fn_length (<=80L), cc_fn_params (<=5), cc_nesting_depth (<=6), cc_unsafe_usage

Modos:
  (sin flags)        modo RATCHET para pre-commit/CI contra `clean_code_baseline.json`
  --file PATH        Scorecard 0-100 individual por archivo
  --only MOD         restringe el escaneo Rust al módulo `src/<MOD>`
  --strict           exige 0 violaciones en el 100% del repositorio
  --update-baseline  regenera `scripts/dev/clean_code_baseline.json`
  --verbose / --json detalle por línea o salida JSON
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
BASELINE_PATH = Path(__file__).resolve().parent / "clean_code_baseline.json"

MAX_FILE_PROD_LINES = 500
MAX_FN_LINES = 80
MAX_FN_PARAMS = 5
MAX_TRAIT_METHODS = 5
MAX_NESTING_DEPTH = 6
DUP_WINDOW_SIZE = 10

EXCLUDED_RS_NAMES = {"test_support.rs", "fbs.rs"}
ALLOWED_ENV_VAR_FILES = {"src/domain/config.rs", "src/grpc/bootstrap.rs", "src/main.rs"}
CORE_MODULES = {
    "domain", "temporal", "codice", "mbf", "eav", "cedar",
    "quota", "janus", "aegis", "iop", "application",
}
FORBIDDEN_CORE_IMPORT = re.compile(
    r"\b(aws_sdk_\w+|tonic|lambda_runtime|crate::grpc|crate::infrastructure)\b"
)
CHECKS = (
    "srp_file_lines", "dip_layer_violation", "isp_fat_trait",
    "dry_duplicate_block", "dry_scattered_env", "dry_python_infra",
    "cc_fn_length", "cc_fn_params", "cc_nesting_depth", "cc_unsafe_usage",
)


def strip_line_comment_and_strings(line: str) -> str:
    """Elimina comentarios `//` y vacía literales de cadena para conteo léxico seguro."""
    out: list[str] = []
    i, n, in_str = 0, len(line), False
    while i < n:
        ch = line[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == '"':
                in_str = False
            i += 1
            continue
        if ch == '"':
            out.append('""')
            in_str = True
            i += 1
            continue
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            break
        out.append(ch)
        i += 1
    return "".join(out)


def is_test_rs(rel_from_src: Path) -> bool:
    """Determina si un archivo `.rs` bajo `src/` es de test o código generado."""
    if any(p in ("tests", "testing") for p in rel_from_src.parts):
        return True
    name = rel_from_src.name
    return name in EXCLUDED_RS_NAMES or bool(re.search(r"(^test_|_tests?\.)", name))


def split_prod_lines(raw_lines: list[str]) -> list[tuple[int, str]]:
    """Devuelve `(line_no, clean_line)` excluyendo regiones `#[cfg(test)] mod ...`."""
    prod: list[tuple[int, str]] = []
    i, n = 0, len(raw_lines)
    while i < n:
        if re.match(r"^\s*#\[cfg\(test\)\]", raw_lines[i]):
            j = i + 1
            while j < min(i + 4, n) and not re.match(r"^\s*(pub(\([^)]*\))?\s+)?mod\s+\w+", raw_lines[j]):
                j += 1
            if j < n and "{" in strip_line_comment_and_strings(raw_lines[j]):
                depth, started, k = 0, False, j
                while k < n:
                    s = strip_line_comment_and_strings(raw_lines[k])
                    depth += s.count("{") - s.count("}")
                    started = started or ("{" in s)
                    if started and depth <= 0:
                        break
                    k += 1
                i = k + 1
                continue
        prod.append((i + 1, strip_line_comment_and_strings(raw_lines[i])))
        i += 1
    return prod


def parse_functions(prod: list[tuple[int, str]]) -> list[dict]:
    """Extrae funciones productivas con su línea de inicio, longitud, parámetros y anidamiento."""
    fns: list[dict] = []
    fn_pat = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?fn\s+(\w+)")
    i, n = 0, len(prod)
    while i < n:
        m = fn_pat.match(prod[i][1])
        if not m:
            i += 1
            continue
        fn_name, start_line = m.group(1), prod[i][0]
        j, sig_parts = i, []
        while j < n and "{" not in prod[j][1] and ";" not in prod[j][1]:
            sig_parts.append(prod[j][1])
            j += 1
        if j >= n or "{" not in prod[j][1]:
            i = j + 1
            continue
        sig_parts.append(prod[j][1].split("{")[0])
        sig_text = " ".join(sig_parts)
        param_count = _count_params(sig_text)
        depth, max_depth, started, k = 0, 0, False, j
        while k < n:
            s = prod[k][1]
            for ch in s:
                if ch == "{":
                    depth += 1
                    started = True
                    max_depth = max(max_depth, depth)
                elif ch == "}":
                    depth -= 1
            if started and depth <= 0:
                break
            k += 1
        end_line = prod[min(k, n - 1)][0]
        fns.append({
            "name": fn_name,
            "line": start_line,
            "length": end_line - start_line + 1,
            "params": param_count,
            "max_depth": max(0, max_depth - 1),
        })
        i = k + 1
    return fns


def _count_params(sig_text: str) -> int:
    """Cuenta los parámetros de una firma Rust excluyendo `self`, `&self` y `&mut self`."""
    start = sig_text.find("(")
    if start == -1:
        return 0
    depth, end = 0, -1
    for idx in range(start, len(sig_text)):
        if sig_text[idx] == "(":
            depth += 1
        elif sig_text[idx] == ")":
            depth -= 1
            if depth == 0:
                end = idx
                break
    if end == -1:
        return 0
    inner = sig_text[start + 1 : end].strip()
    if not inner:
        return 0
    parts, cur, d = [], [], 0
    for ch in inner:
        if ch in "<([":
            d += 1
        elif ch in ">)]":
            d -= 1
        if ch == "," and d == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return sum(
        1
        for p in parts
        if p and not re.match(r"^(&\s*(mut\s+)?)?self\b", p)
    )


def scan_traits_isp(rel_path: str, prod: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """Verifica Regla I1 (ISP): traits en domain/protocols.rs y application/ports.rs <= 5 métodos."""
    if rel_path not in ("src/domain/protocols.rs", "src/application/ports.rs"):
        return []
    violations: list[tuple[int, str]] = []
    trait_pat = re.compile(r"^\s*pub\s+trait\s+(\w+)")
    fn_pat = re.compile(r"^\s*(?:async\s+)?fn\s+\w+")
    i, n = 0, len(prod)
    while i < n:
        m = trait_pat.match(prod[i][1])
        if not m:
            i += 1
            continue
        t_name, t_line = m.group(1), prod[i][0]
        depth, methods, started, k = 0, 0, False, i
        while k < n:
            s = prod[k][1]
            if started and depth == 1 and fn_pat.match(s):
                methods += 1
            depth += s.count("{") - s.count("}")
            started = started or ("{" in s)
            if started and depth <= 0:
                break
            k += 1
        if methods > MAX_TRAIT_METHODS:
            violations.append((t_line, f"trait `{t_name}` tiene {methods} métodos (máx {MAX_TRAIT_METHODS})"))
        i = k + 1
    return violations


def scan_rust_file(path: Path) -> tuple[dict[str, list[tuple[int, str]]], list[tuple[int, str]]]:
    """Audita un archivo `.rs` productivo y devuelve sus hallazgos y líneas normalizadas para DRY."""
    rel = path.relative_to(ROOT).as_posix()
    rel_src = path.relative_to(SRC)
    mod = rel_src.parts[0] if len(rel_src.parts) > 1 else "(root)"
    raw_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    prod = split_prod_lines(raw_lines)
    findings: dict[str, list[tuple[int, str]]] = defaultdict(list)

    if len(prod) > MAX_FILE_PROD_LINES:
        findings["srp_file_lines"].append(
            (1, f"{len(prod)} líneas productivas (máx {MAX_FILE_PROD_LINES})")
        )

    for line_no, clean in prod:
        stripped = clean.strip()
        if not stripped:
            continue
        if mod in CORE_MODULES:
            m_dip = FORBIDDEN_CORE_IMPORT.search(stripped)
            if m_dip:
                findings["dip_layer_violation"].append(
                    (line_no, f"import/uso de capa externa `{m_dip.group(1)}` en módulo `{mod}`")
                )
        if rel not in ALLOWED_ENV_VAR_FILES and re.search(r"\benv::var\b", stripped):
            findings["dry_scattered_env"].append(
                (line_no, "lectura directa de `env::var` fuera de `src/domain/config.rs`")
            )
        if re.search(r"\bunsafe\b", stripped):
            findings["cc_unsafe_usage"].append((line_no, "uso de `unsafe` prohibido"))

    for line_no, detail in scan_traits_isp(rel, prod):
        findings["isp_fat_trait"].append((line_no, detail))

    for fn in parse_functions(prod):
        if fn["length"] > MAX_FN_LINES:
            findings["cc_fn_length"].append(
                (fn["line"], f"fn `{fn['name']}` tiene {fn['length']} líneas (máx {MAX_FN_LINES})")
            )
        if fn["params"] > MAX_FN_PARAMS:
            findings["cc_fn_params"].append(
                (fn["line"], f"fn `{fn['name']}` recibe {fn['params']} parámetros (máx {MAX_FN_PARAMS})")
            )
        if fn["max_depth"] > MAX_NESTING_DEPTH:
            findings["cc_nesting_depth"].append(
                (fn["line"], f"fn `{fn['name']}` anida {fn['max_depth']} niveles (máx {MAX_NESTING_DEPTH})")
            )

    norm_lines: list[tuple[int, str]] = []
    attr_depth = 0
    for ln, c in prod:
        s = c.strip()
        if not s:
            continue
        if attr_depth > 0 or s.startswith(("#[", "#![")):
            attr_depth += s.count("[") - s.count("]")
            attr_depth = max(0, attr_depth)
            continue
        if s not in ("{", "}", "};", ");", ") {") and not s.startswith(("use ", "//")):
            norm_lines.append((ln, s))
    return findings, norm_lines


def detect_dry_duplicates(norm_by_file: dict[str, list[tuple[int, str]]]) -> dict[str, list[tuple[int, str]]]:
    """Detecta bloques clonados de >= 10 líneas normalizadas entre distintos archivos Rust."""
    windows: dict[tuple[str, ...], list[tuple[str, int]]] = defaultdict(list)
    for rel, norm in norm_by_file.items():
        for idx in range(len(norm) - DUP_WINDOW_SIZE + 1):
            block = tuple(x[1] for x in norm[idx : idx + DUP_WINDOW_SIZE])
            windows[block].append((rel, norm[idx][0]))

    dup_hits: dict[str, list[tuple[int, str]]] = defaultdict(list)
    recorded_spans: set[tuple[str, int]] = set()
    for _block, locs in windows.items():
        distinct_files = sorted({f for f, _ in locs})
        if len(distinct_files) < 2:
            continue
        for rel, start_line in locs:
            bucket = start_line // DUP_WINDOW_SIZE
            if (rel, bucket) in recorded_spans:
                continue
            recorded_spans.add((rel, bucket))
            others = [f for f in distinct_files if f != rel]
            dup_hits[rel].append(
                (start_line, f"bloque duplicado (>= {DUP_WINDOW_SIZE} líneas) con {', '.join(others[:2])}")
            )
    return dup_hits


def scan_python_files() -> dict[str, dict[str, list[tuple[int, str]]]]:
    """Audita scripts y tests Python para detectar violaciones DRY de infraestructura y SRP."""
    py_findings: dict[str, dict[str, list[tuple[int, str]]]] = defaultdict(lambda: defaultdict(list))
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if any(p in rel for p in (".venv/", "venv/", "target/", "scripts/proto/", "_pb2")):
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) > MAX_FILE_PROD_LINES and not rel.startswith("tests/"):
            py_findings[rel]["srp_file_lines"].append(
                (1, f"{len(lines)} líneas en script Python (máx {MAX_FILE_PROD_LINES})")
            )
        if rel.startswith("scripts/common/"):
            continue
        for idx, line in enumerate(lines, 1):
            stripped = strip_line_comment_and_strings(line).strip()
            if not stripped or stripped.startswith("#"):
                continue
            if re.search(r"\bhmac\.new\s*\(", stripped):
                py_findings[rel]["dry_python_infra"].append(
                    (idx, "firma `hmac.new(` duplicada; usa `scripts.common.auth`")
                )
            if re.match(r"^class\s+GrpcWebClient\b", stripped):
                py_findings[rel]["dry_python_infra"].append(
                    (idx, "`GrpcWebClient` duplicado; importa desde `scripts.common.grpc_client`")
                )
    return py_findings


def collect_all(only_mod: str | None = None) -> dict[str, dict[str, list[tuple[int, str]]]]:
    """Ejecuta todos los escaneos sobre el árbol y devuelve `{file: {check: [(line, msg)]}}`."""
    all_findings: dict[str, dict[str, list[tuple[int, str]]]] = defaultdict(lambda: defaultdict(list))
    norm_by_file: dict[str, list[tuple[int, str]]] = {}

    for rs in sorted(SRC.rglob("*.rs")):
        rel_src = rs.relative_to(SRC)
        if is_test_rs(rel_src):
            continue
        mod = rel_src.parts[0] if len(rel_src.parts) > 1 else "(root)"
        if only_mod and mod != only_mod:
            continue
        rel = rs.relative_to(ROOT).as_posix()
        f_hits, norm = scan_rust_file(rs)
        norm_by_file[rel] = norm
        for chk, items in f_hits.items():
            if items:
                all_findings[rel][chk].extend(items)

    for rel, d_items in detect_dry_duplicates(norm_by_file).items():
        all_findings[rel]["dry_duplicate_block"].extend(d_items)

    if not only_mod:
        for rel, py_hits in scan_python_files().items():
            for chk, items in py_hits.items():
                if items:
                    all_findings[rel][chk].extend(items)

    return all_findings


def summarize_counts(findings: dict[str, dict[str, list[tuple[int, str]]]]) -> dict[str, dict[str, int]]:
    """Convierte el mapa de hallazgos detallados a conteos por archivo y check."""
    return {
        rel: {chk: len(items) for chk, items in sorted(chks.items()) if items}
        for rel, chks in sorted(findings.items())
        if any(chks.values())
    }


def evaluate_ratchet(current: dict[str, dict[str, int]], baseline_files: dict[str, dict[str, int]]) -> list[str]:
    """Compara el estado actual archivo por archivo contra la línea base (Ratchet)."""
    failures: list[str] = []
    for rel, chks in sorted(current.items()):
        base_chks = baseline_files.get(rel, {})
        for chk, count in chks.items():
            allowed = base_chks.get(chk, 0)
            if count > allowed:
                failures.append(f"{rel} [{chk}]: {count} > línea base {allowed}")
    return failures


def print_file_scorecard(target_rel: str, findings: dict[str, dict[str, list[tuple[int, str]]]]) -> int:
    """Imprime la tarjeta de puntuación SOLID / DRY / Clean Code para un archivo específico."""
    target_path = ROOT / target_rel
    if not target_path.exists():
        print(f"✗ El archivo no existe: {target_rel}", file=sys.stderr)
        return 2
    file_hits = findings.get(target_rel, {})
    solid_v = sum(len(file_hits.get(c, [])) for c in ("srp_file_lines", "dip_layer_violation", "isp_fat_trait"))
    dry_v = sum(len(file_hits.get(c, [])) for c in ("dry_duplicate_block", "dry_scattered_env", "dry_python_infra"))
    cc_v = sum(len(file_hits.get(c, [])) for c in ("cc_fn_length", "cc_fn_params", "cc_nesting_depth", "cc_unsafe_usage"))
    total_v = solid_v + dry_v + cc_v
    score = max(0, 100 - (solid_v * 15 + dry_v * 10 + cc_v * 8))

    print(f"═══ Clean Code & SOLID Scorecard: {target_rel} ═══")
    print(f"  Score Global : {score}/100  ({'✓ CUMPLE 100%' if total_v == 0 else f'✗ {total_v} violaciones'})")
    print(f"  SOLID        : {'✓ OK' if solid_v == 0 else f'✗ {solid_v} hallazgo(s)'}")
    print(f"  DRY          : {'✓ OK' if dry_v == 0 else f'✗ {dry_v} hallazgo(s)'}")
    print(f"  Clean Code   : {'✓ OK' if cc_v == 0 else f'✗ {cc_v} hallazgo(s)'}")
    if total_v > 0:
        print("\nDetalle de hallazgos por línea:")
        for chk in CHECKS:
            for line_no, msg in file_hits.get(chk, []):
                print(f"  - [{chk}] {target_rel}:{line_no} → {msg}")
    return 0 if total_v == 0 else 1


def print_summary_report(
    counts: dict[str, dict[str, int]],
    findings: dict[str, dict[str, list[tuple[int, str]]]],
    gate_label: str,
    failures: list[str],
    verbose: bool,
) -> None:
    """Imprime la tabla global por dimensión SOLID, DRY y Clean Code."""
    totals = {c: sum(chks.get(c, 0) for chks in counts.values()) for c in CHECKS}
    rows = [
        ("═══ Auditoría Global SOLID · DRY · Clean Code (metri-engine) ═══", None),
        (f"  Archivos con deuda registrada : {len(counts)}", None),
        ("  ── SOLID ───────────────────────────────────────────────────", None),
        ("    srp_file_lines (>500L)", totals["srp_file_lines"]),
        ("    dip_layer_violation", totals["dip_layer_violation"]),
        ("    isp_fat_trait (>5 métodos)", totals["isp_fat_trait"]),
        ("  ── DRY ─────────────────────────────────────────────────────", None),
        ("    dry_duplicate_block (>=10L)", totals["dry_duplicate_block"]),
        ("    dry_scattered_env", totals["dry_scattered_env"]),
        ("    dry_python_infra", totals["dry_python_infra"]),
        ("  ── Clean Code ──────────────────────────────────────────────", None),
        ("    cc_fn_length (>80L)", totals["cc_fn_length"]),
        ("    cc_fn_params (>5 params)", totals["cc_fn_params"]),
        ("    cc_nesting_depth (>6 niv.)", totals["cc_nesting_depth"]),
        ("    cc_unsafe_usage", totals["cc_unsafe_usage"]),
    ]
    for label, val in rows:
        print(label if val is None else f"{label:<32}: {val:4d}")
    print(f"\nGate: {gate_label}")
    if failures:
        print(f"\n✗ FALLOS DEL GATE ({len(failures)}):")
        for fail in failures[:25]:
            print(f"  · {fail}")
    if verbose:
        for rel, chks in sorted(findings.items()):
            for chk in CHECKS:
                for line_no, msg in chks.get(chk, []):
                    print(f"  [{chk}] {rel}:{line_no}  {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag, hlp in (("--file", "Archivo único"), ("--only", "Módulo src/<MOD>")):
        parser.add_argument(flag, help=hlp)
    for flag in ("--strict", "--update-baseline", "--verbose", "--json"):
        parser.add_argument(flag, action="store_true")
    args = parser.parse_args()

    findings = collect_all(args.only)
    counts = summarize_counts(findings)
    if args.file:
        return print_file_scorecard(Path(args.file).resolve().relative_to(ROOT).as_posix(), findings)
    if args.update_baseline:
        payload = {"generated_at": date.today().isoformat(), "files": counts}
        BASELINE_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"✓ Línea base actualizada en {BASELINE_PATH.relative_to(ROOT)} ({len(counts)} archivos)")
        return 0
    if args.strict:
        failures = [f"{rel} [{chk}]: {n}" for rel, chks in counts.items() for chk, n in chks.items()]
        gate_label = "ESTRICTO (0 violaciones toleradas)"
    else:
        base = json.loads(BASELINE_PATH.read_text(encoding="utf-8")).get("files", {}) if BASELINE_PATH.exists() else {}
        failures = evaluate_ratchet(counts, base)
        gate_label = "RATCHET (no-regresión por archivo) — OK" if not failures else "RATCHET — FALLÓ"
    if args.json:
        print(json.dumps({"gate": gate_label, "ok": not failures, "failures": failures, "files": counts}, indent=2))
    else:
        print_summary_report(counts, findings, gate_label, failures, args.verbose)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
