#!/usr/bin/env python3
"""
metres_schema.py — CLI del Metri Schema Engine.

Uso:
  python metres_schema.py full     <proto> [--output <edn>] [--edn <edn>]
  python metres_schema.py analyse  <proto>
  python metres_schema.py generate <proto> [--output <edn>]
  python metres_schema.py dedup    <proto> [--edn <edn>] [--fix]
  python metres_schema.py domains  <proto>
  python metres_schema.py inspect  <proto>
"""
from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from metri_schema_engine.descriptor_walker import build_descriptor_tree
from metri_schema_engine.contract_synthesizer import synthesize
from metri_schema_engine.edn_emitter import emit_edn
from metri_schema_engine.dedup_checker import (
    ProtoAuditor, EDNAuditor, CrossValidator, DupFixer,
    AuditReport, SEVERITY_ERROR, SEVERITY_WARNING, SEVERITY_INFO,
)

SEP  = "═" * 62
SEP2 = "─" * 58


# ─────────────────────────────────────────────────────────────────────────────
# MODO 1 — analyse
# ─────────────────────────────────────────────────────────────────────────────

def cmd_analyse(proto_path: Path) -> None:
    # Lazy imports — requieren networkx
    from metri_schema_engine.graph_engine import build_proto_graph
    from metri_schema_engine.graph_metrics import full_report
    from metri_schema_engine.coherence import evaluate as score_coherence

    print(f"\n[1/3] Analizando grafo de {proto_path.name}...")
    pg     = build_proto_graph(proto_path)
    source = proto_path.read_text(encoding="utf-8")

    print(f"\n{SEP}")
    print(f"  GRAPH ANALYSIS — {pg.service}")
    print(SEP)
    print(f"  Nodos:    {pg.node_count:>4}  (service + rpcs + messages + enums)")
    print(f"  Aristas:  {pg.edge_count:>4}  (referencias entre descriptors)")
    print(f"  Messages: {len(pg.messages):>4}")
    print(f"  Enums:    {len(pg.enums):>4}  (globales top-level)")
    print(f"  RPCs:     {len(pg.rpcs):>4}")

    report = full_report(pg)
    print(f"\n  MÉTRICAS DEL GRAFO")
    print(f"  {SEP2}")
    density = report["degree_stats"].get("density", 0)
    max_fo  = max((v["fan_out"]  for v in report["fan_in_out"].values()), default=0)
    max_fi  = max((v["fan_in"]   for v in report["fan_in_out"].values()), default=0)
    depth   = report["depth_profile"].get("max_depth", 0)
    isolates= len(report["isolate_nodes"])
    print(f"  Densidad:          {density:.4f}  {'✅' if density < 0.15 else '⚠️  > 0.15'}")
    print(f"  Fan-out máximo:    {max_fo}  (max referencias desde un nodo)")
    print(f"  Fan-in máximo:     {max_fi}  (max nodos que apuntan a uno)")
    print(f"  Depth BFS máximo:  {depth}  {'✅' if depth <= 8 else '⚠️  > 8'}")
    print(f"  Nodos aislados:    {isolates}  {'✅' if isolates == 0 else '⚠️'}")

    pr       = report["pagerank"]
    top5     = sorted(pr.items(), key=lambda x: -x[1])[:5]
    print(f"\n  TOP-5 POR PAGERANK  (centralidad de influencia)")
    print(f"  {SEP2}")
    for node, score in top5:
        bar = "█" * int(score * 160)
        print(f"  {node:<38} {score:.4f}  {bar}")

    bet = report.get("betweenness", {})
    if bet:
        top_bet = sorted(bet.items(), key=lambda x: -x[1])[:3]
        print(f"\n  TOP-3 BETWEENNESS  (nodos puente críticos)")
        print(f"  {SEP2}")
        for node, score in top_bet:
            print(f"  {node:<38} {score:.4f}")

    coherence = score_coherence(pg, source)
    print(f"\n  COHERENCE SCORE")
    print(f"  {SEP2}")
    bar_len = int(coherence.score / 100 * 40)
    bar     = "█" * bar_len + "░" * (40 - bar_len)
    print(f"  [{bar}] {coherence.score:.2f}/100")
    print(f"  {coherence.verdict}")

    if coherence.penalties:
        print(f"\n  Penalizaciones:")
        for p in coherence.penalties:
            print(f"    🔴 [{p['rule']}] -{p['deducted']:.1f}pts — {p['reason']}")
    if coherence.bonuses:
        print(f"\n  Bonificaciones:")
        for b in coherence.bonuses:
            desc = b.get("reason") or b.get("description", "")
            pts  = b.get("points") or b.get("deducted", 0)
            print(f"    ✅ +{pts:.1f}pts — {desc}")

    print(f"{SEP}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODO 2 — generate
# ─────────────────────────────────────────────────────────────────────────────

def cmd_generate(proto_path: Path, output_path: Path) -> None:
    print(f"\n[2/3] Generando contrato EDN desde {proto_path.name}...")
    tree     = build_descriptor_tree(proto_path)
    contract = synthesize(
        tree,
        source_proto=str(proto_path),
        generated_at=datetime.datetime.utcnow().isoformat() + "Z",
    )
    edn_str = emit_edn(contract)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(edn_str, encoding="utf-8")

    enum_specs = [s for s in contract.specs if "enum"             in s.tags]
    recursive  = [s for s in contract.specs if "recursive"        in s.tags]
    zt_specs   = [s for s in contract.specs if "zero-trust-boundary" in s.tags]

    print(f"\n  ✓ Generado: {output_path}")
    print(f"  {'═' * 40}")
    print(f"   Specs emitidas:     {len(contract.specs)}")
    print(f"   ├── Enums:          {len(enum_specs)}")
    print(f"   ├── Messages:       {len(contract.specs) - len(enum_specs)}")
    print(f"   ├── Recursivos:     {len(recursive)}")
    print(f"   └── Zero-Trust:     {len(zt_specs)}")
    print(f"   RPCs:               {len(contract.rpc_contracts)}")
    print(f"   Railway specs:      {len(contract.railway_specs)}")
    print(f"  {'═' * 40}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODO 3 — dedup
# ─────────────────────────────────────────────────────────────────────────────

def _print_findings(report: AuditReport, title: str) -> None:
    icons = {SEVERITY_ERROR: "🔴", SEVERITY_WARNING: "🟡", SEVERITY_INFO: "ℹ️ "}
    if not report.findings:
        print(f"  ✅ {title}: Sin hallazgos")
        return
    print(f"\n  📋 {title}")
    print(f"     {'─' * 54}")
    for f in report.findings:
        icon = icons.get(f.severity, "•")
        print(f"     {icon} [{f.category}]")
        print(f"        Loc: {f.location}")
        print(f"        {f.message}")
        if f.fix:
            print(f"        💡 {f.fix}")
        if f.auto_fixable:
            print(f"        🔧 Auto-fixable")
        print()


def cmd_dedup(proto_path: Path, edn_path: Path | None,
              fix: bool = False, fix_output: Path | None = None) -> int:
    print(f"\n[3/3] Auditando duplicaciones...")
    print(f"\n{SEP}")
    print(f"  DEDUP CHECKER — proto ↔ edn")
    print(SEP)

    proto_report = ProtoAuditor(proto_path).audit()
    _print_findings(proto_report, f"[A] Proto")

    all_findings = list(proto_report.findings)
    if edn_path and edn_path.exists():
        edn_report   = EDNAuditor(edn_path).audit()
        cross_report = CrossValidator(proto_path, edn_path).audit()
        _print_findings(edn_report,   "[B] EDN")
        _print_findings(cross_report, "[C] Cross-validation proto ↔ edn")
        all_findings += edn_report.findings + cross_report.findings

    errors   = [f for f in all_findings if f.severity == SEVERITY_ERROR]
    warnings = [f for f in all_findings if f.severity == SEVERITY_WARNING]
    auto_fix = [f for f in all_findings if f.auto_fixable]

    print(f"\n{SEP}")
    print(f"  RESULTADO")
    print(SEP)
    print(f"  🔴 Errores:      {len(errors)}")
    print(f"  🟡 Warnings:     {len(warnings)}")
    print(f"  🔧 Auto-fixable: {len(auto_fix)}")

    if not all_findings:
        print(f"\n  ✅ CONTRATO LIMPIO — Sin duplicaciones detectadas.")
    elif fix and auto_fix and edn_path:
        print(f"\n  Aplicando {len(auto_fix)} correcciones automáticas...")
        fixer = DupFixer(edn_path)
        fixed, applied = fixer.fix(all_findings)
        out = fix_output or edn_path
        out.write_text(fixed, encoding="utf-8")
        print(f"  ✅ {applied} correcciones aplicadas → {out}")
    elif not fix and auto_fix:
        print(f"\n  💡 Ejecuta con --fix para aplicar {len(auto_fix)} corrección(es) automática(s).")

    print(f"{SEP}\n")
    return 1 if errors else 0


# ─────────────────────────────────────────────────────────────────────────────
# MODO 4 — domains
# ─────────────────────────────────────────────────────────────────────────────

def cmd_domains(proto_path: Path) -> None:
    # Lazy imports — requieren networkx
    from metri_schema_engine.graph_engine import build_proto_graph
    from metri_schema_engine.domain_partitioner import build_domain_profiles

    print(f"\n  Particionando {proto_path.name} en dominios...")
    pg       = build_proto_graph(proto_path)
    profiles = build_domain_profiles(pg)

    print(f"\n{SEP}")
    print(f"  DOMINIOS DEL SCHEMA — {pg.service}")
    print(f"  {len(profiles)} dominios detectados (algoritmo Louvain + Fiedler)")
    print(SEP)

    for dname, domain in sorted(profiles.items(), key=lambda x: -len(x[1].nodes)):
        fiedler = getattr(domain, "fiedler_value", 0.0)
        nodes   = sorted(getattr(domain, "nodes", []))
        print(f"\n  📦 {dname}  →  {len(nodes)} nodos  |  Fiedler: {fiedler:.4f}")
        # Mostrar todos los nodos del dominio
        for i in range(0, len(nodes), 4):
            chunk = nodes[i:i+4]
            print(f"      " + "  ".join(f"{n:<28}" for n in chunk))
        boundary = getattr(domain, "boundary_edges", [])
        if boundary:
            print(f"     🔗 {len(boundary)} contratos inter-dominio")

    print(f"\n{SEP}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODO 5 — inspect
# ─────────────────────────────────────────────────────────────────────────────

def cmd_inspect(proto_path: Path) -> None:
    tree = build_descriptor_tree(proto_path)
    print(f"\n{SEP}")
    print(f"  INSPECTOR — {tree.service_name}")
    print(SEP)

    print(f"\n  RPCs ({len(tree.rpcs)})")
    print(f"  {'─' * 54}")
    for rpc in tree.rpcs:
        stream = "  [server-stream]" if rpc.is_streaming else ""
        zt = "  [zero-trust]" if any(
            tree.messages.get(rpc.request_type, None) and
            tree.messages[rpc.request_type].is_zero_trust_boundary
            for _ in [1]
        ) else ""
        print(f"  {rpc.name:<30} {rpc.request_type} → {rpc.response_type}{stream}{zt}")

    global_enums = {n: e for n, e in tree.enums.items() if n not in tree.nested_enum_owner}
    print(f"\n  ENUMS GLOBALES ({len(global_enums)})")
    print(f"  {'─' * 54}")
    for name, enum in global_enums.items():
        sentinel = f" sentinel={enum.sentinel.name}" if enum.sentinel else " ⚠️ sin sentinel"
        print(f"  {name:<35} {len(enum.values)} valores —{sentinel}")

    print(f"\n  MENSAJES ({len(tree.messages)})")
    print(f"  {'─' * 54}")
    for name, msg in tree.messages.items():
        tags = []
        if msg.is_zero_trust_boundary:  tags.append("ZT")
        if msg.is_recursive:            tags.append("REC")
        if msg.nested_enums:            tags.append(f"{len(msg.nested_enums)}×enum")
        oneof_str = f"  {len(msg.oneofs)} oneof" if msg.oneofs else ""
        tag_str   = f"  [{', '.join(tags)}]" if tags else ""
        print(f"  {name:<38} {len(msg.fields):>2} fields{oneof_str}{tag_str}")

    print(f"\n{SEP}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODO full — todos los modos en secuencia
# ─────────────────────────────────────────────────────────────────────────────

def cmd_full(proto_path: Path, output_path: Path, edn_existing: Path | None,
             fix: bool = False) -> int:
    print(f"\n{SEP}")
    print(f"  METRI SCHEMA ENGINE — Pipeline Completo")
    print(f"  Fuente:  {proto_path}")
    print(f"  Contrato: {output_path}")
    print(SEP)
    cmd_analyse(proto_path)
    cmd_generate(proto_path, output_path)
    return cmd_dedup(proto_path, output_path, fix=fix)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="metri-schema",
        description="Metri Schema Engine — Análisis y transpilación de grpc proto3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
modos:
  full      Análisis + generación + auditoría (completo)
  analyse   Métricas del grafo + coherencia
  generate  Transpila proto → janus-aegis-contract EDN
  dedup     Auditoría de duplicaciones proto ↔ edn
  domains   Partición por dominios (Louvain)
  inspect   Inspector de descriptors (RPCs, enums, messages)
""",
    )
    p.add_argument("mode",  nargs="?", default="full",
        choices=["full", "analyse", "generate", "dedup", "domains", "inspect"])
    p.add_argument("proto", help="Ruta al archivo .proto")
    p.add_argument("--output", "-o", default=None,
        help="Ruta de salida del contrato EDN")
    p.add_argument("--edn", default=None,
        help="Ruta al EDN existente para dedup/cross-validation")
    p.add_argument("--fix", "-f", action="store_true",
        help="Aplicar correcciones automáticas (modo dedup)")
    p.add_argument("--fix-output", default=None,
        help="Archivo de salida del EDN corregido")
    return p


def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()

    proto_path = Path(args.proto)
    if not proto_path.exists():
        print(f"ERROR: {proto_path} no existe", file=sys.stderr)
        return 1

    # Resolver paths de output
    default_edn = proto_path.parent / "resources" / "schema" / "janus-ast-ir.edn"
    edn_out     = Path(args.output) if args.output else default_edn
    edn_in      = Path(args.edn) if args.edn else (edn_out if edn_out.exists() else None)

    if args.mode == "analyse":
        cmd_analyse(proto_path)
    elif args.mode == "generate":
        cmd_generate(proto_path, edn_out)
    elif args.mode == "dedup":
        fix_out = Path(args.fix_output) if args.fix_output else None
        return cmd_dedup(proto_path, edn_in, fix=args.fix, fix_output=fix_out)
    elif args.mode == "domains":
        cmd_domains(proto_path)
    elif args.mode == "inspect":
        cmd_inspect(proto_path)
    else:  # full
        return cmd_full(proto_path, edn_out, edn_in, fix=args.fix)
    return 0


if __name__ == "__main__":
    sys.exit(main())
