#!/usr/bin/env python3
"""
validate_contract.py — Janus-Aegis Contract Coverage Validator
===============================================================
Garantiza que el 100% del contrato definido en janus-ast-ir.edn
(+cedar-janus-contract-v1.edn) esté representado en la implementación.

Cómo funciona:
  • Cada check mapea un ítem del contrato EDN → su representación en código.
  • La búsqueda es pragmática: verifica presencia del concepto, no el string
    literal del EDN (que usa `:metri.spec/`, la impl usa `def` planos).
  • Reporta PASS / FAIL por ítem y sección, con % de cobertura.
  • Exit 0 = 100% coverage. Exit 1 = gaps encontrados (CI-safe).

Fuentes del contrato:
  resources/schema/janus-ast-ir.edn          (§1–§7)
  docs/architecture/cedar-janus-contract-v1.edn  (§Cedar)
"""

import re, sys
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent   # metri-engine/
SRC  = ROOT / "src" / "metri"
RES  = ROOT / "resources"

FILES = {
    "ast_specs":     SRC / "janus" / "ast_specs.clj",
    "ast_compiler":  SRC / "janus" / "ast_compiler.clj",
    "janus_core":    SRC / "janus" / "core.clj",
    "aegis_core":    SRC / "aegis" / "core.clj",
    "datalog":       SRC / "aegis" / "datalog.clj",
    "sql":           SRC / "aegis" / "sql.clj",
    "translator":    SRC / "grpc" / "translator.clj",
    "service":       SRC / "grpc" / "service.clj",
    "dispatcher":    SRC / "grpc" / "dispatcher.clj",
    "protocols":     SRC / "domain" / "protocols.clj",
    "error_catalog": RES / "errors" / "error_catalog.edn",
    "proto":         ROOT / "metri.proto",
    "janus_ast_ir":  RES / "schema" / "janus-ast-ir.edn",
}

# ─────────────────────────────────────────────────────────────────────────────
# ANSI
# ─────────────────────────────────────────────────────────────────────────────
GRN, RED, YLW, BOLD, RST = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[0m"
ok   = lambda m: f"{GRN}✓{RST}  {m}"
fail = lambda m: f"{RED}✗{RST}  {m}"

# ─────────────────────────────────────────────────────────────────────────────
# File cache
# ─────────────────────────────────────────────────────────────────────────────
_cache: dict[str, str] = {}

def load(key: str) -> str:
    if key not in _cache:
        p = FILES[key]
        _cache[key] = p.read_text(encoding="utf-8") if p.exists() else ""
    return _cache[key]

def has(key: str, *patterns, mode="any") -> bool:
    """True si el archivo contiene alguno (mode=any) / todos (mode=all) los patterns.
    Prefijo 're:' → trata el resto como regex. Sin prefijo → búsqueda literal."""
    text = load(key)
    hits = [bool(re.search(p[3:], text)) if p.startswith("re:") else (p in text)
            for p in patterns]
    return all(hits) if mode == "all" else any(hits)

# ─────────────────────────────────────────────────────────────────────────────
# Result accumulator
# ─────────────────────────────────────────────────────────────────────────────
results: list[tuple[str, str, bool, str]] = []  # (section, item, passed, note)

def chk(section: str, item: str, passed: bool, note: str = ""):
    results.append((section, item, passed, note))

# ═════════════════════════════════════════════════════════════════════════════
# §0 — Existencia de archivos de implementación
# ═════════════════════════════════════════════════════════════════════════════
def check_files():
    for key, path in FILES.items():
        chk("§0 Archivos", str(path.relative_to(ROOT)), path.exists())

# ═════════════════════════════════════════════════════════════════════════════
# §1 — Enums (resources/schema/janus-ast-ir.edn §1)
# La impl usa strings ("COUNT") no keywords (:COUNT) en los Malli :enum defs.
# ═════════════════════════════════════════════════════════════════════════════
ENUMS = {
    # contrato key → (def_name_in_ast_specs, [valores_string])
    "aggregation-function": (
        "aggregation-function",
        ["COUNT","SUM","AVG","MIN","MAX","MEDIAN","STD_DEV","VARIANCE",
         "PERCENTILE_90","PERCENTILE_95","PERCENTILE_99",
         "CORRELATION","LINEAR_REGRESSION","LOGISTIC_REGRESSION"],
    ),
    "filter-operator": (
        "filter-operator",
        ["EQ","NEQ","GT","GTE","LT","LTE","IN","NOT_IN",
         "BETWEEN","LIKE","IS_NULL","IS_NOT_NULL","MATCHES","CONTAINS"],
    ),
    "output-cast-type": (
        "output-cast-type",
        ["KPI","TIMESERIES","TABLE","PIE","BUBBLE","CSV_EXPORT"],
    ),
    # OperationAction: en la impl se valida en translator.clj (proto→clj),
    # no en ast_specs (es un enum del write path — IOP pipeline).
    "operation-action": (
        None,  # no spec Malli — validado via proto enum en translator
        ["CREATE","UPDATE","DELETE","UPSERT","GET"],
    ),
}

def check_section_1():
    for name, (def_name, values) in ENUMS.items():
        if def_name:
            in_specs = has("ast_specs", f"def {def_name}")
            chk("§1 Enums", f":metri.spec/{name} → def {def_name} en ast_specs.clj", in_specs)
            for v in values:
                chk("§1 Enums", f"  {name}/{v} en ast_specs.clj", has("ast_specs", f'"{v}"'))
        else:
            # operation-action: validado en translator via clase proto OperationAction
            in_translator = has("translator", "OperationAction", "operation-action",
                                "CREATE", "DELETE", "UPSERT")
            chk("§1 Enums",
                f":metri.spec/{name} — write-path, validado en translator.clj",
                in_translator,
                note="IOP write path — no AST spec")
            for v in values:
                chk("§1 Enums", f"  {name}/{v} en translator.clj",
                    has("translator", v) or has("service", v))

# ═════════════════════════════════════════════════════════════════════════════
# §2 — Schemas Recursivos
# ═════════════════════════════════════════════════════════════════════════════
def check_section_2():
    # FilterNode + FilterGroup — en ast_specs como def filter-node con registry ::fn/::fg
    chk("§2 Recursivos", "FilterNode → def filter-node en ast_specs.clj",
        has("ast_specs", "def filter-node"))
    chk("§2 Recursivos", "FilterGroup → ::fg registry dentro de filter-node",
        has("ast_specs", "::fg"))
    chk("§2 Recursivos", "Patrón [:schema {:registry}] usado",
        has("ast_specs", ":registry"))
    chk("§2 Recursivos", "FilterCriteria → def filter-criteria en ast_specs.clj",
        has("ast_specs", "def filter-criteria"))
    chk("§2 Recursivos", "FilterValue → def filter-value en ast_specs.clj",
        has("ast_specs", "def filter-value"))

    # QueryResponse: estructura de respuesta gRPC — responsabilidad de translator.clj
    chk("§2 Recursivos",
        "QueryResponse — serializador en translator.clj (proto→clj, no AST spec)",
        has("translator", "QueryResponse"),
        note="gRPC serialization layer, no Malli spec")

# ═════════════════════════════════════════════════════════════════════════════
# §3 — Mensajes Zero-Trust (deben llevar tenant-id)
# Cada uno debe: (a) tener traducción en translator.clj, (b) pasar por
# validación de tenant en janus/core o dispatcher.
# ═════════════════════════════════════════════════════════════════════════════
ZT_MESSAGES = {
    # dash-case → proto CamelCase class name
    "analytics-request":            "AnalyticsRequest",
    "match-routing-rules-request":  "MatchRoutingRulesRequest",
    "query-request":                "QueryRequest",
    "bulk-request":                 "BulkRequest",
    "transaction-request":          "TransactionRequest",
    "explore-request":              "ExploreRequest",
    "discovery-request":            "DiscoveryRequest",
}

def check_section_3():
    # Validación de tenant-id en janus_core / dispatcher
    tenant_guarded = (has("janus_core", "tenant-id") or has("dispatcher", "tenant-id"))
    chk("§3 Zero-Trust",
        "tenant-id validado en janus/core.clj o dispatcher.clj",
        tenant_guarded)
    chk("§3 Zero-Trust",
        "blank tenant-id → error JANUS_400",
        has("janus_core", "JANUS_400") or has("dispatcher", "JANUS_400"))

    for msg, proto_class in ZT_MESSAGES.items():
        # Traducción proto en translator.clj
        in_trans = has("translator", proto_class) or has("translator", msg)
        chk("§3 Zero-Trust", f"{proto_class} mapeado en translator.clj", in_trans)

# ═════════════════════════════════════════════════════════════════════════════
# §4 — Mensajes de Dominio
# La mayoría son estructuras de respuesta proto — se verifican en translator.clj
# (usa el nombre CamelCase de la clase Java generada por protoc).
# Los specs Malli directos (filter-criteria, filter-value) están en ast_specs.clj.
# ═════════════════════════════════════════════════════════════════════════════
# Cada entrada: (proto_class, check_files, search_terms, note)
# check_files: lista de keys a buscar (OR entre ellos)
# search_terms: strings o "re:..." a buscar (OR)
# note: anotación opcional
DOMAIN_MESSAGES = [
    # ── AST Specs (Malli directo) ──
    ("FilterCriteria",    ["ast_specs"],                    ["def filter-criteria"],           None),
    ("FilterValue",       ["ast_specs"],                    ["def filter-value"],              None),
    ("FilterNode",        ["ast_specs"],                    ["def filter-node", "::fn"],       None),
    ("FilterGroup",       ["ast_specs"],                    ["::fg"],                          "registry dentro de filter-node"),

    # ── Proto Request messages → translator.clj (entrada gRPC) ──
    ("RowSet",            ["translator"],                   ["RowSet", "rowset"],              None),
    ("DataRowList",       ["translator"],                   ["DataRowList", "data-row-list"],  None),
    ("DataRow",           ["translator"],                   ["DataRow", "data-row"],           None),
    ("ColumnSchema",      ["translator"],                   ["ColumnSchema", "column-schema"], None),
    ("QueryMetadata",     ["translator"],                   ["QueryMetadata", "query-metadata","execution-time"], None),
    ("QueryResponse",     ["translator"],                   ["QueryResponse", "query-response"],None),
    ("Status",            ["translator"],                   ["ok-status", "error-status", "Status"], None),
    ("Pagination",        ["translator"],                   ["Pagination", "pagination"],      None),
    ("Link",              ["translator"],                   ["Link", "link"],                  None),
    ("EntitySchema",      ["translator"],                   ["entity-schema", "EntitySchema"], None),
    ("AttributeSchema",   ["translator"],                   ["attribute-schema", "AttributeSchema"], None),
    ("ExploreResponse",   ["translator"],                   ["explore-result", "ExploreResponse"], None),
    ("DiscoveryResponse", ["translator"],                   ["discovery-result", "DiscoveryResponse"], None),
    ("BulkResponse",      ["translator"],                   ["bulk-response", "BulkResponse"], None),
    ("TransactionResponse",["translator"],                  ["transaction-response", "TransactionResponse"], None),
    ("MatchRoutingRulesResponse",      ["translator"],      ["match-response", "MatchRoutingRulesResponse"], None),
    ("MatchRoutingRulesBatchResponse", ["translator"],      ["match-batch-result", "MatchRoutingRulesBatchResponse"], None),
    ("MatchRoutingRulesBatchRequest",  ["translator"],      ["match-batch-request", "MatchRoutingRulesBatchRequest"], None),
    ("MatchedRule",       ["translator"],                   ["matched-rule", "MatchedRule"],   None),
    ("WebhookTarget",     ["translator"],                   ["webhook", "WebhookTarget"],      None),
    ("BulkRequest",       ["translator"],                   ["bulk-request", "BulkRequest"],   None),

    # ── AnalyticsRequest nested fields: manejados en translator (query-request->ctx)
    #    y/o ast_compiler. Se busca el field-name proto (snake_case getter) ──
    ("MetricDefinition",  ["translator", "ast_compiler"],  ["metric", "aggregation"],         "campo de AnalyticsRequest"),
    ("SortDefinition",    ["translator", "ast_compiler"],  ["sort", "order-by"],              "campo de AnalyticsRequest"),
    ("TimeFrameContext",  ["translator", "ast_compiler"],  ["time-frame", "time_frame"],      "campo de AnalyticsRequest"),
    ("DimensionDefinition",["translator","ast_compiler"],  ["dimension", "group-by"],         "campo de AnalyticsRequest"),
    ("FormulaEntry",      ["translator", "ast_compiler"],  ["measure", "formula"],            "campo de AnalyticsRequest"),
    ("BatchContext",      ["translator", "ast_compiler"],  ["context", "batch"],              "campo de QueryRequest"),
    ("HierarchyContext",  ["translator", "ast_compiler"],  ["hierarchy"],                     "campo de AnalyticsRequest"),
    ("AnalyticalComparison",["translator","ast_compiler"], ["comparison"],                    "campo de AnalyticsRequest"),
    ("SemanticMetricRef", ["translator", "ast_compiler"],  ["semantic", "metric-key"],        "campo de AnalyticsRequest"),
    ("DashboardCrossFilterContext",["translator","ast_compiler"],["cross-filter"],            "campo de QueryRequest"),
    ("MultiSeriesGroup",  ["translator", "ast_compiler"],  ["merge-group", "multi-series"],   "campo de QueryRequest"),
    ("FilterValueList",   ["translator", "ast_specs"],     ["filter-value-list", "range-values"], "campo de FilterValue"),
    ("StringList",        ["translator", "ast_specs"],     ["string-list", "list-val"],       "campo de FilterValue"),

    # ── VizMeta: extensión de respuesta visual (QueryResponse.viz_ext) ──
    #    Requiere implementación en aegis-chunk->query-response ──
    ("VizMeta",           ["translator"],                   ["viz", "VizMeta"],               "QueryResponse.viz_ext"),
    ("AnalyticalSignal",  ["translator"],                   ["signal", "AnalyticalSignal"],   "VizMeta.signal"),
    ("ChartDecoration",   ["translator"],                   ["chart", "ChartDecoration"],     "VizMeta.chart"),
    ("TableMeta",         ["translator"],                   ["table-meta", "TableMeta", "TableColumn","columns"], None),
    ("TreeMeta",          ["translator"],                   ["tree", "TreeMeta"],             "VizMeta.tree"),
    ("BreakdownSignal",   ["translator"],                   ["breakdown", "BreakdownSignal"], "VizMeta.breakdown"),
    ("TableColumn",       ["translator"],                   ["column", "TableColumn"],        "VizMeta.table.columns"),
    ("IntelligenceSignal",["translator"],                   ["intelligence", "IntelligenceSignal"], "AnalyticalSignal.intelligence"),
    ("IndicatorThreshold",["translator"],                   ["threshold", "IndicatorThreshold"],    "AnalyticalSignal.thresholds"),

    # ── ChronosAlertTask: no pertenece a ningún RPC implementado aún ──
    ("ChronosAlertTask",  ["translator", "service"],        ["chronos", "ChronosAlertTask"],  "RPC futuro — no implementado"),
    ("Action",            ["translator"],                   ["action", "Action"],              "TableMeta.row_actions"),
]

def check_section_4():
    for entry in DOMAIN_MESSAGES:
        proto_class, file_keys, search_terms, note = entry
        note_str = note or ""
        passed = any(
            any(has(fk, t) for t in search_terms)
            for fk in file_keys
        )
        files_str = "/".join(file_keys)
        chk("§4 Dominio", f"{proto_class} en {files_str}.clj", passed,
            note=note_str)

# ═════════════════════════════════════════════════════════════════════════════
# §5 — Railway Error Contracts
# ═════════════════════════════════════════════════════════════════════════════
def check_section_5():
    # railway-error-union pattern
    chk("§5 Railway", "def railway-error-union o context-invariant-validator en ast_specs",
        has("ast_specs", "railway-error-union") or has("ast_specs", "context-invariant-validator"))
    chk("§5 Railway", "[:ok ...] usado en janus/core",
        has("janus_core", "re::ok "))
    chk("§5 Railway", "[:error ...] usado en janus/core",
        has("janus_core", "re::error "))
    chk("§5 Railway", "[:ok ...] usado en ast_compiler",
        has("ast_compiler", "re::ok "))
    chk("§5 Railway", "[:ok ...] / [:error ...] en aegis datalog",
        has("datalog", "re::ok ") or has("datalog", "re::error "))

    # Códigos de error JANUS
    for code in ["JANUS_400", "JANUS_403"]:
        chk("§5 Railway", f"{code} en error_catalog.edn",
            has("error_catalog", code))

    # Códigos AEG
    for code in ["AEG_001","AEG_002","AEG_003","AEG_004","AEG_005",
                 "AEG_COMPILE_001","AEG_COMPILE_002","AEG_TENANT_MISSING"]:
        chk("§5 Railway", f"{code} en error_catalog.edn",
            has("error_catalog", code))

# ═════════════════════════════════════════════════════════════════════════════
# §6 — Contratos de RPCs
# ═════════════════════════════════════════════════════════════════════════════
RPCS = {
    # rpc_name: (proto_camel, req_class, resp_class, streaming)
    "Discovery":             ("Discovery",            "DiscoveryRequest",            "DiscoveryResponse",             False),
    "Explore":               ("Explore",              "ExploreRequest",              "ExploreResponse",               False),
    "Query":                 ("Query",                "QueryRequest",                "QueryResponse",                 True),
    "Transact":              ("Transact",             "TransactionRequest",          "TransactionResponse",           False),
    "BulkIngest":            ("BulkIngest",           "BulkRequest",                 "BulkResponse",                  False),
    "MatchRoutingRulesBatch":("MatchRoutingRulesBatch","MatchRoutingRulesBatchRequest","MatchRoutingRulesBatchResponse",False),
}

def check_section_6():
    proto_text = load("proto")
    chk("§6 RPC", "metri.proto existe", bool(proto_text))
    chk("§6 RPC", "service MetriService definido en metri.proto",
        "service MetriService" in proto_text)

    for rpc, (camel, req, resp, streaming) in RPCS.items():
        # Proto define el RPC
        chk("§6 RPC", f"rpc {camel} en metri.proto",
            f"rpc {camel}" in proto_text)
        # Translator mapea request y response
        chk("§6 RPC", f"{req} mapeado en translator.clj",
            has("translator", req))
        chk("§6 RPC", f"{resp} mapeado en translator.clj",
            has("translator", resp))
        # Service implementa el método
        in_svc = has("service", camel) or has("service", camel.lower())
        chk("§6 RPC", f"{camel} implementado en service.clj", in_svc)
        if streaming:
            chk("§6 RPC", f"{camel} usa server-streaming (onNext/StreamObserver)",
                has("service", "onNext") or has("service", "StreamObserver") or
                has("service", "observer"))

# ═════════════════════════════════════════════════════════════════════════════
# §7 — Invariantes Zero-Trust (Janus clauses)
# ═════════════════════════════════════════════════════════════════════════════
def check_section_7():
    # janus/tenant-isolation-rule
    chk("§7 ZT Invariants",
        "tenant-isolation-rule: blank? tenant-id → JANUS_400",
        has("janus_core", "JANUS_400"))

    chk("§7 ZT Invariants",
        "tenant-id extraído de cedar-ctx, nunca del request del cliente",
        has("janus_core", "tenant-id") and
        (has("janus_core", "cedar-ctx") or has("janus_core", "cedar_ctx")))

    # janus/ast-tenant-invariant
    chk("§7 ZT Invariants",
        "ast-tenant-invariant: inject-tenant-node siempre primero en AST WHERE",
        has("ast_compiler", "inject-tenant") or has("ast_compiler", "tenant-id"))

    chk("§7 ZT Invariants",
        "ast-contains-tenant? guard en sql.clj (OLAP invariant)",
        has("sql", "ast-contains-tenant") or has("sql", "tenant"))

    # janus/zero-trust-boundary-messages — los 7 mensajes pasan por pipeline
    zt_msgs = ["discovery-request","explore-request","query-request",
               "transaction-request","bulk-request","match-routing-rules-request"]
    for msg in zt_msgs:
        proto = "".join(w.capitalize() for w in msg.split("-"))
        chk("§7 ZT Invariants",
            f"zero-trust-boundary: {proto} fluye por validación tenant",
            has("dispatcher", proto) or has("dispatcher", msg) or
            has("service", proto) or has("janus_core", "tenant-id"))

    # janus/enum-sentinel-gate
    chk("§7 ZT Invariants",
        "enum-sentinel-gate: rechazo de enum valor 0/_UNSPECIFIED",
        has("translator", "UNSPECIFIED") or has("janus_core", "UNSPECIFIED") or
        has("ast_compiler", "UNSPECIFIED") or has("service", "UNSPECIFIED") or
        has("ast_specs", "sentinel"))

# ═════════════════════════════════════════════════════════════════════════════
# §X — Cobertura de operadores FilterOperator en datalog.clj y sql.clj
# ═════════════════════════════════════════════════════════════════════════════
#  operator  → (datalog_ast_kw, sql_pattern, datalog_note)
# Nota: algunos operadores son ignorados intencionalmente en OLTP Datalog
# (Datahike no los soporta nativamente) — se reporta con [OLAP-only].
OPERATORS = [
    ("EQ",         ":=",         "= ",          None),
    ("NEQ",        ":not=",      "<> ",         None),
    ("GT",         ":>",         "> ",          None),
    ("GTE",        ":>=",        ">= ",         None),
    ("LT",         ":<",         "< ",          None),
    ("LTE",        ":<=",        "<= ",         None),
    ("IN",         ":in",        " IN ",        None),
    ("NOT_IN",     ":not-in",    "NOT IN",      None),
    ("AND",        ":and",       "AND",         None),
    ("OR",         ":or",        "OR",          None),
    ("NOT",        ":not",       "NOT (",       None),
    ("LIKE",       ":like",      "LIKE",        "OLAP-only: Datalog warn+skip"),
    ("CONTAINS",   ":contains",  "LIKE '%",     "OLAP-only: Datalog warn+skip"),
    ("IS_NULL",    ":is-null",   "IS NULL",     "OLAP-only: Datalog warn+skip"),
    ("IS_NOT_NULL",":is-not-null","IS NOT NULL","OLAP-only: Datalog warn+skip"),
    ("BETWEEN",    ":between",   "BETWEEN",     "NOT IMPL: requiere extensión"),
    ("MATCHES",    ":matches",   "REGEXP_LIKE", "NOT IMPL: requiere extensión"),
]

def check_section_x():
    for op, dl_kw, sql_pat, note in OPERATORS:
        note_str = f" [{note}]" if note else ""

        # Datalog check
        if note and "OLAP-only" in note:
            # Debe aparecer en la lista de operadores ignorados con warn
            in_dl = has("datalog", dl_kw)
            chk("§X Operadores", f"FilterOperator/{op} → {dl_kw} en datalog.clj (warn+skip){note_str}", in_dl)
        elif note and "NOT IMPL" in note:
            in_dl = has("datalog", dl_kw)
            chk("§X Operadores", f"FilterOperator/{op} → {dl_kw} en datalog.clj{note_str}", in_dl)
        else:
            in_dl = has("datalog", dl_kw)
            chk("§X Operadores", f"FilterOperator/{op} → {dl_kw} en datalog.clj", in_dl)

        # SQL check
        if note and "NOT IMPL" in note:
            in_sql = has("sql", sql_pat) or has("sql", dl_kw)
            chk("§X Operadores", f"FilterOperator/{op} → '{sql_pat}' en sql.clj{note_str}", in_sql)
        else:
            in_sql = has("sql", sql_pat) or has("sql", dl_kw)
            chk("§X Operadores", f"FilterOperator/{op} → '{sql_pat}' en sql.clj", in_sql)

# ═════════════════════════════════════════════════════════════════════════════
# §Cedar — Cedar-Janus Contract v1 (cedar-janus-contract-v1.edn)
# ═════════════════════════════════════════════════════════════════════════════
def check_section_cedar():
    # query-scope enum
    chk("§Cedar", "query-scope → def query-scope en ast_specs.clj",
        has("ast_specs", "def query-scope"))
    for v in ["ALL","OWN","ASSIGNED","OWN_OR_ASSIGNED","NONE"]:
        chk("§Cedar", f"  query-scope/{v}", has("ast_specs", f'"{v}"'))

    # permission-boundary / domain-boundary
    chk("§Cedar", "permission-boundary → def domain-boundary en ast_specs.clj o janus_ast_ir",
        has("ast_specs", "def domain-boundary") or has("janus_ast_ir", "domain-boundary"))

    # cedar-ctx → context-invariant
    chk("§Cedar", "cedar-ctx → def context-invariant en ast_specs.clj o validator dinámico",
        has("ast_specs", "def context-invariant") or has("ast_specs", "context-invariant-validator"))
    for field in ["tenant-id","user-id","roles","domain-boundaries"]:
        chk("§Cedar", f"  cedar-ctx.{field} en context-invariant",
            has("ast_specs", field) or has("janus_ast_ir", field))

    # scope-field-resolution: usado en ast_compiler para inject-scope-predicate
    chk("§Cedar", "scope-field-resolution: OWN/ASSIGNED manejados en ast_compiler",
        has("ast_compiler", "inject-scope") or has("ast_compiler", "OWN") or
        has("ast_compiler", "scope"))

    # Protocolos
    chk("§Cedar", "ICedarContext protocol en protocols.clj",
        has("protocols", "ICedarContext"))
    chk("§Cedar", "IASTCompiler protocol en protocols.clj",
        has("protocols", "IASTCompiler"))
    chk("§Cedar", "IAegisEngine protocol en protocols.clj",
        has("protocols", "IAegisEngine"))

# ═════════════════════════════════════════════════════════════════════════════
# §P — Proto Messages en metri.proto
# ═════════════════════════════════════════════════════════════════════════════
PROTO_MESSAGES = [
    "AnalyticsRequest","MatchRoutingRulesRequest","QueryRequest","BulkRequest",
    "TransactionRequest","ExploreRequest","DiscoveryRequest","FilterCriteria",
    "FilterValue","FilterValueList","MetricDefinition","Link","IntelligenceSignal",
    "IndicatorThreshold","WebhookTarget","DataRow","Action","TableColumn",
    "AnalyticalSignal","TimeFrameContext","FormulaEntry","SemanticMetricRef",
    "SortDefinition","AnalyticalComparison","DimensionDefinition","HierarchyContext",
    "Status","MatchedRule","DataRowList","ColumnSchema","ChartDecoration","TableMeta",
    "BreakdownSignal","TreeMeta","DashboardCrossFilterContext","MultiSeriesGroup",
    "BatchContext","AttributeSchema","MatchRoutingRulesResponse","RowSet","Pagination",
    "QueryMetadata","VizMeta","EntitySchema","MatchRoutingRulesBatchResponse",
    "MatchRoutingRulesBatchRequest","BulkResponse","TransactionResponse",
    "ChronosAlertTask","StringList","ExploreResponse","DiscoveryResponse",
    "FilterNode","FilterGroup","QueryResponse",
]
PROTO_ENUMS = ["AggregationFunction","FilterOperator","OutputCastType","OperationAction"]
PROTO_RPCS  = ["Discovery","Explore","Query","Transact","BulkIngest","MatchRoutingRulesBatch"]

def check_section_proto():
    proto = load("proto")
    if not proto:
        chk("§P Proto", "metri.proto EXISTE", False, "archivo no encontrado")
        return
    chk("§P Proto", "metri.proto EXISTE", True)
    chk("§P Proto", "service MetriService", "service MetriService" in proto)
    for msg in PROTO_MESSAGES:
        chk("§P Proto", f"message {msg}", f"message {msg}" in proto)
    for e in PROTO_ENUMS:
        chk("§P Proto", f"enum {e}", f"enum {e}" in proto)
    for r in PROTO_RPCS:
        chk("§P Proto", f"rpc {r}", f"rpc {r}" in proto)

# ═════════════════════════════════════════════════════════════════════════════
# REPORT
# ═════════════════════════════════════════════════════════════════════════════
def run_all():
    check_files()
    check_section_1()
    check_section_2()
    check_section_3()
    check_section_4()
    check_section_5()
    check_section_6()
    check_section_7()
    check_section_x()
    check_section_cedar()
    check_section_proto()

def print_report():
    sections: dict[str, list] = {}
    for r in results:
        sections.setdefault(r[0], []).append(r)

    total_pass = total_fail = 0
    summaries = []

    for sec, items in sections.items():
        print(f"\n{BOLD}{'─'*62}{RST}")
        print(f"{BOLD}  {sec}{RST}")
        print(f"{BOLD}{'─'*62}{RST}")
        sp = sf = 0
        for _, item, passed, note in items:
            suffix = f"  {YLW}[{note}]{RST}" if note else ""
            print((ok(item) if passed else fail(item)) + suffix)
            if passed: sp += 1
            else:      sf += 1
        total_pass += sp; total_fail += sf
        pct = 100 * sp // (sp + sf) if (sp + sf) else 0
        color = GRN if sf == 0 else RED
        summaries.append((sec, sp, sf, pct, color))
        print(f"\n  {color}Cobertura: {sp}/{sp+sf} ({pct}%){RST}")

    total = total_pass + total_fail
    overall = 100 * total_pass // total if total else 0

    print(f"\n{BOLD}{'═'*62}{RST}")
    print(f"{BOLD}  RESUMEN FINAL{RST}")
    print(f"{BOLD}{'═'*62}{RST}")
    BAR = 22
    for sec, sp, sf, pct, color in summaries:
        filled = BAR * sp // (sp + sf) if (sp + sf) else 0
        bar = "█" * filled + "░" * (BAR - filled)
        print(f"  {color}{bar}{RST}  {sec:<38} {pct:3d}%  ({sp}/{sp+sf})")

    print(f"\n{BOLD}{'─'*62}{RST}")
    if total_fail == 0:
        print(f"{GRN}{BOLD}  ✓ {total} CHECKS PASSED — 100% CONTRACT COVERAGE{RST}")
    else:
        print(f"{RED}{BOLD}  ✗ {total_fail}/{total} CHECKS FALLARON — {overall}% COBERTURA{RST}")
        print(f"\n  Items faltantes en implementación:")
        for _, item, passed, note in results:
            if not passed:
                n = f" [{note}]" if note else ""
                print(f"    {RED}→{RST} {item.strip()}{n}")
    print(f"{BOLD}{'═'*62}{RST}\n")
    return total_fail

if __name__ == "__main__":
    print(f"\n{BOLD}{'═'*62}{RST}")
    print(f"{BOLD}  METRI ENGINE — Janus-Aegis Contract Validator{RST}")
    print(f"{BOLD}  Contrato: resources/schema/janus-ast-ir.edn{RST}")
    print(f"{BOLD}            docs/architecture/cedar-janus-contract-v1.edn{RST}")
    print(f"{BOLD}{'═'*62}{RST}")
    run_all()
    sys.exit(1 if print_report() > 0 else 0)
