"""
naming.py — Utilidades de nomenclatura compartidas por todo el paquete proto2edn.

Responsabilidad única: convertir nombres entre convenciones de nomenclatura.

  PascalCase  → kebab-case   (para keys Malli/EDN)
  snake_case  → kebab-case   (para field names proto3 → Clojure keywords)

No depende de ningún otro módulo del paquete.
"""
from __future__ import annotations

import re


def to_kebab(name: str) -> str:
    """
    Convierte PascalCase o snake_case a kebab-case.

    Ejemplos:
      QueryRequest   → query-request
      FilterNode     → filter-node
      filter_node    → filter-node
      x_dimension    → x-dimension
      VizMeta        → viz-meta
      RowSet         → row-set
      URLPath        → url-path
    """
    # PascalCase: inserta guión antes de cada mayúscula que sigue a minúsculas
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", name)
    # Acrónimos seguidos de PascalCase: URLPath → URL-Path
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1-\2", s)
    # snake_case → kebab-case
    s = s.replace("_", "-")
    return s.lower().lstrip("-")


def to_clojure_key(name: str) -> str:
    """
    Convierte un field name proto3 (snake_case) a keyword Clojure (kebab-case).

    Ejemplos:
      tenant_id        → tenant-id
      next_cursor      → next-cursor
      error_message    → error-message
      cdc_payload_json → cdc-payload-json
    """
    return name.replace("_", "-")


def to_malli_key(name: str) -> str:
    """
    Convierte un field name proto3 a keyword Malli con prefijo ':'.

    Ejemplos:
      tenant_id → :tenant-id
      page_size → :page-size
    """
    return f":{to_clojure_key(name)}"


def to_spec_key(message_or_enum_name: str, prefix: str = "metri.spec") -> str:
    """
    Convierte un nombre de Message o Enum proto3 a keyword de spec Malli.

    Ejemplos:
      QueryRequest, "metri.spec"  → :metri.spec/query-request
      FilterNode,   "metri.spec"  → :metri.spec/filter-node
      OperationAction, "metri.spec" → :metri.spec/operation-action
    """
    return f":{prefix}/{to_kebab(message_or_enum_name)}"
