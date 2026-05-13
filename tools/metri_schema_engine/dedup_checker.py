#!/usr/bin/env python3
"""
dedup_checker.py — Analizador y corrector de duplicaciones en el pipeline proto2edn.

Responsabilidad única: detectar, reportar y corregir duplicaciones en:
  1. metri.proto    — proto3 schema source of truth
  2. janus-aegis-contract-v4.edn — contrato Malli generado

Algoritmos implementados:
  A. ProtoAuditor    — analiza duplicaciones estructurales en el proto
  B. EDNAuditor      — analiza duplicaciones semánticas en el contrato EDN
  C. CrossValidator  — valida coherencia bidireccional proto ↔ edn
  D. DupReport       — consolida hallazgos y propone correcciones

Uso:
  python dedup_checker.py ../../metri.proto ../../docs/architecture/janus-aegis-contract-v4.edn
  python dedup_checker.py ../../metri.proto ../../docs/architecture/janus-aegis-contract-v4.edn --fix
"""

from __future__ import annotations

import re
import sys
import argparse
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Modelos de hallazgos
# ─────────────────────────────────────────────────────────────────────────────

SEVERITY_ERROR   = "ERROR"    # Rompe validación Malli / proto inválido
SEVERITY_WARNING = "WARNING"  # Duplicación semántica, degradación de calidad
SEVERITY_INFO    = "INFO"     # Observación / mejora sugerida

@dataclass
class Finding:
    severity: str           # ERROR | WARNING | INFO
    source: str             # "proto" | "edn" | "cross"
    category: str           # "dup-key" | "dup-field-number" | "dup-enum-value" | ...
    location: str           # "message FilterGroup / field conjunction"
    message: str            # Descripción legible
    fix: Optional[str] = None  # Corrección sugerida (texto)
    auto_fixable: bool = False # True si el fixer puede corregirlo automáticamente


@dataclass
class AuditReport:
    findings: list[Finding] = field(default_factory=list)

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == SEVERITY_ERROR]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == SEVERITY_WARNING]

    @property
    def infos(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == SEVERITY_INFO]

    def add(self, **kwargs) -> None:
        self.findings.append(Finding(**kwargs))

    def is_clean(self) -> bool:
        return len(self.errors) == 0 and len(self.warnings) == 0


# ─────────────────────────────────────────────────────────────────────────────
# A. ProtoAuditor — análisis de duplicaciones en el .proto
# ─────────────────────────────────────────────────────────────────────────────

class ProtoAuditor:
    """
    Detecta duplicaciones en el grafo de Descriptors del .proto:
      A1 — Nombres de mensaje duplicados
      A2 — Field numbers duplicados dentro de un mensaje
      A3 — Field names duplicados dentro de un mensaje
      A4 — Enum value names duplicados dentro de un enum
      A5 — Enum value numbers duplicados dentro de un enum
      A6 — Mensajes estructuralmente idénticos (mismo conjunto de campos)
      A7 — Enums sin sentinel UNSPECIFIED = 0 (anti-patrón proto3)
      A8 — Enums con sentinel que NO es el 0 (antipatrón)
      A9 — Field names que violan snake_case
    """

    _RE_MESSAGE   = re.compile(r"^message\s+(\w+)\s*\{", re.MULTILINE)
    _RE_FIELD_NUM = re.compile(r"^\s+(?!//|enum|message|oneof|option)(?:repeated\s+|map<[^>]+>\s+)?[\w.]+\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE)
    _RE_ENUM_GLOBAL = re.compile(r"^enum\s+(\w+)\s*\{([^}]+)\}", re.MULTILINE | re.DOTALL)
    _RE_ENUM_NESTED = re.compile(r"^\s+enum\s+(\w+)\s*\{([^}]+)\}", re.MULTILINE | re.DOTALL)
    _RE_ENUM_VALUE  = re.compile(r"^\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE)
    _RE_SNAKE_FIELD = re.compile(r"^\s+(?:repeated\s+|map<[^>]+>\s+)?[\w.]+\s+([A-Z]\w*)\s*=\s*\d+\s*;", re.MULTILINE)

    def __init__(self, proto_path: Path):
        self.path = proto_path
        self.source = proto_path.read_text(encoding="utf-8")

    def audit(self) -> AuditReport:
        report = AuditReport()
        self._check_a1_duplicate_messages(report)
        self._check_a2_a3_fields_in_messages(report)
        self._check_a4_a5_enum_values(report)
        self._check_a6_structurally_identical_messages(report)
        self._check_a7_a8_enum_sentinel(report)
        self._check_a9_snake_case(report)
        return report

    def _extract_block(self, start: int) -> str:
        depth, i = 1, start
        while i < len(self.source) and depth > 0:
            if self.source[i] == "{":
                depth += 1
            elif self.source[i] == "}":
                depth -= 1
            i += 1
        return self.source[start:i - 1]

    def _check_a1_duplicate_messages(self, report: AuditReport) -> None:
        """A1: Nombres de mensaje definidos más de una vez."""
        names = [m.group(1) for m in self._RE_MESSAGE.finditer(self.source)]
        counts = Counter(names)
        for name, count in counts.items():
            if count > 1:
                report.add(
                    severity=SEVERITY_ERROR,
                    source="proto",
                    category="dup-message-name",
                    location=f"message {name}",
                    message=f"Message '{name}' definido {count} veces en el proto.",
                    fix=f"Renombrar o eliminar la definición duplicada de '{name}'.",
                    auto_fixable=False,
                )

    def _check_a2_a3_fields_in_messages(self, report: AuditReport) -> None:
        """A2: Field numbers duplicados. A3: Field names duplicados dentro de message."""
        for m in self._RE_MESSAGE.finditer(self.source):
            msg_name = m.group(1)
            block = self._extract_block(m.end())
            # Remover nested messages y enums para no contaminar
            clean = re.sub(r"\b(message|enum)\s+\w+\s*\{[^}]+\}", "", block, flags=re.DOTALL)
            clean = re.sub(r"//[^\n]*", "", clean)  # remover comentarios

            field_numbers: dict[str, str] = {}  # number → field_name
            field_names: list[str] = []

            for fm in self._RE_FIELD_NUM.finditer(clean):
                fname, fnum = fm.group(1), fm.group(2)
                # A2
                if fnum in field_numbers:
                    report.add(
                        severity=SEVERITY_ERROR,
                        source="proto",
                        category="dup-field-number",
                        location=f"message {msg_name}",
                        message=f"Field number {fnum} usado por '{field_numbers[fnum]}' Y '{fname}'.",
                        fix=f"Asignar un field number único a '{fname}'.",
                        auto_fixable=False,
                    )
                else:
                    field_numbers[fnum] = fname
                # A3
                if fname in field_names:
                    report.add(
                        severity=SEVERITY_ERROR,
                        source="proto",
                        category="dup-field-name",
                        location=f"message {msg_name}",
                        message=f"Field name '{fname}' aparece más de una vez.",
                        fix=f"Renombrar o eliminar el campo duplicado '{fname}'.",
                        auto_fixable=False,
                    )
                else:
                    field_names.append(fname)

    def _check_a4_a5_enum_values(self, report: AuditReport) -> None:
        """A4: Enum value names dup. A5: Enum value numbers dup."""
        def check_enum(enum_name: str, body: str, context: str) -> None:
            clean = re.sub(r"//[^\n]*", "", body)
            val_names: list[str] = []
            val_numbers: dict[str, str] = {}  # number → name

            for vm in self._RE_ENUM_VALUE.finditer(clean):
                vname, vnum = vm.group(1), vm.group(2)
                if vname in val_names:
                    report.add(
                        severity=SEVERITY_ERROR,
                        source="proto",
                        category="dup-enum-value-name",
                        location=f"{context} / enum {enum_name}",
                        message=f"Enum value name '{vname}' duplicado.",
                        fix=f"Eliminar el valor duplicado '{vname}'.",
                        auto_fixable=False,
                    )
                else:
                    val_names.append(vname)

                if vnum in val_numbers:
                    report.add(
                        severity=SEVERITY_ERROR,
                        source="proto",
                        category="dup-enum-value-number",
                        location=f"{context} / enum {enum_name}",
                        message=f"Enum value number {vnum} usado por '{val_numbers[vnum]}' Y '{vname}'.",
                        fix="Agregar 'option allow_alias = true;' o asignar número único.",
                        auto_fixable=False,
                    )
                else:
                    val_numbers[vnum] = vname

        # Enums globales
        for m in self._RE_ENUM_GLOBAL.finditer(self.source):
            check_enum(m.group(1), m.group(2), "global")

        # Enums nested dentro de messages
        for msg_m in self._RE_MESSAGE.finditer(self.source):
            msg_name = msg_m.group(1)
            block = self._extract_block(msg_m.end())
            for em in self._RE_ENUM_NESTED.finditer(block):
                check_enum(em.group(1), em.group(2), f"message {msg_name}")

    def _check_a6_structurally_identical_messages(self, report: AuditReport) -> None:
        """A6: Mensajes con conjuntos de campos idénticos (posible refactor)."""
        msg_fields: dict[str, frozenset] = {}

        for m in self._RE_MESSAGE.finditer(self.source):
            msg_name = m.group(1)
            block = self._extract_block(m.end())
            clean = re.sub(r"//[^\n]*", "", block)
            clean = re.sub(r"\b(message|enum)\s+\w+\s*\{[^}]+\}", "", clean, flags=re.DOTALL)
            fields = frozenset(
                fm.group(1) for fm in self._RE_FIELD_NUM.finditer(clean)
            )
            if len(fields) < 2:
                continue  # Mensajes de 0-1 campo son intencionales
            for other_name, other_fields in msg_fields.items():
                if fields == other_fields and fields:
                    report.add(
                        severity=SEVERITY_WARNING,
                        source="proto",
                        category="structurally-identical-messages",
                        location=f"message {msg_name} ↔ message {other_name}",
                        message=f"'{msg_name}' y '{other_name}' tienen campos idénticos: {sorted(fields)}",
                        fix="Considerar unificar en un único message o crear un mensaje base común.",
                        auto_fixable=False,
                    )
            msg_fields[msg_name] = fields

    def _check_a7_a8_enum_sentinel(self, report: AuditReport) -> None:
        """A7: Enums sin sentinel=0. A8: Sentinel que no sigue *_UNSPECIFIED.

        SCOPE: solo enums GLOBALES (top-level).
        Los nested enums están EXENTOS: su valor = 0 es el default de proto3
        y es semánticamente válido (e.g. AND=0, LEFT=0, CUSTOM_RANGE=0).
        El gate :JANUS_400 aplica únicamente a enums globales del servicio.
        """
        def check_sentinel(enum_name: str, body: str, context: str) -> None:
            clean = re.sub(r"//[^\n]*", "", body)
            values = {
                int(m.group(2)): m.group(1)
                for m in self._RE_ENUM_VALUE.finditer(clean)
            }
            if not values:
                return
            has_zero = 0 in values
            zero_is_unspecified = has_zero and "UNSPECIFIED" in values[0].upper()

            if not has_zero:
                report.add(
                    severity=SEVERITY_WARNING,
                    source="proto",
                    category="enum-missing-sentinel",
                    location=f"{context} / enum {enum_name}",
                    message=f"Enum global '{enum_name}' no tiene valor 0 (sentinel requerido en proto3).",
                    fix=f"Agregar '{enum_name}_UNSPECIFIED = 0;' como primer valor.",
                    auto_fixable=False,
                )
            elif not zero_is_unspecified:
                report.add(
                    severity=SEVERITY_WARNING,
                    source="proto",
                    category="enum-sentinel-not-unspecified",
                    location=f"{context} / enum {enum_name}",
                    message=f"El valor 0 del enum global '{enum_name}' es '{values[0]}', "
                            f"no sigue el patrón *_UNSPECIFIED esperado por Janus.",
                    fix=f"Renombrar '{values[0]}' a '{enum_name}_UNSPECIFIED'.",
                    auto_fixable=False,
                )

        # SOLO enums globales — los nested usan = 0 como default proto3 válido
        for m in self._RE_ENUM_GLOBAL.finditer(self.source):
            check_sentinel(m.group(1), m.group(2), "global")

    def _check_a9_snake_case(self, report: AuditReport) -> None:
        """A9: Field names que no siguen snake_case (proto3 convention)."""
        for m in self._RE_MESSAGE.finditer(self.source):
            msg_name = m.group(1)
            block = self._extract_block(m.end())
            clean = re.sub(r"//[^\n]*", "", block)
            for fm in self._RE_SNAKE_FIELD.finditer(clean):
                bad_name = fm.group(1)
                report.add(
                    severity=SEVERITY_WARNING,
                    source="proto",
                    category="field-name-not-snake-case",
                    location=f"message {msg_name}",
                    message=f"Field '{bad_name}' no sigue snake_case (proto3 convention).",
                    fix=f"Renombrar a '{_to_snake(bad_name)}'.",
                    auto_fixable=False,
                )


# ─────────────────────────────────────────────────────────────────────────────
# B. EDNAuditor — análisis de duplicaciones en el contrato EDN
# ─────────────────────────────────────────────────────────────────────────────

class EDNAuditor:
    """
    Detecta duplicaciones en el contrato janus-aegis-contract-v4.edn:
      B1 — Spec keys duplicadas en el registry
      B2 — Enum values duplicados dentro de un [:enum ...] form
      B3 — Enum values que aparecen tanto inline como en Sección 1
      B4 — Railway specs idénticas (todas tienen el mismo patrón)
      B5 — RPC keys que no son kebab-case
      B6 — Keys sin valor EDN válido (dangling keys)
    """

    _RE_SPEC_KEY = re.compile(r"^\s{2}(:[\w./\-]+)\s*$", re.MULTILINE)
    _RE_ENUM_FORM = re.compile(r"\[:enum((?:\s+:[A-Z_\d]+)+)\]")
    _RE_RPC_KEY   = re.compile(r":metri\.rpc/(\w+)")

    def __init__(self, edn_path: Path):
        self.path = edn_path
        self.source = edn_path.read_text(encoding="utf-8")

    def audit(self) -> AuditReport:
        report = AuditReport()
        self._check_b1_duplicate_keys(report)
        self._check_b2_duplicate_enum_values(report)
        self._check_b3_inline_vs_section1_overlap(report)
        self._check_b4_identical_railway_specs(report)
        self._check_b5_rpc_key_naming(report)
        self._check_b6_dangling_keys(report)
        return report

    def _check_b1_duplicate_keys(self, report: AuditReport) -> None:
        """B1: Spec keys que aparecen más de una vez en el registry."""
        # Eliminar comentarios del análisis
        clean = re.sub(r";;[^\n]*", "", self.source)
        keys = self._RE_SPEC_KEY.findall(clean)
        counts = Counter(keys)
        for key, count in counts.items():
            if count > 1:
                report.add(
                    severity=SEVERITY_ERROR,
                    source="edn",
                    category="dup-spec-key",
                    location=key,
                    message=f"Spec key '{key}' aparece {count} veces en el registry.",
                    fix=f"Eliminar la entrada duplicada de '{key}'. Dejar solo la definición canónica.",
                    auto_fixable=True,
                )

    def _check_b2_duplicate_enum_values(self, report: AuditReport) -> None:
        """B2: Valores duplicados dentro de un [:enum ...] form."""
        for m in self._RE_ENUM_FORM.finditer(self.source):
            vals_str = m.group(1).strip()
            vals = re.findall(r":[A-Z_\d]+", vals_str)
            counts = Counter(vals)
            for val, count in counts.items():
                if count > 1:
                    # Encontrar el contexto (línea cercana con key)
                    pos = m.start()
                    context_line = self.source.rfind(":", 0, pos)
                    key_ctx = self.source[max(0, context_line - 30):context_line + 50].strip()
                    report.add(
                        severity=SEVERITY_ERROR,
                        source="edn",
                        category="dup-enum-value-in-spec",
                        location=f"cerca de: ...{key_ctx}...",
                        message=f"Enum value '{val}' duplicado {count} veces dentro del [:enum ...] form.",
                        fix=f"Eliminar la ocurrencia duplicada de '{val}'.",
                        auto_fixable=True,
                    )

    def _check_b3_inline_vs_section1_overlap(self, report: AuditReport) -> None:
        """
        B3: Detecta valores de enum que aparecen tanto en la Sección 1 (definición global)
        como inlineados en campos de mensajes de las Secciones 2-4.
        Estos inlines deben ser de nested enums — si no lo son, es una duplicación.
        """
        # Extraer enums de la Sección 1
        section1_match = re.search(
            r";; SECCIÓN 1.*?;; SECCIÓN 2",
            self.source, re.DOTALL
        )
        if not section1_match:
            return

        section1 = section1_match.group(0)
        section1_enum_vals: set[str] = set(re.findall(r":[A-Z_]{2,}", section1))

        # Buscar [:enum ...] en secciones 2-4
        rest_of_doc = self.source[section1_match.end():]
        for m in self._RE_ENUM_FORM.finditer(rest_of_doc):
            vals = re.findall(r":[A-Z_\d]{2,}", m.group(1))
            overlap = set(vals) & section1_enum_vals
            if overlap:
                pos = section1_match.end() + m.start()
                # Encontrar la spec key más cercana antes de este [:enum ...]
                before = self.source[:pos]
                key_match = list(self._RE_SPEC_KEY.finditer(before))
                ctx = key_match[-1].group(1) if key_match else "desconocido"
                report.add(
                    severity=SEVERITY_WARNING,
                    source="edn",
                    category="enum-inline-overlap-section1",
                    location=ctx,
                    message=f"Valores {sorted(overlap)} aparecen en Sección 1 Y en '{ctx}'. "
                            f"Si son enums globales, usar [:ref ...] en lugar de inline.",
                    fix=f"Reemplazar el [:enum ...] inline por [:ref :metri.spec/enum-name] si es un enum global.",
                    auto_fixable=False,
                )

    def _check_b4_identical_railway_specs(self, report: AuditReport) -> None:
        """B4: Railway specs que tienen exactamente el mismo contenido (sólo difieren en el key)."""
        section5_match = re.search(
            r";; SECCIÓN 5.*?;; SECCIÓN 6",
            self.source, re.DOTALL
        )
        if not section5_match:
            return

        section5 = section5_match.group(0)
        # Extraer pares key → spec
        pattern = re.compile(r"(:[\w./\-]+-railway)\s*\n\s*(\[:or[\s\S]+?\]\s*\n\s*\]\s*\n)", re.MULTILINE)
        railway_specs: dict[str, str] = {}

        # Simplificado: verificar si todos los contenidos de railway son idénticos
        railway_blocks = re.findall(r"(:[\w./\-]+-railway)", section5)
        or_blocks = re.findall(r"\[:or[\s\S]+?\n   \]\]", section5)

        if len(set(or_blocks)) == 1 and len(or_blocks) > 1:
            report.add(
                severity=SEVERITY_INFO,
                source="edn",
                category="identical-railway-pattern",
                location="SECCIÓN 5",
                message=f"Todas las {len(or_blocks)} specs Railway-Oriented tienen el mismo patrón [:or ok fail]. "
                        "Considerar definir un único spec base :metri.spec/railway-base y usar [:merge ...].",
                fix="Definir :metri.spec/railway-base una sola vez y referenciarla en cada *-railway spec.",
                auto_fixable=False,
            )

    def _check_b5_rpc_key_naming(self, report: AuditReport) -> None:
        """B5: Verifica que las keys de RPC usen kebab-case correcto."""
        for m in self._RE_RPC_KEY.finditer(self.source):
            name = m.group(1)
            # Debe ser kebab-case: solo a-z, 0-9, guiones
            if not re.match(r"^[a-z][a-z0-9\-]*$", name):
                report.add(
                    severity=SEVERITY_WARNING,
                    source="edn",
                    category="rpc-key-not-kebab",
                    location=f":metri.rpc/{name}",
                    message=f"RPC key ':metri.rpc/{name}' no es kebab-case.",
                    fix=f"Usar ':metri.rpc/{_to_kebab_edn(name)}' en su lugar.",
                    auto_fixable=True,
                )

    def _check_b6_dangling_keys(self, report: AuditReport) -> None:
        """B6: Keys en el registry que no tienen valor EDN (seguidas por otra key o por ;;)."""
        # Remover comentarios para análisis
        clean_lines = []
        for line in self.source.splitlines():
            stripped = line.strip()
            if not stripped.startswith(";;"):
                clean_lines.append(line)

        content = "\n".join(clean_lines)

        # Calcular, para cada posición en el texto, si estamos dentro de un
        # vector/set (bracket depth > 0 relativo al map raíz del registry).
        # Una keyword con 2 espacios de indentación que sea un ELEMENTO de un
        # [:set] o [:vector] multilinea NO es una clave de map — es un valor.
        bracket_depth_at: list[int] = []
        depth = 0
        for ch in content:
            bracket_depth_at.append(depth)
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth = max(0, depth - 1)

        key_pattern = re.compile(r"^\s{2}(:[\w./\-]+)\s*\n\s{2}(:[\w./\-]+)", re.MULTILINE)
        for m in key_pattern.finditer(content):
            k1, k2 = m.group(1), m.group(2)

            # Excluir cláusulas especiales con valores en namespaces conocidos
            if "janus/" in k1 or "cedar/" in k1:
                continue

            # Excluir si k1 está dentro de un vector/set (bracket depth > 0)
            # — se trata de un valor, no de una clave de map
            pos = m.start(1)
            if pos < len(bracket_depth_at) and bracket_depth_at[pos] > 0:
                continue

            report.add(
                severity=SEVERITY_ERROR,
                source="edn",
                category="dangling-key",
                location=k1,
                message=f"Key '{k1}' no tiene valor EDN — aparece directamente seguida por '{k2}'.",
                fix=f"Agregar un valor EDN válido a '{k1}' o eliminar la entrada.",
                auto_fixable=False,
            )


# ─────────────────────────────────────────────────────────────────────────────
# C. CrossValidator — coherencia bidireccional proto ↔ edn
# ─────────────────────────────────────────────────────────────────────────────

class CrossValidator:
    """
    Valida coherencia entre el proto y el contrato EDN:
      C1 — Mensajes en proto sin spec en EDN (orphan message)
      C2 — Specs en EDN sin message en proto (ghost spec)
      C3 — RPCs en proto sin contrato en EDN
      C4 — Enums globales en proto sin spec en EDN
      C5 — Field count mismatch (mensaje tiene más/menos campos en EDN vs proto)
    """

    _RE_MESSAGE_NAMES = re.compile(r"^message\s+(\w+)\s*\{", re.MULTILINE)
    _RE_ENUM_NAMES    = re.compile(r"^enum\s+(\w+)\s*\{", re.MULTILINE)
    _RE_RPC_NAMES     = re.compile(r"rpc\s+(\w+)\s*\(", re.MULTILINE)
    _RE_EDN_SPEC_KEY  = re.compile(r"^\s{2}:metri\.spec/([\w\-]+)\s*$", re.MULTILINE)
    _RE_EDN_RPC_KEY   = re.compile(r"^\s{2}:metri\.rpc/([\w\-]+)\s*$", re.MULTILINE)

    def __init__(self, proto_path: Path, edn_path: Path):
        self.proto_src = proto_path.read_text(encoding="utf-8")
        self.edn_src   = edn_path.read_text(encoding="utf-8")

    def audit(self) -> AuditReport:
        report = AuditReport()
        self._check_c1_orphan_messages(report)
        self._check_c2_ghost_specs(report)
        self._check_c3_orphan_rpcs(report)
        self._check_c4_orphan_enums(report)
        return report

    def _to_kebab(self, name: str) -> str:
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", name)
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1-\2", s)
        return s.replace("_", "-").lower()

    def _check_c1_orphan_messages(self, report: AuditReport) -> None:
        """C1: Mensajes en proto que no tienen spec en EDN."""
        proto_msgs = {m.group(1) for m in self._RE_MESSAGE_NAMES.finditer(self.proto_src)}
        edn_keys   = {m.group(1) for m in self._RE_EDN_SPEC_KEY.finditer(self.edn_src)}

        for msg in sorted(proto_msgs):
            expected_key = self._to_kebab(msg)
            if expected_key not in edn_keys:
                report.add(
                    severity=SEVERITY_ERROR,
                    source="cross",
                    category="orphan-message",
                    location=f"message {msg}",
                    message=f"Message '{msg}' existe en proto pero NO tiene spec ':metri.spec/{expected_key}' en el EDN.",
                    fix="Regenerar el contrato con proto2edn para incluir el mensaje.",
                    auto_fixable=True,
                )

    def _check_c2_ghost_specs(self, report: AuditReport) -> None:
        """C2: Specs en EDN que no corresponden a ningún message del proto."""
        proto_msgs  = {self._to_kebab(m.group(1)) for m in self._RE_MESSAGE_NAMES.finditer(self.proto_src)}
        proto_enums = {self._to_kebab(m.group(1)) for m in self._RE_ENUM_NAMES.finditer(self.proto_src)}
        proto_known = proto_msgs | proto_enums

        edn_keys = {m.group(1) for m in self._RE_EDN_SPEC_KEY.finditer(self.edn_src)}

        for key in sorted(edn_keys):
            # Railway specs son derivadas — ignorar
            if key.endswith("-railway"):
                continue
            # Nested enums tienen formato "message.enum"
            if "." in key:
                continue
            if key not in proto_known:
                report.add(
                    severity=SEVERITY_WARNING,
                    source="cross",
                    category="ghost-spec",
                    location=f":metri.spec/{key}",
                    message=f"Spec ':metri.spec/{key}' en el EDN NO tiene message o enum correspondiente en proto.",
                    fix="Eliminar la spec del EDN o agregar el message al proto.",
                    auto_fixable=False,
                )

    def _check_c3_orphan_rpcs(self, report: AuditReport) -> None:
        """C3: RPCs en proto sin contrato en EDN."""
        proto_rpcs = {self._to_kebab(m.group(1)) for m in self._RE_RPC_NAMES.finditer(self.proto_src)}
        edn_rpcs   = {m.group(1) for m in self._RE_EDN_RPC_KEY.finditer(self.edn_src)}

        for rpc in sorted(proto_rpcs):
            if rpc not in edn_rpcs:
                report.add(
                    severity=SEVERITY_ERROR,
                    source="cross",
                    category="orphan-rpc",
                    location=f"rpc {rpc}",
                    message=f"RPC '{rpc}' existe en proto pero NO tiene contrato ':metri.rpc/{rpc}' en el EDN.",
                    fix="Regenerar el contrato con proto2edn.",
                    auto_fixable=True,
                )

    def _check_c4_orphan_enums(self, report: AuditReport) -> None:
        """C4: Enums globales en proto sin spec en EDN."""
        proto_enums = {m.group(1) for m in self._RE_ENUM_NAMES.finditer(self.proto_src)}
        edn_keys    = {m.group(1) for m in self._RE_EDN_SPEC_KEY.finditer(self.edn_src)}

        for enum_name in sorted(proto_enums):
            expected = self._to_kebab(enum_name)
            if expected not in edn_keys:
                report.add(
                    severity=SEVERITY_WARNING,
                    source="cross",
                    category="orphan-global-enum",
                    location=f"enum {enum_name}",
                    message=f"Enum global '{enum_name}' sin spec ':metri.spec/{expected}' en el EDN.",
                    fix="Regenerar el contrato con proto2edn.",
                    auto_fixable=True,
                )


# ─────────────────────────────────────────────────────────────────────────────
# D. DupFixer — corrección automática de hallazgos auto_fixable
# ─────────────────────────────────────────────────────────────────────────────

class DupFixer:
    """
    Aplica correcciones automáticas a los hallazgos auto_fixable.
    Actualmente soporta:
      - B1: Eliminar spec keys duplicadas en el EDN
      - B2: Eliminar enum values duplicados en [:enum ...] forms
      - B5: Corregir RPC keys a kebab-case
    """

    def __init__(self, edn_path: Path):
        self.edn_path = edn_path
        self.source   = edn_path.read_text(encoding="utf-8")

    def fix(self, findings: list[Finding]) -> tuple[str, int]:
        """
        Aplica todos los fixes auto_fixable.
        Devuelve el EDN corregido y el número de fixes aplicados.
        """
        fixed_source = self.source
        applied = 0

        auto_fixes = [f for f in findings if f.auto_fixable]
        categories = {f.category for f in auto_fixes}

        # B2: Deduplicar enum values
        if "dup-enum-value-in-spec" in categories:
            fixed_source, n = self._fix_duplicate_enum_values(fixed_source)
            applied += n

        # B5: Normalizar RPC keys a kebab-case
        if "rpc-key-not-kebab" in categories:
            fixed_source, n = self._fix_rpc_key_naming(fixed_source)
            applied += n

        # B1: Eliminar spec keys duplicadas (último, después de otros fixes)
        if "dup-spec-key" in categories:
            fixed_source, n = self._fix_duplicate_spec_keys(fixed_source)
            applied += n

        return fixed_source, applied

    def _fix_duplicate_enum_values(self, source: str) -> tuple[str, int]:
        """Deduplica valores dentro de [:enum ...] forms."""
        count = 0
        def dedup_enum(m: re.Match) -> str:
            nonlocal count
            vals_str = m.group(1)
            vals = re.findall(r":[A-Z_\d]+", vals_str)
            seen, unique = set(), []
            for v in vals:
                if v not in seen:
                    unique.append(v)
                    seen.add(v)
                else:
                    count += 1
            return "[:enum " + " ".join(unique) + "]"

        pattern = re.compile(r"\[:enum((?:\s+:[A-Z_\d]+)+)\]")
        return pattern.sub(dedup_enum, source), count

    def _fix_rpc_key_naming(self, source: str) -> tuple[str, int]:
        """Corrige nombres de RPC keys a kebab-case."""
        count = 0
        def fix_rpc(m: re.Match) -> str:
            nonlocal count
            name = m.group(1)
            fixed = _to_kebab_edn(name)
            if fixed != name:
                count += 1
            return f":metri.rpc/{fixed}"
        return re.compile(r":metri\.rpc/(\w+)").sub(fix_rpc, source), count

    def _fix_duplicate_spec_keys(self, source: str) -> tuple[str, int]:
        """Elimina entradas duplicadas de spec keys, dejando solo la primera aparición."""
        count = 0
        seen_keys: set[str] = set()
        lines = source.splitlines()
        result_lines = []
        skip_block = False
        current_key = None

        i = 0
        while i < len(lines):
            line = lines[i]
            m = re.match(r"^\s{2}(:[\w./\-]+)\s*$", line)
            if m and not line.strip().startswith(";;"):
                key = m.group(1)
                if key in seen_keys:
                    skip_block = True
                    current_key = key
                    count += 1
                    i += 1
                    # Skip until blank line or next top-level key
                    while i < len(lines):
                        nl = lines[i].strip()
                        if not nl or (re.match(r"^(:[\w./\-]+)\s*$", nl) and nl != current_key):
                            break
                        i += 1
                    continue
                else:
                    seen_keys.add(key)
                    skip_block = False

            if not skip_block:
                result_lines.append(line)
            i += 1

        return "\n".join(result_lines), count


# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de nomenclatura
# ─────────────────────────────────────────────────────────────────────────────

def _to_snake(name: str) -> str:
    """PascalCase → snake_case."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return s.lower()

def _to_kebab_edn(name: str) -> str:
    """Convierte cualquier nombre a kebab-case para EDN keys."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1-\2", s)
    return s.replace("_", "-").lower()


# ─────────────────────────────────────────────────────────────────────────────
# CLI — punto de entrada
# ─────────────────────────────────────────────────────────────────────────────

def _print_report(report: AuditReport, title: str) -> None:
    total = len(report.findings)
    if total == 0:
        print(f"  ✅ {title}: Sin hallazgos")
        return

    print(f"\n  📋 {title}")
    print(f"     {'─' * 58}")

    icons = {SEVERITY_ERROR: "🔴", SEVERITY_WARNING: "🟡", SEVERITY_INFO: "ℹ️ "}
    for f in report.findings:
        icon = icons.get(f.severity, "•")
        print(f"     {icon} [{f.category}] @ {f.location}")
        print(f"        {f.message}")
        if f.fix:
            print(f"        💡 Fix: {f.fix}")
        if f.auto_fixable:
            print(f"        🔧 Auto-fixable")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="dedup_checker — Auditor de duplicaciones proto ↔ edn"
    )
    parser.add_argument("proto", help="Ruta al archivo .proto")
    parser.add_argument("edn",   help="Ruta al contrato .edn")
    parser.add_argument(
        "--fix", "-f",
        action="store_true",
        help="Aplicar correcciones automáticas al EDN",
    )
    parser.add_argument(
        "--fix-output", "-o",
        help="Archivo de salida para el EDN corregido (default: sobrescribe el original)",
        default=None,
    )
    args = parser.parse_args()

    proto_path = Path(args.proto)
    edn_path   = Path(args.edn)

    if not proto_path.exists():
        print(f"ERROR: {proto_path} no existe", file=sys.stderr)
        return 1
    if not edn_path.exists():
        print(f"ERROR: {edn_path} no existe", file=sys.stderr)
        return 1

    print("═" * 64)
    print("  DEDUP CHECKER — Auditoría de duplicaciones proto2edn")
    print("═" * 64)

    # ── A. Proto Audit ──────────────────────────────────────────────────────
    print("\n[A] Auditando metro.proto...")
    proto_report = ProtoAuditor(proto_path).audit()
    _print_report(proto_report, f"Proto ({proto_path.name})")

    # ── B. EDN Audit ────────────────────────────────────────────────────────
    print("\n[B] Auditando contrato EDN...")
    edn_report = EDNAuditor(edn_path).audit()
    _print_report(edn_report, f"EDN ({edn_path.name})")

    # ── C. Cross-Validation ─────────────────────────────────────────────────
    print("\n[C] Cross-validando proto ↔ edn...")
    cross_report = CrossValidator(proto_path, edn_path).audit()
    _print_report(cross_report, "Cross-validation proto ↔ edn")

    # ── Resumen global ──────────────────────────────────────────────────────
    all_findings = proto_report.findings + edn_report.findings + cross_report.findings
    all_errors   = [f for f in all_findings if f.severity == SEVERITY_ERROR]
    all_warnings = [f for f in all_findings if f.severity == SEVERITY_WARNING]
    all_infos    = [f for f in all_findings if f.severity == SEVERITY_INFO]
    auto_fixable = [f for f in all_findings if f.auto_fixable]

    print("═" * 64)
    print("  RESUMEN GLOBAL")
    print("═" * 64)
    print(f"  🔴 Errores:   {len(all_errors)}")
    print(f"  🟡 Warnings:  {len(all_warnings)}")
    print(f"  ℹ️  Infos:     {len(all_infos)}")
    print(f"  🔧 Auto-fixable: {len(auto_fixable)}")

    if not all_findings:
        print("\n  ✅ CONTRATO LIMPIO — Sin duplicaciones detectadas.")
        print("═" * 64)
        return 0

    # ── D. Auto-fix ─────────────────────────────────────────────────────────
    if args.fix and auto_fixable:
        print(f"\n[D] Aplicando {len(auto_fixable)} correcciones automáticas...")
        fixer = DupFixer(edn_path)
        fixed_source, applied = fixer.fix(all_findings)
        out_path = Path(args.fix_output) if args.fix_output else edn_path
        out_path.write_text(fixed_source, encoding="utf-8")
        print(f"  ✅ {applied} correcciones aplicadas → {out_path}")
    elif args.fix and not auto_fixable:
        print("\n  ℹ️  No hay correcciones automáticas disponibles.")
    elif not args.fix and auto_fixable:
        print(f"\n  💡 Usa --fix para aplicar {len(auto_fixable)} correcciones automáticas.")

    print("═" * 64)
    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main())
