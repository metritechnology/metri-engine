# Codice — Schema Registry

> **English summary:** Single source of truth for the ~60 JSON models in `config/models/`. Compiled once at boot (fail-fast), immutable afterwards. Provides payload coercion, structural validation and id generation (ACID sequences + random Base36 codes).

## Purpose

Definir la forma de todas las entidades del producto sin tocar código: los modelos JSON se cargan, validan, hashean (huella SHA-256) y compilan a un `CodeRegistry` inmutable en el arranque. Si un modelo es inválido, el engine no arranca.

## Responsibilities / non-responsibilities

**Hace:** carga/validación/compilación de modelos; coerción de tipos en payloads; validación estructural y semántica; generación de ids (secuencias con scope y Base36 aleatorio); familias especiales (`procedure`, `work_order_procedure` con jerarquía OT 1-N).
**No hace:** almacenar datos (eav), autorizar (cedar), decidir ruteo (janus_router). El motor **no** materializa OTs — ADR-007 SUPERADO: esa composición vive en metri-cmms-plugin.

## Internal flow

```text
config/models/*.json ──▶ CodeRegistry::build (fail-fast) ──▶ OnceLock global
        payload ──▶ validator.validate_payload ──▶ coerción ──▶ DatomValue
        CREATE  ──▶ generator.inject (seq/Base36) ──▶ payload completo
```

## Invariants

1. **Fail-fast en arranque** — un modelo inválido aborta el bootstrap (ADR-004).
2. **Registry inmutable** — `OnceLock`, lookup O(1), sin mutación post-arranque.
3. **Zero-drop de códigos** — los códigos Base36 conservan las garantías del stack anterior: P(colisión, length 7) < 1/36^7, no predecibles (CSPRNG).
4. **Secuencias ACID** — contadores con `ConditionalExpression` y resolución de scope (exacta → registrada más cercana → global).

## Entry points

- [`codice::registry::CodeRegistry`] + [`codice::global`].
- [`codice::validator::validate_payload`] — la puerta de toda escritura.
- [`codice::coercion::coerce_value`] — conversión segura de tipos.
- [`codice::generator::inject`] — atributos auto-generados.
- [`codice::base36::generate`] — códigos aleatorios.

## Errors

`COD_*` y `COD_SCOPE_*`: modelo inválido, payload que no cumple el modelo, scope de secuencia irresoluble.

## Decisions

- ADR-004 — configuración y validación en arranque.
- ADR-006 — identidad de entidades.
- ADR-007 — SUPERADO: el motor quedó como plano de datos puro.

## Known risks / TODO

- La familia `procedure`/`work_order_procedure` (plantillas + instancias 1-N con provenance) es nueva: vigilar que los validadores de campos tipados crezcan sin duplicar lógica.
