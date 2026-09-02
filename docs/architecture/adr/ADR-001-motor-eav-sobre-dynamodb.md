# ADR-001 — Motor EAV inmutable sobre DynamoDB single-table

**Estado:** aceptada · 2026

## Contexto
Las entidades del CMMS tienen esquemas dinámicos por tenant y requieren auditoría temporal (qué valor tenía un atributo y cuándo dejó de tenerlo).

## Decisión
Modelo EAV inmutable en una única tabla DynamoDB: cada assert/retract es un datom (`entidad, atributo, valor, tx`) con índices secundarios EAVT/AEVT/AVET/VAET. El escritor (`src/eav/writer/`) compone items; los planners de constraints saben de invariantes y ninguno conoce al otro (`src/eav/writer/constraints.rs`).

## Consecuencias
- Time-travel y as-of snapshot son lecturas naturales (`reader/pull.rs::pull_as_of`, `history`).
- `AVET` solo indexa tipos indexables: la regla canónica vive en `eav::types::value_type::attr_type_is_avet_indexable`; la validación de filtros la consulta, no la reimplementa.
- Un update genera retract + assert: el lector siempre ve el valor vigente.
