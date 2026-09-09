# metri-engine Documentation Index

> Índice navegable de toda la documentación del repo. **Cada documento es alcanzable desde aquí** (regla: ningún `.md` huérfano, PLAN_DOCUMENTACION §5.1).
>
> Navegable index of every document in the repo. **Every document is reachable from here** (rule: no orphan `.md`).

## Documentación / Documentation

### Architecture (`docs/architecture/`)

| Documento | Audiencia | Cuándo consultarlo / When to read it |
|---|---|---|
| [00_OVERVIEW.md](architecture/00_OVERVIEW.md) | Todo el equipo | Visión general de la arquitectura / system overview |
| [PLAN_DOCUMENTACION.md](architecture/PLAN_DOCUMENTACION.md) | Equipo + contribuidores | Reglas de documentación (idioma, títulos, cobertura por archivo) y fases |
| [PLAN_PATRON_RESULT.md](architecture/PLAN_PATRON_RESULT.md) | Equipo + contribuidores | Patrón Result: reglas, catálogo de errores y auditor (cumplido) |
| [PLAN_REFACTORIZACION.md](architecture/PLAN_REFACTORIZACION.md) | Equipo | Plan vivo de rediseño de la arquitectura |
| [PLAN_REFACTORIZACION_CEDAR.md](architecture/PLAN_REFACTORIZACION_CEDAR.md) | Equipo | Rediseño del módulo de autorización |
| [PLAN_CORRECCIONES_PENDIENTES.md](architecture/PLAN_CORRECCIONES_PENDIENTES.md) | Equipo | Correcciones planificadas y su estado |
| [PLAN_COSTO_OLAP.md](architecture/PLAN_COSTO_OLAP.md) · [MEDICION_COSTO_OLAP.md](architecture/MEDICION_COSTO_OLAP.md) | Equipo | Costos y medición del canal OLAP |
| [PLAN_IMPLEMENTACION_LIST_ENTITIES.md](architecture/PLAN_IMPLEMENTACION_LIST_ENTITIES.md) | Equipo | RPC `ListEntities` |

#### ADRs (`docs/architecture/adr/`) — decisiones inmutables

| ADR | Decisión / Decision |
|---|---|
| [ADR-001](architecture/adr/ADR-001-motor-eav-sobre-dynamodb.md) | Motor EAV sobre DynamoDB single-table |
| [ADR-002](architecture/adr/ADR-002-zero-trust-cedar.md) | Zero-trust Cedar: fail-closed y censura del tenant maestro |
| [ADR-003](architecture/adr/ADR-003-contrato-unico.md) | Contrato único (proto + FlatBuffers + delta) |
| [ADR-004](architecture/adr/ADR-004-configuracion-en-arranque.md) | Configuración y validación fail-fast en arranque |
| [ADR-005](architecture/adr/ADR-005-codigos-de-error-canonicos.md) | Códigos de error canónicos |
| [ADR-006](architecture/adr/ADR-006-identidad-de-entidades.md) | Identidad de entidades |
| [ADR-007](architecture/adr/ADR-007-materializacion-generica.md) | Materialización genérica — **SUPERADO**: la composición de OTs vive en metri-cmms-plugin |

### Components (`docs/components/`) — fichas por módulo

> En construcción (Fase 6 del plan de documentación): una ficha por componente con propósito, invariantes, puntos de entrada, errores y ADRs. / In progress (Phase 6): one card per component.

### Guides (`docs/guides/`)

| Documento | Cuándo consultarlo / When to read it |
|---|---|
| [desarrollo-local.md](guides/desarrollo-local.md) | Levantar el entorno local (DynamoDB Local, engine, seed) |
| [despliegue.md](guides/despliegue.md) | Desplegar a AWS con SAM |

### Reference (`docs/reference/`)

| Documento | Cuándo consultarlo / When to read it |
|---|---|
| [api-grpc.md](reference/api-grpc.md) | Contrato gRPC para consumidores (metri-app, bff, agentes) |
| [codigos-error.md](reference/codigos-error.md) | Catálogo canónico de errores y su mapeo gRPC |
| [modelos-codice.md](reference/modelos-codice.md) | Esquemas de los modelos del Códice |

### Code documentation (rustdoc)

La referencia de API vive dentro del código y se genera con:

```bash
cargo doc --no-deps --open    # referencia API (reglas: PLAN_DOCUMENTACION.md)
cargo test --doc              # los ejemplos de los doc-comments se ejecutan
python3 scripts/dev/check_docs.py   # auditor de cabeceras //! (ratchet)
```

Política de idioma: títulos y encabezados siempre en inglés; cuerpos en español (ver §1 del plan). Language policy: titles and headings always in English; bodies in Spanish.
