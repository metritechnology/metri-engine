# Cedar — Authorization

> **English summary:** Zero-Trust ABAC authorization on Cedar Policy 3. Every request passes authentication (HMAC), principal consolidation (roles/groups graph) and fail-closed evaluation — an evaluator error is a DENY, never an allow. Master-tenant censorship and system-entity rules are decided in one place.

## Purpose

Decidir qué puede hacer cada principal sobre cada recurso, fail-closed, con cachés que acotan la staleness sin sacrificar el aislamiento (ADR-002).

## Responsibilities / non-responsibilities

**Hace:** autenticación HMAC (`authn`); pipeline de pasos 1→3b (`pipeline`); grafo de principal (expansión de roles/grupos); PDP con dos caminos: mutacional (Cedar real) y analítico (grants → boundaries por dominio); expansión de grants (F5); reglas de entidades de sistema; hidratación ABAC del recurso; cachés con TTL e invalidación por bus.
**No hace:** escribir auditoría (infrastructure/audit), alterar datos, decidir cuotas.

## Internal flow

```text
request ─▶ authn (HMAC, tiempo constante) ─▶ pipeline step1..3b
        ─▶ principal_graph (roles/grupos/vecinos) ─▶ evaluator (step4)
              ├─ mutacional: Cedar real por boundary — basta 1 allow
              └─ analítico: grants ─▶ boundaries por dominio (FLS)
        cachés: principal (TTL) · sesión · invalidación por bus
        error de evaluación ─▶ DENY (fail-closed)
```

## Invariants

1. **Fail-closed** — cualquier error del evaluador es DENY; nunca se permite por duda.
2. **Zero-trust del tenant maestro** — la censura del maestro y las reglas de entidades de sistema se deciden una sola vez (`rules.rs` sobre `engine_config`), no repartidas.
3. **Grants con validación semántica (F5)** — el conjunto plano `dominio:acción` rechaza temprano acciones desconocidas; el wildcard de dominio expande todos los dominios inyectados.
4. **Staleness acotada** — TTL de caché + invalidación por bus (`InvalidationMsg`); si el suscriptor pierde mensajes (ventana `Lagged`), el TTL re-consulta.
5. **Acciones derivadas del schema** — `action_registry` se compila de `cedar-schema.json` una vez por proceso.

## Entry points

- [`cedar::pipeline`] — `intercept` / `get_principal_data`.
- [`cedar::evaluator`] — el PDP (step4).
- [`cedar::authn`] — tokens HMAC `mk_`.
- [`cedar::rules`] — qué es maestro y qué es de sistema.

## Errors

`ABAC_401` / `ABAC_403` canónicos; el motivo detallado no se propaga (se reporta el DENY canónico).

## Decisions

- ADR-002 — Zero-Trust Cedar.
- Diagnósticos de Cedar colapsados a bool — aceptado a cambio de una frontera simple.

## Known risks / TODO

- El camino analítico auto-autoriza acotado a dominios de sistema para dejar pasar el intercept — el chequeo real vive en el servicio; mantener esa frontera visible.
