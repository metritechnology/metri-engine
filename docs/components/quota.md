# Quota — Per-Tenant Quotas

> **English summary:** Atomic per-tenant consumption ledger with a hard ceiling, AI token reservations (tickets in DynamoDB, not memory), a sweeper that reaps unreconciled reservations, and read-time usage projection. One module decides everything about "can this tenant consume?".

## Purpose

Registrar consumo atómico con techo, soportar reservas de IA (tokens de Bedrock) que pueden tardar en conciliarse, y responder siempre con la cuota vigente correcta.

## Responsibilities / non-responsibilities

**Hace:** ledger atómico con techo (fuera del log de datoms); tickets de reserva y su store en DynamoDB; sweeper de reservas huérfanas; proyección `current_usage` en lectura; resolución de qué cuota gobierna cada operación.
**No hace:** bloquear escrituras (eso lo hace `iop::quota_step` con este módulo), cobrar dinero real.

## Internal flow

```text
quota_step ─▶ resolver (¿qué cuota aplica hoy?) ─▶ techo vigente
   └─ fast-path O(1): UPDATE/DELETE/UPSERT no limitadas
reserva IA ─▶ reservations (ticket) ─▶ dynamo_store (en vuelo)
           └─ conciliación ─▶ ledger (contador atómico)
sweeper ──▶ expira/concilia reservas que nadie cerró
lectura  ─▶ projection ─▶ current_usage
```

## Invariants

1. **Atomicidad del ledger** — el contador comitea con condición de techo; no hay sobregiro por carrera.
2. **El contador vive fuera del log de datoms** — moverlo al EAV permitiría perderlo en retracts; aquí es estado deliberado.
3. **Reservas durables** — DynamoDB, no memoria: un despliegue no puede perder tickets en vuelo.
4. **Sweeper tolerante** — solo concilia lo que puede probar; el resto expira con auditoría.

## Entry points

- [`quota::ledger`] — contador atómico.
- [`quota::reservations`] + [`quota::dynamo_store`] — ciclo de vida de reservas.
- [`quota::resolver`] — cuota vigente por (tenant, dominio, tipo de límite).
- [`quota::sweeper`] / [`quota::projection`].

## Errors

`QTA_*`: cuota agotada, reserva inválida, techo excedido.

## Decisions

- DynamoDB y no Redis: misma infraestructura transaccional que el resto del motor; cero dependencias nuevas.

## Known risks / TODO

- Es el módulo mejor documentado en código; mantener el estándar (secciones `# Errors`/`# Examples`) al crecer.
