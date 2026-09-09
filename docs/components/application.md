# Application — Ports

> **English summary:** Dependency-Inversion ports: traits bounded by what the consumer needs, not by what the adapter offers. Concrete implementations (EventBridge, outbox, DynamoDB) live in infrastructure and are wired at the composition roots; tests compose fakes without AWS. The port error contract decides retries vs DLQ.

## Purpose

Definir los puertos de salida de la aplicación orientados al consumidor (DIP): el motor declara qué necesita, la infraestructura decide cómo.

## Responsibilities / non-responsibilities

**Hace:** los traits de puertos y su error tipado (`retryable()` decide reintentos vs DLQ); nada más — sin lógica.
**No hace:** implementaciones concretas, cableado, ni configuración.

## Internal flow

```text
módulo de dominio ── consume ──▶ port trait ── implementado por ──▶ infrastructure
                                    ▲
composition roots (grpc::bootstrap / grpc::server) cablean; tests usan fakes
```

## Invariants

1. **Traits delimitados por el consumidor** — un puerto nuevo nace de una necesidad real, no como espejo de un SDK.
2. **El error es parte del contrato** — `retryable()` es la única señal para sweeper vs DLQ.

## Entry points

- [`application::ports`] — todos los puertos.

## Errors

`PublishError` y el error tipado de puertos, con su clasificación de reintento.

## Decisions

- El subárbol `scheduling/` fue eliminado con el stack de materialización (ADR-007 SUPERADO): el módulo quedó reducido a puertos puros.

## Known risks / TODO

- Mantener la regla: si un port crece hacia "todo lo que hace el adaptador", dividirlo.
