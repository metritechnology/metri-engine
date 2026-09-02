# ADR-005 — Catálogo canónico de errores con bootstrap fail-fast

**Estado:** aceptada · 2026

## Contexto
Los códigos de error son contrato con los clientes: cambiar una cadena rompe integraciones. El catálogo necesita http_status/grpc_status/retryable canónicos y validación en arranque.

## Decisión
`config/errors/error_catalog.toml` es la ÚNICA fuente (prohibido definir códigos fuera). `ErrorCode` (enum) y `DomainError` (struct) mapean 1:1 con el catálogo, que se carga y valida en el bootstrap con fail-fast. `DomainError` mantiene su `context` encajonado (`Option<Box<Value>>`) para no pagar 128+ bytes en cada `Result` del dominio.

## Consecuencias
- Los códigos documentados (`docs/reference/codigos-error.md`, generado) y los reales no pueden divergir.
- Deuda documentada: `Eav003` conserva su cadena histórica aunque el lock optimista que describía no se implementó — cambiar la cadena tocaría el contrato con los clientes.
