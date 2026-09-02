# ADR-002 — Autorización ABAC embebida con Cedar, censura del tenant maestro

**Estado:** aceptada · 2026

## Contexto
Multi-tenancy estricto en salud: un escape de partición es un incidente regulatorio. El tenant maestro necesita administrar todo sin poder leer datos de negocio de otros tenants.

## Decisión
Cedar Policy 3 embebido (sin servicio externo). El preámbulo de autorización de los RPC de lectura es **un único helper** (`MetriGrpcService::authorize_read`); sus divergencias legítimas (estado de la denegación, si el conflicto de tenant emite a Sherlog) son parámetros explícitos. Fallos de autenticación devuelven denegación explícita — nunca una lista vacía, que convertiría un fallo de permisos en "no hay datos" (`ListEntities`).

## Consecuencias
- Una sola superficie de seguridad para endurecer; el compilador no avisa si hay tres copias (lección de la Fase 3 del plan).
- `is_master_tenant` consulta `EngineConfig` (un solo nombre canónico del tenant maestro).
- El guard HMAC del arranque es fail-closed: `ENVIRONMENT` ausente o desconocido se trata como producción (`domain::config::resolve_hmac_secret`, testeado).
