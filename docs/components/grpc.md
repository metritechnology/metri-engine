# gRPC — API Surface

> **English summary:** The Tonic API surface: the server (Lambda Function URL), the interceptor chain with its mandatory order (HMAC → session → Cedar fail-closed), proto↔domain translators, the RPC handler bodies split by domain, and the single DomainError→Status translation point.

## Purpose

Exponer el motor al mundo: `metri.MetriService` (más `QuotaService` y `AgentConfigService` y `MoiraRoutingService`), con seguridad Zero-Trust en la frontera y traducción limpia proto↔dominio.

## Responsibilities / non-responsibilities

**Hace:** servidor HTTP/2 sobre Lambda (`server`); interceptores (`interceptors`); composición de dependencias (`bootstrap`); traducción DTO↔dominio (`translator`); cuerpos de RPC por dominio (`handlers/*`); el mapeo único de errores (`error_status`); servicios auxiliares (quota, agent-config, eda).
**No hace:** lógica de negocio — delega en janus/iop/aegis/eav.

## Internal flow

```text
Function URL ─▶ server ─▶ interceptors (orden obligatorio):
   1. límites WAF  2. HMAC (authn)  3. sesión  4. Cedar (fail-closed)
      ─▶ handlers (por dominio) ─▶ translator ─▶ janus/iop
error de dominio ─▶ error_status (único From) ─▶ tonic::Status
```

## Invariants

1. **Orden de interceptores obligatorio** — WAF → HMAC → sesión → Cedar; alterarlo abre ventanas de seguridad.
2. **Un solo puente de errores (R6)** — `DomainError → Status` ocurre en un único sitio que consulta el catálogo.
3. **El proto es la SSOT** — `proto/metri.proto` manda; `pb` es código generado y queda exento de lints.
4. **Reflection habilitado** — el server es explorable con grpcurl sin importar protos.

## Entry points

- [`grpc::server::start_lambda_grpc_server`].
- [`grpc::interceptors`] — la cadena de seguridad.
- [`grpc::service::MetriGrpcService`] + [`grpc::handlers`].
- [`grpc::error_status`] — el `From` canónico.

## Errors

Todos los errores llegan aquí traducidos: catálogo canónico → `Status` con código gRPC correcto (`docs/reference/codigos-error.md`).

## Decisions

- Handlers divididos por dominio RPC; `service.rs` queda como raíz de dependencias y delegación pura.

## Known risks / TODO

- Al añadir un RPC: tocar proto, translator, handler y `docs/reference/api-grpc.md` en el mismo PR.
