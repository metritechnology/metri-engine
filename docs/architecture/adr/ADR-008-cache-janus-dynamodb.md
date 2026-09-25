# ADR-008 — Caché de consultas Janus sobre DynamoDB con TTL

**Estado:** aceptada · 2026-09-25
**Plan de implementación:** [`PLAN_CACHE_JANUS_DYNAMODB.md`](../PLAN_CACHE_JANUS_DYNAMODB.md)

## Contexto

Las cachés del read path son de proceso (`EAV_CACHE` 60 s, `AEVT_SCAN_CACHE` 15 s) y su invalidación por escritura solo alcanza a la instancia que commitó; el read-your-write multi-instancia se delega al TTL (incidente 2026-09-23: 1,5 h de staleness). El canal OLAP paga una ejecución Athena por query. No existe almacén compartido además de DynamoDB — la VPC con Valkey fue retirada deliberadamente en 2026 (nota en `template.yaml`).

## Decisión

Caché read-through KV en una **tabla DynamoDB dedicada** con TTL nativo, insertada en `janus/router/{oltp,olap}.rs` alrededor del executor:

1. **Qué se cachea** — la salida del executor (`run_oltp_query_fbs` / `QueryResults` de Athena), que es user-independiente. El post-proceso dependiente del llamador (FLS `redact_sensitive_attributes`, omisión de `password_hash`) queda FUERA del perímetro y corre en cada request: ese es el invariante de corrección.
2. **Clave** — SHA-256 del JSON canónico del **AST compilado post-ABAC** + ventana temporal resuelta + fingerprint del schema Códice: el aislamiento tenant/ABAC es por construcción (scopes distintos ⇒ claves distintas); usuarios con scope idéntico comparten entrada.
3. **Expiración dual** — `exp` lógico verificado en lectura; el TTL de DynamoDB (`exp + 1 h`) es solo limpieza física, jamás corrección.
4. **Invalidación por escritura** (fase 3) — sello de generación por tenant (`GEN#QC#<tenant>`, `UpdateItem ADD 1`) en el embudo único `eav::writer::cache_policy::invalidate_entity_caches`; un bump por commit. La lectura valida la generación en el mismo `BatchGetItem`.
5. **Rollout shadow-first** — `QUERY_CACHE_MODE=shadow` mide el hit rate candidato sin gastar RCU/WCU; los gates de fase deciden con aritmética (breakeven §9 del plan), no con intuición. Canary por `QUERY_CACHE_TENANT_ALLOWLIST`. Pruebas de producción T0-T6 (§11.5), incluido un drill de rollback ensayado (T6).
6. **Arquitectura** — puertos `IQueryCache`/`QueryCacheInvalidator` en `domain/protocols.rs`; adaptador `DynamoKvCache` en `infrastructure` (reusa `DynamoClient`); Null Object `NoopQueryCache` (el motor arranca sin la tabla); frontend con modos `Off|Shadow|Ddb` en `janus::cache`.

## Consecuencias

- **(+)** El miss costoso se paga una vez por clave en todo el fleet; `cache_hits`/`cache_ttl_seconds` del contrato proto se activan (existían hardcodeados en 0); rollback = flip de env, sin migración — ensayado en producción (T6) antes de necesitarse.
- **(−)** Un round trip extra en el miss path (p50 +5-10 ms, presupuesto D8); invalidación por tenant gruesa en F3; costo RRU/WRU de la tabla (modelo §9, gates ROI).
- **Alternativas rechazadas:** ElastiCache (resucita la VPC retirada); solo in-process (no resuelve multi-instancia); prefijo `QC#` en la tabla EAV (habilitar TTL allí arriesgaría borrado físico de datoms y rompe la invariante append-only — §5.4); result-reuse de Athena (no evita `start_query` + polling).
