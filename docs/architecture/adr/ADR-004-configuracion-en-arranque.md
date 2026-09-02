# ADR-004 — Configuración leída una sola vez (EngineConfig)

**Estado:** aceptada · 2026

## Contexto
`METRI_MASTER_TENANT_ID` se leía con `env::var` dentro del camino caliente (una llamada al sistema por consulta), el concepto tenía dos nombres (`MASTER_TENANT_ID` y `METRI_MASTER_TENANT_ID`), y los tests no podían fijar configuración sin manipular el entorno global — condición de carrera con tests en paralelo.

## Decisión
`domain::config::EngineConfig` construida en el arranque y guardada en un `OnceLock`. El interceptor y el executor consultan `engine_config()`; `MAX_LIMIT` de `ListEntities` es configuración, no constante de código. `env::var` queda relegado a la raíz de composición (`src/grpc/server.rs`) y al arranque.

## Consecuencias
- Deriva de configuración imposible en caliente; los tests inyectan con `EngineConfig::from_parts` sin tocar el entorno.
- El guard HMAC (`resolve_hmac_secret`) vive junto a la config como función pura testeable.
