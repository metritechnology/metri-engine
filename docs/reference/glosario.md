# Glossary — Glosario EN↔ES

> Términos canónicos del dominio y su equivalencia. Toda la documentación (títulos, traducciones, resúmenes) usa exactamente estos pares — regla del PLAN_DOCUMENTACION §1 y §7.

| English | Español | Nota |
|---|---|---|
| datom | datom | Hecho atómico inmutable (entity, attribute, value, tx, op). No se traduce. |
| event envelope | sobre de eventos | Contrato metri-contracts v1.0, lado productor. |
| delta | delta | Antes/después de una mutación dentro del sobre. |
| transactional outbox | outbox transaccional | La fila del evento nace en la misma transacción que la mutación. |
| outbox draining | drenaje del outbox | Extracción y publicación de las filas `outbox_event`. |
| write path | camino de escritura | janus_router → eav. |
| read path | camino de lectura | janus → aegis → eav/Athena. |
| data plane | plano de datos | El motor como Códice + Transact/Query, sin acoplamiento a work_order. |
| fail-closed | fail-closed | Ante duda o error, denegar. No se traduce. |
| fail-fast | fail-fast | Abortar el arranque si la configuración es inválida. No se traduce. |
| tenant isolation | aislamiento por tenant | `tenant_id` en cada clave; TenantGuard lo valida. |
| master tenant | tenant maestro | Censurado zero-trust por las reglas de sistema. |
| uniqueness claim | claim de unicidad | Ítem dentro de la transacción cuya PK ES la clave natural. |
| ledger | ledger | Contador de consumo con autoridad y techo. No se traduce. |
| ceiling | techo | Límite duro de consumo de una cuota. |
| reservation | reserva | Ticket durable de consumo de IA pendiente de conciliar. |
| sweeper | sweeper | Recolector de reservas que nadie concilió. No se traduce. |
| boundary | boundary | Permite/limita del principal en Cedar. No se traduce. |
| grant | grant | Concesión plana `dominio:acción` del camino analítico. |
| hot partition | partición caliente | Partición DynamoDB saturada por escritura; se mitiga con sharding. |
| shard key | clave de shard | `hash(entity_id) % total_shards`. |
| pull | pull | Lectura del estado vigente de una entidad. No se traduce. |
| hydrate / hydration | hidratar / hidratación | EAV → filas JSON en memoria. |
| output cast | OutputCast | Conversión de filas a la forma de visualización (TABLE, KPI, …). |
| saga | saga | Proyección declarativa de un CREATE hacia `scheduled_job`. |
| allowlist | allowlist | Excepciones explícitas y justificadas (p. ej. invariantes del patrón Result). |
| ratchet | ratchet | Gate que solo admite mejora: la cuenta de violaciones nunca sube. |
| catalog (errors) | catálogo (de errores) | `config/errors/error_catalog.toml` — 1:1 con `ErrorCode`. |
| blue/green window | ventana de doble lectura | Periodo con campos viejos y nuevos conviviendo durante migraciones. |
