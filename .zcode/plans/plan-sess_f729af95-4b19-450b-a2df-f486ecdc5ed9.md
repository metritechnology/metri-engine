# metri-engine sin acoplamiento a work_order — el motor como plano de datos puro

## Contexto clave (explorado)

`ExecuteAction`/`SpawnWorkOrder` y el `WorkOrderMaterializer` **no tienen ningún llamador** en el workspace: la cadena real es schedulers → `system.scheduled_job.fired` → FiredHandler (event-router) → `Transact CREATE work_order` (madre genérica) → `work_order.create` → metri-cmms-iot. Por eso eliminar el stack de materialización es quirúrgico y no rompe producción. Lo que se pierde con el stack: la composición de tareas/ítems de las preventivas (que en el flujo real hoy no ocurre) — pasa a metri-cmms-iot, que la implementa como extensión del composer existente.

## Fase 1 — metri-engine: eliminar el stack de materialización

1. **Borrar** `src/infrastructure/materialization/` (mod, gateway, transactor, subject_resolver, materializers/ con work_order.rs + tests) y `src/application/scheduling/` (planner, hooks, planner_tests). `infrastructure/mod.rs` y `application/mod.rs` pierden las declaraciones.
2. **`src/application/ports.rs`**: conservar SOLO `DomainEventPublisher`/`PublishError`/`publish_detached` (los usan oltp_channel y domain_event_bus). Eliminar `Subject`, `SubjectSource` (variantes Pauta/Stop), `ResolveError`, `EntityGateway`, `SubjectResolver`, `MaterializeError`, `BusinessCodeInjector`, `AtomicTransactor`, `MaterializationContext`, `Materialized`, `EntityMaterializer`, `MaterializerRegistry`, `Clock`/`SystemClock` (muerto).
3. **gRPC**: borrar `src/grpc/orchestration_service.rs` (ExecuteAction/SpawnWorkOrder/MarkJobFailed — cero llamadores; MarkJobFailed ni siquiera lo usa el router, que escribe jobs por Transact), su declaración en `grpc/mod.rs` y el bloque de wiring en `grpc/server.rs` (registry, adaptadores, add_service).
4. **proto/metri.proto**: eliminar el service `OrchestrationService` y sus 6 mensajes (ExecuteAction*, SpawnWorkOrder*, MarkJobFailed*). `build.rs` regenera tonic; reflection/descriptor se actualizan solos.
5. **Códice**: borrar `src/codice/materialization_spec.rs`; quitar de `EntityModel` el campo `materialization`, su parse en el build y `validate_materialization_specs` (COD_MAT_001 deja de existir). Actualizar los constructores de `EntityModel` en tests que ponen `materialization: None` (codice/tests/generator+validator, unique_claim tests, oltp_channel tests).
6. **`config/models/work_order.json`**: eliminar el bloque `materialization` (sin consumidor; la composición vive en metri-cmms-iot). Se conservan unique claim y event_rules (datos, no cableado).
7. **`cedar/evaluator/grants.rs`**: quitar `"work_order"` de `FALLBACK_DOMAINS` (nombre duro de entidad; el fallback real es `registry.entity_names()`).
8. Tests del motor: borrar los de materializador/orquestación; actualizar el candado de registry_tests (sin aserciones de materialization); regenerar docs (`gen_reference.py --check`); `cargo test --lib` completo en verde.
9. **ADR-007**: añadir nota de estado "Superado — la materialización por eventos vive en metri-cmms-iot (R5 resuelto externamente)".

Fuera de alcance (acoplamiento a `scheduled_job`, dominio de scheduling, NO work_order): `saga.rs` (proyección hard-codeada a scheduled_job), sobre metri-contracts en `oltp_channel`, `envelope.rs`. Se documentan como acoplamientos restantes deliberados.

## Fase 2 — metri-cmms-iot: el composer hereda la composición

El `procedure-instantiator` se convierte en **`wo-composer`** (mismo trigger: `work_order.create` con `preventive_maintenance_id`), y pasa a componer la OT preventiva completa:

1. **Resolución de sujetos (puerto Go)**: SINGLE (asset de la pauta vía `PullEntityByID`), ENUMERATED (`work_order_template_stop` por template vía Query), CRITERIA (BFS de locations + filtro de criticality — puertos `ListChildLocations`/`ListAssetsByLocation`).
2. **Composición de tareas**: para cada sujeto, `task_template` de la plantilla (o de la parada) ordenados por `procedure_order` → `work_order_task` CREATE + `work_order_task_item` por cada `task_template_item` (`is_completed: false`).
3. **Hook `sequential_schedule` portado a Go**: encadena `scheduled_start`/`scheduled_end` de las tareas desde la ocurrencia y las duraciones estimadas.
4. **Procedimientos**: la lógica existente (procedure PUBLISHED → instancia + campos tipados) se integra al mismo handler.
5. **Idempotencia**: marca por evento (existente) + guardia de composición (si la OT ya tiene `work_order_task`, no recomponer).
6. `cmd/wo-composer`, regla/cola/DLQ/lambda renombradas en `template.yaml`, README actualizado (4 handlers, responsibilidades).
7. Tests unitarios del composer (fakes) + fixtures de humo extendidos (task_template + items + pauta con asset).

## Fase 3 — Smoke E2E

Motor de humo en 9095 → fixtures (plantilla con task_template+items y procedure+field, pauta con asset) → evento `work_order.create` preventivo → verificar: tareas creadas con ventanas secuenciales, ítems pendientes, procedimiento instanciado, y los otros tres cableados intactos (rollup/cascada/scoring + dedup).

## Notas

- El FiredHandler del event-router sigue creando la madre (escritura genérica, sin acoplamiento) — no se toca metri-event-router.
- Sin cambios en el proto Go de metri-cmms-iot (los nombres de entidad van como strings en Query/Transact).
- Nada se commitea; ambos árboles quedan listos para revisión.