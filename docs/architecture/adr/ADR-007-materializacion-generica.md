> **ESTADO: SUPERADO (2026-09-08).** La materialización por eventos vive en
> metri-cmms-plugin (`wo-composer`): el motor es plano de datos — Códice +
> Transact/Query + eventos que ya emite — sin ningún acoplamiento a
> `work_order`. El stack de materialización (materializers, subject_resolver,
> planner, OrchestrationService, COD_MAT_001) fue eliminado. R5 queda
> resuelto externamente: la composición de la OT preventiva la hace el
> consumidor del bus, no el motor.

# ADR-007: Materialización genérica de acciones del loop PM

**Estado:** Aceptado (2026-09-05)
**Contexto:** Integración metri-engine ↔ metri-schedulers (Tramos 0-3 del plan de integración).

## Contexto y problema

El primer materializador del loop PM nació acoplado a `work_order`: el RPC se
llamaba `SpawnWorkOrder`, el planificador hardcodeaba los tres modelos
(WO/tarea/ítem), la categoría forzada y la trazabilidad, y el adaptador
conocía el writer concreto. El `action_payload` del job — que el Hub devuelve
íntegro en el fired — ya declaraba `entity_type`, pero el motor lo ignoraba y
asumía órdenes de trabajo. Añadir una segunda entidad materializable habría
requerido reabrir canal, handler y contrato.

## Decisión

Cuatro piezas, con el principio **"el job declara QUÉ, el Códice declara
CÓMO, el motor ejecuta genérico"**:

1. **`EntityMaterializer` + `MaterializerRegistry`** (`application/ports.rs`):
   un puerto por entidad materializable, registrado en el composition root
   (`grpc/server.rs`). Fail-closed: entidad sin registro → `ORCH_004`, jamás
   un default a `work_order`.
2. **`ExecuteAction`** (`proto/metri.proto`): contrato genérico que consume el
   mismo vocabulario que metri-contracts (`entity_type`, `action_type`). El
   RPC `SpawnWorkOrder` queda como wrapper de compatibilidad y se retira cuando
   metri-event-router migre (R5).
3. **Spec de materialización en el Códice** (`codice/materialization_spec.rs`,
   bloque `materialization` en el JSON del modelo): madre, hijos de composición
   (entidad/vía/origen), campos forzados, trazabilidad y generador de código.
   Validación cruzada fail-fast en el build (`COD_MAT_001`) — una spec rota no
   despliega. Lo algorítmico que un DSL no expresa (secuenciación temporal de
   tareas) vive en **hooks con nombre** (`application/scheduling/hooks.rs`),
   validados contra `KNOWN_HOOKS` con test candado bidireccional.
4. **La pauta sigue WO-céntrica por dominio**: `preventive_maintenance.template_id
   → work_order_template` queda fija. En mantenimiento, el outcome natural de
   una pauta ES una OT; no hay segundo caso de uso real. El desacoplamiento
   crítico era el motor, no el dominio. Revisar al primer outcome no-WO.

## Alternativas descartadas

- **Inferir hijos del grafo de `entityRef`**: frágil — `work_order_task`
  también referencia `asset`, `user`, `note`; el grafo no distingue "hijo de
  composición" de "referencia incidental". La spec declara; el grafo navega.
- **DSL declarativo puro** para toda la expansión: expresar "la tarea 2 empieza
  cuando termina la 1" en JSON sería inventar un lenguaje de programación.
  Hooks acotados y testeables.
- **`match entity_type` en el handler**: reubicaría el acoplamiento, no lo
  eliminaría. El registry es OCP verificable.

## Estructura resultante

```
src/application/ports.rs                 # framework: EntityMaterializer, Registry, Gateway…
src/application/scheduling/              # planificador puro + hooks (cero entidades)
src/infrastructure/materialization/
├── gateway.rs                           # EntityGateway sobre EAV/OLTP
├── subject_resolver.rs                  # SINGLE / ENUMERATED / CRITERIA
├── transactor.rs                        # escritura atómica + inyección de código
└── materializers/
    ├── mod.rs                           # default_registry(): la ÚNICA lista de entidades
    └── work_order.rs                    # el primer registro (+ sus tests)
```

## Acoplamiento residual documentado

- `EavSubjectResolver` (modo ENUMERATED) consulta `work_order_template_stop`
  por nombre — es la tabla de paradas del dominio de mantenimiento. Camino de
  salida: declarar las fuentes de sujetos en la spec (`subjects.enumerated_from`)
  cuando exista un segundo materializable con rutas.
- `materializers/work_order.rs` menciona task_template/asset/location — es el
  archivo DE la entidad: ahí es donde debe vivir.

## Consecuencias

- Registrar una entidad materializable = escribir su spec en el Códice +
  registrar un adaptador en `server.rs`. La capa `application/` no menciona
  ninguna entidad concreta salvo la fachada documentada del corpus.
- El corpus de caracterización del planificador pasó intacto por la
  refactorización — es la prueba de comportamiento-preservación y debe seguir
  siendo la vara de cualquier cambio futuro.
- `DISPATCH_NOTIFICATION` no necesita materializador (el router ya lo
  ejecuta); el framework es para acciones que **crean entidades**.
- Pendiente (R5): metri-event-router debe llamar `ExecuteAction` leyendo
  `entity_type` del `action_payload` (fallback `"work_order"` para jobs viejos).
  Requiere regenerar stubs protoc del repo Go.

## Pruebas del desacoplamiento

- `orchestration_service::tests::despacha_por_entity_type_al_materializador_registrado`
  — dos entidades distintas, dos materializadores fake.
- `orchestration_service::tests::entidad_sin_materializador_es_orch004_fail_closed`
- `materialization_spec::tests::*` — spec rota no compila.
- `hooks::tests::registry_cubre_los_hooks_conocidos_por_el_codice` — candado.
- Corpus de `spawn_tests` — comportamiento observable intacto.
