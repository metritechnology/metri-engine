# Plan de refactorización — Modelo de datos Códice (simplificación del CMMS)

> **Componente:** `metri-engine` — catálogo Códice (`config/models/*.json`) y sus contratos
> **Verificado contra el árbol:** 9 de septiembre de 2026 (tarde) — commits, diffs y suite ejecutados, no estimados
> **ESTADO (2026-09-09):** Fases 0-2 y 6 **ejecutadas o cerradas por consolidación** — el catálogo
> quedó en **58 modelos** sin refs colgantes, suite 458 en verde y docs regeneradas.
> Pendiente de confirmación: un run **verde** del pipeline de deploy (los fixes
> `a4d2e9b`, `47a2d53` indican iteración en vivo; `gh` no está disponible en este
> entorno para confirmarlo). Restan las fases 3-5, 7 y 8 (deudas con migración de dato).
> **Motivación:** la revisión del modelo (2026-09-09) destapó dos capas muertas ya retiradas
> (`work_order_template` y la capa de tareas `task_template`/`work_order_task`) y un inventario
> de deudas de modelado sin dueño ([diagrama-modelo-datos.md §7](../reference/diagrama-modelo-datos.md))
> **Naturaleza:** el sistema está en producción; ningún commit intermedio puede romperlo
> **Relación:** complementa a [PLAN_REFACTORIZACION.md](PLAN_REFACTORIZACION.md) (código del
> motor) y a [PLAN_CORRECCIONES_PENDIENTES.md](PLAN_CORRECCIONES_PENDIENTES.md) (camino OLAP);
> este plan solo toca el **modelo de datos** y sus contratos

---

## Anexo de ejecución (2026-09-09)

| Fase | Resultado |
|---|---|
| **0 — Sellar el retiro de plantillas** | **CERRADA con matices.** La estela del retiro de `work_order_template` quedó commiteada (`950d679`, `d183e0a` — este último incluye el fix del deploy: bucket dedicado `metri-engine-deploy-*` en lugar de `resolve_s3`). La suite quedó en 458 en verde. **Pendiente:** confirmar un run verde del pipeline; los commits `a4d2e9b` (kms:Decrypt en la CMK) y `47a2d53` (salidas del stack) documentan iteración en vivo sobre el workflow. |
| **1 — Retiro de `task_template`/`task_template_item`** | **EJECUTADA.** Los cuatro modelos de la capa de tareas están fuera: `task_template`/`task_template_item` (este plan) y `work_order_task`/`work_order_task_item` (consolidación `88d8c75` que los precedió). Reencuadres: `note` ancla a `work_order_id`, `labor_log` imputa solo a la OT, `check_list` perdió el anclaje a tareas, prosa de `work_order_procedure` actualizada. Catálogo sin refs colgantes, verificado programáticamente. |
| **2 — Contrato de protocolos por escrito** | **PARCIAL.** La decisión sobre `work_order_task_item` quedó **sin objeto** (la entidad ya no existe). Pendiente lo documental: fijar en `config/models/README.md` que `procedure` (`PUBLISHED`) es la SSOT de protocolos formales y `form_template` queda para checklists/`request`. |
| **6 — Notas de ítem** | **EJECUTADA (variante).** No fue necesaria la vía polimórfica: con la capa de tareas fuera, `note` ancla directamente a `work_order_id` (required + index). La asimetría `note_ids` desapareció junto con las entidades que la declaraban. |

---

## Estado verificado (cierre del 9-sep)

| # | Hecho | Evidencia |
|---|---|---|
| 1 | Catálogo con **58 modelos**, 0 `entityRef` colgantes | verificación programática global; `grep task_template/work_order_task config/ src/` = 0 menciones vivas |
| 2 | Protocolo formal único: `procedure(_field)` → `work_order_procedure(_field)`; checklists vía `form_template` | catálogo; §3 del [diagrama](../reference/diagrama-modelo-datos.md) |
| 3 | Pipeline de deploy con causa raíz corregida y fixes sucesivos en main | `d183e0a` (bucket), `a4d2e9b` (kms:Decrypt), `47a2d53` (YAML); run verde sin confirmar |
| 4 | Árbol con cambios sin commitear: los reencuadres de la Fase 1 + retirada de la regla `WO_HIGH_COST_ALERT` (edición concurrente, no de este plan) | `git status` |
| 5 | Suite **458 passed, 0 failed**; `gen_reference --check` OK ("Total de modelos: 58") | `cargo test` |

## Resumen en cinco líneas

El dominio de órdenes de trabajo quedó reducido a su esqueleto sano: **la OT es el centro y
los procedimientos el único protocolo formal** — plantillas abstractas, paradas de ruta y
capa de tareas ya no existen. El pipeline de deploy tiene su causa raíz corregida y fixes
sucesivos; solo falta ver un run verde de punta a punta. Las deudas que quedan son de
**convergencia y conveniencia**: una sola vía de adjuntos, dinero en unidad menor, una sola
semántica de "grupo" y el retiro del zombi `provider`. Toda migración con dato vivo va con
script de dry-run y conciliación. El validador descarta claves desconocidas en silencio: el
contrato con metri-app se congela en tests, no en esperanzas.

---

## Parte I — Diagnóstico (deudas con evidencia)

Detalle completo en [diagrama-modelo-datos.md §7](../reference/diagrama-modelo-datos.md).
Resumen vigente (las deudas 2 y 5 de la mañana quedaron resueltas con la retirada de la capa
de tareas):

| # | Deuda | Evidencia | Gravedad |
|---|---|---|---|
| 1 | Run verde del pipeline sin confirmar | fixes iterados en main; `gh` no disponible en este entorno | Alta — barata de cerrar |
| 2 | Doble vía de adjuntos: `location.photo_ids`, `check_list_item.response_file_ids`, `work_order_procedure_field.value_file_ids` vs `file.owner_entity_type/id` polimórfica | grep de `_ids` en el catálogo | Media — doble contabilidad |
| 3 | `labor_log.hourly_rate` en `decimal` sin `_cents` ni divisa hermana | `labor_log.json` (attr `hourly_rate`); convención obligatoria en `config/models/README.md` §1.1.C | Media — dinero en flotante |
| 4 | "Grupo" con semántica partida: `iot_alert_rule.notify_groups` → `role`; `scheduled_job`/`reminder` → `user_group` | `iot_alert_rule.json` (attr `notify_groups`) | Media |
| 5 | `provider` ≅ subconjunto pobre de `company` (que ya tiene `company_type: PROVIDER`); ningún modelo lo referencia | `company.json`; `asset.vendor_provider_id` → company | Media — entidad zombi |
| 6 | Higiene: dominio fantasma `"project"` en `FALLBACK_DOMAINS` de Cedar; `dashboardBI` en camelCase | `src/cedar/evaluator/grants.rs:17` | Baja |

## Parte II — Alcance

**Dentro:** `config/models/*.json`, los contratos que el catálogo impone a metri-app y
metri-schedulers, los tests de contrato (`saga_tests`, `registry_tests`), las docs generadas
(`gen_reference.py --check` es gate de CI) y el diagrama de referencia.

**Fuera:** el código del motor (territorio de PLAN_REFACTORIZACION.md, salvo tests de
contrato y comentarios obsoletos), la infraestructura OLAP (PLAN_CORRECCIONES_PENDIENTES.md),
y cualquier migración de datos en otras cuentas. El engine no necesita permisos IAM nuevos
para este plan: toca repositorio, no AWS.

## Parte III — Reglas

Las del plan mayor aplican íntegras (producción, ningún commit rompe, Regla 05 de prueba
end-to-end). Este plan añade cuatro propias del modelo:

| # | Regla | Por qué |
|---|---|---|
| A | **Retirar un campo es seguro para escribir y ciego para leer.** El validador itera el esquema, no el payload (`validator.rs:37-97`): las claves desconocidas se descartan en silencio. Todo retiro con dato vivo exige decidir qué pasa con las lecturas y con el histórico EAV (`track_history` no borra datoms). | El silencio del validador es una trampa a dos bandas: no rompe, pero tampoco avisa. |
| B | **El contrato con metri-app se congela en un test, no en la esperanza.** Cada fase que cambia un payload patrón actualiza (o crea) su test de regresión con el payload EXACTO — patrón `el_payload_de_create_del_panel_valida_y_proyecta`. | Ya rescató una vez al panel (regresión del 2026-09-03). |
| C | **Cada fase = 1 commit + suite verde + docs regeneradas** (`python3 scripts/docs/gen_reference.py --check`). Los contratos metri-app afectados se listan en el mensaje del commit. | El gate de CI ya falla con la referencia desactualizada. |
| D | **Migración de dato vivo: script en `scripts/ops/` con dry-run por defecto y conciliación** — patrón `reingesta_iceberg_failed.py`. Sin `--apply` no toca nada. | El dato en producción manda sobre el esquema, nunca al revés. |

## Parte IV — Las fases

### Fase 0 — Sellar el retiro de la capa plantillas — ✅ CERRADA (con un pendiente)

Commiteada en `950d679` + `d183e0a` (bucket dedicado de artefactos incluido). Los fixes
sucesivos del workflow (`a4d2e9b` kms:Decrypt en la CMK del stack, `47a2d53` salidas del
stack como bloque run) indican despliegues reales iterando.

**Pendiente único:** confirmar explícitamente un run verde de `Deploy producción` (build +
deploy + verificación post-deploy con alias `live` en la versión nueva). Desde este entorno
no hay `gh`; comprobar en GitHub → Actions.

### Fase 1 — Retiro de la capa de tareas — ✅ EJECUTADA

Ver [Anexo de ejecución](#anexo-de-ejecución-2026-09-09). Catálogo: 58 modelos, sin refs
colgantes. **Contrato metri-app resultante:** crear una nota exige `work_order_id` (antes
`work_order_task_id`); los payloads con `task_template_id`, `task_template_item_id` o campos
de tarea se descartan en silencio (Regla A) — dejar de enviarlos.

### Fase 2 — El contrato de protocolos, por escrito — 🔶 PARCIAL

La decisión sobre `work_order_task_item` quedó sin objeto (entidad retirada). Pendiente lo
documental:

1. Documentar en `config/models/README.md`: `procedure` (con `lifecycle_state`: solo
   `PUBLISHED` se instancia) es la SSOT de protocolos formales; `form_template` queda
   únicamente para checklists de inspección y `request`. Un protocolo no cuelga de nada: la
   OT lo instancia directamente.

**Puerta 2**
- [ ] README del Códice describe los dos sistemas y sus fronteras

### Fase 3 — Una sola vía de adjuntos

**1-2 días · decisión + migración (Regla D)**

1. Elegir vía canónica. Recomendación: la **polimórfica** (`file.owner_entity_type/id`) —
   es la que el motor ya trata genéricamente; los arrays fuerzan mantener la inversa a mano.
2. Inventario restante (la capa de tareas se llevó por delante dos de los cinco arrays
   originales): `location.photo_ids`, `check_list_item.response_file_ids`,
   `work_order_procedure_field.value_file_ids` → migrar a datoms `owner_entity_*`, retirar
   los arrays del esquema, conciliar conteos antes/después.
3. Tests de contrato actualizados (Regla B).

**Puerta 3**
- [ ] Conciliación 1:1 entre datoms migrados y objetos en S3 referenciados
- [ ] Ningún modelo declara arrays de file-ids; suite y pipeline en verde

### Fase 4 — Dinero en unidad menor

**Medio día + migración**

1. `labor_log.hourly_rate` (`decimal`) → `hourly_rate_cents` (`number`) + `currency`
   (`string`, `^[A-Z]{3}$`, `is_dimension`), según la convención obligatoria del README
   §1.1.C. La divisa por defecto hereda la del tenant.
2. Migración ×100 del dato vivo (Regla D) y actualización de metri-app en el mismo corte.

**Puerta 4**
- [ ] Conciliación: suma de `hourly_rate_cents` = suma antigua ×100 por tenant
- [ ] `grep -rn "hourly_rate[^_]" config/models/` = 0

### Fase 5 — "Grupo" con una sola semántica

**Medio día + migración**

1. `iot_alert_rule.notify_groups` → `role` mientras todo el resto del sistema usa
   `user_group` para grupos. Decidir (recomendación: `user_group`, que es lo que un
   "grupo de notificación" significa en `scheduled_job` y `reminder`), migrar las refs
   existentes (Regla D) y renombrar el campo si cambia el destino.

**Puerta 5**
- [ ] Todas las referencias a "grupo" del catálogo apuntan a `user_group`
- [ ] Conciliación de refs migradas 1:1

### Fase 6 — Notas — ✅ EJECUTADA (variante)

Ver [Anexo de ejecución](#anexo-de-ejecución-2026-09-09). La vía polimórfica dejó de ser
necesaria: sin capa de tareas, `note` ancla a `work_order_id` y la asimetría desapareció
con las entidades que la declaraban.

### Fase 7 — Retiro de `provider`

**1 día · migración de dato**

1. Confirmar con datos: cuántas filas `provider` vivas hay (vía Athena/OLTP o Transact).
2. Migrar cada `provider` a `company` con `company_type=PROVIDER` (Regla D; map de campos
   es trivial: name/contact/address/status + `services_provided` → `custom_attributes`).
3. Retirar `provider.json`, quitar `"provider"` de `FALLBACK_DOMAINS`
   (`src/cedar/evaluator/grants.rs`) y de los dominios sembrados; tests Cedar al día.

**Puerta 7**
- [ ] 0 filas `provider` en producción; catálogo 57 modelos
- [ ] `grep -rn "provider" src/cedar/ config/models/` sin la entidad (solo `vendor_provider_id`)

### Fase 8 — Higiene (opcional, sin fecha)

- Dominio fantasma `"project"` en `FALLBACK_DOMAINS`: confirmar si algún tenant lo usa en
  `domain_quota`/grants antes de retirarlo.
- `dashboardBI` → `dashboard_bi`: único camelCase del catálogo; exige migración de entidad.
- Recordatorio permanente (Regla A): los campos de `work_order` sin lógica en el engine
  (GPS check-in/out, SLA breached, `completion_percentage`) pueden estar vivos por escritura
  de metri-app — no retirar sin verificar consumo.

## Parte V — Riesgos

| Riesgo | Mitigación | Fase |
|---|---|---|
| El validador silencioso oculta un nombre de campo mal escrito en metri-app | Tests de contrato con payload EXACTO (Regla B); verificación post-deploy del alias | Todas |
| Dato huérfano tras retiros (datoms de campos retirados persisten en EAV/historial) | Decisión explícita de conservación por campo retirado; conciliación Athena si hace falta consultar | 3, 4, 7 |
| metri-app sigue enviando campos retirados | No rompe (Regla A) pero ensucia; lista de contratos en cada commit + aviso al equipo | 1, 3, 4 |
| Migración duplica o pierde dato | Dry-run por defecto, conciliación por conteo/suma, idempotencia probada dos corridas (Regla D) | 3, 4, 5, 7 |
| El pipeline no está aún demostrado verde de punta a punta | Fase 0 pendiente único: confirmar el run; el deploy manual (`make deploy`, perfil admin) sigue como respaldo | 0 |

## Parte VI — Coordinación

| Frente | Efecto |
|---|---|
| metri-app | Dejar de enviar: `template_id` (PM), `work_order_template_id`, `task_template_id`/`task_template_item_id`, campos de tarea; **crear notas con `work_order_id`**; migraciones de adjuntos/dinero/grupos (Fases 3-5) |
| metri-schedulers | La saga de PM ya no proyecta datos de plantilla; el disparo sigue materializando OT con asset + trazabilidad (testeado) |
| GitHub environment `production` | Gate humano de cada deploy; el re-despliegue manual (`workflow_dispatch`) es el mecanismo de rollback |

## Orden si solo hay tiempo para una cosa

Cerrar la **Fase 0**: confirmar el run verde del pipeline — sin entrega no hay resto del
plan, y es la más barata de todas. Después la **Fase 2** (media hora documental) y las
deudas 3-5/7 por orden de dolor: dinero (`hourly_rate`) primero.
