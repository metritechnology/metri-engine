# Plan de refactorización — Modelo de datos Códice (simplificación del CMMS)

> **Componente:** `metri-engine` — catálogo Códice (`config/models/*.json`) y sus contratos
> **Verificado contra el árbol:** 9 de septiembre de 2026 — commits, diffs y suite ejecutados, no estimados
> **Motivación:** la revisión del modelo (2026-09-09) destapó una capa muerta ya retirada
> (`work_order_template`), una segunda capa condenada (`task_template`, sustituida por los
> procedimientos) y un inventario de deudas de modelado sin dueño
> ([diagrama-modelo-datos.md §7](../reference/diagrama-modelo-datos.md))
> **Naturaleza:** el sistema está en producción; ningún commit intermedio puede romperlo
> **Relación:** complementa a [PLAN_REFACTORIZACION.md](PLAN_REFACTORIZACION.md) (código del
> motor) y a [PLAN_CORRECCIONES_PENDIENTES.md](PLAN_CORRECCIONES_PENDIENTES.md) (camino OLAP);
> este plan solo toca el **modelo de datos** y sus contratos

---

## Estado de partida (verificado hoy)

| # | Hecho | Evidencia |
|---|---|---|
| 1 | Catálogo con **62 modelos** | `ls config/models/*.json \| wc -l` = 62 |
| 2 | Capa `work_order_template` + paradas **retirada y commiteada** | commit `d183e0a` (incluye el borrado de ambos JSON) |
| 3 | Limpieza de config muerta (`events` legacy, `calendar_mapping`, `default`→`default_value`, `outbox_event.status` enum) **commiteada** | commit `950d679` |
| 4 | **Sin commitear**: la estela del retiro — 5 modelos (PM sin `template_id` y saga recortada, `procedure`/`task_template` sin ancla, `work_order_task` sin puntero a parada, prosa de la constraint), 2 tests actualizados, 2 docs regeneradas y el fix del doctest roto de `comparison.rs` | `git status` = 10 ficheros; `cargo test` = **458 passed, 0 failed** |
| 5 | Causa raíz del fallo de deploy corregida en `d183e0a` (`resolve_s3` → bucket dedicado `metri-engine-deploy-*`; el rol de CI no debía tocar el stack `aws-sam-cli-managed-default`) | commit `d183e0a`, `samconfig.toml` + `bootstrap_github_oidc.py` |
| 6 | **Decisión de producto ya tomada**: los procedimientos (`procedure`/`procedure_field`) serán el único sistema de protocolo; `task_template`/`task_template_item` se retiran | pedido explícito del 9-sep |
| 7 | `task_template` hoy: **0 referencias en `src/`**; solo 2 campos colgantes en otros modelos (`work_order_task.task_template_id`, `work_order_task_item.task_template_item_id`) | `grep -rn task_template src/ config/` |

## Resumen en cinco líneas

El modelo ya perdió su capa muerta (plantillas de OT) y el pipeline de deploy tiene su causa
raíz corregida; falta **sellar lo hecho** (commit + un run verde) y ejecutar el retiro de
`task_template`, que es decisión tomada y de riesgo mínimo. Después, las deudas reales son de
**convergencia**: una sola vía de adjuntos, dinero en unidad menor, semántica de "grupo",
notas de ítem y el retiro de `provider`. Cada fase es independiente, con contrato de metri-app
explícito y migración de dato con dry-run cuando haya dato vivo. El validador descarta claves
desconocidas en silencio: los retiros no rompen escrituras, pero tampoco avisan — el contrato
con metri-app se congela en tests, no en esperanzas.

---

## Parte I — Diagnóstico (deudas con evidencia)

Detalle completo en [diagrama-modelo-datos.md §7](../reference/diagrama-modelo-datos.md).
Resumen priorizado:

| # | Deuda | Evidencia | Gravedad |
|---|---|---|---|
| 1 | Estela del retiro de plantillas sin commitear (bloquea todo lo demás) | `git status`, suite 458 en verde | Alta — barata de cerrar |
| 2 | Pipeline de deploy sin run verde verificado tras el fix | push de `d183e0a` dispara el workflow (paths incluye `samconfig.toml`) | Alta — sin pipeline no hay entrega |
| 3 | `task_template(_item)` condenados: decisión tomada, ejecución pendiente | pedido 9-sep; 0 refs en `src/` | Alta — decisión tomada |
| 4 | Doble vía de adjuntos: arrays `*_file_ids` (5 modelos) vs `file.owner_entity_type/id` polimórfica | `location.json:204`, `check_list_item.json:68`, `work_order_task.json`, `work_order_task_item.json`, `work_order_procedure_field.json:117` vs `file.json:41` | Media — doble contabilidad |
| 5 | `labor_log.hourly_rate` en `decimal` sin `_cents` ni divisa hermana | `labor_log.json:57`; convención obligatoria en `config/models/README.md` §1.1.C | Media — dinero en flotante |
| 6 | "Grupo" con semántica partida: `iot_alert_rule.notify_groups` → `role`; `scheduled_job`/`reminder` → `user_group` | `iot_alert_rule.json:77` | Media |
| 7 | Nota de ítem imposible: `note.work_order_task_id` es `required` pero `work_order_task_item.note_ids` declara la inversa | `note.json:29`, `work_order_task_item.json:36` | Baja |
| 8 | `provider` ≅ subconjunto pobre de `company` (que ya tiene `company_type: PROVIDER`); ningún modelo lo referencia | `company.json:38`, `asset.vendor_provider_id` → company | Media — entidad zombi |
| 9 | Higiene: dominio fantasma `"project"` en `FALLBACK_DOMAINS` de Cedar; `dashboardBI` en camelCase; campos GPS/SLA/`completion_percentage` sin lógica en el engine | `src/cedar/evaluator/grants.rs:18` | Baja — no borrar sin confirmar consumo de metri-app |

## Parte II — Alcance

**Dentro:** `config/models/*.json`, los contratos que el catálogo impone a metri-app y
metri-schedulers, los tests de contrato (`saga_tests`, `registry_tests`), las docs generadas
(`gen_reference.py --check` es gate de CI) y el diagrama de referencia.

**Fuera:** el código del motor (es territorio de PLAN_REFACTORIZACION.md, salvo tests de
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

### Fase 0 — Sellar el retiro de la capa plantillas

**Horas · bloqueante de todo lo demás**

1. Revisar y commitear los 10 ficheros en el árbol (estela de `work_order_template`: PM sin
   `template_id`, protocolos sin ancla, task sin puntero a parada, tests de contrato, docs,
   doctest). La suite ya está en verde (458).
2. Push a main y **verificar el run del pipeline de deploy** — es también la primera prueba
   end-to-end del fix del bucket (`d183e0a`). Si el push de `d183e0a` ya disparó un run,
   evaluar ese; si no, re-despliegue manual (`workflow_dispatch`) del commit.
3. Verificación post-deploy del workflow: stack `UPDATE_COMPLETE` + alias `live` en la
   versión nueva.

**Puerta 0**
- [ ] Árbol limpio, suite 458+ en verde en CI
- [ ] Un run verde de `Deploy producción` con el fix del bucket
- [ ] Alias `live` apuntando a la versión recién publicada

### Fase 1 — Retiro de `task_template`/`task_template_item` (procedimientos como único protocolo)

**Medio día · decisión ya tomada · 0 refs en `src/`**

1. `git rm -f config/models/task_template.json task_template_item.json` (`-f`: el fichero
   lleva la modificación local de la Fase anterior).
2. Limpiar los 2 refs colgantes: `work_order_task.task_template_id` y
   `work_order_task_item.task_template_item_id` (hoy la única referencia al sistema).
3. `work_order_task` queda como tarea simple (descripción, asignación, tiempos, evidencias);
   el protocolo vive a nivel de OT vía `work_order_procedure`. Revisar prosa que hable de
   "protocolo o plantilla estandarizada".
4. Actualizar [diagrama-modelo-datos.md](../reference/diagrama-modelo-datos.md): §1 (dominio
   de protocolos), §3 (dos sistemas, no tres), §7 (deuda 3 → ejecutada). Regenerar
   `modelos-codice.md` (62 → 60).
5. **Contrato metri-app**: dejar de enviar `task_template_id`/`task_template_item_id`
   (mientras tanto, el validador las descarta — Regla A). Congelar en test si existía
   payload patrón.

**Puerta 1**
- [ ] 60 modelos, 0 refs colgantes (`grep -rn task_template config/ src/` = 0 fuera de ADRs)
- [ ] Suite verde + `gen_reference --check` verde
- [ ] Pipeline de deploy en verde

### Fase 2 — El contrato de protocolos, por escrito

**Medio día · documental, con una decisión pequeña**

1. Documentar en `config/models/README.md`: `procedure` (con `lifecycle_state`: solo
   `PUBLISHED` se instancia) es la SSOT de protocolos formales; `form_template` queda
   únicamente para checklists de inspección y `request`. Un protocolo no vive ya colgado de
   una plantilla: la OT lo instancia directamente.
2. **Decisión pequeña de producto**: `work_order_task_item` (sub-paso) nació anclado a
   `task_template_item`; sin ese ancla, decidir si queda como sub-paso manual (tiene
   evidencias, notas y labor propio) o se fusiona en `work_order_task`. Recomendación:
   quedarse con él — ya es la unidad de imputación de `labor_log`.

**Puerta 2**
- [ ] README del Códice describe los dos sistemas y sus fronteras
- [ ] Decisión sobre `work_order_task_item` escrita en este plan (anexo de ejecución)

### Fase 3 — Una sola vía de adjuntos

**1-2 días · decisión + migración (Regla D)**

1. Elegir vía canónica. Recomendación: la **polimórfica** (`file.owner_entity_type/id`) —
   es la que el motor ya trata genéricamente; los arrays `*_file_ids` fuerzan mantener la
   inversa a mano en cada modelo.
2. Inventario de 5 modelos con arrays (tabla de la Parte I #4) → migración: crear datoms
   `owner_entity_*` desde cada array, retirar los arrays del esquema, conciliar conteos
   antes/después.
3. Tests de contrato de los 5 payload patrones actualizados (Regla B).

**Puerta 3**
- [ ] Conciliación 1:1 entre datoms migrados y objetos en S3 referenciados
- [ ] Ningún modelo declara `*_file_ids`; suite y pipeline en verde

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

### Fase 6 — Notas de ítem

**Horas**

1. Decidir modelo de nota genérica: `note` con `target_entity_type/id` polimórfica (coherente
   con la Fase 3) en lugar del `work_order_task_id` obligatorio; retirar los `note_ids` de
   arrays (misma vía de adjuntos/notas). Alternativa mínima: campo espejo opcional en
   `note`. Recomendación: polimórfica — elimina la asimetría de raíz.

**Puerta 6**
- [ ] Ningún array `note_ids` en el catálogo; la prueba de la asimetría (`work_order_task_item.note_ids`)
    ya no existe o tiene campo espejo real

### Fase 7 — Retiro de `provider`

**1 día · migración de dato**

1. Confirmar con datos: `SELECT count(*) FROM provider` vía Athena/OLTP — ¿hay filas vivas?
2. Migrar cada `provider` a `company` con `company_type=PROVIDER` (Regla D; map de campos
   es trivial: name/contact/address/status + `services_provided` → `custom_attributes`).
3. Retirar `provider.json`, quitar `"provider"` de `FALLBACK_DOMAINS`
   (`src/cedar/evaluator/grants.rs:23`) y de los dominios sembrados; tests Cedar al día.

**Puerta 7**
- [ ] 0 filas `provider` en producción; catálogo 59 modelos
- [ ] `grep -rn "provider" src/cedar/ config/models/` sin la entidad (solo `vendor_provider_id`)

### Fase 8 — Higiene (opcional, sin fecha)

- Dominio fantasma `"project"` en `FALLBACK_DOMAINS`: confirmar si algún tenant lo usa en
  `domain_quota`/grants antes de retirarlo.
- `dashboardBI` → `dashboard_bi`: único camelCase del catálogo; exige migración de entidad.
- Campos GPS/SLA/`completion_percentage` de `work_order` sin lógica en el engine: confirmar
  con metri-app quién los escribe/lee. **No retirar** hasta verificar consumo (Regla A:
  quizá los escriba la app hoy).

## Parte V — Riesgos

| Riesgo | Mitigación | Fase |
|---|---|---|
| El validador silencioso oculta un nombre de campo mal escrito en metri-app | Tests de contrato con payload EXACTO (Regla B); verificación post-deploy del alias | Todas |
| Dato huérfano tras retiros (datoms de campos retirados persisten en EAV/historial) | Decisión explícita de conservación por campo retirado; conciliación Athena si hace falta consultar | 1, 3, 7 |
| metri-app sigue enviando campos retirados | No rompe (Regla A) pero ensucia; lista de contratos en cada commit + aviso al equipo | 1, 3, 4 |
| Migración duplica o pierde dato | Dry-run por defecto, conciliación por conteo/suma, idempotencia probada dos corridas (Regla D) | 3, 4, 5, 7 |
| El pipeline de deploy falla por algo distinto del bucket (el fix aún no tiene run verde) | Fase 0 existe exactamente para eso; el deploy manual (`make deploy`, perfil admin) sigue como respaldo | 0 |

## Parte VI — Coordinación

| Frente | Efecto |
|---|---|
| metri-app | Dejar de enviar: `template_id` (PM), `work_order_template_id` (protocolos), `task_template_id`/`task_template_item_id` (Fase 1); migraciones de adjuntos/dinero/grupos (Fases 3-5) |
| metri-schedulers | La saga de PM ya no proyecta datos de plantilla; el disparo sigue materializando OT con asset + trazabilidad (testeado) |
| GitHub environment `production` | Gate humano de cada deploy; el re-despliegue manual (`workflow_dispatch`) es el mecanismo de rollback |

## Orden si solo hay tiempo para una cosa

La **Fase 0**: sellar lo ya hecho y conseguir un run verde del pipeline — sin entrega no hay
resto del plan, y es la más barata de todas. La Fase 1 va justo detrás: decisión tomada,
cero referencias en código, medio día de trabajo.
