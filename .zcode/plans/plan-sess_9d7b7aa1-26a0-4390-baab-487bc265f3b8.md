# Reestructuración de `preventive_maintenance` (v2, sin client_id) + limpieza de `work_order`

**Contexto verificado:** el motor ya no consume los campos de disparo de PM (el `shadow_sagas_mapping` se retiró; las trampas las crea el pm-orchestrator externo). `work_order` ya no tiene `cron_job`. Cambio de **contrato JSON + docs, cero Rust**. `client_id` de WO no tiene consumidores en src/ ni scripts — verificado, solo modelo + docs.

## A. `config/models/preventive_maintenance.json`

### A1. Nuevo bloque de recurrencia (4 campos, diseño «ancla»)

```json
{ "name": "recurrence_start_date", "type": "epoch",
  "description": "Primera ocurrencia teórica (ancla del ciclo). Su día de semana (unidad WEEKS), día de mes (MONTHS) y mes+día (YEARS) fijan el patrón; el intervalo avanza desde esta fecha." },
{ "name": "recurrence_interval", "type": "number", "default_value": 1,
  "description": "Cada N unidades de recurrence_unit (1, 2, 3, 5… — los presets del panel). ≥1." },
{ "name": "recurrence_unit", "type": "enum",
  "options": ["HOURS", "DAYS", "WEEKS", "MONTHS", "YEARS"], "default_value": "MONTHS",
  "description": "Unidad del ciclo temporal." },
{ "name": "recurrence_at_time", "type": "string",
  "description": "Hora local (HH:MM) de la ocurrencia, interpretada en iana_timezone. Obligatoria con unidades ≥ DAYS; con HOURS manda el minuto de recurrence_start_date." }
```

### A2. `cron_expression` queda como DERIVADA (description nueva)

El bloque estructurado es la fuente de verdad; `cron_expression` la compila el cliente/orchestrator al guardar cuando el patrón es expresable en cron Linux de 5 campos. Patrones no expresables («cada 2 años», «cada 45 días») la dejan vacía y el disparo vive como `EXACT_TIME` en el `scheduled_job` (mecanismo ya soportado). Se marca «derivada, no editar a mano» y se anota que pm-orchestrator aún la lee (legado). Se corrige el typo «Expesión».

### A3. Reordenación en dos secciones (el «muy similar a WO» visible)

1. **El molde (espejo de WO):** asset_id, location_id, title, description, priority, status, estimated_duration_hours, assignees, assigned_group_ids, require_location_verification, template_id
2. **El disparo temporal (la diferencia):** recurrence_start_date, recurrence_interval, recurrence_unit, recurrence_at_time, cron_expression, iana_timezone, next_due_date, recurrence_basis, advance_notice_value/unit/time, prenotify_before_minutes
3. **Disparo telemétrico:** meter_based_trigger

`iana_timezone` actualiza su description: también gobierna `recurrence_at_time` (no solo el cron).

### A4. Constraint nueva: toda pauta declara un disparo

```json
{ "type": "requires_any",
  "any": ["recurrence_start_date", "cron_expression", "meter_based_trigger"],
  "description": "Una pauta sin disparo temporal o telemétrico no proyecta nada: fail-closed. Precedente: shift_pattern.json." }
```

Evalúa la vista fusionada (estado vivo + payload), así que las pautas legacy con solo `cron_expression` siguen pasando sus updates.

## B. `config/models/work_order.json` — eliminar `client_id`

Las OT son internas, no externas: se elimina el atributo `client_id` completo (work_order.json:40-46). Sin consumidores en el motor; los payloads que lo sigan enviando se ignoran (el validador solo itera atributos del modelo). Mismo patrón que la eliminación de `cron_job`.

## C. Documentación

- Regenerar `docs/reference/modelos-codice.md` con `python3 scripts/docs/gen_reference.py` — limpia `client_id` y el stale `cron_job` de la fila WO, y refleja el bloque nuevo de PM.
- Actualizar a mano `docs/reference/diagrama-modelo-datos.md`: quitar la arista `WO -->|"client_id"| CO["company (CLIENT)"]` (línea 82) y refrescar la sección PM (hoy dibuja PM→saga→scheduled_job y `next_generation_at`, ya inexistentes).

## D. Verificación

- `python3 -m json.tool` sobre ambos JSON.
- Regeneración de docs + revisión de las filas PM y WO.
- Sin `cargo test` (licencia Xcode sin aceptar en la máquina). Chequeo estático equivalente: atributos solo aditivos en PM, sustractivos sin consumidores en WO; el validador materializa defaults al nacer (sin backfill — pautas existentes no fallan); `requires_any` corre sobre la vista fusionada.

## E. Notas de migración (fuera de este repo — metri-cmms-plugin)

- El panel/orchestrator pasa a leer el bloque, compilar `cron_expression` cuando sea expresable y usar trampa `EXACT_TIME` desde `next_due_date` cuando no; presets 1/2/3/5.
- Pautas existentes: quedan sin `recurrence_*` hasta editarse (sin backfill); `cron_expression` las ampara.
- El panel deja de enviar/leer `client_id` en OT (si lo hacía).

## Fuera de alcance

- `is_measure` → `is_metric` en `estimated_duration_hours` (flag vivo vs muerto) — un cambio de palabra por modelo si lo pides.
- Renombrar el trío `advance_notice_*`: semántica intacta, coste sin beneficio pedido.