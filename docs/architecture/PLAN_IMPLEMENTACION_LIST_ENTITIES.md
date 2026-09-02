# Plan de Implementación — `ListEntities` RPC

> **Componente:** Metri Engine Core (Rust) · contrato `metri.proto`  
> **Origen:** bloqueador 6 de [COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md](COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md)  
> **Naturaleza:** exponer una capacidad existente, no construir una nueva

---

## 1. El hueco

`MetriService` expone `Discovery`, `Explore`, `Query`, `Transact`, `BulkIngest` y
`MatchRoutingRulesBatch`. **Ninguno responde "dame las entidades que cumplen X".**

| RPC | Qué devuelve | Por qué no sirve |
|---|---|---|
| `Transact` + `GET` | Una entidad por id | Hay que conocer el id de antemano |
| `Explore` | Valores distintos de un atributo | Devuelve `["ACTIVE","FAILED"]`, no quiénes los tienen |
| `Query` | Consulta analítica de dashboards | `AnalyticsRequest`, multi-series, cross-filter |

**No es un hueco de un consumidor concreto, es un hueco del API.** La señal está en el
propio repositorio: el `WatchdogHandler` del Event Router necesitó `ResetOrphanedEvents`,
un RPC a medida, porque no había alternativa genérica. Añadir un segundo RPC a medida
para Schedulers repetiría el error.

Consumidores bloqueados hoy o a corto plazo:

- El reconciliador de deriva de Metri Schedulers (§12.2 del Componente Externo 05)
- Recarga en frío de reglas por tenant en el Rule Synchronizer de Metri IoT
- Cualquier proceso de migración o auditoría que recorra entidades por estado

---

## 2. Estado verificado del motor

Comprobado contra el código, no contra la documentación.

| Pieza | Estado | Ubicación |
|---|---|---|
| Plan de consulta nativo | **Existe** — `AvetSingle`, `AvetIntersect`, `AevtScan` | `src/eav/reader/query.rs:55` |
| Ejecutor | **Existe** — `execute_native_plan` | `src/eav/reader/query.rs:112` |
| Índice AVET | **Existe** — `GSI-AVET`, PK `T#{tenant}#AV#{attr}` | `execute_avet_single_for_tenant` |
| `entity_type` indexable | **Sí** — se escribe como datom `Str`, y `is_avet_indexable()` sólo excluye `Bytes`, `Array` y `Null` | `src/eav/types/value_type.rs:48` |
| Pipeline de autorización | **Existe** — `cedar_step` → `quota_step` → `janus_step` | `src/iop/` |
| Paginación con cursor | **NO existe** | ver §3 |

**La consulta que hace falta ya es expresable:**

```rust
NativeQueryPlan::AvetIntersect {
    tenant_id,
    filters: vec![
        ("entity_type".into(), DatomValue::Str("scheduled_job".into())),
        ("status".into(),      DatomValue::Str("ACTIVE".into())),
    ],
}
```

Es una intersección de índices, no un escaneo de tabla.

---

## 3. El problema real: paginar una intersección

Es lo único que no es un envoltorio fino, y conviene entenderlo antes de estimar nada.

`execute_avet_intersection` ejecuta **N consultas completas y las intersecta en memoria**:

```rust
for (attr_name, value) in filters {
    let ids = self.execute_avet_single(tenant, attr_name, value).await?;
    // ... HashSet::retain
}
```

Y `DynamoClient::query` **auto-pagina internamente** hasta agotar el índice cuando no se
le pasa `limit`: devuelve todo, sin exponer cursor.

El resultado es que, para `entity_type = scheduled_job AND status = ACTIVE`:

- El primer filtro devuelve **todos** los `scheduled_job` del tenant.
- El segundo devuelve **todas** las entidades `ACTIVE` del tenant, de cualquier tipo.
- Ambos conjuntos se materializan completos en RAM antes de intersectarse.

> **Ninguno de los dos lados es selectivo por separado.** Es lo que hace que
> **paginar una intersección no se resuelva paginando un lado**: para saber qué
> entra en la página 2 hay que haber intersectado, y para intersectar hay que
> tener ambos lados.

Tres salidas, en orden de preferencia:

| Estrategia | Cómo | Coste |
|---|---|---|
| **A. Cursor sobre el lado conductor** | Paginar el filtro más selectivo con `ExclusiveStartKey`; el resto se materializa una vez y se reutiliza como conjunto de contraste dentro de la petición | Un lado completo en RAM. Aceptable si ese lado es acotado |
| **B. Cursor opaco compuesto** | El `page_token` codifica el `last_evaluated_key` del lado conductor más el hash de los filtros | Correcto y sin estado, pero el token deja de ser trivial |
| **C. Sin paginación, con tope duro** | `limit` obligatorio y error explícito al excederlo | Trivial de implementar; traslada el problema al llamador |

**Recomendación: empezar por C y evolucionar a B.** Un tope duro con error explícito es
honesto y desbloquea al reconciliador; una paginación mal hecha sobre una intersección
devuelve resultados incompletos **en silencio**, que es justo el modo de fallo que este
sistema lleva persiguiendo desde el principio.

---

## 4. Fases

### L1 — Contrato

**Entregable:** `metri.proto` con el mensaje, y bindings regenerados.

```protobuf
message ListEntitiesRequest {
  string tenant_id   = 1;
  string entity_type = 2;
  // Igualdad sobre atributos INDEXADOS. Se intersectan (AND).
  map<string, string> filters = 3;
  int32  limit       = 4;   // obligatorio en L2; ver §3 estrategia C
  string page_token  = 5;   // reservado para L4
}

message ListEntitiesResponse {
  Status status              = 1;
  repeated string entity_ids = 2;
  string next_page_token     = 3;
  bool   truncated           = 4;   // true si se alcanzó el limit
}
```

**Devuelve sólo ids, no entidades.** Mantiene la respuesta acotada; quien necesite el
contenido ya tiene `Transact GET`. Para el reconciliador los ids son justo lo que hace falta.

**`truncated` es deliberado:** sin él, un resultado recortado es indistinguible de uno
completo, y el reconciliador **borraría las trampas de los ids que no llegaron**.

**Aceptación:** `protoc` genera sin errores; los clientes existentes siguen compilando
(campos nuevos, ningún número reutilizado).

---

### L2 — Implementación con tope duro

**Entregable:** el RPC responde, acotado por `limit`, sin paginación.

1. Traducir la petición a `NativeQueryPlan::AvetIntersect`, añadiendo siempre
   `entity_type` como primer filtro.
2. Rechazar filtros sobre atributos no indexados: un filtro que el índice no puede
   resolver degeneraría en escaneo. El Códice ya sabe el tipo de cada atributo.
3. Aplicar el `limit` y marcar `truncated` al alcanzarlo.
4. Rechazar `limit` ausente o superior al máximo del servicio.

**Aceptación:** `entity_type=scheduled_job, status=ACTIVE` devuelve exactamente los ids
esperados en un tenant de prueba; con `limit` por debajo del total, `truncated=true`.

---

### L3 — Autorización

**Entregable:** el RPC atraviesa el mismo pipeline que cualquier otra operación.

Es el punto delicado y **no es opcional**.

1. Enganchar a `cedar_step`: qué entidades puede listar cada principal, con sus
   `domain_boundaries`.
2. Verificar que el `tenant_id` **del contexto autenticado** manda sobre el del cuerpo.
   Confiar en el del cuerpo sería un salto de partición trivial.
3. Respetar la censura del Tenant Master que declara el Overview: un endpoint de listado
   que no la aplicara sería una fuga transversal.
4. Pasar por `quota_step`: listar es barato por consulta y caro en bucle.

**Aceptación:** un principal sin permiso sobre `scheduled_job` recibe denegación, no una
lista vacía —confundirlos oculta el fallo de permisos—. Un `tenant_id` en el cuerpo
distinto al del token no devuelve datos ajenos.

---

### L4 — Paginación real

**Entregable:** `page_token` funcional (estrategia B de §3).

1. Exponer una variante de `DynamoClient::query` que devuelva `last_evaluated_key` en
   vez de auto-paginar.
2. Elegir el lado conductor por selectividad estimada.
3. Codificar el token: cursor del lado conductor + hash de los filtros, para que un
   token no se pueda reutilizar con otros filtros.

**Aceptación:** recorrer un tenant con más entidades que el `limit` devuelve el conjunto
completo sin repetidos ni ausencias, y un token de otra consulta se rechaza.

---

### L5 — Cliente Go y desbloqueo del reconciliador

**Entregable:** `metri-event-router` puede listar, y el comparador del censo se construye.

1. Método `ListEntities` en el cliente gRPC-Web, calcado del `Transact` de F4.
2. Comparador: ensamblar el censo, contrastar y aplicar `census.SafeToApply`.
3. **Respetar `truncated`:** un listado recortado debe abortar la reconciliación por la
   misma razón que un censo incompleto (§12.2 del Componente Externo 05).

**Aceptación:** borrar un Schedule a mano en la consola se detecta en el siguiente ciclo;
en régimen estable el censo reporta cero divergencias.

---

## 5. Riesgos

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Paginación incompleta en silencio | El reconciliador **borra trampas legítimas** | `truncated` explícito; L4 sólo tras L2 estable |
| `tenant_id` del cuerpo sin verificar | Salto de partición entre inquilinos | L3 antes de exponer el RPC fuera del entorno de desarrollo |
| Filtro sobre atributo no indexado | Escaneo encubierto | Rechazo explícito en L2, consultando el Códice |
| API de listado genérica que crece | Un motor de consultas paralelo al analítico | Acotar a igualdad sobre atributos indexados: sin rangos, sin OR, sin ordenación |
| Intersección con ambos lados grandes | Memoria del Lambda | Medir con datos reales antes de L4; puede exigir un GSI compuesto |

---

## 6. Lo que este plan no cubre

- **Estimaciones de esfuerzo.** No conozco al equipo del motor ni su velocidad.
- **Si hace falta un GSI compuesto** (`entity_type` + `status` en una sola clave). Sería
  la solución definitiva al problema de §3, pero exige migración de datos y medir antes.
- **Filtros que no sean igualdad.** Rangos, `IN`, negaciones: fuera de alcance a propósito.
- **Ordenación estable.** El orden actual sale de un `HashSet`, es decir, no determinista.
  Paginar sin orden estable es incorrecto; L4 debe resolverlo o documentar el orden que
  garantiza.
