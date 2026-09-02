# Fase 06: CedarAuthorizer — Interceptor de Autorización Zero-Trust en Rust

**Nombre del Manifiesto:** `CedarAuthorizer`
**Tipo:** Interceptor / Middleware de Autorización — Paso 1 del IOP Pipeline
**Caller:** gRPC Request Handlers / Tower Middleware (síncrono, bloqueante)
**Consumidor del output:** Janus Query Engine (valida `cedar_ctx` vs `:metri.cedar/context-invariant`)
**Contrato de output:** `CedarContext` — enviado al path OLTP o inyectado en el AST IR para Janus Router

El **CedarAuthorizer** es el único punto de entrada de autorización del Metri Engine.
Es un **interceptor/middleware** — no un servicio, no un microservicio, no una Lambda independiente.
Vive dentro del mismo proceso de `metri-engine` y se ejecuta síncronamente antes de cualquier operación de negocio o consulta a base de datos.

---

## DOMINIO 0: Diseño del Interceptor — Contrato de Entrada y Salida

### La decisión de diseño clave

> **¿El Interceptor extrae el token internamente, o recibe el token como parámetro?**

**Decisión: El Interceptor recibe el `tonic::Request<T>` completo. El token es extraído como Paso 1 dentro del interceptor.**

| Argumento | Explicación |
| :--- | :--- |
| **SRP del interceptor** | El interceptor ES la frontera entre el transporte (gRPC) y el dominio de autorización. Extraer el token del header es su trabajo — no del orquestador principal. |
| **El IOP no debe saber cómo se transporta el token** | Hoy es `authorization: Bearer <token>`, mañana puede ser `x-metri-token` o cookies HTTP. El orquestador general de la Lambda nunca debería cambiar por eso. |
| **El output incluye el `request` original** | El interceptor devuelve `Ok(CedarContext)` conteniendo el request original al orquestador para que Janus lo procese. |
| **Protocolo-agnóstico por fuera, específico por dentro** | El orquestador llama `intercept(request, deps)` — una sola firma limpia. Solo el Paso 1 interno conoce los metadatos de Tonic. |

**Lo que NO hace el orquestador principal:**

```rust
// ❌ MAL — el orquestador no extrae el token ni conoce los headers
let token = req.metadata().get("authorization").unwrap();
cedar_authorizer::intercept(token, deps).await;

// ✅ CORRECTO — el orquestador pasa el request completo
cedar_authorizer::intercept(req, deps).await;
```

---

### Entrada del Interceptor

```
Entrada: req (&tonic::Request<T>)

┌─────────────────────────────────────────────────────────────┐
│ request (Tonic Request)                                     │
│  ├─ metadata                                                │
│  │    └─ "authorization"  "Bearer sha256-abc..."  ← TOKEN   │
│  ├─ payload / body                                          │
│  │    ├─ tenant_id        "tnt_01J..." (validado contra sesión)│
│  │    ├─ entity_type      "asset" | "location"    ← DOMAIN  │
│  │    ├─ action           "CREATE" | "GET"        ← ACTION  │
│  │    └─ ... raw payload EAV                                │
└─────────────────────────────────────────────────────────────┘

Deps inyectadas (Arc / Traits):
  valkey_store   ISessionStore   → resolver opaque token
  eav_reader     EavReader       → pull user/role/groups (snapshot)
  principal_cache IPrincipalCache → TTL 10s por user-id (Caffeine/MiniMoka)
  cedar_engine   CedarEngine     → PolicySet cache + evaluación ABAC de Cedar
```

> [!NOTE]
> El mismo `tonic::Request` viaja intacto por todo el pipeline — nadie lo modifica. Cada componente lee lo que necesita:
>
> - **CedarAuthorizer** lee: `metadata("authorization")` (Paso 1) + `entity_type` y `action` (Paso 4 → Cedar Action y Resource)
> - **QuotaGuard** lee: `entity_type` (→ `resource_domain` de DynamoDB) + `action` (→ mapping `limit_type`)
> - **Janus** lee: `entity_type` (→ `codice/load-schema`) + `payload` (→ EAV validation)

### Salida del Interceptor

```
Salida: Result<CedarContext, DomainError>

Ok(CedarContext)
┌────────────────────────────────────────────────────────────────────┐
│ CedarContext (Evoluciona según el consumidor final)                │
│  ├─ tenant_id       "tnt_01J..."  ← Zero-Trust: inyectado por Cedar│
│  ├─ user_id         "usr_01J..."  ← identidad soberana del sujeto  │
│  ├─ roles           HashSet<"tenant-admin">                        │
│  │                                                                  │
│  │  SI ES ANALÍTICA (Janus Router / action="VIEW"):                 │
│  ├─ domain_boundaries                                               │
│  │    {"work_order" -> [{query_scope: "ALL", ...}],   ← raíz 1     │
│  │     "asset"      -> [{...}, {...}]}                 ← raíz 2     │
│  │   ⚠ Solo entidades evaluadas como raíz por Cedar.               │
│  │                                                                  │
│  │  SI ES MUTACIONAL (IOP OLTP / action="CREATE/UPDATE/DELETE"):    │
│  ├─ domain_boundaries  {}  ← Vacío (Cedar validó ABAC en memoria)   │
│  │                                                                  │
│  └─ request         tonic::Request<T> ← passthrough original       │
└────────────────────────────────────────────────────────────────────┘

Err(DomainError)
┌─────────────────────────────────────────────────────────────┐
│ DomainError                                                 │
│  ├─ code    ErrorCode::Abac401 (token ausente o expirado)   │
│  │        | ErrorCode::Abac403 (usuario suspendido, DENY    │
│  │                            fuera de ventana temporal)    │
│  └─ message string     (descripción detallada)              │
└─────────────────────────────────────────────────────────────┘
  ↑ El orquestador retorna gRPC Status (401/403) inmediatamente sin ir a Janus.
```

### Diagrama de contrato

```
                  ┌──────────────────────────────────────────┐
Orquestador       │          CedarAuthorizer                 │
                  │                                          │
(cedar::intercept │  Paso 1: token ← metadata("authorization")│
  request         │  Paso 2: user/role/groups ← EavReader    │
  deps)    ──────►│  Paso 3: expand locations/assets (async) │
                  │  Paso 3b: ventana temporal (chrono)      │
                  │  Paso 4: action  ← request.action        │
                  │           domains ← request(entities)    │
                  │           Cedar ABAC → ALLOW | DENY       │
                  │                                          │
          ◄───────│  Ok(CedarContext) | Err(DomainError)     │
                  └──────────────────────────────────────────┘
```

---

## DOMINIO I: Algoritmo de Autorización — 5 Pasos en Rust

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                     CedarAuthorizer — Pipeline interno                          │
│                                                                                  │
│  ┌─────────┐   ┌──────────────────────┐   ┌─────────────────────────────────┐   │
│  │ PASO 1  │   │       PASO 2         │   │           PASO 3                │   │
│  │ Extraer │   │  Consulta EAV        │   │      Consolidar Fronteras       │   │
│  │ Token   │──►│  1 pull recursivo    │──►│  tokio::spawn en paralelo       │   │
│  │ del     │   │  user→roles→groups   │   │  ┌─ expand locations (Tokio) ─┐ │   │
│  │ request │   │  CACHE TTL 10s ✦     │   │  └─ expand assets    (Tokio) ─┘ │   │
│  │ Valkey  │   │                      │   │  futures::future::join_all      │   │
│  └─────────┘   └──────────────────────┘   └─────────────────────────────────┘   │
│                                                                                  │
│  ┌─────────────────────┐   ┌────────────────────────────────────────────────┐   │
│  │       PASO 3b       │   │                    PASO 4                      │   │
│  │  Ventana Temporal   │──►│          Motor Cedar ABAC (Rust Crate)             │   │
│  │  Función Pura       │   │  PolicySet cache por (role-id, grants-hash) ✦  │   │
│  │  Chrono (L-V 8-18)  │   │  → ALLOW | DENY en microsegundos               │   │
│  └─────────────────────┘   └────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
   ✦ = optimizaciones de rendimiento
```

---

### Infraestructura — Traits y Protocolos en Rust

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::DatomValue;

// ── Trait PrincipalCache — abstracción testeable ───────────────────────────
#[async_trait]
pub trait PrincipalCache: Send + Sync {
    async fn lookup_principal(&self, user_id: &str) -> Option<PrincipalData>;
    async fn store_principal(&self, user_id: &str, principal: PrincipalData) -> Result<(), DomainError>;
    async fn evict_user(&self, user_id: &str) -> Result<(), DomainError>;
    async fn evict_by_role(&self, role_id: &str) -> Result<(), DomainError>;
}

// ── Estructura de Datos del Principal ──────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBoundary {
    pub role_id: String,
    pub grants: Vec<serde_json::Value>,
    pub permitted_locations: Vec<String>,
    pub permitted_assets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRestriction {
    pub days_of_week: Vec<i32>,
    pub start_minute: u32,
    pub end_minute: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalData {
    pub user_id: String,
    pub tenant_id: String,
    pub status: String,
    pub roles: HashSet<String>,
    pub roles_boundaries: Vec<RoleBoundary>,
    pub time_restrictions: Vec<TimeRestriction>,
}

// Límite de expansión jerárquica O(profundidad)
const MAX_HIERARCHY_DEPTH: usize = 10;
```

---

### Paso 1 — Extraer Token del Request (Frontera de Transporte)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub kid: String,
    pub tenant_id: String,
    pub user_id: String,
    pub expires_at: i64,
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn resolve_opaque(&self, token: &str) -> Result<Option<Session>, DomainError>;
}

/// Paso 1: el interceptor opera sobre el request Tonic completo.
pub async fn step1_extract_token<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn SessionStore,
) -> Result<Session, DomainError> {
    let auth_header = req.metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| DomainError::cedar(ErrorCode::Abac401, "Missing authorization header"))?;

    let token = auth_header.strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .unwrap_or(auth_header)
        .trim();

    let session = valkey_store.resolve_opaque(token).await?
        .ok_or_else(|| DomainError::cedar(ErrorCode::Abac401, "Invalid or expired token"))?;

    // Guard contra payload malformado de Valkey
    if session.user_id.is_empty() || session.tenant_id.is_empty() {
        return Err(DomainError::cedar(ErrorCode::Abac401, "Malformed session payload from Valkey"));
    }

    Ok(session)
}
```

---

### Paso 2 — Consulta EAV con Cache Inteligente

```rust
use crate::eav::reader::pull::EavReader;

/// Cache-aside pattern.
/// HIT:  0 queries a DynamoDB → retorno inmediato en microsegundos.
/// MISS: 1 pull recursivo de EAV → cachea → retorna.
pub async fn step2_query_oltp(
    eav_reader: &EavReader,
    tenant_id: &str,
    user_id: &str,
    cache: &dyn PrincipalCache,
) -> Result<PrincipalData, DomainError> {
    if let Some(cached) = cache.lookup_principal(user_id).await {
        return Ok(cached);
    }

    // Pull recursivo del grafo de usuario (1 sola interacción agregada en DynamoDB)
    let user_map = eav_reader.pull(tenant_id, user_id, None).await?;
    if user_map.is_empty() {
        return Err(DomainError::cedar(ErrorCode::Abac403, format!("User {user_id} not found")));
    }

    let status = user_map.get("user/status")
        .and_then(|v| match v {
            DatomValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("ACTIVE");

    if status == "SUSPENDED" {
        return Err(DomainError::cedar(ErrorCode::Abac403, format!("User {user_id} is suspended")));
    }

    let principal = assemble_principal_graph(eav_reader, tenant_id, user_id, user_map).await?;
    cache.store_principal(user_id, principal.clone()).await?;

    Ok(principal)
}
```

---

### Paso 3 — Consolidar + Expansión Jerárquica en Paralelo (Tokio)

```rust
/// Expande recursivamente los ancestros/descendientes usando tokio::spawn paralelos
pub async fn step3_consolidate(
    eav_reader: &EavReader,
    tenant_id: &str,
    user_data: PrincipalData,
) -> Result<PrincipalData, DomainError> {
    if user_data.roles.is_empty() {
        return Err(DomainError::cedar(ErrorCode::Abac403, "User has no roles assigned"));
    }

    let mut consolidated_boundaries = Vec::new();

    for boundary in &user_data.roles_boundaries {
        let eav_reader_clone = eav_reader.clone();
        let tenant_clone = tenant_id.to_string();
        let roots_locations = boundary.permitted_locations.clone();
        
        // Ejecutar expansión de locaciones en un hilo ligero de Tokio
        let locs_handle = tokio::spawn(async move {
            expand_hierarchy(&eav_reader_clone, &tenant_clone, "location", roots_locations).await
        });

        let eav_reader_clone2 = eav_reader.clone();
        let tenant_clone2 = tenant_id.to_string();
        let roots_assets = boundary.permitted_assets.clone();

        // Ejecutar expansión de activos en paralelo
        let assets_handle = tokio::spawn(async move {
            expand_hierarchy(&eav_reader_clone2, &tenant_clone2, "asset", roots_assets).await
        });

        let (locs_res, assets_res) = tokio::join!(locs_handle, assets_handle);
        
        let expanded_locations = locs_res.map_err(|e| DomainError::infra(ErrorCode::Infra001, e.to_string()))??;
        let expanded_assets = assets_res.map_err(|e| DomainError::infra(ErrorCode::Infra001, e.to_string()))??;

        consolidated_boundaries.push(RoleBoundary {
            role_id: boundary.role_id.clone(),
            grants: boundary.grants.clone(),
            permitted_locations: expanded_locations,
            permitted_assets: expanded_assets,
        });
    }

    let mut final_principal = user_data;
    final_principal.roles_boundaries = consolidated_boundaries;
    Ok(final_principal)
}

/// Función recursiva pura para recorrer jerarquías en EAV
async fn expand_hierarchy(
    eav_reader: &EavReader,
    tenant_id: &str,
    entity_type: &str,
    roots: Vec<String>,
) -> Result<Vec<String>, DomainError> {
    if roots.is_empty() {
        return Ok(vec![]);
    }

    let mut expanded = HashSet::new();
    let mut queue = roots;

    for depth in 0..MAX_HIERARCHY_DEPTH {
        if queue.is_empty() {
            break;
        }

        let mut next_level = Vec::new();
        for id in queue {
            if expanded.insert(id.clone()) {
                // Consultar hijos directos en DynamoDB GSI-AVET: parent_id == id
                let children = fetch_children_eav(eav_reader, tenant_id, entity_type, &id).await?;
                next_level.extend(children);
            }
        }
        queue = next_level;
    }

    Ok(expanded.into_iter().collect())
}
```

---

### Paso 3b — Validación de Ventana Temporal (Chrono)

```rust
use chrono::{DateTime, Utc, Datelike, Timelike};

/// Valida la ventana temporal del principal de forma 100% pura y testeable.
pub fn step3b_validate_time_window(
    principal: &PrincipalData,
    now: DateTime<Utc>,
) -> Result<(), DomainError> {
    if principal.time_restrictions.is_empty() {
        return Ok(());
    }

    // Obtener día de la semana (Lunes = 1, Domingo = 7) y minuto del día
    let day = now.weekday().number_from_monday() as i32;
    let minute = now.hour() * 60 + now.minute();

    let matches_any = principal.time_restrictions.iter().any(|r| {
        r.days_of_week.contains(&day) && minute >= r.start_minute && minute <= r.end_minute
    });

    if !matches_any {
        return Err(DomainError::cedar(ErrorCode::Abac403, "Access outside allowed time window"));
    }

    Ok(())
}
```

---

### Paso 4 — Motor Cedar ABAC (Rust `cedar-policy` crate)

```rust
use cedar_policy::{Authorizer, Context, Entities, PolicySet, Request, Decision};

pub fn step4_evaluate_cedar(
    cedar_engine: &Authorizer,
    policy_cache: &HashMap<String, PolicySet>,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
    body: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    if is_mutational_action(action) {
        step4_mutational(cedar_engine, policy_cache, principal, action, resource, body)
    } else {
        step4_analytical(cedar_engine, principal, resource)
    }
}

fn step4_mutational(
    cedar_engine: &Authorizer,
    policy_cache: &HashMap<String, PolicySet>,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
    body: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    let mut allowed = false;

    // Mutaciones gRPC: OR lógico entre todos los roles asignados del usuario
    for boundary in &principal.roles_boundaries {
        let policy_set = policy_cache.get(&boundary.role_id)
            .ok_or_else(|| DomainError::cedar(ErrorCode::Abac403, "PolicySet not compiled for role"))?;

        let request = Request::new(
            Some(principal.user_id.parse().unwrap()),
            Some(action.parse().unwrap()),
            Some(resource.get("entity_id").and_then(|v| v.as_str()).unwrap_or("").parse().unwrap()),
            Context::empty(),
            None,
        ).map_err(|e| DomainError::cedar(ErrorCode::Abac403, e.to_string()))?;

        let entities = Entities::empty();
        let response = cedar_engine.is_authorized(&request, policy_set, &entities);

        if response.decision() == Decision::Allow {
            allowed = true;
            break;
        }
    }

    if !allowed {
        return Err(DomainError::cedar(
            ErrorCode::Abac403,
            format!("Cedar DENY (Mutational ABAC Failed for action={action})")
        ));
    }

    Ok(serde_json::json!({}))
}

fn step4_analytical(
    cedar_engine: &Authorizer,
    principal: &PrincipalData,
    resource: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    // Para VIEW analítica, compilamos y retornamos el mapa de límites geográficos RLS (domain-boundaries)
    let mut domain_dict = serde_json::Map::new();

    // Recolectar boundaries para cada dominio solicitado
    if let Some(domains) = resource.get("domains").and_then(|v| v.as_array()) {
        for d_val in domains {
            let domain = d_val.as_str().unwrap_or("");
            let mut boundaries_json = Vec::new();

            for boundary in &principal.roles_boundaries {
                for grant in &boundary.grants {
                    if grant.get("domain").and_then(|v| v.as_str()) == Some(domain) {
                        let scope = grant.get("scope").and_then(|v| v.as_str()).unwrap_or("NONE");
                        boundaries_json.push(serde_json::json!({
                            "query_scope": scope,
                            "permitted_locations": boundary.permitted_locations,
                            "permitted_assets": boundary.permitted_assets,
                        }));
                    }
                }
            }

            if boundaries_json.is_empty() {
                return Err(DomainError::cedar(
                    ErrorCode::Abac403,
                    format!("Cedar DENY: No role authorized for domain {domain}")
                ));
            }

            domain_dict.insert(domain.to_string(), serde_json::Value::Array(boundaries_json));
        }
    }

    Ok(serde_json::Value::Object(domain_dict))
}

fn is_mutational_action(action: &str) -> bool {
    matches!(action, "CREATE" | "UPDATE" | "DELETE" | "UPSERT")
}
```

---

### Orquestador Principal — `intercept` (Frontera Pública)

```rust
pub struct CedarContext {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: HashSet<String>,
    pub domain_boundaries: serde_json::Value,
}

/// Middleware / Interceptor principal de metri-engine
pub async fn intercept<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn SessionStore,
    eav_reader: &EavReader,
    cache: &dyn PrincipalCache,
    cedar_engine: &Authorizer,
    policy_cache: &HashMap<String, PolicySet>,
) -> Result<CedarContext, DomainError> {
    // 1. Extraer token opaco del header y validar sesión
    let session = step1_extract_token(req, valkey_store).await?;

    // 2. Resolver principal (HIT en cache en microsegundos, MISS requiere pull EAV)
    let raw_principal = step2_query_oltp(eav_reader, &session.tenant_id, &session.user_id, cache).await?;

    // 3. Consolidar raíces y resolver expansión recursiva en paralelo (tokio::spawn)
    let principal = step3_consolidate(eav_reader, &session.tenant_id, raw_principal).await?;

    // 3b. Validar ventana temporal del Chrono
    step3b_validate_time_window(&principal, Utc::now())?;

    // 4. Evaluar con el motor Cedar Policy nativo en Rust
    // Se mapean los metadatos gRPC a conceptos canónicos de Cedar
    let action = extract_cedar_action(req)?;
    let resource = extract_cedar_resource(req)?;
    let body = extract_req_body(req)?;

    let domain_dict = step4_evaluate_cedar(
        cedar_engine,
        policy_cache,
        &principal,
        &action,
        &resource,
        &body,
    )?;

    // 5. Retornar contexto verificado e inyectado
    Ok(CedarContext {
        tenant_id: session.tenant_id,
        user_id: session.user_id,
        roles: principal.roles,
        domain_boundaries: domain_dict,
    })
}
```

---

## DOMINIO II: Resolución de Identidad — Opaque Token en Valkey

### Protocolo Zero-Network

```
Login (una sola vez):
  1. Autenticación contra IdP externo (Auth0 / Cognito)
  2. Session mínima guardada en Valkey:
       valkey[KID] = { tenant_id, user_id, expires_at }
       ← NUNCA se guardan roles, grupos, status ni permisos en el token
  3. Cliente recibe: "Authorization: Bearer <OPAQUE_TOKEN>"
     (SHA-256 aleatorio, sin información decodificable en tránsito)

Por cada request:
  Interceptor:
    Paso 1 → extrae token del header Tonic → GET valkey[token]
             ← Session{ tenant_id, user_id, expires_at }

    Paso 2 → pull recursivo EAV en DynamoDB[user-id]
             ← { status, roles, groups, locations, assets }
                ↑ siempre el estado actual e inmutable
```

### Schema de Sesión (Valkey JSON) — Mínimo de identidad

```json
{
  "kid": "sha256-abc...",
  "tenant_id": "tnt_01J...",
  "user_id": "usr_01J...",
  "expires_at": 1744234567000
}
```

> [!IMPORTANT]
> **Sin status, sin role_id, sin group_ids en la sesión Valkey.**
> Cambiar rol → efecto inmediato en el próximo request.
> Bloquear usuario → efecto inmediato en el próximo request.
> **Sin brechas de vulnerabilidad por TTL prolongado del token.**

---

## DOMINIO III: Hidratación del Principal — Jerarquías Direccionales EAV

### La Regla Jerárquica — Dirección Única: Padre → Hijo

> [!IMPORTANT]
> **Invariante de jerarquía:** Si un usuario tiene acceso a una `location` (o `asset`), puede ver **todos sus descendientes**. Nunca puede ver sus **ancestros**. Dirección: exclusivamente **↓ descendente**.

```
Árbol de locaciones:

  CORPORACIÓN                  ← 🚫 NO visible (ancestro)
    └─ PLANTA_NORTE            ← ✅ allowed_location (techo asignado)
         ├─ TALLER_MANT           ← ✅ descendiente — visible
         │    ├─ BAHÍA_A          ← ✅ descendiente — visible
         │    └─ BAHÍA_B          ← ✅ descendiente — visible
         └─ LÍNEA_PROD_1          ← ✅ descendiente — visible
```

La expansión en Rust (`expand_hierarchy`) realiza una búsqueda en anchura recursiva (BFS) consultando los datoms mediante el índice `AVET` de DynamoDB de la relación `parent_id` hasta un máximo estricto de `MAX_HIERARCHY_DEPTH` niveles para evitar loops infinitos en grafos corruptos.

---

## DOMINIO IV: Políticas Cedar ABAC — `metri.cedar` v3.0

Las políticas de Cedar son estáticas y definen la lógica de decisión formal.

```cedar
// ── [F1] FORBID si el usuario está SUSPENDIDO ──────────────────────────────
forbid(principal is Metri::User, action, resource)
when { principal.status == "SUSPENDED" };

// ── [F2] FORBID si la visibilidad es nula ──────────────────────────────────
forbid(principal is Metri::User, action, resource is Metri::MetriResource)
when { resource.query_scope == "NONE" };

// ── [F3] FORBID de seguridad cardinal cross-tenant ─────────────────────────
forbid(principal is Metri::User, action, resource is Metri::MetriResource)
when {
    principal.tenant_id != resource.tenant_id &&
    !principal.is_super_master
};

// ── [P1] PERMIT para Super-Master de plataforma ────────────────────────────
permit(principal is Metri::User, action, resource is Metri::MetriResource)
when {
    principal.is_super_master &&
    (principal.cross_tenant_scope == "READ_ALL" || principal.cross_tenant_scope == "FULL")
};

// ── [P2] PERMIT base: Acción y tenant autorizados ──────────────────────────
permit(principal is Metri::User, action, resource is Metri::MetriResource)
when {
    principal.granted_action_keys.contains(resource.action_key) &&
    principal.tenant_id == resource.tenant_id
};
```

---

## DOMINIO VII: Variables de Entorno y Configuración

El ciclo de arranque de `metri-engine` valida de forma estricta las dependencias en cold start.

### Variables de entorno requeridas

| Variable | Descripción | Ejemplo |
| :--- | :--- | :--- |
| `VALKEY_HOST` | Host de base de datos Valkey | `valkey.internal` |
| `VALKEY_PORT` | Puerto Valkey | `6379` |
| `AWS_REGION` | Región AWS para DynamoDB EAV | `us-east-1` |
| `EAV_TABLE_NAME` | Nombre de la tabla de DynamoDB | `metri-dynamo` |
| `METRI_MASTER_TENANT_ID` | UUID permanente de plataforma Master | `tnt-prod-master-001` |
| `METRI_MASTER_USER_ID` | UUID de la identidad Platform Admin | `usr-prod-master-001` |

---

## DOMINIO VIII: Observabilidad OpenTelemetry

El interceptor emite telemetría estructurada integrada con OpenTelemetry y AWS CloudWatch:

| Nombre del Span | Atributos Semánticos |
| :--- | :--- |
| `cedar.authorize` | `user_id`, `tenant_id`, `action`, `operation` |
| `cedar.step1.token_extraction` | `has_header`, `is_expired` |
| `cedar.step2.cache_state` | `cache_hit` (bool) |
| `cedar.step3.hierarchy_expansion` | `locations_count`, `assets_count`, `depth_reached` |
| `cedar.step4.decision` | `decision` ("ALLOW" / "DENY"), `roles_count` |

---

## DOMINIO IX: Matriz TDD para Rust

La suite de pruebas automatizadas en `src/cedar/authorizer.rs` valida de forma determinista y sin efectos secundarios colaterales:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_missing() {
        // gRPC Request sin metadata authorization debe fallar con Abac401
    }

    #[tokio::test]
    async fn test_user_suspended() {
        // Usuario con status == "SUSPENDED" debe lanzar Abac403 de inmediato
    }

    #[tokio::test]
    async fn test_cache_hit_prevents_db_query() {
        // Segundo request del mismo usuario debe resolver vía cache in-memory con 0 queries DynamoDB
    }

    #[test]
    fn test_time_window_validation() {
        // Validar ventanas de Chrono: Lunes 9am -> Ok, Sábado 9am -> Err(Abac403)
    }

    #[test]
    fn test_expand_hierarchy_respects_depth_limit() {
        // Árbol jerárquico artificial de 12 niveles debe cortarse estrictamente en MAX_HIERARCHY_DEPTH (10)
    }

    #[tokio::test]
    async fn test_cedar_deny_rejection() {
        // Evaluar políticas Cedar denegando accesos no configurados en grants
    }
}
```

---

## 🔒 DOMINIO X: Recomendaciones de Seguridad (PKCE & Mitigación de XSS)

Dado que los tokens `mk_...` de Metri son auto-contenidos, stateless y poseen la identidad del usuario y del tenant firmados criptográficamente, su ciclo de vida y almacenamiento deben protegerse rigurosamente contra vectores de ataque en clientes públicos (web y móviles).

### 1. Enforzar PKCE (Proof Key for Code Exchange) en la Ingestión / Login
El flujo de autenticación de Metri (ej. a través de `metri-panel` o cualquier SPA) **debe obligatoriamente** utilizar la extensión **PKCE (RFC 7636)** sobre OAuth 2.0 / OIDC para evitar ataques de interceptación del código de autorización:
- **Flujo**:
  1. El cliente genera un secreto aleatorio de alta entropía (`code_verifier`) y calcula su hash SHA-256 (`code_challenge`).
  2. Al redirigir al servidor de identidad (IdP), envía el `code_challenge`.
  3. Tras la autenticación, el servidor retorna un `code`.
  4. El cliente intercambia el `code` enviando el `code_verifier` original. El servidor valida que el hash coincida antes de emitir el token HMAC final.
- **Beneficio**: Protege a los clientes públicos (Single Page Applications y apps móviles) que no pueden guardar un `client_secret` de forma segura, anulando ataques de interceptación de URIs y de red en el canal de retorno.

### 2. Blindaje Estricto contra Ataques XSS (Almacenamiento de Tokens)
Si un atacante logra inyectar código malicioso en el frontend (Cross-Site Scripting), cualquier token almacenado en `localStorage` o `sessionStorage` puede ser extraído de inmediato (`token hijacking`). Para anular este riesgo, se recomiendan las siguientes directrices:

#### A) Almacenamiento en Cookies `HttpOnly`
- En lugar de exponer el token al entorno de ejecución de JavaScript, el backend o el API Gateway debe retornar el token en una cookie HTTP con los siguientes flags habilitados obligatoriamente:
  - **`HttpOnly`**: Impide que JavaScript acceda a la cookie mediante `document.cookie`, haciendo al token totalmente invisible a scripts XSS.
  - **`Secure`**: Exige al navegador enviar la cookie únicamente sobre canales encriptados HTTPS.
  - **`SameSite=Strict` o `SameSite=Lax`**: Protege al sistema contra ataques de CSRF (Cross-Site Request Forgery).

#### B) API Gateway / Envoy Proxy Cookie-to-Header Mapping
- Puesto que Metri Engine consume peticiones gRPC con la cabecera `Authorization: Bearer <token>`, se recomienda delegar al API Gateway (ej. Envoy Proxy o AWS CloudFront / API Gateway) la responsabilidad de:
  1. Recibir la petición HTTP/HTTPS del navegador.
  2. Extraer de forma segura la cookie `HttpOnly` encriptada que contiene el token.
  3. Inyectar su valor en la cabecera gRPC de metadatos `Authorization` antes de enrutar la petición internamente hacia `metri-engine`.
  - Este diseño mantiene al cliente web 100% libre de la posesión física del token en su memoria accesible por JS, logrando el más alto estándar de resiliencia frente a XSS en la web moderna.

