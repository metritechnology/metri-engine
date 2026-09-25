//! DynamoDB key-value cache adapter for the Janus read path.
//!
//! PLAN_CACHE_JANUS_DYNAMODB.md §4.3/§5. Tabla DEDICADA con TTL nativo (ver
//! §5.4 del plan: blast radius del TTL, invariante append-only de la EAV,
//! aislamiento de throughput). El adaptador es almacenamiento puro: no
//! decide elegibilidad ni degrada — eso lo hace `janus::cache::QueryCacheFrontend`.
//!
//! Item layout (§5.1): `PK` (S) entrada · `t` tenant (verificación defensiva)
//! · `v` payload JSON · `exp` epoch-secs LÓGICO · `ttl` = exp+3600 FÍSICO
//! (atributo con TTL habilitado — limpieza best-effort, jamás corrección)
//! · `gen` generación del tenant al escribir · `wa` written_at epoch-ms.
//!
//! Invalidación por generación (D4): item de control `GEN#QC#<tenant>` con
//! contador `v`; la lectura compara `entry.gen` contra la generación actual
//! en el MISMO BatchGetItem (lectura fuerte — acota la visibilidad del bump
//! al ~segundo; la cota dura sigue siendo el TTL).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::warn;

use crate::domain::protocols::{
    CacheEntry, CachePut, DomainResult, IQueryCache, QueryCacheInvalidator,
};
use crate::infrastructure::dynamodb::{av_number, av_string, DynamoClient};

/// Margen entre la expiración lógica y la física: el TTL de DynamoDB borra
/// en horas (best-effort ≤ 48 h); dar una hora de colchón evita borrar
/// entradas aún servibles por si el reloj de la app va adelantado.
const PHYSICAL_TTL_GRACE_SECS: i64 = 3600;

/// PK del item de control de generación de un tenant.
fn generation_key(tenant_id: &str) -> String {
    format!("GEN#QC#{tenant_id}")
}

/// Caché KV sobre la tabla DynamoDB dedicada. Reusa `DynamoClient` (mismo
/// cliente HTTP, mismo mapeo de errores — DRY).
pub struct DynamoKvCache {
    ddb: Arc<DynamoClient>,
    table: String,
    /// Invalidación por generación activa (`QUERY_CACHE_GENERATION=on`).
    generation_enabled: bool,
    /// Reloj inyectable para tests de expiración.
    clock: fn() -> i64,
}

impl DynamoKvCache {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        Self {
            ddb,
            table: table.into(),
            generation_enabled: std::env::var("QUERY_CACHE_GENERATION")
                .map(|v| v == "on")
                .unwrap_or(false),
            clock: default_now_secs,
        }
    }

    /// Constructor con reloj inyectable (tests).
    pub fn with_clock(mut self, clock: fn() -> i64) -> Self {
        self.clock = clock;
        self
    }

    /// Generación vigente del tenant (lectura fuerte). `None` si el item de
    /// control aún no existe (tenant sin escrituras desde que se activó F3).
    async fn current_generation(&self, tenant_id: &str) -> DomainResult<Option<u64>> {
        let item = self
            .ddb
            .get_item(&self.table, &generation_key(tenant_id), None)
            .await?;
        Ok(item.and_then(|i| i.get("v").and_then(av_number).and_then(|n| n.parse().ok())))
    }
}

fn default_now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn entry_from_item(item: &HashMap<String, AttributeValue>) -> Option<CacheEntry> {
    let payload =
        serde_json::from_str::<serde_json::Value>(item.get("v").and_then(av_string)?).ok()?;
    Some(CacheEntry {
        payload,
        tenant_id: item.get("t").and_then(av_string)?.to_string(),
        expires_at: item.get("exp").and_then(av_number)?.parse().ok()?,
        generation: item
            .get("gen")
            .and_then(av_number)
            .and_then(|n| n.parse().ok()),
        stored_at_ms: item
            .get("wa")
            .and_then(av_number)
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
    })
}

#[async_trait]
impl IQueryCache for DynamoKvCache {
    async fn get(&self, key: &str, tenant_id: &str) -> DomainResult<Option<CacheEntry>> {
        if !self.generation_enabled {
            let item = self.ddb.get_item(&self.table, key, None).await?;
            return Ok(item.as_ref().and_then(entry_from_item));
        }

        // F3: entrada + generación en UN round trip. La lectura es fuerte
        // para acotar la visibilidad del bump; el costo (2× RRU sobre la
        // entrada) es el precio documentado de invalidar por escritura.
        let mut keys = Vec::with_capacity(2);
        let mut k1 = HashMap::new();
        k1.insert("PK".to_string(), AttributeValue::S(key.to_string()));
        keys.push(k1);
        let mut k2 = HashMap::new();
        k2.insert(
            "PK".to_string(),
            AttributeValue::S(generation_key(tenant_id)),
        );
        keys.push(k2);

        let items = self.ddb.batch_get_item(&self.table, keys, true).await?;

        let entry_item = items
            .iter()
            .find(|i| i.get("PK").and_then(av_string) == Some(key));
        let entry = match entry_item.and_then(entry_from_item) {
            Some(e) => e,
            None => return Ok(None),
        };

        let current_gen: Option<u64> = items
            .iter()
            .find(|i| i.get("PK").and_then(av_string) == Some(generation_key(tenant_id).as_str()))
            .and_then(|i| i.get("v").and_then(av_number))
            .and_then(|n| n.parse().ok());

        // Comparación solo cuando AMBAS generaciones existen: entradas
        // escritas antes de activar F3 (sin gen) o tenants sin item de
        // control siguen regidas por el TTL.
        if let (Some(entry_gen), Some(current)) = (entry.generation, current_gen) {
            if entry_gen != current {
                return Ok(None);
            }
        }

        Ok(Some(entry))
    }

    async fn put(&self, entry: &CachePut) -> DomainResult<()> {
        let now = (self.clock)();
        let expires_at = now + entry.ttl_secs as i64;

        let gen = if self.generation_enabled {
            self.current_generation(&entry.tenant_id).await?
        } else {
            None
        };

        let mut item = HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S(entry.key.clone()));
        item.insert("t".to_string(), AttributeValue::S(entry.tenant_id.clone()));
        item.insert(
            "v".to_string(),
            AttributeValue::S(entry.payload.to_string()),
        );
        item.insert(
            "len".to_string(),
            AttributeValue::N(entry.payload.to_string().len().to_string()),
        );
        item.insert("exp".to_string(), AttributeValue::N(expires_at.to_string()));
        item.insert(
            "ttl".to_string(),
            AttributeValue::N((expires_at + PHYSICAL_TTL_GRACE_SECS).to_string()),
        );
        item.insert(
            "wa".to_string(),
            AttributeValue::N((now * 1000).to_string()),
        );
        if let Some(g) = gen {
            item.insert("gen".to_string(), AttributeValue::N(g.to_string()));
        }

        self.ddb.put_item(&self.table, item).await
    }
}

/// Invalidador por generación (D4/F3): `UpdateItem` atómico `ADD 1` sobre el
/// item de control del tenant. Se inyecta en el embudo post-commit del
/// escritor (`eav::writer::cache_policy`) — un bump por commit, no por entidad.
pub struct DynamoGenInvalidator {
    ddb: Arc<DynamoClient>,
    table: String,
}

impl DynamoGenInvalidator {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        Self {
            ddb,
            table: table.into(),
        }
    }
}

#[async_trait]
impl QueryCacheInvalidator for DynamoGenInvalidator {
    async fn invalidate_tenant(&self, tenant_id: &str) -> DomainResult<()> {
        match self
            .ddb
            .update_item_add(&self.table, &generation_key(tenant_id), "v", 1)
            .await
        {
            Ok(new_gen) => {
                tracing::debug!("query-cache: generación de tenant '{tenant_id}' → {new_gen}");
                Ok(())
            }
            Err(e) => {
                // Best-effort (D4): el TTL es la cota dura; un bump fallido
                // se registra, no tumba la escritura que lo disparó.
                warn!("query-cache: bump de generación falló para '{tenant_id}' (TTL acota): {e}");
                Ok(())
            }
        }
    }
}

// ── Tests de integración (DynamoDB Local) ────────────────────────────────────
//
// `make infra && make seed && make test-integration` — mismo patrón que
// src/quota/tests/dynamo_store_tests.rs. La tabla la crea
// scripts/dev/reset_local.py (TABLES_CONFIG); el test la garantiza por si
// se corre suelto.

#[cfg(test)]
mod integration_tests {
    use super::*;

    fn endpoint() -> String {
        std::env::var("DYNAMODB_ENDPOINT").unwrap_or_else(|_| "http://localhost:8000".to_string())
    }

    async fn client_with_table() -> DynamoKvCache {
        std::env::set_var("AWS_ACCESS_KEY_ID", "local");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "local");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        std::env::set_var("DYNAMODB_ENDPOINT", endpoint());

        let ddb = Arc::new(DynamoClient::new("metri-eav-local").await);
        ensure_table(&ddb).await;
        DynamoKvCache::new(ddb, "metri-query-cache-local")
    }

    async fn ensure_table(ddb: &DynamoClient) {
        let table = "metri-query-cache-local";
        let exists = ddb
            .client
            .describe_table()
            .table_name(table)
            .send()
            .await
            .is_ok();
        if exists {
            return;
        }
        #[allow(clippy::expect_used)] // invariante de test: builders infalibles
        let attr_def = aws_sdk_dynamodb::types::AttributeDefinition::builder()
            .attribute_name("PK")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
            .build()
            .expect("AttributeDefinition");
        #[allow(clippy::expect_used)]
        let key_schema = aws_sdk_dynamodb::types::KeySchemaElement::builder()
            .attribute_name("PK")
            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
            .build()
            .expect("KeySchemaElement");

        let _ = ddb
            .client
            .create_table()
            .table_name(table)
            .attribute_definitions(attr_def)
            .key_schema(key_schema)
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await;
    }

    fn put(key: &str, tenant: &str, ttl: u64) -> CachePut {
        CachePut {
            tenant_id: tenant.to_string(),
            key: key.to_string(),
            payload: serde_json::json!({"data": [1, 2, 3], "total": 3}),
            ttl_secs: ttl,
        }
    }

    #[tokio::test]
    #[ignore = "requiere DynamoDB Local (make infra)"]
    async fn put_then_get_roundtrip() {
        let cache = client_with_table().await;
        cache.put(&put("QC#qc1#rt", "t-rt", 60)).await.unwrap();

        let hit = cache
            .get("QC#qc1#rt", "t-rt")
            .await
            .unwrap()
            .expect("hit esperado");
        assert_eq!(hit.tenant_id, "t-rt");
        assert_eq!(hit.payload["total"], serde_json::json!(3));
        assert!(hit.expires_at > 0);

        // Miss de otra clave y de otro tenant (verificación defensiva).
        assert!(cache
            .get("QC#qc1#inexistente", "t-rt")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[ignore = "requiere DynamoDB Local (make infra)"]
    async fn expired_entry_is_logical_miss() {
        // Reloj inyectable: escribe con TTL 1 y avanza el reloj de la MISMA
        // instancia — el item físico sigue en la tabla (DynamoDB Local no
        // ejecuta TTL), pero la expiración LÓGICA lo vuelve miss.
        static NOW: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1_000_000);
        let cache = client_with_table()
            .await
            .with_clock(|| NOW.load(std::sync::atomic::Ordering::SeqCst));

        cache.put(&put("QC#qc1#exp", "t-exp", 10)).await.unwrap();
        assert!(cache.get("QC#qc1#exp", "t-exp").await.unwrap().is_some());

        NOW.fetch_add(11, std::sync::atomic::Ordering::SeqCst);
        assert!(cache.get("QC#qc1#exp", "t-exp").await.unwrap().is_none());
    }

    #[tokio::test]
    #[ignore = "requiere DynamoDB Local (make infra)"]
    async fn generation_bump_invalidates_entries() {
        // Generación activada manualmente (el constructor lee env una vez).
        std::env::set_var("QUERY_CACHE_GENERATION", "on");
        let cache = client_with_table().await;
        let table = cache.table.clone();
        let ddb = DynamoGenInvalidator::new(Arc::clone(&cache.ddb), table);

        cache.put(&put("QC#qc1#gen", "t-gen", 60)).await.unwrap();
        // La entrada lleva la generación vigente ⇒ hit.
        assert!(cache.get("QC#qc1#gen", "t-gen").await.unwrap().is_some());

        // Una escritura del tenant bumpa la generación ⇒ miss lógico.
        ddb.invalidate_tenant("t-gen").await.unwrap();
        assert!(cache.get("QC#qc1#gen", "t-gen").await.unwrap().is_none());

        // Re-put (adopta la nueva generación) ⇒ hit de nuevo.
        cache.put(&put("QC#qc1#gen", "t-gen", 60)).await.unwrap();
        assert!(cache.get("QC#qc1#gen", "t-gen").await.unwrap().is_some());
        std::env::set_var("QUERY_CACHE_GENERATION", "off");
    }
}
