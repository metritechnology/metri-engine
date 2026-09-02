// [NUEVO — reemplaza: src/metri/infrastructure/datahike.clj + tenant_guard.clj]
// eav/writer/transact.rs — Write path ACID del motor EAV.
// Blueprint: Metri EAV - OLPT.md §IV — ACID Write Path
//
// En Clojure: d/transact → DynamoDB (blob monolítico via Datahike)
// En Rust:    TransactWriteItems con datoms EAVT/AEVT/AVET/VAET individuales
//
// Este módulo es el reemplazo directo de la corrupción de blobs de Datahike.
// Cada atributo es un datom independiente — sin contención bajo concurrencia.

use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use tracing::warn;
use ulid::Ulid;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::{Datom, DatomValue},
    encoding::{build_aevt_sk, build_avet_sk, build_eavt_sk, build_vaet_sk},
};
use crate::infrastructure::dynamodb::DynamoClient;

/// Contexto de una transacción EAV.
/// Equivale al `TransactPayload` implícito en el pipeline IOP de Clojure.
#[derive(Debug, Clone)]
pub struct TransactPayload {
    pub tenant_id: String,
    pub entity_id: Option<String>, // None = nuevo (se genera ULID)
    pub entity_type: String,
    pub attrs: HashMap<String, DatomValue>, // nombre_attr → valor nuevo
    pub op: TransactOp,
}

/// Operación de la transacción.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactOp {
    /// Crear entidad nueva
    Create,
    /// Actualizar atributos existentes (genera retract + assert)
    Update,
    /// Retract completo (marca todos los atributos como inactivos)
    Delete,
}

/// Resultado de una transacción exitosa.
#[derive(Debug, Clone)]
pub struct TransactResult {
    pub entity_id: String,
    pub tx_id: u64,
    pub datoms: usize, // número de datoms escritos
    pub outbox_count: usize,
}

/// EavWriter — motor de escritura ACID.
/// Reemplaza d/transact de Datahike con TransactWriteItems de DynamoDB.
#[derive(Clone)]
pub struct EavWriter {
    ddb: Arc<DynamoClient>,
    table: String,
    /// Planificadores de restricciones, inyectados.
    ///
    /// El escritor compone items; los planificadores saben de invariantes.
    /// Ninguno de los dos conoce al otro, así que una restricción nueva no
    /// obliga a tocar el camino de escritura, y un test puede inyectar los
    /// suyos sin DynamoDB.
    planners: Arc<Vec<Box<dyn crate::eav::writer::constraints::ConstraintPlanner>>>,
}

impl EavWriter {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        Self::with_planners(
            ddb,
            table,
            Arc::new(crate::eav::writer::constraints::default_planners()),
        )
    }

    /// Compone el escritor con planificadores explícitos.
    pub fn with_planners(
        ddb: Arc<DynamoClient>,
        table: impl Into<String>,
        planners: Arc<Vec<Box<dyn crate::eav::writer::constraints::ConstraintPlanner>>>,
    ) -> Self {
        EavWriter {
            ddb,
            table: table.into(),
            planners,
        }
    }

    /// Obtiene referencia al cliente DynamoDB (útil para inyección de dependencias)
    pub fn client(&self) -> &Arc<DynamoClient> {
        &self.ddb
    }

    /// Obtiene el nombre de la tabla de DynamoDB
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Ejecuta una transacción ACID.
    /// [PORTED_FROM: (d/transact conn tx-data) → DynamoDB blob]
    ///
    /// Proceso:
    /// 1. Generar TX_ID (ULID epoch ms monotónico)
    /// 2. Generar entity_id si es creación nueva
    /// 3. Para UPDATE: generar par retract+assert por cada atributo
    /// 4. Construir TransactWriteItems para EAVT + GSIs activos
    /// 5. Chunk a 100 items por transacción (DynamoDB limit)
    /// 6. Ejecutar TransactWriteItems
    /// Reader sobre la misma tabla — necesario para resolver mappings que
    /// atraviesan referencias (ver janus_router::saga).
    pub fn reader(&self) -> crate::eav::reader::pull::EavReader {
        crate::eav::reader::pull::EavReader::new(self.ddb.clone(), self.table.clone())
    }

    /// Contador de cuota sobre la misma tabla.
    ///
    /// Vive fuera del log de datoms a propósito —ver `quota/ledger.rs`—, pero
    /// comparte tabla y cliente, así que se fabrica desde aquí igual que el
    /// reader, y quien compone el servicio no necesita conocer la conexión.
    pub fn quota_ledger(&self) -> crate::quota::QuotaLedger {
        crate::quota::QuotaLedger::new(self.ddb.clone(), self.table.clone())
    }

    pub async fn transact(&self, payload: TransactPayload) -> Result<TransactResult, DomainError> {
        self.transact_with_projections(payload, Vec::new()).await
    }

    /// Igual que `transact`, pero además escribe N entidades proyectadas
    /// **en la misma TransactWriteItems**, cada una con su propio outbox.
    ///
    /// Es el mecanismo que sostiene la promesa de "madre + scheduled_job en una sola
    /// transacción ACID" (Componente Externo 05 §6).
    pub async fn transact_with_projections(
        &self,
        payload: TransactPayload,
        projections: Vec<crate::janus_router::saga::SagaProjection>,
    ) -> Result<TransactResult, DomainError> {
        // 1. Verificar entity_type en registry
        crate::codice::global()
            .get_model(&payload.entity_type)
            .ok_or_else(|| {
                DomainError::eav(
                    ErrorCode::Eav004,
                    format!("entity_type '{}' no en registry", payload.entity_type),
                )
            })?;

        // 2. Generar TX_ID — ULID epoch ms garantiza monotonía
        let tx_id = Ulid::new().timestamp_ms();

        // 3. Generar entity_id si es creación
        let entity_id = match &payload.entity_id {
            Some(id) => id.clone(),
            None => Ulid::new().to_string(),
        };

        // 4. Construir datoms y FTS requests
        let mut datoms: Vec<Datom> = Vec::new();
        let mut fts_write_requests = Vec::new();

        let active_attrs = if payload.op == TransactOp::Update || payload.op == TransactOp::Delete {
            self.get_active_attributes(&payload.tenant_id, &entity_id)
                .await?
        } else {
            std::collections::HashMap::new()
        };

        if payload.op == TransactOp::Delete {
            for (attr_name, (attr_id, val)) in active_attrs {
                let retract = Datom::retract(
                    &payload.tenant_id,
                    &entity_id,
                    attr_name,
                    attr_id,
                    val,
                    tx_id,
                );
                datoms.push(retract);
            }
        } else {
            for (attr_name, new_value) in &payload.attrs {
                let attr_desc = crate::codice::global()
                    .get_attribute(&payload.entity_type, attr_name)
                    .ok_or_else(|| {
                        DomainError::eav(
                            ErrorCode::Eav004,
                            format!(
                                "Atributo '{attr_name}' no en registry para '{}'",
                                payload.entity_type
                            ),
                        )
                    })?;

                // Para UPDATE: añadir retract del valor anterior
                if payload.op == TransactOp::Update {
                    if let Some((attr_id, old_value)) = active_attrs.get(attr_name) {
                        let retract = Datom::retract(
                            &payload.tenant_id,
                            &entity_id,
                            attr_name,
                            *attr_id,
                            old_value.clone(),
                            tx_id,
                        );
                        datoms.push(retract);
                    }
                }

                // Assert — el nuevo valor
                let assert = Datom::assert(
                    &payload.tenant_id,
                    &entity_id,
                    attr_name,
                    Datom::hash_attr_name(&attr_desc.name),
                    new_value.clone(),
                    tx_id,
                );

                // Generar FTS si el atributo lo requiere
                if attr_desc.fts {
                    let reqs = crate::eav::fts::trigram::build_fts_items(&assert, attr_desc);
                    fts_write_requests.extend(reqs);
                }

                datoms.push(assert);
            }

            // 4.5. Enriquecer con system attributes (incluyendo meta/created_at y meta/updated_at)
            let is_create = payload.op == TransactOp::Create;
            crate::eav::writer::enricher::enrich_datoms(
                &mut datoms,
                &entity_id,
                &payload.entity_type,
                &payload.tenant_id,
                tx_id,
                is_create,
                &active_attrs,
            );

            // Para mantener compatibilidad con consultas legacy que busquen "entity_type" sin slash:
            if !is_create {
                if let Some((_, DatomValue::Str(old_type))) = active_attrs.get("entity_type") {
                    if old_type != &payload.entity_type {
                        let entity_type_retract = Datom::retract(
                            &payload.tenant_id,
                            &entity_id,
                            "entity_type",
                            Datom::hash_attr_name("entity_type"),
                            DatomValue::Str(old_type.clone()),
                            tx_id,
                        );
                        datoms.push(entity_type_retract);
                    }
                }
            }
            let entity_type_datom = Datom::assert(
                &payload.tenant_id,
                &entity_id,
                "entity_type",
                Datom::hash_attr_name("entity_type"),
                DatomValue::Str(payload.entity_type.clone()),
                tx_id,
            );
            datoms.push(entity_type_datom);
        }

        // 4.6. Opcionalmente generar outbox event
        let mut outbox_count = 0;
        if let Some(outbox_datoms) = self.maybe_generate_outbox(
            &payload.tenant_id,
            &payload.entity_type,
            &entity_id,
            payload.op.clone(),
            &payload.attrs,
            tx_id,
        )? {
            datoms.extend(outbox_datoms);
            outbox_count = 1;
        }

        // 4.7. Entidades proyectadas (sagas) — misma TX ACID, cada una con su outbox.
        // Sin esto la promesa de "madre + scheduled_job atómicos" sería falsa.
        for proj in &projections {
            for (attr_name, val) in &proj.attrs {
                let attr_desc = crate::codice::global()
                    .get_attribute(&proj.entity_type, attr_name)
                    .ok_or_else(|| {
                        DomainError::eav(
                            ErrorCode::Eav004,
                            format!(
                                "Atributo '{attr_name}' no en registry para '{}'",
                                proj.entity_type
                            ),
                        )
                    })?;
                datoms.push(Datom::assert(
                    &payload.tenant_id,
                    &proj.entity_id,
                    attr_name,
                    Datom::hash_attr_name(&attr_desc.name),
                    val.clone(),
                    tx_id,
                ));
            }

            // enrich_datoms sólo AÑADE los datoms meta de esta entidad (ulid, type,
            // tenant, timestamps); no toca los ya acumulados.
            crate::eav::writer::enricher::enrich_datoms(
                &mut datoms,
                &proj.entity_id,
                &proj.entity_type,
                &payload.tenant_id,
                tx_id,
                true,
                &std::collections::HashMap::new(),
            );

            datoms.push(Datom::assert(
                &payload.tenant_id,
                &proj.entity_id,
                "entity_type",
                Datom::hash_attr_name("entity_type"),
                DatomValue::Str(proj.entity_type.clone()),
                tx_id,
            ));

            if let Some(proj_outbox) = self.maybe_generate_outbox(
                &payload.tenant_id,
                &proj.entity_type,
                &proj.entity_id,
                TransactOp::Create,
                &proj.attrs,
                tx_id,
            )? {
                datoms.extend(proj_outbox);
                outbox_count += 1;
            }
        }

        // 5. Construir TransactWriteItems para la capa ACID
        let mut write_items = self.build_write_items(&datoms)?;
        let datom_count = datoms.len();

        // 5b. Items de reclamación de las restricciones declaradas.
        //
        // Van en la MISMA transacción: es lo que convierte la unicidad en un
        // invariante en lugar de una comprobación con ventana de carrera. Y
        // van en el chunk atómico por construcción, porque se añaden antes
        // del troceado.
        //
        // Hoy no producen nada: ningún modelo declara `constraints`. Activarlo
        // con duplicados vivos en la base haría fallar la siguiente escritura
        // de esos tenants, así que el orden correcto es reconciliar primero
        // (F3) y declarar después (F4).
        {
            let model = crate::codice::global().get_model(&payload.entity_type);
            if let Some(model) = model {
                let ctx = crate::eav::writer::constraints::ConstraintContext {
                    tenant_id: &payload.tenant_id,
                    entity_id: &entity_id,
                    model: &model,
                    attrs: &payload.attrs,
                    op: payload.op.clone(),
                    table: &self.table,
                };
                let claims = crate::eav::writer::constraints::plan_all(&self.planners, &ctx)?;
                if !claims.is_empty() {
                    tracing::debug!(
                        "[EAV] {} items de restricción para {}#{}",
                        claims.len(),
                        payload.entity_type,
                        entity_id
                    );
                    write_items.extend(claims);
                }
            }
        }

        // DynamoDB limita TransactWriteItems a 100. Por encima, transact_write() fragmenta
        // y la atomicidad se pierde EN SILENCIO: si el segundo chunk falla, el primero ya
        // está confirmado. Con proyecciones ese umbral se cruza con facilidad, así que
        // fallamos de forma explícita en vez de escribir a medias.
        if !projections.is_empty() && write_items.len() > 100 {
            return Err(DomainError::eav(
                ErrorCode::Eav001,
                format!(
                    "TX con proyecciones excede el límite atómico de DynamoDB: {} items                      ({} entidad madre + {} proyecciones). Reduzca los offsets de                      pre-notificación de '{}'.",
                    write_items.len(), payload.entity_type, projections.len(), payload.entity_type
                ),
            ));
        }

        tracing::info!(
            "[EAV] transact entity_id={entity_id} tx_id={tx_id} datoms={} ACID_items={} FTS_items={}",
            datom_count, write_items.len(), fts_write_requests.len()
        );

        // 6. Lanzar FTS de forma asíncrona concurrente (Degraded Consistency)
        let mut fts_handles = Vec::new();
        for chunk in fts_write_requests.chunks(25) {
            let chunk_vec = chunk.to_vec();
            let ddb_clone = self.ddb.clone();
            let table_clone = self.table.clone();
            fts_handles.push(tokio::spawn(async move {
                ddb_clone.batch_write_item(&table_clone, chunk_vec).await
            }));
        }

        // 7. Ejecutar capa ACID (síncrona)
        self.ddb.transact_write(write_items).await?;

        // 7.5. Invalidate EAV read cache for this entity to ensure absolute consistency
        let pk = format!("T#{}#E#{}", payload.tenant_id, entity_id);
        if let Ok(mut cache) = crate::eav::reader::pull::EAV_CACHE.write() {
            cache.remove(&pk);
        }

        // 7.6. Invalidate AEVT scan cache for this tenant and entity_type to ensure absolute consistency
        if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
            let key = (payload.tenant_id.clone(), payload.entity_type.clone());
            cache.remove(&key);
            tracing::debug!(
                "AEVT_SCAN_CACHE INVALIDATED for tenant={}, type={}",
                payload.tenant_id,
                payload.entity_type
            );
        }

        // 8. Esperar a que terminen los FTS (no bloquean el ACID pero deben terminar antes de responder)
        for handle in fts_handles {
            if let Ok(Err(e)) = handle.await {
                warn!("[EAV] Error en escritura Batch FTS asíncrona: {e:?}");
            }
        }

        Ok(TransactResult {
            entity_id,
            tx_id,
            datoms: datom_count,
            outbox_count,
        })
    }

    /// Ejecuta una transacción ACID sin invalidar caches per-row.
    /// Diseñado para bulk operations donde la cache se invalida una sola vez al final.
    /// [OPTIMIZATION: 10x Bulk — evita N RwLock::write() por fila]
    pub async fn transact_bulk_deferred(
        &self,
        payload: TransactPayload,
    ) -> Result<TransactResult, DomainError> {
        // 1. Verificar entity_type en registry
        crate::codice::global()
            .get_model(&payload.entity_type)
            .ok_or_else(|| {
                DomainError::eav(
                    ErrorCode::Eav004,
                    format!("entity_type '{}' no en registry", payload.entity_type),
                )
            })?;

        // 2. Generar TX_ID
        let tx_id = Ulid::new().timestamp_ms();

        // 3. Generar entity_id si es creación
        let entity_id = match &payload.entity_id {
            Some(id) => id.clone(),
            None => Ulid::new().to_string(),
        };

        // 4. Construir datoms y FTS requests
        let mut datoms: Vec<Datom> = Vec::new();
        let mut fts_write_requests = Vec::new();

        let active_attrs = if payload.op == TransactOp::Update || payload.op == TransactOp::Delete {
            self.get_active_attributes(&payload.tenant_id, &entity_id)
                .await?
        } else {
            std::collections::HashMap::new()
        };

        if payload.op == TransactOp::Delete {
            for (attr_name, (attr_id, val)) in active_attrs {
                let retract = Datom::retract(
                    &payload.tenant_id,
                    &entity_id,
                    attr_name,
                    attr_id,
                    val,
                    tx_id,
                );
                datoms.push(retract);
            }
        } else {
            for (attr_name, new_value) in &payload.attrs {
                let attr_desc = crate::codice::global()
                    .get_attribute(&payload.entity_type, attr_name)
                    .ok_or_else(|| {
                        DomainError::eav(
                            ErrorCode::Eav004,
                            format!(
                                "Atributo '{attr_name}' no en registry para '{}'",
                                payload.entity_type
                            ),
                        )
                    })?;

                // Para UPDATE: añadir retract del valor anterior
                if payload.op == TransactOp::Update {
                    if let Some((attr_id, old_value)) = active_attrs.get(attr_name) {
                        let retract = Datom::retract(
                            &payload.tenant_id,
                            &entity_id,
                            attr_name,
                            *attr_id,
                            old_value.clone(),
                            tx_id,
                        );
                        datoms.push(retract);
                    }
                }

                let assert = Datom::assert(
                    &payload.tenant_id,
                    &entity_id,
                    attr_name,
                    Datom::hash_attr_name(&attr_desc.name),
                    new_value.clone(),
                    tx_id,
                );

                if attr_desc.fts {
                    let reqs = crate::eav::fts::trigram::build_fts_items(&assert, attr_desc);
                    fts_write_requests.extend(reqs);
                }

                datoms.push(assert);
            }

            // 4.5. Enriquecer con system attributes
            let is_create = payload.op == TransactOp::Create;
            crate::eav::writer::enricher::enrich_datoms(
                &mut datoms,
                &entity_id,
                &payload.entity_type,
                &payload.tenant_id,
                tx_id,
                is_create,
                &active_attrs,
            );

            // Para mantener compatibilidad con consultas legacy que busquen "entity_type" sin slash:
            if !is_create {
                if let Some((_, DatomValue::Str(old_type))) = active_attrs.get("entity_type") {
                    if old_type != &payload.entity_type {
                        let entity_type_retract = Datom::retract(
                            &payload.tenant_id,
                            &entity_id,
                            "entity_type",
                            Datom::hash_attr_name("entity_type"),
                            DatomValue::Str(old_type.clone()),
                            tx_id,
                        );
                        datoms.push(entity_type_retract);
                    }
                }
            }
            let entity_type_datom = Datom::assert(
                &payload.tenant_id,
                &entity_id,
                "entity_type",
                Datom::hash_attr_name("entity_type"),
                DatomValue::Str(payload.entity_type.clone()),
                tx_id,
            );
            datoms.push(entity_type_datom);
        }

        // 4.6. Opcionalmente generar outbox event
        let mut outbox_count = 0;
        if let Some(outbox_datoms) = self.maybe_generate_outbox(
            &payload.tenant_id,
            &payload.entity_type,
            &entity_id,
            payload.op,
            &payload.attrs,
            tx_id,
        )? {
            datoms.extend(outbox_datoms);
            outbox_count = 1;
        }

        // 5. Construir TransactWriteItems
        let write_items = self.build_write_items(&datoms)?;
        let datom_count = datoms.len();

        // 6. Lanzar FTS de forma asíncrona concurrente
        let mut fts_handles = Vec::new();
        for chunk in fts_write_requests.chunks(25) {
            let chunk_vec = chunk.to_vec();
            let ddb_clone = self.ddb.clone();
            let table_clone = self.table.clone();
            fts_handles.push(tokio::spawn(async move {
                ddb_clone.batch_write_item(&table_clone, chunk_vec).await
            }));
        }

        // 7. Ejecutar capa ACID (síncrona)
        self.ddb.transact_write(write_items).await?;

        // DEFERRED: NO invalidamos EAV_CACHE ni AEVT_SCAN_CACHE aquí.
        // El caller (route_bulk) invalida una sola vez al final del batch completo.

        // 8. Esperar FTS
        for handle in fts_handles {
            if let Ok(Err(e)) = handle.await {
                warn!("[EAV] Error en escritura Batch FTS asíncrona: {e:?}");
            }
        }

        Ok(TransactResult {
            entity_id,
            tx_id,
            datoms: datom_count,
            outbox_count,
        })
    }

    /// Construye todos los TransactWriteItems para los 4 índices EAV.
    /// EAVT = tabla principal, AEVT+AVET+VAET = GSIs separados.
    ///
    /// Blueprint: §III — "Single Table Design con 4 GSIs canónicos"
    fn build_write_items(&self, datoms: &[Datom]) -> Result<Vec<TransactWriteItem>, DomainError> {
        let mut items = Vec::with_capacity(datoms.len() * 4);

        for datom in datoms {
            // ── EAVT (tabla principal) ────────────────────────────────────────
            // PK = "T#<tenant>#E#<eid>"
            // SK = [attr_id:2B][tx_id:8B][op:1B] — binario
            let eavt_pk = datom.eavt_pk();
            let eavt_sk = build_eavt_sk(datom.attr_id, datom.tx_id, datom.op);
            let mut eavt_item = self.base_datom_attrs(datom);
            eavt_item.insert("PK".to_string(), AttributeValue::S(eavt_pk));
            eavt_item.insert("SK".to_string(), av_binary(eavt_sk));
            // GSI-AEVT projection keys
            eavt_item.insert("ap".to_string(), AttributeValue::S(datom.aevt_pk()));
            eavt_item.insert(
                "as".to_string(),
                av_binary(build_aevt_sk(&datom.entity_id, datom.tx_id)),
            );
            // GSI-AVET projection keys (solo si el tipo es indexable)
            if datom.value.value_type().is_avet_indexable() {
                eavt_item.insert("vp".to_string(), AttributeValue::S(datom.avet_pk()));
                eavt_item.insert(
                    "vs".to_string(),
                    av_binary(build_avet_sk(&datom.value, &datom.entity_id)),
                );
            }
            // GSI-VAET (solo para Reference)
            if let Some(vaet_pk) = datom.vaet_pk() {
                eavt_item.insert("rp".to_string(), AttributeValue::S(vaet_pk));
                eavt_item.insert(
                    "rs".to_string(),
                    av_binary(build_vaet_sk(datom.attr_id, &datom.entity_id)),
                );
            }

            items.push(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&self.table)
                            .set_item(Some(eavt_item))
                            .build()
                            .map_err(|e| {
                                DomainError::eav(
                                    ErrorCode::Eav001,
                                    format!("Put builder error: {e}"),
                                )
                            })?,
                    )
                    .build(),
            );
        }

        Ok(items)
    }

    fn base_datom_attrs(&self, datom: &Datom) -> HashMap<String, AttributeValue> {
        let mut map = HashMap::new();
        // Zero-redundancy: tid, eid, a, tx, y op están empaquetados en PK y SK.

        // Valor según el tipo del DatomValue
        // [BLUEPRINT: §II.3 — "v = tipo nativo DynamoDB según el tipo del attr"]
        match &datom.value {
            DatomValue::Str(s) | DatomValue::Uuid(s) => {
                map.insert("v".to_string(), AttributeValue::S(s.clone()));
            }
            DatomValue::Long(n) | DatomValue::Instant(n) => {
                map.insert("v".to_string(), AttributeValue::N(n.to_string()));
            }
            DatomValue::Double(d) => {
                map.insert("v".to_string(), AttributeValue::N(d.to_string()));
            }
            DatomValue::Bool(b) => {
                map.insert("v".to_string(), AttributeValue::Bool(*b));
            }
            DatomValue::Ref(eid) => {
                map.insert("v".to_string(), AttributeValue::N(eid.to_string()));
            }
            DatomValue::Array(arr) => {
                map.insert(
                    "v".to_string(),
                    AttributeValue::S(serde_json::to_string(arr).unwrap_or_default()),
                );
            }
            DatomValue::Bytes(b) => {
                map.insert("v".to_string(), av_binary(b.clone()));
            }
            DatomValue::BigInt(n) => {
                map.insert("v".to_string(), AttributeValue::N(n.to_string()));
            }
            DatomValue::Geo { lat, lon } => {
                map.insert("v".to_string(), AttributeValue::S(format!("{lat},{lon}")));
            }
            DatomValue::Null => {
                map.insert("v".to_string(), AttributeValue::Null(true));
            }
        }

        map
    }

    async fn get_active_attributes(
        &self,
        tenant_id: &str,
        entity_id: &str,
    ) -> Result<HashMap<String, (u16, DatomValue)>, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);
        let key_condition = "#pk = :pk".to_string();
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

        let raw_items = self
            .ddb
            .query(
                &self.table,
                None,
                &key_condition,
                attr_names,
                attr_values,
                true,
                None,
            )
            .await
            .map_err(|e| {
                DomainError::eav(ErrorCode::Eav002, format!("delete pull failed: {e:?}"))
            })?;

        let mut active: HashMap<String, (u16, u64, DatomValue)> = HashMap::new();
        for item in raw_items {
            let sk = match item.get("SK") {
                Some(AttributeValue::B(blob)) => blob.as_ref(),
                _ => continue,
            };
            if sk.len() != 11 {
                continue;
            }
            let attr_id = u16::from_be_bytes(sk[0..2].try_into().unwrap());
            let tx_id = u64::from_be_bytes(sk[2..10].try_into().unwrap());
            let op = sk[10] != 0;

            let attr_name = match attr_id {
                0 => "entity/ulid".to_string(),
                1 => "entity/type".to_string(),
                2 => "tenant/id".to_string(),
                3 => "meta/created_at".to_string(),
                4 => "meta/updated_at".to_string(),
                h if h == Datom::hash_attr_name("entity_type") => "entity_type".to_string(),
                _ => match crate::codice::global().get_attr_name(attr_id) {
                    Some(n) => n.to_string(),
                    None => format!("attr_{}", attr_id),
                },
            };

            let should_update = match active.get(&attr_name) {
                None => true,
                Some((_, prev_tx, _)) => tx_id > *prev_tx,
            };

            if should_update {
                if op {
                    let val = match item.get("v") {
                        Some(AttributeValue::S(s)) => DatomValue::Str(s.clone()),
                        Some(AttributeValue::N(n)) => {
                            if let Ok(i) = n.parse::<i64>() {
                                DatomValue::Long(i)
                            } else {
                                n.parse::<f64>()
                                    .ok()
                                    .map(DatomValue::Double)
                                    .unwrap_or(DatomValue::Null)
                            }
                        }
                        Some(AttributeValue::Bool(b)) => DatomValue::Bool(*b),
                        Some(AttributeValue::B(_)) => DatomValue::Bytes(vec![]),
                        Some(AttributeValue::Null(_)) => DatomValue::Null,
                        _ => DatomValue::Null,
                    };
                    active.insert(attr_name, (attr_id, tx_id, val));
                } else {
                    active.remove(&attr_name);
                }
            }
        }

        let result = active
            .into_iter()
            .map(|(k, (attr_id, _, val))| (k, (attr_id, val)))
            .collect();
        Ok(result)
    }
}

/// Helper: AttributeValue::B desde Vec<u8>
fn av_binary(bytes: Vec<u8>) -> AttributeValue {
    AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(bytes))
}

pub(crate) fn datom_map_to_json(
    map: &std::collections::HashMap<String, crate::eav::types::datom::DatomValue>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in map {
        use crate::eav::types::datom::DatomValue;
        let json_val = match v {
            DatomValue::Null => serde_json::Value::Null,
            DatomValue::Bool(b) => serde_json::Value::Bool(*b),
            DatomValue::Long(n) | DatomValue::Instant(n) => {
                serde_json::Value::Number(serde_json::Number::from(*n))
            }
            DatomValue::Double(d) => serde_json::Number::from_f64(*d)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            DatomValue::Str(s) | DatomValue::Uuid(s) => {
                if (s.starts_with('{') && s.ends_with('}'))
                    || (s.starts_with('[') && s.ends_with(']'))
                {
                    serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.clone()))
                } else {
                    serde_json::Value::String(s.clone())
                }
            }
            DatomValue::Ref(r) => serde_json::Value::String(r.to_string()),
            DatomValue::Array(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|s| {
                        if (s.starts_with('{') && s.ends_with('}'))
                            || (s.starts_with('[') && s.ends_with(']'))
                        {
                            serde_json::from_str::<serde_json::Value>(s)
                                .unwrap_or_else(|_| serde_json::Value::String(s.clone()))
                        } else {
                            serde_json::Value::String(s.clone())
                        }
                    })
                    .collect(),
            ),
            DatomValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            )),
            DatomValue::BigInt(n) => serde_json::Value::String(n.to_string()),
            DatomValue::Geo { lat, lon } => {
                let mut geo_obj = serde_json::Map::new();
                if let Some(la) = serde_json::Number::from_f64(*lat) {
                    geo_obj.insert("lat".to_string(), serde_json::Value::Number(la));
                }
                if let Some(lo) = serde_json::Number::from_f64(*lon) {
                    geo_obj.insert("lon".to_string(), serde_json::Value::Number(lo));
                }
                serde_json::Value::Object(geo_obj)
            }
        };
        obj.insert(k.clone(), json_val);
    }
    serde_json::Value::Object(obj)
}

impl EavWriter {
    fn maybe_generate_outbox(
        &self,
        tenant_id: &str,
        entity_type: &str,
        entity_id: &str,
        op: TransactOp,
        attrs: &HashMap<String, DatomValue>,
        tx_id: u64,
    ) -> Result<Option<Vec<Datom>>, DomainError> {
        let disable_eda = crate::codice::global()
            .get_model(entity_type)
            .map(|m| m.disable_eda)
            .unwrap_or(false);

        if entity_type == "outbox_event" || disable_eda {
            return Ok(None);
        }

        let outbox_ulid = Ulid::new().to_string();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let op_str = match op {
            TransactOp::Create => "create",
            TransactOp::Update => "update",
            TransactOp::Delete => "delete",
        };

        let detail_type = format!("{}.{}", entity_type, op_str);

        let mut flat_attrs = HashMap::new();
        for (k, v) in attrs {
            flat_attrs.insert(k.clone(), v.clone());
        }
        flat_attrs.insert("id".to_string(), DatomValue::Str(entity_id.to_string()));

        let payload_json = datom_map_to_json(&flat_attrs);
        let payload_str = serde_json::to_string(&payload_json).unwrap_or_default();

        let mut outbox_attrs = HashMap::new();
        outbox_attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
        outbox_attrs.insert("detail_type".to_string(), DatomValue::Str(detail_type));
        outbox_attrs.insert("payload".to_string(), DatomValue::Str(payload_str));
        outbox_attrs.insert("retry_count".to_string(), DatomValue::Long(0));
        outbox_attrs.insert("created_at".to_string(), DatomValue::Instant(now_ms));

        let mut outbox_datoms = Vec::new();
        for (attr_name, val) in outbox_attrs {
            let attr_desc = crate::codice::global()
                .get_attribute("outbox_event", &attr_name)
                .ok_or_else(|| {
                    DomainError::eav(
                        ErrorCode::Eav004,
                        format!("Atributo '{attr_name}' no en registry para 'outbox_event'"),
                    )
                })?;
            let assert = Datom::assert(
                tenant_id,
                &outbox_ulid,
                &attr_name,
                Datom::hash_attr_name(&attr_desc.name),
                val,
                tx_id,
            );
            outbox_datoms.push(assert);
        }

        crate::eav::writer::enricher::enrich_datoms(
            &mut outbox_datoms,
            &outbox_ulid,
            "outbox_event",
            tenant_id,
            tx_id,
            true, // is_create
            &HashMap::new(),
        );

        let outbox_type_datom = Datom::assert(
            tenant_id,
            &outbox_ulid,
            "entity_type",
            Datom::hash_attr_name("entity_type"),
            DatomValue::Str("outbox_event".to_string()),
            tx_id,
        );
        outbox_datoms.push(outbox_type_datom);

        Ok(Some(outbox_datoms))
    }
}

#[cfg(test)]
#[path = "tests/writer_tests.rs"]
mod tests;
