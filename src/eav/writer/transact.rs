//! ACID write path of the EAV engine.
//!
//! # Origin
//! [NUEVO — reemplaza: src/metri/infrastructure/datahike.clj + tenant_guard.clj]
//! Write path ACID del motor EAV.
//! Blueprint: Metri EAV - OLPT.md §IV — ACID Write Path
//!
//! En el stack anterior: d/transact → DynamoDB (blob monolítico via Datahike)
//! En Rust:    TransactWriteItems con datoms EAVT/AEVT/AVET/VAET individuales
//!
//! Este módulo es el reemplazo directo de la corrupción de blobs de Datahike.
//! Cada atributo es un datom independiente — sin contención bajo concurrencia.

use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use ulid::Ulid;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::{Datom, DatomValue},
    encoding::{build_aevt_sk, build_avet_sk, build_eavt_sk, build_vaet_sk},
};
use crate::eav::writer::cache_policy::CachePolicy;
use crate::infrastructure::dynamodb::DynamoClient;

/// Opciones internas de una transacción. Las variantes públicas (`transact`,
/// `transact_with_projections`, `transact_bulk_deferred`, `transact_with_tx`)
/// son composiciones de estas opciones sobre UN único camino de ejecución —
/// antes dos métodos duplicaban ~200 líneas que ya empezaban a diverger.
#[derive(Default)]
struct TxOptions<'a> {
    /// Entidades proyectadas (sagas) escritas en la misma TX ACID.
    projections: Vec<crate::janus_router::saga::SagaProjection>,
    /// Actor de sesión para la atribución del audit trail.
    actor: Option<&'a str>,
    /// Costura de prueba: fuerza el tx_id para verificar la colisión EAV_TX_004.
    forced_tx: Option<u64>,
    /// Política de invalidación de caches tras el commit.
    cache: CachePolicy,
    /// Si se planifican claims de restricción en esta TX.
    constraints: ConstraintPolicy,
}

/// ¿Planifica esta transacción los claims de las restricciones declaradas?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ConstraintPolicy {
    /// Sí — el camino normal: la unicidad se vuelve invariante en la TX.
    #[default]
    Plan,
    /// No — la ruta bulk salta las verificaciones por fila (comportamiento
    /// histórico preservado; hoy ningún modelo declara constraints).
    Skip,
}

/// Contexto de una transacción EAV.
/// Equivale al `TransactPayload` implícito en el pipeline IOP de el stack anterior.
#[derive(Debug, Clone)]
pub struct TransactPayload {
    pub tenant_id: String,
    pub entity_id: Option<String>, // None = nuevo (se genera ULID)
    pub entity_type: String,
    pub attrs: HashMap<String, DatomValue>, // nombre_attr → valor nuevo
    pub op: TransactOp,
    /// true → la mutación no genera outbox ni eventos de dominio.
    ///
    /// Es la PRIMERA barrera contra el ciclo de realimentación del Hub
    /// (metri-schedulers §10.4): las escrituras de bitácora del ejecutor
    /// (last_run_at, run_count, last_error) la usan para no reprovisionar
    /// trampas. La segunda barrera — independiente del escritor — es el guard
    /// de delta en el sobre (`domain::events::is_bookkeeping_only`).
    #[doc(hidden)]
    pub suppress_events: bool,
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
    /// ULID monotónico de ESTA mutación. Es la llave de orden del ledger del
    /// Hub (metri-schedulers §10.3) y coincide con el id de la fila outbox
    /// generada — la fila outbox ES el registro durable de la mutación.
    pub mutation_ulid: String,
    /// Before/after de un UPDATE (sólo atributos que cambiaron), en el formato
    /// del contrato metri-contracts. None en CREATE y DELETE.
    pub delta: Option<crate::domain::events::Delta>,
    /// Atributos activos previos de un DELETE — el sobre `deleted` del Hub
    /// necesita trigger_type/action_payload para eliminar la trampa correcta,
    /// y después del retract ya no existen en el almacén.
    pub deleted_attrs: Option<HashMap<String, DatomValue>>,
}

/// Deduplica datoms por clave de almacenamiento (entity, attr_id, tx, op),
/// conservando la ÚLTIMA ocurrencia — el intento final de la transacción.
///
/// Hoy el enriquecimiento meta puede re-emitir datoms que el payload ya trajo;
/// sin dedup, `TransactWriteItems` rechazaría la transacción entera por claves
/// duplicadas (AWS real lo hace; DynamoDB Local las colapsa en silencio, que es
/// peor: el despliegue descubre el bug en producción).
fn dedup_datoms_por_sk(datoms: Vec<Datom>) -> Vec<Datom> {
    let mut index: HashMap<(String, u16, u64, bool), usize> = HashMap::new();
    let mut unique: Vec<Datom> = Vec::with_capacity(datoms.len());
    for datom in datoms {
        let key = (
            datom.entity_id.clone(),
            datom.attr_id,
            datom.tx_id,
            datom.op,
        );
        match index.get(&key) {
            Some(&pos) => unique[pos] = datom,
            None => {
                index.insert(key, unique.len());
                unique.push(datom);
            }
        }
    }
    unique
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

    /// Fábrica del lector sobre la misma tabla — necesario para resolver
    /// mappings que atraviesan referencias (ver janus_router::saga).
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

    /// Ejecuta una transacción ACID.
    ///
    /// Proceso: valida el entity_type contra el Códice, genera `tx_id` (reloj
    /// monotónico) y `entity_id` si es creación, planifica los datoms de forma
    /// pura (`datom_plan::plan_payload_datoms`), añade outbox y proyecciones
    /// a la MISMA `TransactWriteItems`, ejecuta la capa ACID y — ya confirmada
    /// — despacha el FTS e invalida caches según la política de la variante.
    pub async fn transact(&self, payload: TransactPayload) -> Result<TransactResult, DomainError> {
        self.transact_with_projections(payload, Vec::new(), None)
            .await
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
        actor: Option<&str>,
    ) -> Result<TransactResult, DomainError> {
        self.transact_inner(
            payload,
            TxOptions {
                projections,
                actor,
                ..TxOptions::default()
            },
        )
        .await
    }

    /// Costura de prueba: fuerza el `tx_id` para verificar que la condición
    /// append-only rechaza la colisión con `EAV_TX_004` en lugar de
    /// sobrescribir. Solo visible dentro del crate (tests del writer).
    pub(crate) async fn transact_with_tx(
        &self,
        payload: TransactPayload,
        projections: Vec<crate::janus_router::saga::SagaProjection>,
        actor: Option<&str>,
        forced_tx: u64,
    ) -> Result<TransactResult, DomainError> {
        self.transact_inner(
            payload,
            TxOptions {
                projections,
                actor,
                forced_tx: Some(forced_tx),
                ..TxOptions::default()
            },
        )
        .await
    }

    /// Ejecuta una transacción ACID sin invalidar caches per-row.
    /// Diseñado para bulk operations donde la cache se invalida una sola vez al final.
    /// [OPTIMIZATION: 10x Bulk — evita N RwLock::write() por fila]
    pub async fn transact_bulk_deferred(
        &self,
        payload: TransactPayload,
        actor: Option<&str>,
    ) -> Result<TransactResult, DomainError> {
        self.transact_inner(
            payload,
            TxOptions {
                actor,
                cache: CachePolicy::Deferred,
                // Comportamiento histórico de la ruta bulk: sin claims de
                // restricción por fila (el canal bulk salta las verificaciones
                // de unicidad por registro; hoy ningún modelo declara constraints).
                constraints: ConstraintPolicy::Skip,
                ..TxOptions::default()
            },
        )
        .await
    }

    /// Evalúa las constraints `ref_state` del modelo contra los atributos
    /// escritos: la entidad apuntada por cada atributo con `ref_state` debe
    /// cumplir las condiciones declaradas. Solo dispara cuando el atributo
    /// referenciado se está escribiendo (create o re-punteo explícito).
    async fn check_refs_de(
        &self,
        tenant_id: &str,
        model: &crate::codice::registry::EntityModel,
        attrs: &HashMap<String, DatomValue>,
    ) -> Result<(), DomainError> {
        use crate::codice::registry::ConstraintKind;
        for c in model.constraints.iter().filter(|c| c.kind == ConstraintKind::RefState) {
            let Some(attr) = c.attributes.first() else { continue };
            let Some(DatomValue::Str(ref_id)) = attrs.get(attr) else {
                continue;
            };
            let Some(target) = model
                .attributes
                .iter()
                .find(|a| a.name == *attr)
                .and_then(|a| a.entity_ref.clone())
            else {
                continue;
            };
            let Some(condiciones) = c.when.as_deref() else { continue };
            let ref_attrs = self.get_active_attributes(tenant_id, ref_id).await?;
            let ref_attrs: HashMap<String, DatomValue> = ref_attrs
                .into_iter()
                .map(|(k, (_, v))| (k, v))
                .collect();
            crate::eav::writer::constraints::check_ref_conditions(
                &target, ref_id, &ref_attrs, condiciones,
            )?;
        }
        Ok(())
    }

    async fn transact_inner(
        &self,
        payload: TransactPayload,
        opts: TxOptions<'_>,
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

        // 2. Generar TX_ID — reloj monotónico (o el forzado por el test)
        let tx_id = opts
            .forced_tx
            .unwrap_or_else(crate::eav::writer::generate_tx_id);

        // 3. Generar entity_id si es creación
        let entity_id = match &payload.entity_id {
            Some(id) => id.clone(),
            None => Ulid::new().to_string(),
        };

        // 4. Atributos activos (I/O de lectura) y plan puro de datoms: guards,
        // retract+assert, sistema, delta del contrato y deleted_attrs.
        let active_attrs = if payload.op == TransactOp::Update || payload.op == TransactOp::Delete {
            self.get_active_attributes(&payload.tenant_id, &entity_id)
                .await?
        } else {
            std::collections::HashMap::new()
        };

        // 4.2. Restricciones declaradas de estado y de referencia para TODAS
        // las entidades de la transacción (madre + proyecciones): mismas
        // reglas para todas — ninguna privilegiada.
        //
        // Con la vista fusionada (previo + payload) ya en mano —que el update
        // leyó de todos modos— se evalúan `requires_when` y `at_most` sin
        // lecturas adicionales. `ref_state` sí lee la entidad apuntada: es la
        // comprobación tolerada documentada en `check_ref_conditions` (fallar
        // aquí es más barato y más claro que descubrirlo en la UI).
        let mut entidades_tx = Vec::new();
        if opts.constraints == ConstraintPolicy::Plan {
            if let Some(model) = crate::codice::global().get_model(&payload.entity_type) {
                entidades_tx.push(crate::eav::writer::constraints::EntidadEnTx {
                    entity_type: payload.entity_type.as_str(),
                    entity_id: entity_id.as_str(),
                    attrs: &payload.attrs,
                    op: payload.op.clone(),
                    model,
                });
                // Vista fusionada: get_active_attributes trae (attr_id, valor).
                let previo: std::collections::HashMap<String,
                    crate::eav::types::datom::DatomValue> = active_attrs
                    .iter()
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                    .collect();
                crate::eav::writer::constraints::check_state_constraints(
                    model,
                    payload.op.clone(),
                    &payload.attrs,
                    &previo,
                )?;
                self.check_refs_de(&payload.tenant_id, model, &payload.attrs).await?;
            }
            for proj in &opts.projections {
                if let Some(model) = crate::codice::global().get_model(&proj.entity_type) {
                    entidades_tx.push(crate::eav::writer::constraints::EntidadEnTx {
                        entity_type: proj.entity_type.as_str(),
                        entity_id: proj.entity_id.as_str(),
                        attrs: &proj.attrs,
                        op: TransactOp::Create,
                        model,
                    });
                    crate::eav::writer::constraints::check_state_constraints(
                        model,
                        TransactOp::Create,
                        &proj.attrs,
                        &std::collections::HashMap::new(),
                    )?;
                    self.check_refs_de(&payload.tenant_id, model, &proj.attrs).await?;
                }
            }
        }

        let plan = crate::eav::writer::datom_plan::plan_payload_datoms(
            &payload,
            &entity_id,
            tx_id,
            &active_attrs,
        )?;
        let delta = plan.delta;
        let deleted_attrs = plan.deleted_attrs;
        let mut datoms: Vec<Datom> = plan.datoms;
        let fts_write_requests = plan.fts_requests;

        // 4.6. Opcionalmente generar outbox event.
        //
        // El ULID de la mutación se genera UNA vez aquí: la fila outbox que lo
        // porta ES el registro durable de la mutación, y su id es la llave de
        // orden que el ledger de metri-schedulers usa (§10.3). Con
        // `suppress_events` no hay fila — y el canal tampoco emite.
        let mutation_ulid = Ulid::new().to_string();
        let mut outbox_count = 0;
        if !payload.suppress_events {
            if let Some(outbox_datoms) = crate::eav::writer::outbox::generate_outbox_datoms(
                &payload.tenant_id,
                &payload.entity_type,
                &entity_id,
                payload.op.clone(),
                &payload.attrs,
                tx_id,
                &mutation_ulid,
            )? {
                datoms.extend(outbox_datoms);
                outbox_count = 1;
            }
        }

        // 4.7. Entidades proyectadas (sagas) — misma TX ACID, cada una con su
        // outbox. Sin esto la promesa de "madre + scheduled_job atómicos" sería
        // falsa. Siempre nacen como Create sin atributos previos, y el outbox
        // obedece el MISMO `suppress_events` que la madre: una escritura de
        // bitácora con proyecciones no puede reprovisionar trampas por la
        // puerta trasera.
        for proj in &opts.projections {
            let entity_plan = crate::eav::writer::datom_plan::plan_entity_datoms(
                &payload.tenant_id,
                &proj.entity_type,
                &proj.entity_id,
                &proj.attrs,
                tx_id,
                true,
                &std::collections::HashMap::new(),
            )?;
            datoms.extend(entity_plan.datoms);
            // Los FTS de las proyecciones hoy no se despachan — EntityPlan los
            // trae por completitud; despacharlos es decisión del orquestador.

            if !payload.suppress_events {
                if let Some(proj_outbox) = crate::eav::writer::outbox::generate_outbox_datoms(
                    &payload.tenant_id,
                    &proj.entity_type,
                    &proj.entity_id,
                    TransactOp::Create,
                    &proj.attrs,
                    tx_id,
                    &Ulid::new().to_string(),
                )? {
                    datoms.extend(proj_outbox);
                    outbox_count += 1;
                }
            }
        }

        // 5. Construir TransactWriteItems para la capa ACID
        let datoms = dedup_datoms_por_sk(datoms);
        let mut write_items = self.build_write_items(&datoms)?;
        // Registro de la transacción: atribución de actor para el audit trail.
        write_items.push(self.tx_registry_item(
            &payload.tenant_id,
            tx_id,
            &payload,
            &entity_id,
            opts.actor,
        )?);
        let datom_count = datoms.len();

        // 5b. Items de reclamación de las restricciones declaradas.
        //
        // Van en la MISMA transacción: es lo que convierte la unicidad en un
        // invariante en lugar de una comprobación con ventana de carrera. Y
        // van en el chunk atómico por construcción, porque se añaden antes
        // del troceado. Madre y proyecciones reclaman por igual; un conflicto
        // intra-composite (dos entidades con la misma clave) se detecta en
        // `plan_claims_para` como error de dominio, no como
        // ValidationException a mitad del commit.
        if opts.constraints == ConstraintPolicy::Plan && !entidades_tx.is_empty() {
            let claims = crate::eav::writer::constraints::plan_claims_para(
                &self.planners,
                &payload.tenant_id,
                &self.table,
                &entidades_tx,
            )?;
            if !claims.is_empty() {
                tracing::debug!(
                    "[EAV] {} items de restricción para {}#{} (+{} proyecciones)",
                    claims.len(),
                    payload.entity_type,
                    entity_id,
                    opts.projections.len()
                );
                write_items.extend(claims);
            }
        }

        // DynamoDB limita TransactWriteItems a 100. Por encima, transact_write()
        // fragmenta y la atomicidad se pierde EN SILENCIO: si el segundo chunk
        // falla, el primero ya está confirmado. Con proyecciones ese umbral se
        // cruza con facilidad, así que fallamos de forma explícita en vez de
        // escribir a medias.
        if !opts.projections.is_empty() && write_items.len() > 100 {
            return Err(DomainError::eav(
                ErrorCode::Eav001,
                format!(
                    "TX compuesta excede el límite atómico de DynamoDB: {} items ({} raíz + {} proyecciones). Divida el composite o reduzca atributos.",
                    write_items.len(),
                    payload.entity_type,
                    opts.projections.len()
                ),
            ));
        }

        tracing::info!(
            "[EAV] transact entity_id={entity_id} tx_id={tx_id} datoms={} ACID_items={} FTS_items={}",
            datom_count, write_items.len(), fts_write_requests.len()
        );

        // 6. Ejecutar capa ACID (síncrona)
        self.ddb.transact_write(write_items).await?;

        // 7. Invalidar caches de lectura de TODAS las entidades escritas — la
        // madre y cada proyección; antes sólo se invalidaba la madre y una
        // entidad proyectada quedaba invisible tras un scan cacheado de su tipo.
        if opts.cache == CachePolicy::Immediate {
            let mut escritas = vec![crate::eav::writer::cache_policy::WrittenEntity {
                entity_id: &entity_id,
                entity_type: &payload.entity_type,
            }];
            for proj in &opts.projections {
                escritas.push(crate::eav::writer::cache_policy::WrittenEntity {
                    entity_id: &proj.entity_id,
                    entity_type: &proj.entity_type,
                });
            }
            crate::eav::writer::cache_policy::invalidate_entity_caches(
                &payload.tenant_id,
                &escritas,
            );
        }
        // DEFERRED: el caller (route_bulk) invalida una sola vez al final del
        // lote — ver cache_policy::invalidate_aevt_scan.

        // 8. FTS — DESPUÉS del commit: si la transacción falló, ya no quedan
        // trigramas de datoms que nunca se confirmaron contaminando el índice.
        // Se espera antes de responder; los errores no fallan la mutación ya
        // confirmada (el índice es reconstruible, la escritura no).
        crate::eav::writer::fts_dispatch::FtsDispatch::spawn(
            self.ddb.clone(),
            self.table.clone(),
            fts_write_requests,
        )
        .await_all()
        .await;

        Ok(TransactResult {
            entity_id,
            tx_id,
            datoms: datom_count,
            outbox_count,
            mutation_ulid,
            delta,
            deleted_attrs,
        })
    }

    /// Item de registro de la transacción (partición `T#<tenant>#TX`).
    ///
    /// La transacción pasa a ser una entidad de primera clase: guarda quién
    /// (actor de sesión), qué (entidad y operación) y cuándo (el propio tx_id
    /// es epoch ms). La timeline de auditoría consulta esta partición para
    /// atribuir cada datom — sin ella, `user_id` es null en el histórico.
    fn tx_registry_item(
        &self,
        tenant_id: &str,
        tx_id: u64,
        payload: &TransactPayload,
        entity_id: &str,
        actor: Option<&str>,
    ) -> Result<TransactWriteItem, DomainError> {
        let op_str = match &payload.op {
            TransactOp::Create => "CREATE",
            TransactOp::Update => "UPDATE",
            TransactOp::Delete => "DELETE",
        };
        let actor = actor.unwrap_or("system").to_string();
        let mut item = HashMap::new();
        item.insert(
            "PK".to_string(),
            AttributeValue::S(format!("T#{tenant_id}#TX")),
        );
        item.insert("SK".to_string(), av_binary(tx_id.to_be_bytes().to_vec()));
        item.insert("actor".to_string(), AttributeValue::S(actor));
        item.insert(
            "entity_type".to_string(),
            AttributeValue::S(payload.entity_type.clone()),
        );
        item.insert(
            "entity_id".to_string(),
            AttributeValue::S(entity_id.to_string()),
        );
        item.insert("op".to_string(), AttributeValue::S(op_str.to_string()));
        let put = Put::builder()
            .table_name(&self.table)
            .set_item(Some(item))
            // Un tx_id repetido no es idempotencia: es una colisión.
            .condition_expression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
            .build()
            .map_err(|e| DomainError::eav(ErrorCode::Eav001, format!("Put builder error: {e}")))?;
        Ok(TransactWriteItem::builder().put(put).build())
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
                            // Invariante append-only: el SK ya codifica
                            // (attr_id, tx_id, op); si la clave existe, algo
                            // intenta reescribir historia (colisión de tx,
                            // reintento, reloj atrás). La condición lo vuelve
                            // un fallo de transacción (EAV_TX_004), nunca una
                            // sobrescritura silenciosa.
                            .condition_expression(
                                "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                            )
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
                DomainError::eav(
                    ErrorCode::Eav002,
                    format!("lectura de atributos activos falló para '{entity_id}': {e:?}"),
                )
            })?;

        let mut active: HashMap<String, (u16, u64, DatomValue)> = HashMap::new();
        for item in raw_items {
            let sk = match item.get("SK") {
                Some(AttributeValue::B(blob)) => blob.as_ref(),
                _ => continue,
            };
            let Some((attr_id, tx_id, op)) = crate::eav::types::encoding::decode_eavt_sk(sk) else {
                continue;
            };

            let attr_name = match crate::eav::writer::system_attrs::name_of(attr_id) {
                Some(n) => n.to_string(),
                None if attr_id == Datom::hash_attr_name("entity_type") => {
                    "entity_type".to_string()
                }
                None => match crate::codice::global().get_attr_name(attr_id) {
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

#[cfg(test)]
#[path = "tests/writer_tests.rs"]
mod tests;
