// quota/dynamo_store.rs — Las reservas en vuelo, en DynamoDB.
//
// POR QUÉ DYNAMODB Y NO REDIS
// ───────────────────────────
// El contador ya vive en DynamoDB, y el apunte de cierre tiene que caer con su
// marca de idempotencia en la misma transacción (`QuotaCounter::settle_once`).
// Con la reserva en otro sistema esa atomicidad no existe y habría que
// reconstruirla a mano. Añádase que el motor no tiene hoy ninguna dependencia
// de Redis —el `valkey_store` del wiring es un `HmacTokenStore` sobre DynamoDB,
// el nombre es herencia— y que la tabla de cuota ya está declarada. Redis daría
// menos latencia; el precio sería un cluster que operar y una garantía menos.
//
// EL TTL NO DEVUELVE TOKENS
// ─────────────────────────
// El atributo `ttl` sirve para que los tickets cerrados desaparezcan solos. No
// es el recolector: DynamoDB borra hasta 48 h más tarde y el borrado no puede
// decrementar el contador. Quien devuelve los tokens es el barrido, que
// consulta `GSI-SWEEP` por `expires_at`. Confundir las dos cosas es el error
// clásico de esta arquitectura, y se paga en cuota que nunca vuelve.
//
// CLAVES
// ──────
//   PK  = T#{tenant}#RSV#{ulid}      SK = "RSV"
//   GSI-SWEEP:  gp = "OPEN#{shard}"  gs = expires_at (N)
//
// `gp` y `gs` solo existen mientras la reserva está abierta: al cerrarla se
// borran, así que el índice contiene exactamente lo que queda por cerrar y no
// crece con el histórico.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, ReturnValuesOnConditionCheckFailure};
use tracing::{debug, info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::infrastructure::dynamodb::DynamoClient;
use crate::quota::reservations::{
    shard_of, ClaimResult, CloseReason, Reservation, ReservationStore,
};

/// Sort key fija: solo hay un item por reserva.
const RESERVATION_SK: &str = "RSV";

/// Índice por el que barre cualquier réplica.
const SWEEP_INDEX: &str = "GSI-SWEEP";

const STATUS_OPEN: &str = "OPEN";
const STATUS_CLOSED: &str = "CLOSED";

/// Cuánto sobrevive el item después de vencer. Solo limpieza física: para
/// entonces el barrido ya cerró la reserva hace mucho.
const RESERVATION_TTL_SECS: i64 = 24 * 60 * 60;

pub struct DynamoReservationStore {
    ddb: Arc<DynamoClient>,
    table: String,
}

impl DynamoReservationStore {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        let table = table.into();
        info!(tabla = %table, "[Reservations] Store de reservas sobre DynamoDB");
        DynamoReservationStore { ddb, table }
    }

    fn key(tenant_id: &str, id: &str) -> (AttributeValue, AttributeValue) {
        (
            AttributeValue::S(format!("T#{tenant_id}#RSV#{id}")),
            AttributeValue::S(RESERVATION_SK.to_string()),
        )
    }

    fn sweep_pk(shard: u8) -> String {
        format!("{STATUS_OPEN}#{shard}")
    }

    fn infra(msg: String) -> DomainError {
        DomainError::new(ErrorCode::Infra001, msg).with_stage("quota")
    }

    /// Reconstruye la reserva desde el item.
    ///
    /// Un item al que le falte un campo obligatorio no se adivina: se descarta
    /// con traza. Adivinar `estimated` sería inventarse cuántos tokens hay que
    /// devolver.
    fn to_reservation(item: &HashMap<String, AttributeValue>) -> Option<Reservation> {
        let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
        let n = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_n().ok())
                .and_then(|v| v.parse::<i64>().ok())
        };

        Some(Reservation {
            id: s("i")?,
            tenant_id: s("t")?,
            quota_id: s("q")?,
            estimated: n("e")?,
            debited: item
                .get("d")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            expires_at: n("xa")?,
        })
    }
}

#[async_trait::async_trait]
impl ReservationStore for DynamoReservationStore {
    async fn open(&self, r: &Reservation) -> Result<(), DomainError> {
        let (pk, sk) = Self::key(&r.tenant_id, &r.id);

        let mut item = HashMap::new();
        item.insert("PK".to_string(), pk);
        item.insert("SK".to_string(), sk);
        item.insert("i".to_string(), AttributeValue::S(r.id.clone()));
        item.insert("t".to_string(), AttributeValue::S(r.tenant_id.clone()));
        item.insert("q".to_string(), AttributeValue::S(r.quota_id.clone()));
        item.insert("e".to_string(), AttributeValue::N(r.estimated.to_string()));
        item.insert("d".to_string(), AttributeValue::Bool(r.debited));
        item.insert("s".to_string(), AttributeValue::S(STATUS_OPEN.to_string()));
        item.insert(
            "xa".to_string(),
            AttributeValue::N(r.expires_at.to_string()),
        );
        item.insert(
            "ttl".to_string(),
            AttributeValue::N((r.expires_at + RESERVATION_TTL_SECS).to_string()),
        );
        // Índice de barrido. Existe solo mientras la reserva está abierta.
        item.insert(
            "gp".to_string(),
            AttributeValue::S(Self::sweep_pk(shard_of(&r.id))),
        );
        item.insert(
            "gs".to_string(),
            AttributeValue::N(r.expires_at.to_string()),
        );

        self.ddb
            .client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            // Un ULID repetido sería un fallo del generador, no una carrera;
            // pero sobrescribir una reserva viva perdería su débito.
            .condition_expression("attribute_not_exists(PK)")
            .send()
            .await
            .map_err(|e| {
                let err = e.into_service_error();
                if matches!(err, PutItemError::ConditionalCheckFailedException(_)) {
                    return Self::infra(format!("La reserva {} ya existía", r.id));
                }
                Self::infra(format!("No se pudo abrir la reserva {}: {err}", r.id))
            })?;

        debug!(reserva = %r.id, tenant = %r.tenant_id, estimado = r.estimated, "[Reservations] Abierta");
        Ok(())
    }

    async fn mark_debited(&self, tenant_id: &str, id: &str) -> Result<(), DomainError> {
        let (pk, sk) = Self::key(tenant_id, id);

        self.ddb
            .client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            .condition_expression("attribute_exists(PK)")
            .update_expression("SET #d = :si")
            .expression_attribute_names("#d", "d")
            .expression_attribute_values(":si", AttributeValue::Bool(true))
            .send()
            .await
            .map_err(|e| {
                Self::infra(format!(
                    "No se pudo marcar el débito de la reserva {id}: {}",
                    e.into_service_error()
                ))
            })?;

        Ok(())
    }

    async fn claim(
        &self,
        tenant_id: &str,
        id: &str,
        lease: Duration,
    ) -> Result<ClaimResult, DomainError> {
        let (pk, sk) = Self::key(tenant_id, id);
        let now = chrono::Utc::now().timestamp();
        let until = now + lease.as_secs() as i64;

        let result = self
            .ddb
            .client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            // Abierta y sin dueño vivo. Que el lease venza es, por sí solo, que
            // vuelva a estar libre: no hace falta ninguna escritura que la
            // devuelva a su sitio, y por tanto no hay ninguna que pueda fallar.
            .condition_expression(
                "attribute_exists(PK) AND #s = :abierta AND (attribute_not_exists(#lu) OR #lu < :ahora)",
            )
            .update_expression("SET #lu = :hasta")
            .expression_attribute_names("#s", "s")
            .expression_attribute_names("#lu", "lu")
            .expression_attribute_values(":abierta", AttributeValue::S(STATUS_OPEN.to_string()))
            .expression_attribute_values(":ahora", AttributeValue::N(now.to_string()))
            .expression_attribute_values(":hasta", AttributeValue::N(until.to_string()))
            .return_values(ReturnValue::AllNew)
            // Para poder decir POR QUÉ no se pudo: sin el item, «no existe» y
            // «ya se cerró» son indistinguibles, y el cliente merece saberlo.
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await;

        match result {
            Ok(out) => {
                let item = out.attributes().ok_or_else(|| {
                    Self::infra(format!("La reserva {id} se reclamó sin devolver el item"))
                })?;
                let reservation = Self::to_reservation(item).ok_or_else(|| {
                    Self::infra(format!("La reserva {id} está incompleta en el store"))
                })?;
                Ok(ClaimResult::Claimed(reservation))
            }
            Err(err) => {
                let err = err.into_service_error();
                let UpdateItemError::ConditionalCheckFailedException(ref failed) = err else {
                    return Err(Self::infra(format!(
                        "No se pudo reclamar la reserva {id}: {err}"
                    )));
                };

                let Some(item) = failed.item().filter(|i| !i.is_empty()) else {
                    return Ok(ClaimResult::NotFound);
                };
                let Some(reservation) = Self::to_reservation(item) else {
                    return Err(Self::infra(format!(
                        "La reserva {id} está incompleta en el store"
                    )));
                };

                let cerrada = item
                    .get("s")
                    .and_then(|v| v.as_s().ok())
                    .map(String::as_str)
                    == Some(STATUS_CLOSED);
                if cerrada {
                    let razon = item
                        .get("cr")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| CloseReason::parse(s))
                        .unwrap_or(CloseReason::Settled);
                    return Ok(ClaimResult::Closed(reservation, razon));
                }
                Ok(ClaimResult::Leased(reservation))
            }
        }
    }

    async fn close(
        &self,
        tenant_id: &str,
        id: &str,
        reason: CloseReason,
    ) -> Result<(), DomainError> {
        let (pk, sk) = Self::key(tenant_id, id);

        self.ddb
            .client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            .condition_expression("attribute_exists(PK)")
            // Al borrar las claves del GSI la reserva sale del índice de
            // barrido: lo que queda ahí es exactamente lo que falta por cerrar.
            .update_expression("SET #s = :cerrada, #cr = :razon, #lu = :cero REMOVE #gp, #gs")
            .expression_attribute_names("#s", "s")
            .expression_attribute_names("#cr", "cr")
            .expression_attribute_names("#lu", "lu")
            .expression_attribute_names("#gp", "gp")
            .expression_attribute_names("#gs", "gs")
            .expression_attribute_values(":cerrada", AttributeValue::S(STATUS_CLOSED.to_string()))
            .expression_attribute_values(":razon", AttributeValue::S(reason.as_str().to_string()))
            .expression_attribute_values(":cero", AttributeValue::N("0".to_string()))
            .send()
            .await
            .map_err(|e| {
                Self::infra(format!(
                    "No se pudo cerrar la reserva {id}: {}",
                    e.into_service_error()
                ))
            })?;

        debug!(reserva = %id, razon = reason.as_str(), "[Reservations] Cerrada");
        Ok(())
    }

    async fn sweep(
        &self,
        shard: u8,
        now: i64,
        limit: i32,
    ) -> Result<Vec<Reservation>, DomainError> {
        let out = self
            .ddb
            .client
            .query()
            .table_name(&self.table)
            .index_name(SWEEP_INDEX)
            .key_condition_expression("#gp = :p AND #gs < :ahora")
            .expression_attribute_names("#gp", "gp")
            .expression_attribute_names("#gs", "gs")
            .expression_attribute_values(":p", AttributeValue::S(Self::sweep_pk(shard)))
            .expression_attribute_values(":ahora", AttributeValue::N(now.to_string()))
            .limit(limit)
            .send()
            .await
            .map_err(|e| {
                Self::infra(format!(
                    "No se pudo barrer la partición {shard}: {}",
                    e.into_service_error()
                ))
            })?;

        let mut vencidas = Vec::new();
        for item in out.items() {
            match Self::to_reservation(item) {
                Some(r) => vencidas.push(r),
                None => warn!(
                    item = ?item.get("PK"),
                    "[Reservations] Item de reserva incompleto en el índice — se ignora"
                ),
            }
        }
        Ok(vencidas)
    }
}

#[cfg(test)]
#[path = "tests/dynamo_store_tests.rs"]
mod tests;
