// quota/ledger.rs — Contador atómico con techo, fuera del log de datoms.
//
// [MOVIDO_DESDE: eav/writer/counter.rs — fase 0 del refactor de QuotaGuard]
//
// POR QUÉ NO VIVE EN EL LOG EAV
// ─────────────────────────────
// El log es append-only: «el valor actual» de un atributo es el datom con el
// tx_id más alto. Un contador sobre esa forma obliga a leer, sumar en memoria y
// escribir el resultado, y esa secuencia no se puede proteger con una condición
// de DynamoDB: una ConditionExpression solo puede hablar del item que nombra, y
// el item que haría falta nombrar —el datom que otro escritor está a punto de
// añadir— todavía no existe. No hay forma de expresar «nadie ha escrito después
// que yo» sobre un log de sólo-añadir.
//
// Una cuota necesita exactamente esa garantía, así que el contador vive en su
// propio item mutable, donde el incremento es atómico y el techo es una
// condición evaluada por DynamoDB en la misma operación. Sin ventana entre
// comprobar y debitar, porque son la misma escritura.
//
// Aquí hubo un helper de bloqueo optimista (`eav/writer/optimistic.rs`) que
// pretendía resolver esto condicionando sobre un atributo de versión. No podía
// funcionar, por la razón de arriba y por dos más: el id que usaba era el de
// `entity/type`, y la clave que componía no correspondía a ningún datom escrito.
// Se retiró; la nota de diseño de lo que haría falta para un bloqueo optimista
// de verdad está en el blueprint, §XIII.2.
//
// CLAVES
// ──────
// PK `T#{tenant}#QC#{quota_id}` — contador. Espacio propio, no colisiona con
// los datoms, que viven bajo `T#{tenant}#E#{entity}`. Como el id de cuota es
// distinto en cada periodo, el contador es automáticamente por periodo: al
// rotar el ciclo hay una fila `domain_quota` nueva y por tanto un contador
// nuevo, que arranca en cero sin necesidad de reiniciar nada.
//
// PK `T#{tenant}#QI#{idem_key}` — marca de idempotencia de `settle_once`.
// Misma tabla a propósito: una `TransactWriteItems` solo es atómica sobre lo
// que abarca, y la marca tiene que caer con el apunte o no caer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeValue, Put, ReturnValue, ReturnValuesOnConditionCheckFailure, TransactWriteItem,
    Update,
};
use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::infrastructure::dynamodb::DynamoClient;

/// Sort key fija del item de contador. Binaria porque el esquema de la tabla
/// declara SK de tipo B; corta a propósito, para no poder coincidir con las
/// claves que produce `build_eavt_sk` (attr + tx_id + flag).
const COUNTER_SK: &[u8] = b"QC";

/// Sort key fija de la marca de idempotencia. Mismo criterio que `COUNTER_SK`.
const IDEM_SK: &[u8] = b"QI";

/// Nombre del atributo numérico. Una letra porque se escribe en cada operación.
const USAGE_ATTR: &str = "n";

/// Cuánto sobrevive una marca de idempotencia. Cubre de sobra la vida de una
/// reserva (90 s) y cualquier reintento razonable, sin dejar basura eterna.
///
/// Requiere TTL habilitado sobre el atributo `ttl` en la tabla; sin él la marca
/// simplemente no se borra sola. Se sigue escribiendo igual: el día que se
/// habilite, empieza a limpiar sin migración.
const IDEM_TTL_SECS: i64 = 24 * 60 * 60;

/// Intentos de una escritura transaccional antes de rendirse.
///
/// Solo se reintenta la contención (`TransactionConflict`), que es la
/// contrapartida de usar una transacción sobre un item caliente: un
/// `UpdateItem` plano nunca choca consigo mismo, una `TransactWriteItems` sí.
const TX_MAX_ATTEMPTS: u32 = 3;

/// La condición que impone el techo, en la gramática que DynamoDB acepta.
///
/// AQUÍ ESTUVO EL DEFECTO. La condición era
/// `if_not_exists(#n, :seed) < :max`, y `if_not_exists` **no existe en una
/// ConditionExpression**: la gramática de condición admite seis funciones
/// —`attribute_exists`, `attribute_not_exists`, `attribute_type`,
/// `begins_with`, `contains`, `size`— y `if_not_exists` es de UpdateExpression.
/// DynamoDB respondía `ValidationException: Invalid ConditionExpression`, así
/// que **todo débito fallaba**, y con él toda alta de una entidad con cuota.
///
/// No lo vio ningún test porque los que ejercitan cuota sustituyen
/// `QuotaCounter` por un doble: esta `UpdateItem` nunca llegó a formarse contra
/// un DynamoDB de verdad.
///
/// La semántica que hay que conservar es la de antes, en dos casos:
///
///   · **El contador existe** — se admite si `n < max`.
///   · **No existe** — se admite si `seed < max`, porque el valor de arranque
///     es `seed` (lo pone `if_not_exists` en la UpdateExpression, donde sí es
///     legal).
///
/// `seed` se conoce al construir la petición, así que la segunda rama se
/// resuelve aquí en vez de dentro de la expresión. Cuando el arranque ya no
/// cabe, la condición se queda en `#n < :max`: sobre un item ausente la
/// comparación es falsa, que es exactamente el rechazo que toca.
fn debit_condition(seed: i64, max_limit: i64) -> &'static str {
    if seed < max_limit {
        "attribute_not_exists(#n) OR #n < :max"
    } else {
        "#n < :max"
    }
}

/// Qué pasó al intentar consumir una unidad de cuota.
#[derive(Debug, Clone, PartialEq)]
pub enum DebitOutcome {
    /// Debitado. `new_usage` es el valor autoritativo tras el incremento.
    Debited { new_usage: i64 },
    /// El techo lo impidió. `current_usage` es el valor real en el momento del
    /// rechazo, no la lectura previa de quien llamó.
    Exhausted { current_usage: i64, limit: i64 },
}

/// Qué pasó al aplicar un ajuste con clave de idempotencia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleOutcome {
    /// El apunte se aplicó ahora.
    Applied,
    /// Ya estaba aplicado por esta misma clave. No se ha tocado el contador.
    AlreadyApplied,
}

/// Contador de cuota por tenant y periodo, atómico frente a concurrencia.
#[derive(Clone)]
pub struct QuotaLedger {
    ddb:   Arc<DynamoClient>,
    table: String,
}

/// Lo que necesitan de este contador quienes aplican cuota. Existe para poder
/// sustituirlo en los tests sin hablar con DynamoDB.
///
/// Tres operaciones, y lo que las separa es el techo y quién puede repetirlas:
///
///   · `try_debit`   consume ANTES de que ocurra el gasto, así que puede negarse.
///   · `settle`      ajusta DESPUÉS, cuando el gasto ya pasó o ya se deshizo, y
///     por eso no tiene techo: negarse a apuntar algo que ya sucedió no lo
///     desharía, solo perdería la cuenta.
///   · `settle_once` es `settle` para cuando quien llama puede no ser el único
///     —otra réplica, un reintento, un barrido— y repetir el apunte sería
///     contarlo dos veces.
#[async_trait::async_trait]
pub trait QuotaCounter: Send + Sync {
    /// Consume `amount` si y solo si todavía queda sitio.
    ///
    /// La condición es «el consumo actual es menor que el límite», igual que la
    /// comprobación que había antes en cada llamante; lo que cambia es que ahora
    /// la evalúa DynamoDB dentro de la misma escritura. Que un `amount` grande
    /// pueda rebasar el techo es la política que ya existía, y cambiarla es otra
    /// decisión: aquí solo se cierra la carrera.
    ///
    /// `seed` es el valor de arranque cuando el contador todavía no existe —el
    /// `current_usage` que lleva la fila `domain_quota`—, para que activar esto
    /// sobre un tenant en marcha no le regale el consumo ya gastado. Una vez
    /// creado el item, `seed` se ignora.
    async fn try_debit(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        amount:    i64,
        max_limit: i64,
        seed:      i64,
    ) -> Result<DebitOutcome, DomainError>;

    /// Ajusta el contador sin techo y devuelve el valor resultante.
    ///
    /// `delta` positivo carga, negativo devuelve. Nunca baja de cero: una
    /// devolución mayor que lo apuntado lo deja en cero en vez de en negativo.
    /// Con `delta` cero no cambia nada y sirve para leer el valor vigente.
    ///
    /// NO es idempotente: repetirlo lo aplica otra vez. Vale mientras haya un
    /// único responsable de cada apunte —el camino IOP, donde quien debita y
    /// quien compensa son el mismo proceso—; en cuanto el apunte lo puede hacer
    /// más de uno, es `settle_once`.
    async fn settle(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
    ) -> Result<i64, DomainError>;

    /// Aplica `delta` UNA sola vez para `idem_key`. Repetirlo no hace nada.
    ///
    /// Contador y marca caen en la misma `TransactWriteItems`: o se aplican los
    /// dos, o ninguno. Sin eso no hay forma de distinguir «este reintegro no se
    /// llegó a aplicar» de «se aplicó y me morí antes de apuntarlo», que es la
    /// pregunta que se hace cualquiera que recoja una reserva ajena.
    ///
    /// No devuelve el nuevo consumo: una transacción de DynamoDB no devuelve
    /// valores en el caso de éxito. Quien necesite la cifra la lee después con
    /// `settle(.., 0)`.
    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
        idem_key:  &str,
    ) -> Result<SettleOutcome, DomainError>;

    /// Consumo real de varias cuotas de un tenant, en una sola llamada.
    ///
    /// Es una LECTURA de verdad: no crea el contador si no existe. Esa
    /// diferencia importa —crearlo a cero anularía el `seed` de `try_debit`—, y
    /// es la razón de que no se resuelva con `settle(.., 0)`.
    ///
    /// Las cuotas que no aparecen en el resultado son las que todavía no tienen
    /// contador. Para ellas el valor bueno es el que lleva la fila EAV.
    async fn read_many(
        &self,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>, DomainError>;

    /// Devuelve `amount` unidades. Azúcar sobre `settle` para que el sitio que
    /// deshace se lea como lo que es.
    async fn release(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        amount:    i64,
    ) -> Result<(), DomainError> {
        self.settle(tenant_id, quota_id, -amount).await.map(|_| ())
    }
}

/// Cuántas claves caben en un BatchGetItem de DynamoDB.
const BATCH_GET_MAX: usize = 100;

impl QuotaLedger {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        QuotaLedger { ddb, table: table.into() }
    }


    fn key(tenant_id: &str, quota_id: &str) -> (AttributeValue, AttributeValue) {
        (
            AttributeValue::S(format!("T#{tenant_id}#QC#{quota_id}")),
            AttributeValue::B(Blob::new(COUNTER_SK.to_vec())),
        )
    }

    fn idem_key(tenant_id: &str, idem_key: &str) -> (AttributeValue, AttributeValue) {
        (
            AttributeValue::S(format!("T#{tenant_id}#QI#{idem_key}")),
            AttributeValue::B(Blob::new(IDEM_SK.to_vec())),
        )
    }

    /// Lee el contador de un mapa de atributos devuelto por DynamoDB.
    fn read_usage(
        attrs: Option<&std::collections::HashMap<String, AttributeValue>>,
    ) -> Option<i64> {
        attrs?
            .get(USAGE_ATTR)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<i64>().ok())
    }
}

#[async_trait::async_trait]
impl QuotaCounter for QuotaLedger {
    /// Consumo real de varias cuotas de un tenant, en una sola llamada.
    ///
    /// Es una LECTURA de verdad —`BatchGetItem`, no `UpdateItem`—, así que no
    /// crea el item si no existe. Esa diferencia importa: crear el contador a
    /// cero anularía el `seed` de `try_debit` y le regalaría al tenant todo lo
    /// que hubiera gastado antes de que hubiera contador.
    ///
    /// Las cuotas que no aparecen en el resultado son las que todavía no tienen
    /// contador. Para ellas el valor bueno es el que lleva la fila EAV.
    async fn read_many(
        &self,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>, DomainError> {
        let mut usos = std::collections::HashMap::new();

        for lote in quota_ids.chunks(BATCH_GET_MAX) {
            let mut keys = Vec::with_capacity(lote.len());
            for quota_id in lote {
                let (pk, sk) = Self::key(tenant_id, quota_id);
                let mut key = HashMap::new();
                key.insert("PK".to_string(), pk);
                key.insert("SK".to_string(), sk);
                keys.push(key);
            }

            let peticion = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                .set_keys(Some(keys))
                // La PK vuelve para poder emparejar cada contador con su cuota:
                // BatchGetItem no conserva el orden de las claves pedidas.
                .projection_expression("PK, #n")
                .expression_attribute_names("#n", USAGE_ATTR)
                .build()
                .map_err(|e| DomainError::new(
                    ErrorCode::Infra001,
                    format!("Lectura de contadores mal formada: {e}"),
                ).with_stage("quota"))?;

            let out = self.ddb.client
                .batch_get_item()
                .request_items(&self.table, peticion)
                .send()
                .await
                .map_err(|e| DomainError::new(
                    ErrorCode::Infra001,
                    format!("Fallo al leer contadores: {}", e.into_service_error()),
                ).with_stage("quota"))?;

            let Some(items) = out.responses().and_then(|r| r.get(&self.table)) else {
                continue;
            };

            let prefijo = format!("T#{tenant_id}#QC#");
            for item in items {
                let Some(pk) = item.get("PK").and_then(|v| v.as_s().ok()) else { continue };
                let Some(quota_id) = pk.strip_prefix(&prefijo) else { continue };
                if let Some(n) = Self::read_usage(Some(item)) {
                    usos.insert(quota_id.to_string(), n);
                }
            }
        }

        Ok(usos)
    }

    async fn try_debit(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        amount:    i64,
        max_limit: i64,
        seed:      i64,
    ) -> Result<DebitOutcome, DomainError> {
        let (pk, sk) = Self::key(tenant_id, quota_id);

        // Incremento y techo en la MISMA operación. `if_not_exists` cubre el
        // primer uso: si el item no existe, parte de `seed`; si existe, del
        // valor almacenado, y `seed` no interviene.
        //
        // UpdateItem plano, no transacción: este es el camino caliente —corre
        // en cada CREATE y cada GET del motor— y una escritura simple nunca
        // choca con otra sobre el mismo item, mientras que dos transacciones sí.
        let result = self.ddb.client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            .update_expression("SET #n = if_not_exists(#n, :seed) + :amount")
            .condition_expression(debit_condition(seed, max_limit))
            .expression_attribute_names("#n", USAGE_ATTR)
            .expression_attribute_values(":seed",   AttributeValue::N(seed.to_string()))
            .expression_attribute_values(":amount", AttributeValue::N(amount.to_string()))
            .expression_attribute_values(":max",    AttributeValue::N(max_limit.to_string()))
            .return_values(ReturnValue::UpdatedNew)
            // Para que el rechazo pueda informar del consumo REAL y no de la
            // lectura previa de quien llamó, que puede estar desactualizada.
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await;

        match result {
            Ok(out) => {
                let new_usage = Self::read_usage(out.attributes()).unwrap_or(seed + amount);
                info!(
                    tenant = %tenant_id, quota = %quota_id, amount, new_usage,
                    "[QuotaLedger] Débito atómico confirmado"
                );
                Ok(DebitOutcome::Debited { new_usage })
            }
            Err(err) => {
                let service_err = err.into_service_error();
                if let UpdateItemError::ConditionalCheckFailedException(ref failed) = service_err {
                    let current_usage = Self::read_usage(failed.item()).unwrap_or(max_limit);
                    warn!(
                        tenant = %tenant_id, quota = %quota_id, current_usage, limit = max_limit,
                        "[QuotaLedger] Débito rechazado por el techo"
                    );
                    return Ok(DebitOutcome::Exhausted { current_usage, limit: max_limit });
                }
                // El mensaje del servicio, no solo su tipo. `Display` de un
                // error no modelado —`ValidationException` no lo está para
                // `UpdateItem`— imprime «unhandled error (ValidationException)»
                // y DESCARTA el texto, que es justo donde DynamoDB dice qué
                // parte de la petición está mal. Sin esto, un fallo de forma se
                // investiga a ciegas: fue el caso de esta misma función.
                let detalle = service_err
                    .meta()
                    .message()
                    .unwrap_or("sin mensaje del servicio");
                Err(DomainError::new(
                    ErrorCode::Infra001,
                    format!("Fallo al debitar cuota {quota_id}: {service_err}: {detalle}"),
                ).with_stage("quota"))
            }
        }
    }

    async fn settle(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
    ) -> Result<i64, DomainError> {
        // ── Cargar (o leer, con delta cero) ──────────────────────────────────
        if delta >= 0 {
            let (pk, sk) = Self::key(tenant_id, quota_id);
            let out = self.ddb.client
                .update_item()
                .table_name(&self.table)
                .key("PK", pk)
                .key("SK", sk)
                .update_expression("SET #n = if_not_exists(#n, :zero) + :delta")
                .expression_attribute_names("#n", USAGE_ATTR)
                .expression_attribute_values(":zero",  AttributeValue::N("0".to_string()))
                .expression_attribute_values(":delta", AttributeValue::N(delta.to_string()))
                .return_values(ReturnValue::UpdatedNew)
                .send()
                .await
                .map_err(|e| DomainError::new(
                    ErrorCode::Infra001,
                    format!("Fallo al cargar cuota {quota_id}: {}", e.into_service_error()),
                ).with_stage("quota"))?;

            let new_usage = Self::read_usage(out.attributes()).unwrap_or(0);
            info!(tenant = %tenant_id, quota = %quota_id, delta, new_usage, "[QuotaLedger] Cargo aplicado");
            return Ok(new_usage);
        }

        // ── Devolver ────────────────────────────────────────────────────────
        let amount = -delta;
        let (pk, sk) = Self::key(tenant_id, quota_id);

        let result = self.ddb.client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            // Solo se resta lo que hay. Si el contador tuviera menos de lo que
            // se devuelve, restar a ciegas lo dejaría en negativo y regalaría
            // cuota; el caso se recoge abajo poniéndolo a cero.
            .condition_expression("attribute_exists(#n) AND #n >= :amount")
            .update_expression("SET #n = #n - :amount")
            .expression_attribute_names("#n", USAGE_ATTR)
            .expression_attribute_values(":amount", AttributeValue::N(amount.to_string()))
            .return_values(ReturnValue::UpdatedNew)
            .send()
            .await;

        match result {
            Ok(out) => {
                let new_usage = Self::read_usage(out.attributes()).unwrap_or(0);
                info!(
                    tenant = %tenant_id, quota = %quota_id, amount, new_usage,
                    "[QuotaLedger] Devolución aplicada"
                );
                Ok(new_usage)
            }
            Err(err) => {
                let service_err = err.into_service_error();
                if !matches!(service_err, UpdateItemError::ConditionalCheckFailedException(_)) {
                    return Err(DomainError::new(
                        ErrorCode::Infra001,
                        format!("Fallo al devolver cuota {quota_id}: {service_err}"),
                    ).with_stage("quota"));
                }
                self.clamp_to_zero(tenant_id, quota_id, amount).await
            }
        }
    }

    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
        idem_key:  &str,
    ) -> Result<SettleOutcome, DomainError> {
        for attempt in 0..TX_MAX_ATTEMPTS {
            let items = self.settle_items(tenant_id, quota_id, delta, idem_key, Floor::Guard)?;

            let result = self.ddb.client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await;

            let err = match result {
                Ok(_) => {
                    info!(
                        tenant = %tenant_id, quota = %quota_id, delta, idem = %idem_key,
                        "[QuotaLedger] Apunte idempotente aplicado"
                    );
                    return Ok(SettleOutcome::Applied);
                }
                Err(e) => e.into_service_error(),
            };

            match classify(&err) {
                // La marca ya existía: alguien —otra réplica, un reintento, el
                // barrido— aplicó este mismo apunte. Nada que hacer, y eso es
                // justamente lo que se pedía.
                Cancellation::AlreadyApplied => {
                    info!(
                        tenant = %tenant_id, quota = %quota_id, idem = %idem_key,
                        "[QuotaLedger] Apunte ya aplicado por esta clave — no se repite"
                    );
                    return Ok(SettleOutcome::AlreadyApplied);
                }
                // La devolución era mayor que lo apuntado. Se deja en cero, con
                // la misma marca, para que siga siendo una sola aplicación.
                Cancellation::WouldGoNegative => {
                    return self.settle_clamped(tenant_id, quota_id, delta, idem_key).await;
                }
                Cancellation::Conflict if attempt + 1 < TX_MAX_ATTEMPTS => {
                    let backoff = backoff_ms(attempt);
                    warn!(
                        tenant = %tenant_id, quota = %quota_id, attempt, backoff,
                        "[QuotaLedger] Contención transaccional sobre el contador — reintentando"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
                _ => {
                    return Err(DomainError::new(
                        ErrorCode::Infra001,
                        format!("Fallo al aplicar el apunte {idem_key} sobre la cuota {quota_id}: {err}"),
                    ).with_stage("quota"));
                }
            }
        }

        Err(DomainError::new(
            ErrorCode::Infra001,
            format!("Contención persistente al aplicar {idem_key} sobre la cuota {quota_id}"),
        ).with_stage("quota"))
    }
}

/// Si el apunte negativo debe protegerse de dejar el contador bajo cero, o si
/// ya se sabe que hay que aplastarlo a cero.
#[derive(Clone, Copy, PartialEq)]
enum Floor {
    /// Condición «hay al menos tanto como se devuelve».
    Guard,
    /// Sin condición aritmética: se escribe cero.
    Clamp,
}

impl QuotaLedger {
    /// Los dos items de `settle_once`: el apunte y su marca.
    ///
    /// Van juntos en la misma transacción a propósito. Separarlos —apuntar y
    /// luego marcar— reintroduce exactamente el agujero que esto viene a tapar:
    /// una muerte entre las dos escrituras deja un apunte sin marca, y el
    /// siguiente que pase lo aplica otra vez.
    fn settle_items(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
        idem_key:  &str,
        floor:     Floor,
    ) -> Result<Vec<TransactWriteItem>, DomainError> {
        let (pk, sk) = Self::key(tenant_id, quota_id);

        let mut update = Update::builder()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            .expression_attribute_names("#n", USAGE_ATTR);

        update = match floor {
            Floor::Clamp => update
                // Solo se aplasta lo que existe. Crear el item a cero borraría
                // el efecto del `seed` de `try_debit` y el tenant recuperaría
                // gratis todo lo gastado antes de que hubiera contador.
                .condition_expression("attribute_exists(#n)")
                .update_expression("SET #n = :zero")
                .expression_attribute_values(":zero", AttributeValue::N("0".to_string())),
            Floor::Guard if delta < 0 => update
                .condition_expression("attribute_exists(#n) AND #n >= :amount")
                .update_expression("SET #n = #n - :amount")
                .expression_attribute_values(":amount", AttributeValue::N((-delta).to_string())),
            Floor::Guard => update
                .update_expression("SET #n = if_not_exists(#n, :zero) + :delta")
                .expression_attribute_values(":zero",  AttributeValue::N("0".to_string()))
                .expression_attribute_values(":delta", AttributeValue::N(delta.to_string())),
        };

        let update = update.build().map_err(|e| {
            DomainError::new(ErrorCode::Infra001, format!("Update de cuota mal formado: {e}"))
                .with_stage("quota")
        })?;

        let (idem_pk, idem_sk) = Self::idem_key(tenant_id, idem_key);
        let mut marca = HashMap::new();
        marca.insert("PK".to_string(), idem_pk);
        marca.insert("SK".to_string(), idem_sk);
        // Traza para cuando alguien se pregunte de dónde salió un apunte.
        marca.insert("q".to_string(), AttributeValue::S(quota_id.to_string()));
        marca.insert("d".to_string(), AttributeValue::N(delta.to_string()));
        marca.insert(
            "ttl".to_string(),
            AttributeValue::N((chrono::Utc::now().timestamp() + IDEM_TTL_SECS).to_string()),
        );

        let put = Put::builder()
            .table_name(&self.table)
            .set_item(Some(marca))
            .condition_expression("attribute_not_exists(PK)")
            .build()
            .map_err(|e| {
                DomainError::new(ErrorCode::Infra001, format!("Marca de cuota mal formada: {e}"))
                    .with_stage("quota")
            })?;

        // El orden fija la lectura de `cancellation_reasons`: 0 = contador,
        // 1 = marca. `classify` depende de él.
        Ok(vec![
            TransactWriteItem::builder().update(update).build(),
            TransactWriteItem::builder().put(put).build(),
        ])
    }

    /// Segunda pasada de `settle_once` cuando la devolución era mayor que lo
    /// apuntado: se deja el contador en cero en vez de en negativo, con la
    /// misma marca para que siga contando como una sola aplicación.
    ///
    /// Que se llegue aquí significa que la cuenta se descuadró en algún sitio
    /// —un reinicio manual de la fila, un periodo rotado a mitad de reserva—,
    /// así que deja traza aunque el resultado sea correcto.
    async fn settle_clamped(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        delta:     i64,
        idem_key:  &str,
    ) -> Result<SettleOutcome, DomainError> {
        let items = self.settle_items(tenant_id, quota_id, delta, idem_key, Floor::Clamp)?;

        let result = self.ddb.client
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await;

        let err = match result {
            Ok(_) => {
                warn!(
                    tenant = %tenant_id, quota = %quota_id, delta, idem = %idem_key,
                    "[QuotaLedger] Se devolvía más de lo apuntado — contador a cero"
                );
                return Ok(SettleOutcome::Applied);
            }
            Err(e) => e.into_service_error(),
        };

        match classify(&err) {
            Cancellation::AlreadyApplied => Ok(SettleOutcome::AlreadyApplied),
            // El contador no existe: no había nada que devolver. La marca no se
            // escribe, y no pasa nada: sin contador tampoco hay apunte que
            // pudiera repetirse.
            Cancellation::WouldGoNegative => {
                warn!(
                    tenant = %tenant_id, quota = %quota_id,
                    "[QuotaLedger] Nada que devolver — el contador no existe"
                );
                Ok(SettleOutcome::Applied)
            }
            _ => Err(DomainError::new(
                ErrorCode::Infra001,
                format!("Fallo al poner a cero la cuota {quota_id}: {err}"),
            ).with_stage("quota")),
        }
    }

    /// Segunda pasada de `settle` (sin clave) cuando la devolución era mayor
    /// que lo apuntado.
    async fn clamp_to_zero(
        &self,
        tenant_id: &str,
        quota_id:  &str,
        amount:    i64,
    ) -> Result<i64, DomainError> {
        let (pk, sk) = Self::key(tenant_id, quota_id);

        let result = self.ddb.client
            .update_item()
            .table_name(&self.table)
            .key("PK", pk)
            .key("SK", sk)
            .condition_expression("attribute_exists(#n)")
            .update_expression("SET #n = :zero")
            .expression_attribute_names("#n", USAGE_ATTR)
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .send()
            .await;

        match result {
            Ok(_) => {
                warn!(
                    tenant = %tenant_id, quota = %quota_id, amount,
                    "[QuotaLedger] Se devolvía más de lo apuntado — contador a cero"
                );
                Ok(0)
            }
            Err(err) => {
                let service_err = err.into_service_error();
                if matches!(service_err, UpdateItemError::ConditionalCheckFailedException(_)) {
                    // El contador no existe: no había nada que devolver.
                    warn!(
                        tenant = %tenant_id, quota = %quota_id,
                        "[QuotaLedger] Nada que devolver — el contador no existe"
                    );
                    return Ok(0);
                }
                Err(DomainError::new(
                    ErrorCode::Infra001,
                    format!("Fallo al poner a cero la cuota {quota_id}: {service_err}"),
                ).with_stage("quota"))
            }
        }
    }
}

/// Por qué canceló DynamoDB la transacción.
#[derive(Debug, PartialEq)]
enum Cancellation {
    /// Falló la condición de la marca (item 1): la clave ya se usó.
    AlreadyApplied,
    /// Falló la condición del contador (item 0): no hay tanto que devolver, o
    /// el contador no existe.
    WouldGoNegative,
    /// Otra transacción tocaba el mismo item. Se reintenta.
    Conflict,
    /// Cualquier otra cosa: no es recuperable aquí.
    Other,
}

/// Traduce el error de `TransactWriteItems` a la razón que importa.
///
/// La marca se mira primero: si las dos condiciones fallaron, la que manda es
/// que el apunte ya estaba hecho —la del contador es entonces una consecuencia,
/// no una causa.
fn classify(err: &TransactWriteItemsError) -> Cancellation {
    let TransactWriteItemsError::TransactionCanceledException(cancelled) = err else {
        return Cancellation::Other;
    };

    let reasons = cancelled.cancellation_reasons();
    let code = |i: usize| reasons.get(i).and_then(|r| r.code()).unwrap_or("None");

    if code(1) == "ConditionalCheckFailed" {
        return Cancellation::AlreadyApplied;
    }
    if code(0) == "ConditionalCheckFailed" {
        return Cancellation::WouldGoNegative;
    }
    if reasons.iter().any(|r| r.code() == Some("TransactionConflict")) {
        return Cancellation::Conflict;
    }
    Cancellation::Other
}

/// Espera antes de reintentar: exponencial con jitter, para que dos réplicas que
/// chocan no vuelvan a chocar sincronizadas.
fn backoff_ms(attempt: u32) -> u64 {
    let base = 25_u64 << attempt;
    let jitter = {
        use rand::Rng;
        rand::thread_rng().gen_range(0..=base)
    };
    base + jitter
}

#[cfg(test)]
#[path = "tests/ledger_tests.rs"]
mod tests;
