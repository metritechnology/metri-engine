//! current_usage computed at read time.
//!
//! `current_usage` calculado en lectura.
//!
//! EL PROBLEMA QUE RESUELVE
//! ────────────────────────
//! El consumo real lo lleva el contador atómico, fuera del log de datoms. Para
//! que el panel pudiera enseñarlo, hasta la fase 3 cada débito escribía además
//! una `TransactWriteItems` completa —n datoms, sus índices y su posible evento
//! de outbox— sobre la fila `domain_quota`, solo para dejar ahí una copia del
//! mismo número.
//!
//! Eso salía caro en el sitio peor: el paso IOP corre en cada CREATE y cada GET
//! del motor, y pagaba una transacción multi-item por operación. Y con todo,
//! la copia no era fiable: se escribía en best-effort, así que cualquier fallo
//! la dejaba atrás sin que nadie lo notara.
//!
//! Ahora no se escribe. Cuando alguien lee filas de `domain_quota`, se les
//! superpone el consumo vigente del contador — una sola llamada para todas las
//! filas de la página, no una por fila.
//!
//! QUÉ QUEDA DE `current_usage` EN EL LOG
//! ──────────────────────────────────────
//! El valor SEMILLA. `try_debit` lo usa como punto de partida la primera vez que
//! toca un contador que aún no existe, para que activar esto sobre un tenant en
//! marcha no le regale lo ya gastado. Una vez creado el contador, deja de
//! intervenir — y este retoque deja de devolver el valor almacenado para
//! devolver el del contador. Los dos casos encajan sin condiciones especiales:
//! mientras no hay contador, el valor guardado ES el bueno.

use std::sync::Arc;

use serde_json::Value;
use tracing::warn;

use crate::aegis::oltp::executor::RowOverlay;
use crate::quota::ledger::QuotaCounter;

/// Entidad cuyas filas se retocan.
const QUOTA_ENTITY: &str = "domain_quota";

/// La columna que deja de vivir en el log.
const USAGE_FIELD: &str = "current_usage";

pub struct QuotaUsageOverlay {
    counter: Arc<dyn QuotaCounter>,
}

impl QuotaUsageOverlay {
    pub fn new(counter: Arc<dyn QuotaCounter>) -> Self {
        QuotaUsageOverlay { counter }
    }

    /// La clave con la que esta fila trae la columna, si la trae.
    ///
    /// Aegis devuelve las columnas cualificadas o desnudas según el camino de
    /// lectura. Se retoca la que exista, y solo si existe: si el usuario no
    /// pidió `current_usage`, añadirlo sería devolverle una columna que no
    /// seleccionó.
    fn usage_key(row: &Value) -> Option<&'static str> {
        const QUALIFIED: &str = "domain_quota/current_usage";
        if row.get(QUALIFIED).is_some() {
            Some(QUALIFIED)
        } else if row.get(USAGE_FIELD).is_some() {
            Some(USAGE_FIELD)
        } else {
            None
        }
    }

    fn row_id(row: &Value) -> Option<&str> {
        row.get("id")
            .or_else(|| row.get("domain_quota/id"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Contra qué contador cuenta esta fila HOY.
    ///
    /// Tiene que decidirlo igual que `quota/resolver.rs`, o el panel enseña un
    /// número que no es el que el motor está aplicando. El caso que lo obliga es
    /// el ciclo renovado sobre la marcha: la fila sigue diciendo «julio» y el
    /// consumo vigente vive en el contador de agosto.
    ///
    /// Lo que esta función NO puede resolver es la fila comodín `*`: gobierna
    /// muchos dominios, cada uno con su contador, y una fila no puede enseñar
    /// varios números. Se queda con el valor almacenado, que para esa fila es lo
    /// honesto — el consumo real está repartido.
    fn counter_id(row: &Value) -> Option<String> {
        let id = Self::row_id(row)?;

        let period_key = row
            .get("period_key")
            .or_else(|| row.get("domain_quota/period_key"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if period_key.is_empty()
            || crate::quota::resolver::covers(period_key, chrono::Utc::now().date_naive())
        {
            return Some(id.to_string());
        }

        let estrategia = row
            .get("reset_strategy")
            .or_else(|| row.get("domain_quota/reset_strategy"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match crate::quota::resolver::renewed_period(estrategia, chrono::Utc::now().date_naive()) {
            Some(actual)
                if period_key
                    .split('_')
                    .next()
                    .is_some_and(|i| i < actual.as_str()) =>
            {
                Some(format!("{id}#{actual}"))
            }
            _ => Some(id.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl RowOverlay for QuotaUsageOverlay {
    fn entity(&self) -> &str {
        QUOTA_ENTITY
    }

    async fn apply(&self, tenant_id: &str, rows: &mut [Value]) {
        // Solo las filas que traen la columna: pedir contadores para filas que
        // no la muestran sería gastar lecturas en nada.
        let ids: Vec<String> = rows
            .iter()
            .filter(|r| Self::usage_key(r).is_some())
            .filter_map(Self::counter_id)
            .collect();

        if ids.is_empty() {
            return;
        }

        let usos = match self.counter.read_many(tenant_id, &ids).await {
            Ok(u) => u,
            Err(e) => {
                // El valor almacenado es viejo, no falso. Devolverlo es mejor
                // que tumbar la consulta.
                warn!(
                    tenant = %tenant_id, error = ?e,
                    "[QuotaProjection] No se pudo leer el contador — se devuelve el valor almacenado"
                );
                return;
            }
        };

        for row in rows.iter_mut() {
            let Some(key) = Self::usage_key(row) else {
                continue;
            };
            let Some(id) = Self::counter_id(row) else {
                continue;
            };
            // Sin contador todavía: el valor guardado es el bueno, es el que
            // `try_debit` usará como semilla.
            let Some(uso) = usos.get(&id) else { continue };

            if let Some(obj) = row.as_object_mut() {
                obj.insert(key.to_string(), Value::from(*uso));
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/projection_tests.rs"]
mod tests;
