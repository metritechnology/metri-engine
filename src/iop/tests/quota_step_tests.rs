use super::*;
use crate::iop::core::IopContext;
use crate::quota::SettleOutcome;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

struct MockOltpExecutor {
    mock_rows: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl OltpQueryRunner for MockOltpExecutor {
    async fn run_oltp_query(
        &self,
        _tenant_id: &str,
        _ast_ir: &Value,
    ) -> Result<Value, DomainError> {
        let rows = self.mock_rows.lock().unwrap().clone();
        Ok(Value::Array(rows))
    }
}

/// Doble del contador atómico.
///
/// Reproduce lo que hace DynamoDB: el item no existe hasta el primer débito
/// —de ahí el `Option`—, y entonces arranca del `seed`, que es el
/// `current_usage` que traía la fila. Después el seed se ignora, y esa es la
/// diferencia que importa: permite montar el caso en que la LECTURA dice que
/// hay sitio y el contador ya está lleno.
struct MockQuotaCounter {
    usage: Mutex<Option<i64>>,
    releases: Mutex<Vec<String>>,
    /// Claves de idempotencia ya consumidas, como la marca de DynamoDB.
    aplicadas: Mutex<HashSet<String>>,
    caido: bool,
}

impl MockQuotaCounter {
    /// Contador todavía sin crear: se sembrará con lo que diga la fila.
    fn nuevo() -> Self {
        MockQuotaCounter {
            usage: Mutex::new(None),
            releases: Mutex::new(vec![]),
            aplicadas: Mutex::new(HashSet::new()),
            caido: false,
        }
    }
    /// Contador ya existente con este consumo real.
    fn en(n: i64) -> Self {
        MockQuotaCounter {
            usage: Mutex::new(Some(n)),
            releases: Mutex::new(vec![]),
            aplicadas: Mutex::new(HashSet::new()),
            caido: false,
        }
    }
    fn roto() -> Self {
        MockQuotaCounter {
            usage: Mutex::new(None),
            releases: Mutex::new(vec![]),
            aplicadas: Mutex::new(HashSet::new()),
            caido: true,
        }
    }
}

#[async_trait::async_trait]
impl QuotaCounter for MockQuotaCounter {
    async fn try_debit(
        &self,
        _tenant_id: &str,
        _quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<DebitOutcome, DomainError> {
        if self.caido {
            return Err(DomainError::new(
                ErrorCode::Infra001,
                "ledger no disponible",
            ));
        }
        let mut guard = self.usage.lock().unwrap();
        let actual = guard.unwrap_or(seed);
        if actual >= max_limit {
            return Ok(DebitOutcome::Exhausted {
                current_usage: actual,
                limit: max_limit,
            });
        }
        *guard = Some(actual + amount);
        Ok(DebitOutcome::Debited {
            new_usage: actual + amount,
        })
    }

    async fn settle(
        &self,
        _tenant_id: &str,
        quota_id: &str,
        delta: i64,
    ) -> Result<i64, DomainError> {
        let mut guard = self.usage.lock().unwrap();
        let nuevo = std::cmp::max(0, guard.unwrap_or(0) + delta);
        *guard = Some(nuevo);
        if delta < 0 {
            self.releases.lock().unwrap().push(quota_id.to_string());
        }
        Ok(nuevo)
    }

    async fn read_many(
        &self,
        _tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError> {
        let usage = *self.usage.lock().unwrap();
        Ok(quota_ids
            .iter()
            .filter_map(|id| usage.map(|n| (id.clone(), n)))
            .collect())
    }

    /// Reproduce la marca de idempotencia: la primera vez aplica, las
    /// siguientes no tocan nada.
    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<SettleOutcome, DomainError> {
        {
            let mut aplicadas = self.aplicadas.lock().unwrap();
            if !aplicadas.insert(idem_key.to_string()) {
                return Ok(SettleOutcome::AlreadyApplied);
            }
        }
        self.settle(tenant_id, quota_id, delta).await?;
        Ok(SettleOutcome::Applied)
    }
}

#[tokio::test]
async fn test_quota_guard_bypass_for_non_monitored_operations() {
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "domain_quota",
        "UPDATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_ok());
    let res_ctx = result.unwrap();
    assert!(res_ctx.quota_reservation.is_none());
    assert_eq!(
        *step.counter.usage.lock().unwrap(),
        None,
        "ni se tocó el contador"
    );
}

#[tokio::test]
async fn test_quota_guard_missing_quota_returns_error() {
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Quota001);
}

#[tokio::test]
async fn test_quota_guard_exhausted_quota_returns_error() {
    let active_quota = json!({
        "id": "quota_01",
        "max_limit": 10,
        "current_usage": 10,
        "period_key": "LIFETIME"
    });

    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![active_quota]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Quota001);
}

#[tokio::test]
async fn test_quota_guard_successful_quota_debit() {
    let active_quota = json!({
        "id": "quota_01",
        "max_limit": 10,
        "current_usage": 3,
        "period_key": "LIFETIME"
    });

    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![active_quota]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_ok());
    let res_ctx = result.unwrap();

    let reservation = res_ctx.quota_reservation.expect("should have reservation");
    assert_eq!(reservation.get("id").unwrap().as_str(), Some("quota_01"));
    assert_eq!(reservation.get("debit").unwrap().as_i64(), Some(1));
    // «pending», no «confirmed»: el débito solo queda firme si el pipeline
    // entero termina bien. Quien lo devuelve es el orquestador.
    assert_eq!(reservation.get("status").unwrap().as_str(), Some("pending"));
    assert_eq!(reservation.get("new_usage").unwrap().as_i64(), Some(4));

    // Y el débito es TODO lo que se escribe. Hasta la fase 3 este mismo camino
    // hacía además una TransactWriteItems completa sobre `domain_quota` para
    // dejar ahí una copia de este número; ahora se calcula al leerlo.
    assert_eq!(*step.counter.usage.lock().unwrap(), Some(4));
}

#[tokio::test]
async fn test_quota_guard_dynamic_period_matching() {
    let today = chrono::Utc::now().date_naive();
    let start_date = (today - chrono::Duration::days(2))
        .format("%Y-%m-%d")
        .to_string();
    let end_date = (today + chrono::Duration::days(5))
        .format("%Y-%m-%d")
        .to_string();
    let period_key = format!("{}_{}", start_date, end_date);

    let active_quota = json!({
        "id": "quota_02",
        "max_limit": 100,
        "current_usage": 5,
        "period_key": period_key
    });

    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![active_quota]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_ok());
    let res_ctx = result.unwrap();

    let reservation = res_ctx.quota_reservation.expect("should have reservation");
    assert_eq!(reservation.get("id").unwrap().as_str(), Some("quota_02"));
}

#[tokio::test]
async fn test_quota_guard_expired_period_returns_error() {
    let today = chrono::Utc::now().date_naive();
    let start_date = (today - chrono::Duration::days(10))
        .format("%Y-%m-%d")
        .to_string();
    let end_date = (today - chrono::Duration::days(2))
        .format("%Y-%m-%d")
        .to_string();
    let period_key = format!("{}_{}", start_date, end_date);

    let expired_quota = json!({
        "id": "quota_expired",
        "max_limit": 100,
        "current_usage": 5,
        "period_key": period_key
    });

    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![expired_quota]),
    };

    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );

    let result = step.execute(ctx).await;
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// La carrera, y lo que la cierra
// ─────────────────────────────────────────────────────────────────────────────

/// El caso que el código anterior no podía distinguir.
///
/// La consulta OLTP devuelve una foto: «3 de 10, hay sitio». Entre esa lectura y
/// la escritura, otras siete altas llenaron la cuota. El código anterior decidía
/// con la foto —`if current_usage >= max_limit`— y debitaba. Dos peticiones
/// simultáneas leían las dos el mismo valor y pasaban las dos.
///
/// Ahora quien decide es el contador, en la misma escritura que incrementa.
#[tokio::test]
async fn el_contador_manda_sobre_la_lectura_previa() {
    let quota_con_sitio_aparente = json!({
        "id": "quota_01",
        "max_limit": 10,
        "current_usage": 3,      // la foto vieja
        "period_key": "LIFETIME"
    });

    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![quota_con_sitio_aparente]),
    };
    // El estado real: lleno.
    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::en(10));

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );
    let result = step.execute(ctx).await;

    assert!(
        result.is_err(),
        "la lectura decía que había sitio; el contador manda"
    );
    assert_eq!(result.unwrap_err().code, ErrorCode::Quota001);

    assert_eq!(
        *step.counter.usage.lock().unwrap(),
        Some(10),
        "el contador no se movió"
    );
}

/// El rechazo informa del consumo REAL, no del que traía la lectura.
///
/// Importa porque este contexto es lo que viaja al cliente por
/// `Status.error_context`. Decirle «3 de 10» a quien acaba de ser rechazado
/// sería peor que no decirle nada.
#[tokio::test]
async fn el_rechazo_lleva_las_cifras_del_contador() {
    let quota = json!({
        "id": "quota_01", "max_limit": 10, "current_usage": 3, "period_key": "2026-01-01_2027-01-01"
    });
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![quota]),
    };
    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::en(10));

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );
    let err = step.execute(ctx).await.unwrap_err();

    let contexto = err
        .context
        .expect("el rechazo por cuota debe llevar contexto");
    assert_eq!(
        contexto.get("current_usage").unwrap().as_i64(),
        Some(10),
        "no la foto vieja de 3"
    );
    assert_eq!(contexto.get("limit").unwrap().as_i64(), Some(10));
    assert_eq!(
        contexto.get("resource_domain").unwrap().as_str(),
        Some("llm:aws:nova-pro")
    );
}

/// Si el contador no responde, NO se deja pasar. Es la cuota: ante un fallo del
/// mecanismo que la aplica, el paso falla y el cliente lo ve.
#[tokio::test]
async fn un_contador_caido_no_deja_pasar() {
    let quota = json!({"id":"quota_01","max_limit":10,"current_usage":3,"period_key":"LIFETIME"});
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![quota]),
    };
    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::roto());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );
    assert!(step.execute(ctx).await.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// Devolver la reserva
// ─────────────────────────────────────────────────────────────────────────────

/// Sin esto, cada escritura que fallaba después del paso 2 dejaba el contador un
/// punto más alto, para siempre, hasta el cambio de periodo.
#[tokio::test]
async fn compensar_devuelve_la_unidad_debitada() {
    let quota = json!({"id":"quota_01","max_limit":10,"current_usage":3,"period_key":"LIFETIME"});
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![quota]),
    };
    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "llm:aws:nova-pro",
        "CREATE",
        serde_json::Map::new(),
    );
    let res_ctx = step.execute(ctx).await.unwrap();
    assert_eq!(*step.counter.usage.lock().unwrap(), Some(4), "debitó");

    // Aquí es donde falla Janus y la escritura nunca ocurre.
    step.compensate(&res_ctx).await;

    assert_eq!(
        *step.counter.usage.lock().unwrap(),
        Some(3),
        "y lo devolvió"
    );
    assert_eq!(
        *step.counter.releases.lock().unwrap(),
        vec!["quota_01".to_string()]
    );
}

/// Compensar un contexto que nunca reservó no debe tocar nada — es el caso de
/// los pass-through, que son la mayoría de las peticiones.
#[tokio::test]
async fn compensar_sin_reserva_no_hace_nada() {
    let oltp = MockOltpExecutor {
        mock_rows: Mutex::new(vec![]),
    };
    let step = QuotaGuardStep::new(oltp, MockQuotaCounter::nuevo());

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "asset",
        "UPDATE",
        serde_json::Map::new(),
    );
    step.compensate(&ctx).await;

    assert!(step.counter.releases.lock().unwrap().is_empty());
}
