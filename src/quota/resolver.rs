//! Which quota governs this operation, and with what ceiling.
//!
//! Qué cuota gobierna esta operación, y con qué techo.
//!
//! Una consulta a Aegis por (tenant, resource_domain, limit_type) devuelve todas
//! las filas `domain_quota` de ese dominio: la del periodo en curso, las de los
//! periodos ya cerrados y las de los que aún no han empezado. Elegir entre ellas
//! es lo único que hace este archivo.
//!
//! LO QUE DEVUELVE NO DECIDE
//! ─────────────────────────
//! `QuotaSpec::seed_usage` es la foto de `current_usage` en el momento de la
//! consulta, y NO sirve para comprobar si queda sitio: entre esa lectura y la
//! escritura cabe otra petición. Quien decide es `ledger::QuotaCounter`, que
//! resuelve techo e incremento en una sola operación condicional. El `seed` solo
//! se usa como valor de arranque la primera vez que se toca un contador que
//! todavía no existe, para no regalarle a un tenant en marcha lo que ya gastó.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{Datelike, NaiveDate};
use serde_json::{json, Value};
use tracing::warn;

use crate::domain::errors::{DomainError, ErrorCode};

/// Entidad del Códice que guarda la configuración de cuota.
const QUOTA_ENTITY: &str = "domain_quota";

/// Cuánto vale una resolución antes de volver a preguntar.
///
/// Esta consulta corre en cada CREATE y cada GET del motor, y su respuesta
/// cambia dos veces al mes: cuando rota el periodo y cuando alguien cambia de
/// plan. Treinta segundos es el retardo con el que se ve un cambio de
/// `max_limit`; a cambio, el camino caliente deja de consultar.
const DEFAULT_CACHE_TTL_SECS: u64 = 30;

/// Techo de filas por dominio. Un tenant tiene una fila por periodo, así que
/// esto son años de historia; si alguna vez se rozara, el filtro de periodo
/// seguiría siendo correcto pero podría dejar fuera la fila activa, y entonces
/// tocaría filtrar por periodo en la propia consulta.
const MAX_QUOTA_ROWS: i64 = 100;

/// Dominio comodín: la cuota POR DEFECTO del tenant.
///
/// El catálogo de modelos del motor tiene muchas más entidades de las que nadie
/// tarifa —`work_order_checklist_item`, `document_chunk`—, y
/// casi ninguna se crea desde una pantalla propia: aparecen como parte de otra
/// cosa. Con la política fail-closed, cada una de ellas era un alta imposible
/// hasta que alguien se acordara de darle su fila, y el síntoma no era «falta
/// configurar» sino «no se puede guardar».
///
/// Una fila `resource_domain = "*"` es el plan por defecto del tenant: se usa
/// solo cuando no hay fila exacta, y **no es un cubo común** — cada dominio
/// recibe su propio contador derivado de ella (ver `counter_id`). Es una
/// plantilla, no una bolsa compartida.
pub const DEFAULT_QUOTA_DOMAIN: &str = "*";

/// La fila `domain_quota` que gobierna una operación, ya normalizada.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotaSpec {
    /// Id de la fila. Identifica la CONFIGURACIÓN, y es lo que se enseña.
    pub id: String,
    /// Contra qué contador se debita. Normalmente es `id`, y entonces la
    /// distinción no se nota. Deja de serlo en los dos casos derivados:
    ///
    ///   · **Ciclo renovado** — `{id}#{periodo}`. La fila trae el periodo
    ///     anterior y su estrategia dice que renueva, así que el ciclo en curso
    ///     estrena contador sin que nadie haya tenido que crear una fila.
    ///   · **Cuota por defecto** — `{id}#{dominio}`. Una sola fila `*` gobierna
    ///     muchos dominios, y cada uno cuenta por separado.
    ///
    /// Antes esto era siempre `id` porque se daba por hecho que existía una fila
    /// por periodo y por dominio. Cuando no existía —y no existía casi nunca—,
    /// el resultado no era «sin límite»: era `Quota001` y nada se podía crear.
    pub counter_id: String,
    pub max_limit: i64,
    /// `current_usage` de la fila EAV. Valor de arranque del contador, no una
    /// cifra sobre la que decidir — ver la cabecera de este archivo.
    ///
    /// Es cero en los casos derivados: el consumo del ciclo cerrado no es el del
    /// que empieza, y el de la fila `*` no es el de este dominio. Sembrar con
    /// ellos regalaría o cobraría consumo ajeno.
    pub seed_usage: i64,
    pub period_key: String,
}

/// Trait para abstraer las consultas OLTP y permitir mockearlas en tests.
#[async_trait::async_trait]
pub trait OltpQueryRunner: Send + Sync {
    async fn run_oltp_query(&self, tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError>;
}

#[async_trait::async_trait]
impl OltpQueryRunner for crate::aegis::oltp::executor::OltpExecutor {
    async fn run_oltp_query(&self, tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        self.run_oltp_query(tenant_id, ast_ir).await
    }
}

/// Lo que identifica una resolución: tenant, dominio y tipo de límite.
type CacheKey = (String, String, String);

/// Localiza la cuota vigente de un dominio, con memoria corta.
pub struct QuotaResolver<E> {
    oltp: E,
    /// (tenant, dominio, tipo) → cuota vigente y cuándo se resolvió.
    ///
    /// Solo se guardan los aciertos. Un tenant sin cuota configurada es un
    /// tenant que ya está fallando, así que ahorrarle la consulta no compensa
    /// que su alta tarde medio minuto en surtir efecto después de crearle el
    /// plan.
    cache: Mutex<HashMap<CacheKey, (Instant, QuotaSpec)>>,
    ttl: Duration,
}

impl<E: OltpQueryRunner> QuotaResolver<E> {
    pub fn new(oltp: E) -> Self {
        let ttl = std::env::var("QUOTA_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CACHE_TTL_SECS);
        Self::with_ttl(oltp, Duration::from_secs(ttl))
    }

    /// `ttl` cero desactiva la memoria: cada resolución consulta.
    pub fn with_ttl(oltp: E, ttl: Duration) -> Self {
        QuotaResolver {
            oltp,
            cache: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Acceso al ejecutor para quien además necesite consultar otra cosa.
    pub fn oltp(&self) -> &E {
        &self.oltp
    }

    /// La cuota activa hoy, o `None` si el dominio no tiene ninguna configurada
    /// para el periodo en curso.
    ///
    /// `None` no es un error de infraestructura: significa «este tenant no
    /// tiene plan para esto». Cada llamante decide qué hacer con ello —el paso
    /// IOP lo convierte en `QUOTA_001`, el servicio gRPC en
    /// `QUOTA_NOT_CONFIGURED`—, y por eso no se decide aquí.
    pub async fn active_quota(
        &self,
        tenant_id: &str,
        resource_domain: &str,
        limit_type: &str,
    ) -> Result<Option<QuotaSpec>, DomainError> {
        let key = (
            tenant_id.to_string(),
            resource_domain.to_string(),
            limit_type.to_string(),
        );

        if let Some(spec) = self.cached(&key) {
            return Ok(Some(spec));
        }

        let today = chrono::Utc::now().date_naive();

        let spec = match self
            .query_domain(tenant_id, resource_domain, limit_type, today)
            .await?
        {
            Some(spec) => Some(spec),
            // Sin fila propia, el plan por defecto del tenant. Solo aquí: es una
            // segunda consulta y no debe pagarla quien sí tiene la suya. Y como
            // este camino era el que devolvía `Quota001`, cambiar de «no hay
            // nada» a «hay una consulta más» no le quita rendimiento a nadie.
            None if resource_domain != DEFAULT_QUOTA_DOMAIN => self
                .query_domain(tenant_id, DEFAULT_QUOTA_DOMAIN, limit_type, today)
                .await?
                .map(|plantilla| derive_for_domain(plantilla, resource_domain)),
            None => None,
        };

        if let Some(spec) = &spec {
            self.remember(key, spec.clone());
        }
        Ok(spec)
    }

    /// Las filas de un dominio, ya reducidas a la que gobierna hoy.
    async fn query_domain(
        &self,
        tenant_id: &str,
        resource_domain: &str,
        limit_type: &str,
        today: NaiveDate,
    ) -> Result<Option<QuotaSpec>, DomainError> {
        let ast = json!({
            "entity": QUOTA_ENTITY,
            "select": ["id", "tenant_id", "resource_domain", "limit_type", "max_limit", "current_usage", "period_key", "reset_strategy"],
            "where": [
                "and",
                ["=", "resource_domain", resource_domain],
                ["=", "limit_type", limit_type]
            ],
            "limit": MAX_QUOTA_ROWS
        });

        let result = self.oltp.run_oltp_query(tenant_id, &ast).await?;

        let rows = result.as_array().ok_or_else(|| {
            DomainError::new(
                ErrorCode::Infra001,
                "La consulta de cuotas no devolvió una lista",
            )
            .with_stage("quota")
        })?;

        Ok(pick_active(rows, today))
    }

    /// La resolución guardada, si todavía vale.
    ///
    /// Una entrada caducada se retira aquí mismo en vez de dejarla ocupando
    /// sitio: el mapa lo indexan tenant y dominio, así que sin esto crecería
    /// con cada tenant que pasara por el motor y no volviera.
    fn cached(&self, key: &CacheKey) -> Option<QuotaSpec> {
        if self.ttl.is_zero() {
            return None;
        }
        let mut cache = self.cache.lock().ok()?;
        match cache.get(key) {
            Some((resuelta, spec)) if resuelta.elapsed() < self.ttl => Some(spec.clone()),
            Some(_) => {
                cache.remove(key);
                None
            }
            None => None,
        }
    }

    fn remember(&self, key: CacheKey, spec: QuotaSpec) {
        if self.ttl.is_zero() {
            return;
        }
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(key, (Instant::now(), spec));
        }
    }
}

/// La fila que gobierna hoy.
///
/// DOS PASADAS, Y EL ORDEN IMPORTA
/// ───────────────────────────────
/// 1. La fila cuyo periodo cubre hoy. Es la respuesta de siempre y sigue
///    mandando: si existe, no se deriva nada.
/// 2. Solo si no hay ninguna, la fila más reciente cuya estrategia dice que
///    **renueva** —diaria, mensual, anual—, con el ciclo en curso calculado.
///
/// La segunda pasada existe porque el ciclo no lo rotaba nadie. El motor espera
/// una fila por periodo (`quota/ledger.rs`: el contador cuelga del id de la
/// fila) y no hay ningún proceso que la cree al cambiar el mes, así que una
/// cuota `MONTHLY` funcionaba hasta fin de ciclo y después dejaba al tenant sin
/// poder crear nada de ese dominio. El síntoma —`Quota001: No quota
/// configured`— es idéntico al de no haberla configurado nunca, y por eso
/// costaba verlo: la cuota estaba, y estaba bien; lo que había pasado es que
/// era de agosto.
///
/// Renovar aquí no relaja el techo: el límite es el de la fila, y el contador
/// del ciclo nuevo arranca en cero porque el consumo del ciclo cerrado no es de
/// este. Que es exactamente lo que habría pasado si alguien hubiera creado la
/// fila a mano.
///
/// De la primera pasada se queda con la primera coincidencia y no comprueba si
/// hay más: dos filas activas para el mismo (dominio, tipo) son un error de
/// configuración, y elegir una cualquiera de forma estable es mejor que fallar
/// la operación del usuario por ello.
pub fn pick_active(rows: &[Value], today: NaiveDate) -> Option<QuotaSpec> {
    if let Some(vigente) = rows.iter().find_map(|row| exact_match(row, today)) {
        return Some(vigente);
    }

    renew_latest(rows, today)
}

/// La fila tal cual, si su periodo cubre hoy.
fn exact_match(row: &Value, today: NaiveDate) -> Option<QuotaSpec> {
    let period_key = field(row, "period_key")?.as_str()?;
    if !covers(period_key, today) {
        return None;
    }

    let id = row_id(row)?;

    Some(QuotaSpec {
        counter_id: id.clone(),
        id,
        max_limit: field(row, "max_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        seed_usage: field(row, "current_usage")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        period_key: period_key.to_string(),
    })
}

/// El ciclo en curso de la fila renovable más reciente.
///
/// «Más reciente» por fecha de inicio del periodo: es la que lleva el límite que
/// alguien configuró la última vez, y volver a una anterior degradaría el plan
/// del tenant en silencio.
///
/// SI DESPUÉS ALGUIEN CREA LA FILA DEL CICLO
/// El panel también sabe abrir el periodo nuevo. Cuando lo hace, esa fila pasa a
/// ganar por la primera pasada y el contador cambia del derivado al suyo, que
/// nace en cero: el consumo apuntado mientras tanto se queda atrás y el tenant
/// recibe, una vez, algo de margen de más. Se acepta a propósito — el error va
/// en la dirección de no bloquear a nadie, es acotado por ciclo, y la
/// alternativa —fabricar aquí la fila— convertiría una LECTURA en una escritura
/// en el camino caliente de cada CREATE.
fn renew_latest(rows: &[Value], today: NaiveDate) -> Option<QuotaSpec> {
    let mut mejor: Option<(String, QuotaSpec)> = None;

    for row in rows {
        let Some(period_key) = field(row, "period_key").and_then(|v| v.as_str()) else {
            continue;
        };
        let estrategia = field(row, "reset_strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // El periodo vigente solo se deriva hacia DELANTE. Una fila del ciclo
        // que viene todavía no gobierna, y adelantarla daría techo nuevo antes
        // de tiempo.
        let Some(actual) = renewed_period(estrategia, today) else {
            continue;
        };
        let Some(inicio) = period_key.split('_').next() else {
            continue;
        };
        if inicio >= actual.as_str() {
            continue;
        }

        let Some(id) = row_id(row) else { continue };

        let spec = QuotaSpec {
            counter_id: format!("{id}#{actual}"),
            id,
            max_limit: field(row, "max_limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            // Ciclo nuevo, cuenta nueva.
            seed_usage: 0,
            period_key: actual,
        };

        if mejor
            .as_ref()
            .is_none_or(|(prev, _)| inicio > prev.as_str())
        {
            mejor = Some((inicio.to_string(), spec));
        }
    }

    if let Some((_, spec)) = &mejor {
        warn!(
            quota_id = %spec.id,
            periodo = %spec.period_key,
            "[QuotaResolver] Ciclo renovado sobre la marcha — no había fila para el periodo en curso"
        );
    }

    mejor.map(|(_, spec)| spec)
}

/// Adapta la cuota POR DEFECTO del tenant a un dominio concreto.
///
/// El techo y el ciclo son los de la fila `*`; lo que cambia es contra qué
/// contador se apunta, para que cada dominio gaste el suyo. La fila `*` describe
/// el plan; no es una bolsa común.
fn derive_for_domain(plantilla: QuotaSpec, resource_domain: &str) -> QuotaSpec {
    QuotaSpec {
        counter_id: format!("{}#{}", plantilla.counter_id, resource_domain),
        // El consumo de la fila `*` no es el de este dominio.
        seed_usage: 0,
        ..plantilla
    }
}

/// El id de la fila, o nada.
///
/// Sin id no hay contador que tocar: la clave sería `T#{tenant}#QC#` y todas las
/// cuotas sin id del tenant compartirían cuenta. Antes se resolvía con
/// `unwrap_or("")` y se debitaba contra ese contador basura.
fn row_id(row: &Value) -> Option<String> {
    match field(row, "id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        Some(id) => Some(id.to_string()),
        None => {
            warn!(
                fila = %row,
                "[QuotaResolver] Fila de cuota sin id — se ignora, no hay contador al que apuntar"
            );
            None
        }
    }
}

/// El `period_key` del ciclo que corre hoy, según la estrategia de reinicio.
///
/// `None` para `FIXED` y para cualquier cosa que no se reconozca: una cuota que
/// no renueva no se renueva sola, y ante una estrategia desconocida lo seguro es
/// no inventarse un ciclo. El formato es el mismo que escribe el panel
/// —inicio de ciclo a inicio del siguiente—, para que las dos partes nombren el
/// mismo periodo.
pub fn renewed_period(reset_strategy: &str, today: NaiveDate) -> Option<String> {
    let (inicio, fin) = match reset_strategy.to_uppercase().as_str() {
        "DAILY" => (today, today.succ_opt()?),
        "MONTHLY" => {
            let inicio = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)?;
            let fin = if today.month() == 12 {
                NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)?
            } else {
                NaiveDate::from_ymd_opt(today.year(), today.month() + 1, 1)?
            };
            (inicio, fin)
        }
        "YEARLY" => (
            NaiveDate::from_ymd_opt(today.year(), 1, 1)?,
            NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)?,
        ),
        _ => return None,
    };

    Some(format!(
        "{}_{}",
        inicio.format("%Y-%m-%d"),
        fin.format("%Y-%m-%d")
    ))
}

/// ¿Este `period_key` cubre el día de hoy?
///
/// `LIFETIME` no caduca. El resto es `INICIO_FIN` en `%Y-%m-%d` con rango
/// semiabierto `[inicio, fin)`, para que el último día de un ciclo y el primero
/// del siguiente no cuenten los dos.
pub fn covers(period_key: &str, today: NaiveDate) -> bool {
    if period_key == "LIFETIME" {
        return true;
    }

    let Some((start, end)) = period_key.split_once('_') else {
        return false;
    };

    match (
        NaiveDate::parse_from_str(start, "%Y-%m-%d"),
        NaiveDate::parse_from_str(end, "%Y-%m-%d"),
    ) {
        (Ok(start), Ok(end)) => start <= today && today < end,
        _ => false,
    }
}

/// Aegis devuelve las columnas unas veces cualificadas (`domain_quota/max_limit`)
/// y otras desnudas (`max_limit`), según el camino de lectura que resolvió la
/// consulta. Cada llamante tenía su propia mezcla de `get` y `or_else`, y no la
/// misma: aquí hay una sola forma de leer una columna.
fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    row.get(format!("{QUOTA_ENTITY}/{name}").as_str())
        .or_else(|| row.get(name))
}

#[cfg(test)]
#[path = "tests/resolver_tests.rs"]
mod tests;
