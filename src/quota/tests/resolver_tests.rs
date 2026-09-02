use super::*;
use serde_json::json;
use std::sync::Mutex;

fn hoy() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 8, 31).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Qué periodo cubre hoy
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn lifetime_no_caduca() {
    assert!(covers("LIFETIME", hoy()));
}

/// El rango es semiabierto: el día de cierre pertenece al ciclo siguiente. Si
/// fuera cerrado, el último día de un periodo y el primero del otro contarían
/// los dos, y una misma operación podría debitar en cualquiera de ellos según
/// el orden en que Aegis devolviera las filas.
#[test]
fn el_rango_es_semiabierto() {
    assert!(
        covers("2026-08-31_2026-09-30", hoy()),
        "el día de inicio entra"
    );
    assert!(covers("2026-08-01_2026-09-01", hoy()));
    assert!(
        !covers("2026-08-01_2026-08-31", hoy()),
        "el día de cierre no entra"
    );
    assert!(
        !covers("2026-09-01_2026-10-01", hoy()),
        "aún no ha empezado"
    );
    assert!(!covers("2026-07-01_2026-08-01", hoy()), "ya cerró");
}

#[test]
fn un_period_key_mal_formado_no_cubre_nada() {
    assert!(!covers("", hoy()));
    assert!(!covers("basura", hoy()));
    assert!(!covers("2026-08-01", hoy()), "sin separador");
    assert!(!covers("2026-08-01_no-es-fecha", hoy()));
    assert!(!covers("2026-08-01_2026-09-01_extra", hoy()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Elegir la fila
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn elige_la_fila_del_periodo_en_curso() {
    let filas = vec![
        json!({"id": "q_viejo",  "max_limit": 10, "current_usage": 10, "period_key": "2026-07-01_2026-08-01"}),
        json!({"id": "q_activo", "max_limit": 50, "current_usage": 7,  "period_key": "2026-08-01_2026-09-01"}),
        json!({"id": "q_futuro", "max_limit": 99, "current_usage": 0,  "period_key": "2026-09-01_2026-10-01"}),
    ];

    let spec = pick_active(&filas, hoy()).expect("hay una fila vigente");
    assert_eq!(spec.id, "q_activo");
    assert_eq!(spec.max_limit, 50);
    assert_eq!(spec.seed_usage, 7);
    assert_eq!(spec.period_key, "2026-08-01_2026-09-01");
}

/// Aegis devuelve las columnas cualificadas o desnudas según el camino de
/// lectura que resolvió la consulta. Los dos llamantes de esto leían formas
/// distintas —y solo uno contemplaba las dos—, así que una misma fila daba
/// `max_limit = 0` en un sitio y el valor real en el otro.
#[test]
fn lee_las_columnas_cualificadas_y_las_desnudas() {
    let cualificada = vec![json!({
        "domain_quota/id":            "q_01",
        "domain_quota/max_limit":     30,
        "domain_quota/current_usage": 12,
        "domain_quota/period_key":    "LIFETIME"
    })];

    let spec = pick_active(&cualificada, hoy()).expect("fila vigente");
    assert_eq!(spec.id, "q_01");
    assert_eq!(spec.max_limit, 30);
    assert_eq!(spec.seed_usage, 12);
}

/// Una fila sin id no tiene contador al que apuntar: su clave sería
/// `T#{tenant}#QC#` y todas las cuotas rotas del tenant compartirían cuenta.
/// Antes se resolvía con `unwrap_or("")` y se debitaba contra ese contador.
#[test]
fn una_fila_sin_id_se_ignora() {
    let filas = vec![
        json!({"max_limit": 10, "current_usage": 0, "period_key": "LIFETIME"}),
        json!({"id": "", "max_limit": 10, "current_usage": 0, "period_key": "LIFETIME"}),
        json!({"id": "q_bueno", "max_limit": 10, "current_usage": 0, "period_key": "LIFETIME"}),
    ];

    let spec = pick_active(&filas, hoy()).expect("la tercera sí sirve");
    assert_eq!(spec.id, "q_bueno");
}

#[test]
fn sin_filas_vigentes_no_hay_cuota() {
    assert!(pick_active(&[], hoy()).is_none());

    let caducadas = vec![json!({"id": "q", "max_limit": 1, "period_key": "2020-01-01_2020-02-01"})];
    assert!(pick_active(&caducadas, hoy()).is_none());
}

/// Faltar `max_limit` deja el techo en cero, que es lo mismo que decir «sin
/// sitio»: el contador rechazará el débito. Cerrado es el fallo correcto para
/// una cuota mal configurada.
#[test]
fn una_fila_sin_techo_vale_cero() {
    let filas = vec![json!({"id": "q", "period_key": "LIFETIME"})];
    let spec = pick_active(&filas, hoy()).unwrap();
    assert_eq!(spec.max_limit, 0);
    assert_eq!(spec.seed_usage, 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// El resolutor completo
// ─────────────────────────────────────────────────────────────────────────────

struct OltpFalso {
    respuesta: Value,
    consultas: Mutex<Vec<Value>>,
}

impl OltpFalso {
    fn con(respuesta: Value) -> Self {
        OltpFalso {
            respuesta,
            consultas: Mutex::new(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl OltpQueryRunner for OltpFalso {
    async fn run_oltp_query(&self, _tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        self.consultas.lock().unwrap().push(ast_ir.clone());
        Ok(self.respuesta.clone())
    }
}

#[tokio::test]
async fn consulta_por_dominio_y_tipo_de_limite() {
    let oltp = OltpFalso::con(json!([
        {"id": "q_01", "max_limit": 5, "current_usage": 1, "period_key": "LIFETIME"}
    ]));
    let resolver = QuotaResolver::new(oltp);

    let spec = resolver
        .active_quota("tnt_01", "llm:aws:nova-pro", "TOKEN_COUNT")
        .await
        .unwrap()
        .expect("cuota vigente");

    assert_eq!(spec.id, "q_01");

    let consultas = resolver.oltp().consultas.lock().unwrap();
    let ast = &consultas[0];
    assert_eq!(ast["entity"], "domain_quota");
    assert_eq!(
        ast["where"][1],
        json!(["=", "resource_domain", "llm:aws:nova-pro"])
    );
    assert_eq!(ast["where"][2], json!(["=", "limit_type", "TOKEN_COUNT"]));
}

/// Sin cuota configurada no es un fallo de infraestructura: es una respuesta.
/// Cada llamante la traduce a su código —`QUOTA_001` en el paso IOP,
/// `QUOTA_NOT_CONFIGURED` en el servicio gRPC—, y por eso no se decide aquí.
#[tokio::test]
async fn sin_cuota_configurada_devuelve_none_no_error() {
    let resolver = QuotaResolver::new(OltpFalso::con(json!([])));
    let spec = resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    assert!(spec.is_none());
}

#[tokio::test]
async fn una_respuesta_que_no_es_lista_si_es_error() {
    let resolver = QuotaResolver::new(OltpFalso::con(json!({"error": "algo"})));
    let err = resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Infra001);
}

// ─────────────────────────────────────────────────────────────────────────────
// Memoria corta
//
// Esta consulta corre en cada CREATE y cada GET del motor. Recordarla es lo que
// deja el camino caliente en una sola escritura condicional.
// ─────────────────────────────────────────────────────────────────────────────

fn consultas(resolver: &QuotaResolver<OltpFalso>) -> usize {
    resolver.oltp().consultas.lock().unwrap().len()
}

#[tokio::test]
async fn una_cuota_resuelta_no_se_vuelve_a_consultar() {
    let oltp = OltpFalso::con(json!([
        {"id": "q_01", "max_limit": 5, "current_usage": 1, "period_key": "LIFETIME"}
    ]));
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::from_secs(30));

    for _ in 0..5 {
        let spec = resolver
            .active_quota("tnt_01", "asset", "WRITE_COUNT")
            .await
            .unwrap();
        assert_eq!(spec.unwrap().id, "q_01");
    }

    assert_eq!(consultas(&resolver), 1, "cinco operaciones, una consulta");
}

/// Cada (tenant, dominio, tipo) se recuerda por separado: si compartieran
/// entrada, un tenant vería el techo de otro.
#[tokio::test]
async fn la_memoria_no_mezcla_tenants_ni_dominios() {
    let oltp = OltpFalso::con(json!([
        {"id": "q_01", "max_limit": 5, "current_usage": 1, "period_key": "LIFETIME"}
    ]));
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::from_secs(30));

    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_02", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_01", "work_order", "WRITE_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_01", "asset", "READ_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();

    assert_eq!(
        consultas(&resolver),
        4,
        "cuatro claves distintas, la quinta repetía"
    );
}

/// Un tenant sin cuota configurada NO se recuerda: es un tenant que ya está
/// fallando, y ahorrarle la consulta no compensa que su alta tarde medio minuto
/// en surtir efecto después de crearle el plan.
#[tokio::test]
async fn la_ausencia_de_cuota_no_se_recuerda() {
    let resolver = QuotaResolver::with_ttl(
        OltpFalso::con(json!([])),
        std::time::Duration::from_secs(30),
    );

    resolver
        .active_quota("tnt_nuevo", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_nuevo", "asset", "WRITE_COUNT")
        .await
        .unwrap();

    // Dos por resolución: la del dominio y, al no haber nada, la de la cuota por
    // defecto del tenant (`*`). Esa segunda consulta solo la paga quien iba a
    // recibir `Quota001` de todas formas.
    assert_eq!(consultas(&resolver), 4, "se vuelve a preguntar");
}

#[tokio::test]
async fn con_ttl_cero_se_consulta_siempre() {
    let oltp = OltpFalso::con(json!([
        {"id": "q_01", "max_limit": 5, "current_usage": 1, "period_key": "LIFETIME"}
    ]));
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::ZERO);

    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();

    assert_eq!(consultas(&resolver), 2);
}

/// Una entrada caducada se retira al consultarla. Sin eso, el mapa crecería con
/// cada tenant que pasara por el motor y no volviera.
#[tokio::test]
async fn una_entrada_caducada_se_vuelve_a_consultar() {
    let oltp = OltpFalso::con(json!([
        {"id": "q_01", "max_limit": 5, "current_usage": 1, "period_key": "LIFETIME"}
    ]));
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::from_millis(20));

    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap();

    assert_eq!(consultas(&resolver), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// Ciclo renovado sobre la marcha
//
// El motor espera una fila por periodo y NADIE la crea al cambiar el mes. Una
// cuota `MONTHLY` funcionaba hasta fin de ciclo y después dejaba al tenant sin
// poder crear nada de ese dominio, con el mismo error que si no la hubiera
// configurado nunca. Eso es lo que cierran estos tests.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn el_periodo_en_curso_se_calcula_por_estrategia() {
    let hoy = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();

    assert_eq!(
        renewed_period("DAILY", hoy).unwrap(),
        "2026-08-31_2026-09-01"
    );
    assert_eq!(
        renewed_period("MONTHLY", hoy).unwrap(),
        "2026-08-01_2026-09-01"
    );
    assert_eq!(
        renewed_period("YEARLY", hoy).unwrap(),
        "2026-01-01_2027-01-01"
    );
}

/// Diciembre es el caso que rompe la aritmética ingenua de meses.
#[test]
fn el_ciclo_mensual_cruza_el_año() {
    let nochevieja = NaiveDate::from_ymd_opt(2026, 12, 15).unwrap();
    assert_eq!(
        renewed_period("MONTHLY", nochevieja).unwrap(),
        "2026-12-01_2027-01-01"
    );
}

/// `FIXED` no renueva: es su definición. Y una estrategia que no se reconoce
/// tampoco, porque inventarle un ciclo sería regalar techo por una errata.
#[test]
fn lo_que_no_renueva_no_se_renueva_solo() {
    let hoy = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();

    assert!(renewed_period("FIXED", hoy).is_none());
    assert!(renewed_period("", hoy).is_none());
    assert!(renewed_period("CADA_LUNA_LLENA", hoy).is_none());
}

#[test]
fn una_cuota_mensual_caducada_estrena_ciclo_en_vez_de_bloquear() {
    let filas = vec![json!({
        "id": "q_julio", "max_limit": 1000, "current_usage": 998,
        "period_key": "2026-07-01_2026-08-01", "reset_strategy": "MONTHLY"
    })];

    let spec = pick_active(&filas, hoy()).expect("el ciclo se renueva");

    assert_eq!(
        spec.id, "q_julio",
        "la configuración sigue siendo la misma fila"
    );
    assert_eq!(spec.period_key, "2026-08-01_2026-09-01");
    assert_eq!(spec.max_limit, 1000, "el techo es el que configuraron");
    assert_eq!(
        spec.counter_id, "q_julio#2026-08-01_2026-09-01",
        "contador propio del ciclo"
    );
    assert_eq!(spec.seed_usage, 0, "el consumo de julio no es el de agosto");
}

/// La fila del periodo en curso manda sobre cualquier renovación: si existe, es
/// la que alguien creó a propósito, con su propio contador.
#[test]
fn la_fila_vigente_gana_a_la_renovacion() {
    let filas = vec![
        json!({"id": "q_julio",  "max_limit": 10, "current_usage": 10, "period_key": "2026-07-01_2026-08-01", "reset_strategy": "MONTHLY"}),
        json!({"id": "q_agosto", "max_limit": 50, "current_usage": 7,  "period_key": "2026-08-01_2026-09-01", "reset_strategy": "MONTHLY"}),
    ];

    let spec = pick_active(&filas, hoy()).unwrap();

    assert_eq!(spec.id, "q_agosto");
    assert_eq!(spec.counter_id, "q_agosto", "sin derivar: la fila existe");
    assert_eq!(spec.seed_usage, 7);
}

/// Entre varias caducadas se renueva la más reciente: es la que lleva el límite
/// que se configuró la última vez. Volver a una anterior degradaría el plan del
/// tenant sin que nadie lo hubiera pedido.
#[test]
fn se_renueva_la_mas_reciente() {
    let filas = vec![
        json!({"id": "q_mayo",  "max_limit": 10,   "period_key": "2026-05-01_2026-06-01", "reset_strategy": "MONTHLY"}),
        json!({"id": "q_julio", "max_limit": 5000, "period_key": "2026-07-01_2026-08-01", "reset_strategy": "MONTHLY"}),
    ];

    let spec = pick_active(&filas, hoy()).unwrap();

    assert_eq!(spec.id, "q_julio");
    assert_eq!(spec.max_limit, 5000);
}

/// Una fila del ciclo que viene no gobierna todavía, y renovarla sería
/// adelantar techo. Solo se deriva hacia delante.
#[test]
fn una_fila_futura_no_se_adelanta() {
    let filas = vec![json!({
        "id": "q_septiembre", "max_limit": 10,
        "period_key": "2026-09-01_2026-10-01", "reset_strategy": "MONTHLY"
    })];

    assert!(pick_active(&filas, hoy()).is_none());
}

/// Una cuota `FIXED` con periodo cerrado se queda cerrada: alguien decidió que
/// no renueva, y renovarla aquí sería contradecirle.
#[test]
fn una_fija_caducada_sigue_caducada() {
    let filas = vec![json!({
        "id": "q", "max_limit": 10,
        "period_key": "2020-01-01_2021-01-01", "reset_strategy": "FIXED"
    })];

    assert!(pick_active(&filas, hoy()).is_none());
}

// ─────────────────────────────────────────────────────────────────────────────
// La cuota por defecto del tenant
// ─────────────────────────────────────────────────────────────────────────────

/// Responde según el dominio por el que se pregunte, para poder ejercitar el
/// camino de respaldo.
struct OltpPorDominio {
    por_dominio: std::collections::HashMap<String, Value>,
    consultas: Mutex<Vec<Value>>,
}

impl OltpPorDominio {
    fn con(pares: Vec<(&str, Value)>) -> Self {
        OltpPorDominio {
            por_dominio: pares.into_iter().map(|(d, v)| (d.to_string(), v)).collect(),
            consultas: Mutex::new(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl OltpQueryRunner for OltpPorDominio {
    async fn run_oltp_query(&self, _tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        self.consultas.lock().unwrap().push(ast_ir.clone());
        let dominio = ast_ir["where"][1][2].as_str().unwrap_or("").to_string();
        Ok(self
            .por_dominio
            .get(&dominio)
            .cloned()
            .unwrap_or_else(|| json!([])))
    }
}

#[tokio::test]
async fn un_dominio_sin_fila_cae_en_la_cuota_por_defecto() {
    // El caso del incidente: `form_template` no tenía fila y, siendo el motor
    // fail-closed, no se podía crear ni una plantilla.
    let oltp = OltpPorDominio::con(vec![(
        DEFAULT_QUOTA_DOMAIN,
        json!([{"id": "q_defecto", "max_limit": 100000, "current_usage": 4200, "period_key": "LIFETIME"}]),
    )]);
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::ZERO);

    let spec = resolver
        .active_quota("tnt_01", "form_template", "WRITE_COUNT")
        .await
        .unwrap()
        .expect("gobierna la cuota por defecto");

    assert_eq!(spec.max_limit, 100000);
    assert_eq!(
        spec.counter_id, "q_defecto#form_template",
        "cada dominio cuenta lo suyo"
    );
    assert_eq!(
        spec.seed_usage, 0,
        "el consumo de la fila `*` no es el de este dominio"
    );
}

/// La fila propia manda: la cuota por defecto es un respaldo, no un techo que se
/// superponga.
#[tokio::test]
async fn la_fila_propia_gana_a_la_cuota_por_defecto() {
    let oltp = OltpPorDominio::con(vec![
        (
            "asset",
            json!([{"id": "q_asset", "max_limit": 100, "current_usage": 3, "period_key": "LIFETIME"}]),
        ),
        (
            DEFAULT_QUOTA_DOMAIN,
            json!([{"id": "q_defecto", "max_limit": 100000, "period_key": "LIFETIME"}]),
        ),
    ]);
    let resolver = QuotaResolver::with_ttl(oltp, std::time::Duration::ZERO);

    let spec = resolver
        .active_quota("tnt_01", "asset", "WRITE_COUNT")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(spec.counter_id, "q_asset");
    assert_eq!(spec.max_limit, 100);
    assert_eq!(
        resolver.oltp().consultas.lock().unwrap().len(),
        1,
        "no se consulta el respaldo si no hace falta"
    );
}

/// Sin fila propia y sin cuota por defecto se sigue rechazando: la política
/// fail-closed no cambia, lo que cambia es que ahora hay una forma de
/// configurarla de una vez para todo el catálogo.
#[tokio::test]
async fn sin_respaldo_se_sigue_rechazando() {
    let resolver = QuotaResolver::with_ttl(OltpPorDominio::con(vec![]), std::time::Duration::ZERO);

    let spec = resolver
        .active_quota("tnt_01", "form_template", "WRITE_COUNT")
        .await
        .unwrap();

    assert!(spec.is_none());
}

/// El comodín no se busca a sí mismo: sería una consulta de más y, si alguien
/// preguntara por `*`, una recursión sin sentido.
#[tokio::test]
async fn el_comodin_no_se_respalda_a_si_mismo() {
    let resolver = QuotaResolver::with_ttl(OltpPorDominio::con(vec![]), std::time::Duration::ZERO);

    resolver
        .active_quota("tnt_01", DEFAULT_QUOTA_DOMAIN, "WRITE_COUNT")
        .await
        .unwrap();

    assert_eq!(resolver.oltp().consultas.lock().unwrap().len(), 1);
}
