//! Tests for `quota::projection`.
use super::*;

use serde_json::json;

use crate::quota::test_fakes::FakeCounter;

fn overlay(counter: Arc<FakeCounter>) -> QuotaUsageOverlay {
    QuotaUsageOverlay::new(counter)
}

/// La entidad es la clave con la que el ejecutor decide si este retoque le
/// toca a una fila. Una errata aquí no rompe nada: simplemente el retoque no se
/// aplicaría nunca y el panel se quedaría en la semilla para siempre.
#[test]
fn se_declara_para_domain_quota() {
    let o = overlay(Arc::new(FakeCounter::new()));
    assert_eq!(RowOverlay::entity(&o), "domain_quota");
}

#[tokio::test]
async fn el_consumo_lo_pone_el_contador_no_la_fila() {
    let counter = Arc::new(FakeCounter::at("q_01", 47));
    let o = overlay(Arc::clone(&counter));

    // La fila trae el valor semilla, que puede llevar meses sin tocarse.
    let mut filas = vec![json!({"id": "q_01", "max_limit": 100, "current_usage": 3})];
    o.apply("tnt_01", &mut filas).await;

    assert_eq!(filas[0]["current_usage"], 47);
    assert_eq!(filas[0]["max_limit"], 100, "lo demás se queda como estaba");
}

/// Aegis devuelve las columnas cualificadas o desnudas según el camino de
/// lectura; se retoca la que venga.
#[tokio::test]
async fn funciona_con_la_columna_cualificada() {
    let counter = Arc::new(FakeCounter::at("q_01", 47));
    let o = overlay(counter);

    let mut filas = vec![json!({
        "domain_quota/id": "q_01",
        "domain_quota/current_usage": 3
    })];
    o.apply("tnt_01", &mut filas).await;

    assert_eq!(filas[0]["domain_quota/current_usage"], 47);
}

/// Mientras no hay contador, el valor guardado ES el bueno: es el que
/// `try_debit` usará como semilla. Sustituirlo por un cero le regalaría al
/// tenant todo lo que gastó antes de que existiera el contador.
#[tokio::test]
async fn sin_contador_se_devuelve_el_valor_guardado() {
    let counter = Arc::new(FakeCounter::new());
    let o = overlay(counter);

    let mut filas = vec![json!({"id": "q_nuevo", "current_usage": 12})];
    o.apply("tnt_01", &mut filas).await;

    assert_eq!(filas[0]["current_usage"], 12);
}

/// Si el usuario no pidió la columna, no se le añade: devolverle un campo que
/// no seleccionó rompería a cualquiera que cuente columnas.
#[tokio::test]
async fn no_se_anade_una_columna_que_nadie_pidio() {
    let counter = Arc::new(FakeCounter::at("q_01", 47));
    let o = overlay(counter);

    let mut filas = vec![json!({"id": "q_01", "max_limit": 100})];
    o.apply("tnt_01", &mut filas).await;

    assert!(filas[0].get("current_usage").is_none());
}

/// El valor almacenado es viejo, no falso. Tumbar la consulta de un usuario
/// porque el contador no responde sería peor que enseñarle una cifra atrasada.
#[tokio::test]
async fn si_el_contador_no_responde_se_devuelve_lo_almacenado() {
    let counter = Arc::new(FakeCounter::at("q_01", 47));
    counter.break_it("contador caído");
    let o = overlay(counter);

    let mut filas = vec![json!({"id": "q_01", "current_usage": 3})];
    o.apply("tnt_01", &mut filas).await;

    assert_eq!(filas[0]["current_usage"], 3);
}

/// Una página entera se resuelve con una sola lectura, no con una por fila.
/// Con una por fila, listar las cuotas de un tenant costaría tantas lecturas
/// como cuotas tuviera.
#[tokio::test]
async fn una_pagina_entera_en_una_sola_lectura() {
    let counter = Arc::new(FakeCounter::new());
    counter.settle("tnt_01", "q_01", 10).await.unwrap();
    counter.settle("tnt_01", "q_02", 20).await.unwrap();
    let o = overlay(Arc::clone(&counter));

    let mut filas = vec![
        json!({"id": "q_01", "current_usage": 0}),
        json!({"id": "q_02", "current_usage": 0}),
        json!({"id": "q_sin_contador", "current_usage": 5}),
    ];
    o.apply("tnt_01", &mut filas).await;

    assert_eq!(filas[0]["current_usage"], 10);
    assert_eq!(filas[1]["current_usage"], 20);
    assert_eq!(filas[2]["current_usage"], 5);
    assert_eq!(counter.read_calls(), 1, "una llamada para las tres filas");
}

#[tokio::test]
async fn sin_filas_no_se_consulta_nada() {
    let counter = Arc::new(FakeCounter::new());
    let o = overlay(Arc::clone(&counter));

    let mut filas: Vec<Value> = vec![];
    o.apply("tnt_01", &mut filas).await;
    assert_eq!(counter.read_calls(), 0);

    let mut sin_columna = vec![json!({"id": "q_01", "max_limit": 10})];
    o.apply("tnt_01", &mut sin_columna).await;
    assert_eq!(counter.read_calls(), 0, "tampoco si nadie pidió el consumo");
}
