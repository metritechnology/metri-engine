//! Pruebas unitarias de `quota::ledger` sobre `MbfStorageClient`.
use super::*;

const TABLA: &str = "metri-mbf-local";

async fn ledger() -> QuotaLedger {
    QuotaLedger::new(Arc::new(DynamoClient::new_stub()), TABLA)
}

fn cuota_nueva(prefijo: &str) -> String {
    format!("{prefijo}_{}", ulid::Ulid::new())
}

async fn usage(l: &QuotaLedger, tenant: &str, quota: &str) -> i64 {
    l.settle(tenant, quota, 0).await.unwrap()
}

#[tokio::test]
async fn el_mismo_apunte_no_se_cuenta_dos_veces() {
    let l = ledger().await;
    let quota = cuota_nueva("q_idem");
    let clave = format!("{quota}#final");

    assert_eq!(
        l.settle_once("tnt_test", &quota, 40, &clave).await.unwrap(),
        SettleOutcome::Applied
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 40);

    assert_eq!(
        l.settle_once("tnt_test", &quota, 40, &clave).await.unwrap(),
        SettleOutcome::AlreadyApplied
    );
    assert_eq!(
        usage(&l, "tnt_test", &quota).await,
        40,
        "el contador no se movió"
    );
}

#[tokio::test]
async fn dos_replicas_reintegrando_a_la_vez_solo_apuntan_una() {
    let l = ledger().await;
    let quota = cuota_nueva("q_carrera");
    let clave = format!("{quota}#final");

    l.settle("tnt_test", &quota, 100).await.unwrap();

    let (a, b) = tokio::join!(
        l.settle_once("tnt_test", &quota, -40, &clave),
        l.settle_once("tnt_test", &quota, -40, &clave),
    );

    let resultados = [a.unwrap(), b.unwrap()];
    assert_eq!(
        resultados
            .iter()
            .filter(|r| **r == SettleOutcome::Applied)
            .count(),
        1,
        "exactamente una de las dos aplica: {resultados:?}"
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 60);
}

#[tokio::test]
async fn devolver_de_mas_deja_el_contador_en_cero() {
    let l = ledger().await;
    let quota = cuota_nueva("q_clamp");
    let clave = format!("{quota}#final");

    l.settle("tnt_test", &quota, 3).await.unwrap();

    assert_eq!(
        l.settle_once("tnt_test", &quota, -10, &clave)
            .await
            .unwrap(),
        SettleOutcome::Applied
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 0);

    assert_eq!(
        l.settle_once("tnt_test", &quota, -10, &clave)
            .await
            .unwrap(),
        SettleOutcome::AlreadyApplied
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 0);
}

#[tokio::test]
async fn claves_distintas_se_aplican_las_dos() {
    let l = ledger().await;
    let quota = cuota_nueva("q_claves");

    l.settle_once("tnt_test", &quota, 30, &format!("{quota}#debit"))
        .await
        .unwrap();
    l.settle_once("tnt_test", &quota, -12, &format!("{quota}#final"))
        .await
        .unwrap();

    assert_eq!(usage(&l, "tnt_test", &quota).await, 18);
}

#[tokio::test]
async fn el_seed_solo_cuenta_la_primera_vez() {
    let l = ledger().await;
    let quota = cuota_nueva("q_seed");

    let primero = l.try_debit("tnt_test", &quota, 1, 100, 7).await.unwrap();
    assert_eq!(
        primero,
        DebitOutcome::Debited { new_usage: 8 },
        "arranca en el seed"
    );

    let segundo = l.try_debit("tnt_test", &quota, 1, 100, 999).await.unwrap();
    assert_eq!(
        segundo,
        DebitOutcome::Debited { new_usage: 9 },
        "el seed ya no interviene"
    );
}
