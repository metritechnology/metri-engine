//! Tests for `quota::ledger`.
use super::*;
use aws_sdk_dynamodb::types::error::TransactionCanceledException;
use aws_sdk_dynamodb::types::CancellationReason;

// ─────────────────────────────────────────────────────────────────────────────
// Lectura de la cancelación
//
// Todo `settle_once` depende de leer bien por qué canceló DynamoDB, y el orden
// de los items de la transacción es lo único que distingue «ya estaba aplicado»
// de «no hay tanto que devolver». Es un contrato implícito entre dos funciones,
// así que se prueba explícitamente.
// ─────────────────────────────────────────────────────────────────────────────

fn cancelada(codigos: &[&str]) -> TransactWriteItemsError {
    let mut ex = TransactionCanceledException::builder();
    for c in codigos {
        let mut razon = CancellationReason::builder();
        if *c != "None" {
            razon = razon.code(*c);
        }
        ex = ex.cancellation_reasons(razon.build());
    }
    TransactWriteItemsError::TransactionCanceledException(ex.build())
}

#[test]
fn la_marca_existente_se_lee_como_ya_aplicado() {
    assert_eq!(
        classify(&cancelada(&["None", "ConditionalCheckFailed"])),
        Cancellation::AlreadyApplied
    );
}

#[test]
fn el_techo_del_contador_se_lee_como_saldo_insuficiente() {
    assert_eq!(
        classify(&cancelada(&["ConditionalCheckFailed", "None"])),
        Cancellation::WouldGoNegative
    );
}

/// Cuando fallan las dos condiciones manda la marca: si el apunte ya estaba
/// hecho, que el contador no tenga saldo para repetirlo es la consecuencia, no
/// la causa. Leerlo al revés aplastaría el contador a cero por un reintento.
#[test]
fn si_fallan_las_dos_manda_la_marca() {
    assert_eq!(
        classify(&cancelada(&[
            "ConditionalCheckFailed",
            "ConditionalCheckFailed"
        ])),
        Cancellation::AlreadyApplied
    );
}

#[test]
fn la_contencion_se_reintenta() {
    assert_eq!(
        classify(&cancelada(&["TransactionConflict", "None"])),
        Cancellation::Conflict
    );
    assert_eq!(
        classify(&cancelada(&["None", "TransactionConflict"])),
        Cancellation::Conflict
    );
}

#[test]
fn lo_demas_no_se_interpreta() {
    assert_eq!(
        classify(&cancelada(&["ValidationError", "None"])),
        Cancellation::Other
    );
    assert_eq!(classify(&cancelada(&[])), Cancellation::Other);
}

#[test]
fn el_backoff_crece_y_esta_acotado() {
    for intento in 0..TX_MAX_ATTEMPTS {
        let espera = backoff_ms(intento);
        let base = 25_u64 << intento;
        assert!(
            espera >= base && espera <= base * 2,
            "intento {intento}: {espera} ms"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Contra DynamoDB Local
//
// `make infra` levanta el contenedor; estos tests se ejecutan con
// `cargo test --lib quota:: -- --ignored`.
// ─────────────────────────────────────────────────────────────────────────────

const TABLA: &str = "metri-eav-local";

async fn ledger() -> QuotaLedger {
    std::env::set_var("DYNAMODB_ENDPOINT", "http://localhost:8000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    QuotaLedger::new(Arc::new(DynamoClient::new(TABLA).await), TABLA)
}

/// Cada test estrena cuota y claves: así son repetibles sin limpiar la tabla.
fn cuota_nueva(prefijo: &str) -> String {
    format!("{prefijo}_{}", ulid::Ulid::new())
}

/// El valor vigente del contador. `settle` con delta cero no cambia nada.
async fn usage(l: &QuotaLedger, tenant: &str, quota: &str) -> i64 {
    l.settle(tenant, quota, 0).await.unwrap()
}

#[tokio::test]
#[ignore]
async fn el_mismo_apunte_no_se_cuenta_dos_veces() {
    let l = ledger().await;
    let quota = cuota_nueva("q_idem");
    let clave = format!("{quota}#final");

    assert_eq!(
        l.settle_once("tnt_test", &quota, 40, &clave).await.unwrap(),
        SettleOutcome::Applied
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 40);

    // El reintento de quien no supo si llegó a aplicarse.
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

/// El caso que obliga a que esto exista: dos réplicas creen que les toca cerrar
/// la misma reserva. Sin la marca, el reintegro se aplicaría dos veces y el
/// tenant recuperaría el doble de lo que reservó.
#[tokio::test]
#[ignore]
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

/// Devolver más de lo apuntado deja el contador en cero, nunca en negativo: un
/// contador negativo es cuota regalada en el siguiente débito.
#[tokio::test]
#[ignore]
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

    // Y el aplastamiento también quedó marcado: repetirlo no lo repite.
    assert_eq!(
        l.settle_once("tnt_test", &quota, -10, &clave)
            .await
            .unwrap(),
        SettleOutcome::AlreadyApplied
    );
    assert_eq!(usage(&l, "tnt_test", &quota).await, 0);
}

/// La marca es por clave, no por cuota: el débito y el reintegro de una misma
/// reserva son dos apuntes distintos y los dos tienen que poder aplicarse.
#[tokio::test]
#[ignore]
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

/// El `seed` solo entra en juego mientras el contador no existe. Es lo que
/// impide que activar el contador sobre un tenant en marcha le regale todo lo
/// que ya había gastado.
#[tokio::test]
#[ignore]
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

// ─────────────────────────────────────────────────────────────────────────────
// La condición del techo
//
// Estos tests existen porque no había ninguno: los que ejercitan cuota
// sustituyen `QuotaCounter` por un doble, así que la `UpdateItem` real nunca se
// formaba y su ConditionExpression podía ser sintácticamente imposible sin que
// nada lo notara. Lo era —usaba `if_not_exists`, que es de UpdateExpression— y
// el resultado en producción fue que NINGUNA entidad con cuota se podía crear.
// ─────────────────────────────────────────────────────────────────────────────

/// Las seis funciones que la gramática de condición de DynamoDB admite.
/// Cualquier otra produce `ValidationException` en tiempo de petición, que es
/// exactamente el fallo que estos tests impiden repetir.
const FUNCIONES_LEGALES: &[&str] = &[
    "attribute_exists",
    "attribute_not_exists",
    "attribute_type",
    "begins_with",
    "contains",
    "size",
];

/// Los nombres de función que aparecen en una expresión, por la forma `nombre(`.
fn funciones_de(expr: &str) -> Vec<String> {
    let mut encontradas = Vec::new();
    for (i, _) in expr.match_indices('(') {
        let inicio = expr[..i]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map_or(0, |p| p + 1);
        let nombre = &expr[inicio..i];
        if !nombre.is_empty() {
            encontradas.push(nombre.to_string());
        }
    }
    encontradas
}

#[test]
fn la_condicion_solo_usa_funciones_validas_en_una_condition_expression() {
    // Las dos ramas, porque la expresión depende de `seed`: si solo se mirara
    // una, la otra podría llevar años rota sin que nadie la ejecutara.
    for (seed, max) in [(0_i64, 100_i64), (100, 100), (500, 100)] {
        let expr = debit_condition(seed, max);
        for f in funciones_de(expr) {
            assert!(
                FUNCIONES_LEGALES.contains(&f.as_str()),
                "«{f}» no es válida en una ConditionExpression (seed={seed}, max={max}): {expr}"
            );
        }
        assert!(
            !expr.contains("if_not_exists"),
            "`if_not_exists` es de UpdateExpression y aquí produce ValidationException: {expr}"
        );
    }
}

#[test]
fn sin_contador_previo_manda_el_arranque() {
    // El item todavía no existe, así que la condición tiene que dejar pasar la
    // rama de ausencia: el valor de arranque cabe bajo el techo.
    let expr = debit_condition(0, 100);
    assert!(
        expr.contains("attribute_not_exists(#n)"),
        "un contador que aún no existe debe poder crearse: {expr}"
    );
    assert!(
        expr.contains("#n < :max"),
        "y el que ya existe sigue con techo: {expr}"
    );
}

#[test]
fn un_arranque_que_ya_rebasa_el_techo_no_crea_contador() {
    // `seed` es el `current_usage` que traía la fila `domain_quota`. Si ya
    // llega agotado, activar el contador no puede regalar una unidad: sobre un
    // item ausente `#n < :max` es falso, y DynamoDB rechaza.
    for (seed, max) in [(100_i64, 100_i64), (500, 100)] {
        let expr = debit_condition(seed, max);
        assert_eq!(
            expr, "#n < :max",
            "con el arranque ya en el techo la rama de ausencia debe fallar (seed={seed}, max={max})"
        );
    }
}
