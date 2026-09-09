//! Tests for `cedar::authn.rs`.
// cedar/tests/authn.rs — Autenticación: extracción de token y verificación HMAC.

use crate::cedar::cache::session::InMemorySessionStore;
use crate::cedar::request::AuthRequest;
use crate::cedar::{step1_extract_token, verify_hmac_token_local_in_step, HmacTokenVerifier};
use crate::domain::errors::ErrorCode;

#[tokio::test]
async fn test_token_missing() {
    let valkey = InMemorySessionStore::new();
    let req = tonic::Request::new(());
    let res = step1_extract_token(&AuthRequest::from_tonic(&req), &valkey).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Auth401);
}

#[test]
fn test_verify_hmac_token() {
    // El verificador se construye con el secreto explícito — sin tocar el
    // entorno global (set_var es una carrera entre tests en paralelo).
    let secret = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2";
    let verifier = HmacTokenVerifier::new(secret);
    let exp = chrono::Utc::now().timestamp() + 3600;
    let token = crate::cedar::authn::tests::mint_for_tests(
        &verifier,
        exp,
        "e2575d40e197f5180121dcfb9470b3dc",
        "system",
        "usr_system_bff",
    );

    // Verificación estricta del camino Cedar (skew 0).
    let res = verifier.verify(&token, 0);
    assert!(res.is_some());
    let verified = res.unwrap();
    assert_eq!(verified.tenant_id, "system");
    assert_eq!(verified.user_id, "usr_system_bff");

    // Un token firmado con un secreto distinto al de la configuración del
    // proceso jamás verifica en el envoltorio estricto del camino Cedar
    // (aserción determinista: el secreto de test es único en el proceso).
    let wrapper = crate::cedar::authn::tests::mint_for_tests(
        &verifier,
        chrono::Utc::now().timestamp() + 3600,
        "jti_2",
        "tnt_01",
        "usr_2",
    );
    assert!(verify_hmac_token_local_in_step(&wrapper).is_none());
}
