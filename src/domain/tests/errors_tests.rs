use super::*;

#[test]
fn test_error_code_canonical_lookup() {
    assert_eq!(ErrorCode::Janus400.canonical_code(), "JANUS_400");
    assert_eq!(ErrorCode::Eav002.canonical_code(), "EAV_005");
    assert_eq!(ErrorCode::Auth403.canonical_code(), "GRPC_AUTH_004");
    assert_eq!(ErrorCode::Quota001.canonical_code(), "QUOTA_001");
    assert_eq!(ErrorCode::Infra002.canonical_code(), "INFRA_S3_001");
    assert_eq!(ErrorCode::CodMat001.canonical_code(), "COD_MAT_001");
}

#[test]
fn test_error_code_stage_resolution() {
    assert_eq!(ErrorCode::Janus400.stage(), "janus");
    assert_eq!(ErrorCode::Aeg001.stage(), "aegis");
    assert_eq!(ErrorCode::Eav001.stage(), "eav");
    assert_eq!(ErrorCode::Cod001.stage(), "codice");
    assert_eq!(ErrorCode::Iop001.stage(), "iop");
    assert_eq!(ErrorCode::Quota001.stage(), "quota");
}

#[test]
fn test_domain_error_retryability() {
    // JnsTx001 is retryable
    let err_retryable = DomainError::new(ErrorCode::JnsTx001, "Optimistic lock error");
    assert!(err_retryable.retryable);

    // Janus400 is not retryable
    let err_non_retryable = DomainError::new(ErrorCode::Janus400, "Bad input syntax");
    assert!(!err_non_retryable.retryable);
}

#[test]
fn test_domain_error_constructors() {
    let err = DomainError::codice(ErrorCode::Cod001, "Registry parse error")
        .with_stage("custom_stage")
        .with_context(serde_json::json!({"file": "test.json"}));

    assert_eq!(err.code, ErrorCode::Cod001);
    assert_eq!(err.stage, "custom_stage");
    assert_eq!(err.detail, "Registry parse error");
    assert_eq!(
        err.context().cloned(),
        Some(serde_json::json!({"file": "test.json"}))
    );
}
