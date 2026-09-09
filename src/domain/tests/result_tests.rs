//! Tests for `domain::result`.
use super::*;
use crate::domain::errors::{DomainError, ErrorCode};

#[test]
fn test_is_ok_and_is_error() {
    let ok_res: Railway<i32> = Ok(100);
    let err_res: Railway<i32> = Err(DomainError::new(ErrorCode::Janus400, "error detail"));

    assert!(is_ok(&ok_res));
    assert!(!is_error(&ok_res));

    assert!(is_error(&err_res));
    assert!(!is_ok(&err_res));
}
