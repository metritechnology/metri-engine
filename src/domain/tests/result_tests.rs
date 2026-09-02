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

#[test]
fn test_unwrap_railway_success() {
    let ok_res: Railway<i32> = Ok(42);
    let val = unwrap_railway(ok_res, "unwrapping 42");
    assert_eq!(val, 42);
}

#[test]
#[should_panic(expected = "Cannot unwrap an error result")]
fn test_unwrap_railway_panic() {
    let err_res: Railway<i32> = Err(DomainError::new(ErrorCode::Janus400, "error detail"));
    unwrap_railway(err_res, "test panic");
}
