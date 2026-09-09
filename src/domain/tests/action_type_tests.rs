//! Tests for `domain::action_type`.
use super::*;

#[test]
fn ok_is_write() {
    assert_eq!(derive_action_type(true, None), ActionType::Write);
}

#[test]
fn cedar_deny_is_access_denied() {
    assert_eq!(
        derive_action_type(false, Some("cedar")),
        ActionType::AccessDenied
    );
}

#[test]
fn quota_is_quota_exhausted() {
    assert_eq!(
        derive_action_type(false, Some("quota")),
        ActionType::QuotaExhausted
    );
}

#[test]
fn janus_is_write_error() {
    assert_eq!(
        derive_action_type(false, Some("janus")),
        ActionType::WriteError
    );
}

#[test]
fn unknown_stage_is_unknown() {
    assert_eq!(
        derive_action_type(false, Some("infra")),
        ActionType::Unknown
    );
    assert_eq!(derive_action_type(false, None), ActionType::Unknown);
}
