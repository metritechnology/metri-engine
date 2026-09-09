//! Tests for `codice::coercion`.
use super::*;
use serde_json::Value;

#[test]
fn test_coerce_integer() {
    let val = Value::String("12345".to_string());
    let coerced = coerce_value(&val, &AttrType::Number);
    assert_eq!(coerced, Some(Value::Number(12345.into())));
}

#[test]
fn test_coerce_float() {
    let val = Value::String("123.45".to_string());
    let coerced = coerce_value(&val, &AttrType::Decimal);
    assert!(coerced.is_some());
    let unwrapped = coerced.unwrap();
    assert_eq!(unwrapped.as_f64(), Some(123.45));
}

#[test]
fn test_coerce_invalid_number() {
    let val = Value::String("not_a_number".to_string());
    let coerced = coerce_value(&val, &AttrType::Number);
    assert_eq!(coerced, None);
}

#[test]
fn test_coerce_boolean_true() {
    let val = Value::String("TrUe".to_string());
    let coerced = coerce_value(&val, &AttrType::Boolean);
    assert_eq!(coerced, Some(Value::Bool(true)));
}

#[test]
fn test_coerce_boolean_false() {
    let val = Value::String("FALSE".to_string());
    let coerced = coerce_value(&val, &AttrType::Boolean);
    assert_eq!(coerced, Some(Value::Bool(false)));
}

#[test]
fn test_coerce_boolean_invalid() {
    let val = Value::String("not_a_bool".to_string());
    let coerced = coerce_value(&val, &AttrType::Boolean);
    assert_eq!(coerced, None);
}

#[test]
fn test_coerce_non_string_ignored() {
    let val = Value::Number(100.into());
    let coerced = coerce_value(&val, &AttrType::Boolean);
    assert_eq!(coerced, None);
}
