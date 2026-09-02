use super::*;
use serde_json::json;

#[test]
fn test_json_to_value_and_back() {
    let original = json!({
        "name": "hvac_system",
        "is_active": true,
        "temperature": 22.5,
        "metadata": {
            "vendor": "carrier",
            "version": 1
        },
        "tags": ["critical", "cooling"]
    });

    // Translate to prost_types::Struct
    let proto_struct = value_to_struct(&original);

    // Translate back to serde_json::Value
    let converted = struct_to_value(proto_struct);

    assert_eq!(converted, original);
}

#[test]
fn test_value_to_json_null() {
    let proto_val = prost_types::Value {
        kind: Some(prost_types::value::Kind::NullValue(0)),
    };
    let json_val = value_to_json(proto_val);
    assert_eq!(json_val, Value::Null);
}
