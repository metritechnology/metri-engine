use crate::aegis::label_template::*;
use serde_json::json;

#[test]
fn test_format_value() {
    assert_eq!(format_value(None), "");
    assert_eq!(format_value(Some(&json!(null))), "");
    assert_eq!(format_value(Some(&json!(42))), "42");
    assert_eq!(format_value(Some(&json!(45.234))), "45.23");
    assert_eq!(format_value(Some(&json!(45.0))), "45");
    assert_eq!(format_value(Some(&json!("Pump A"))), "Pump A");
}

#[test]
fn test_interpolate() {
    let row = json!({
        "asset_name": "Pump A",
        "area_value": 45.2
    });
    let res = interpolate("{{asset_name}} - {{area_value}} KW", &row);
    assert_eq!(res.unwrap(), "Pump A - 45.20 KW");

    let res2 = interpolate("missing {{foo}}", &row);
    assert_eq!(res2.unwrap(), "missing ");
}

#[test]
fn test_extract_fields() {
    let fields = extract_fields("{{asset_name}} - {{area_value}} KW");
    assert_eq!(fields, vec!["asset_name", "area_value"]);
}
