use crate::aegis::pagination::*;
use serde_json::json;
use serde_json::Value;

#[test]
fn decode_cursor_none_returns_defaults() {
    assert_eq!(decode_cursor(None, 50), (0, 50));
    assert_eq!(decode_cursor(Some(""), 50), (0, 50));
}

#[test]
fn encode_decode_roundtrip() {
    let cursor = encode_cursor(10, 20);
    assert_eq!(decode_cursor(Some(&cursor), 50), (10, 20));
}

#[test]
fn build_pagination_first_page() {
    let pag = build_pagination(0, 10, 100);
    assert_eq!(pag["has_next"], json!(true));
    assert_eq!(pag["has_previous"], json!(false));
    assert!(pag["next_cursor"].is_string());
    assert!(pag.get("previous_cursor").map(|v| v.is_null()).unwrap_or(true));
}

#[test]
fn build_pagination_last_page() {
    let pag = build_pagination(90, 10, 100);
    assert_eq!(pag["has_next"], json!(false));
    assert_eq!(pag["has_previous"], json!(true));
    assert!(pag.get("next_cursor").map(|v| v.is_null()).unwrap_or(true));
}

#[test]
fn paginate_rows_slices_correctly() {
    let rows: Vec<Value> = (0..10).map(|i| json!({"n": i})).collect();
    let page = paginate_rows(rows, 3, 4);
    assert_eq!(page.len(), 4);
    assert_eq!(page[0]["n"], json!(3));
    assert_eq!(page[3]["n"], json!(6));
}
