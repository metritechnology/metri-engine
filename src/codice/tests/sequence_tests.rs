//! Tests for `codice::sequence`.
use super::*;

#[test]
fn build_sequence_code_without_scope() {
    let code = build_sequence_code("tenant-1", "work_order_code", None);
    assert_eq!(code, "tenant-1:work_order_code_seq");
}

#[test]
fn build_sequence_code_with_scope() {
    let code = build_sequence_code("tenant-1", "work_order_code", Some("L1"));
    assert_eq!(code, "tenant-1:work_order_code:L1_seq");
}

#[test]
fn format_code_zero_pads() {
    assert_eq!(format_code("WO-", None, 4, 7), "WO-0007");
    assert_eq!(format_code("WO-", None, 4, 100), "WO-0100");
    assert_eq!(format_code("", None, 6, 1), "000001");
    // Con segmento (el tag de la location): el formato del pattern del
    // Códice — ^WO(-[A-Z0-9-]+)?-\d{4,6}$ → "WO-L-K92MXA-0043".
    assert_eq!(
        format_code("WO-", Some("L-K92MXA"), 4, 43),
        "WO-L-K92MXA-0043"
    );
    // Un segmento vacío es sin segmento.
    assert_eq!(format_code("WO-", Some(""), 4, 7), "WO-0007");
}
