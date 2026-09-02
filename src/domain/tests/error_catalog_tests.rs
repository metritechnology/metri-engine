use super::*;

#[test]
fn loads_catalog_from_file() {
    let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
    assert!(catalog.entry_count() > 10);
    assert!(catalog.get("EAV_002").is_some());
    assert!(catalog.get("JANUS_400").is_some());
    assert!(catalog.get("AEG_001").is_some());
}

#[test]
fn http_status_lookup() {
    let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
    assert_eq!(catalog.http_status("JANUS_403"), 403);
    assert_eq!(catalog.http_status("EAV_TX_003"), 409);
    assert_eq!(catalog.http_status("INFRA_DDB_002"), 429);
}

#[test]
fn retryable_flag() {
    let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
    assert!(catalog.is_retryable("EAV_TX_003")); // ConcurrentModification → retry
    assert!(!catalog.is_retryable("JANUS_400")); // Bad request → no retry
}

#[test]
fn unknown_code_returns_none() {
    let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
    assert!(catalog.get("NONEXISTENT_CODE").is_none());
    assert_eq!(catalog.http_status("NONEXISTENT_CODE"), 500); // fallback
}
