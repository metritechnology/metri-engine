use super::*;
use std::sync::Arc;

#[tokio::test]
async fn test_cedar_step_instantiation() {
    std::env::set_var("METRI_TEST_MODE", "1");
    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");

    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key-development-metri-256-bits!!!"
            .to_string()
            .into_bytes(),
        Arc::clone(&ddb_client),
        "metri-eav-local".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::authorizer::InMemoryPrincipalCache::new());

    let step = CedarAuthorizerStep::new(valkey_store, pull_read, principal_cache);

    // We expect policies cache to be initialized for standard roles
    assert!(step.policy_cache.contains_key("admin"));
    assert!(step.policy_cache.contains_key("user"));
}
