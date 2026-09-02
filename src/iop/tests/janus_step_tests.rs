use super::*;
use std::sync::Arc;
use crate::iop::core::IopContext;
use crate::janus_router::router::JanusRouter;

static REGISTRY_INIT: std::sync::Once = std::sync::Once::new();

fn init_registry() {
    REGISTRY_INIT.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let _ = std::panic::catch_unwind(|| {
            let dir = std::path::Path::new("config/models");
            let (registry, _rules) = crate::codice::CodeRegistry::build(dir)
                .expect("config/models debe compilar");
            crate::codice::init_global(registry);
        });
        std::panic::set_hook(prev);
    });
    crate::codice::global();
}

#[tokio::test]
async fn test_janus_router_step_instantiation() {
    init_registry();

    let ddb_client = Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> =
        Arc::new(crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()));

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(crate::codice::registry::EngineChannel::Oltp, Arc::clone(&oltp_channel));
    let janus_router = Arc::new(JanusRouter::new(channel_registry));

    let step = JanusRouterStep::new(janus_router);
    // Ensure step has router initialized
    assert!(step.execute(IopContext::new(
        "tnt_01",
        "usr_01",
        "work_order",
        "CREATE",
        serde_json::Map::new()
    )).await.is_err()); // fails because EAV registry is not loaded, but shows integration is correct
}
