use crate::janus::batch_enricher::{apply_batch_context, apply_cross_filter};
use serde_json::json;

#[test]
fn test_apply_batch_context() {
    let queries = json!({
        "q1": {"filters": [{"foo": "bar"}]},
        "q2": {"entity": "assets"}
    });
    let ctx = json!({
        "common_filters": [{"global": "true"}],
        "common_entity": "locations"
    });

    let enriched = apply_batch_context(queries, Some(&ctx));

    assert_eq!(enriched["q1"]["entity"], "locations");
    assert_eq!(enriched["q1"]["filters"].as_array().unwrap().len(), 2);
    assert_eq!(enriched["q1"]["filters"][0]["global"], "true");

    assert_eq!(enriched["q2"]["entity"], "assets");
}

#[test]
fn test_apply_cross_filter() {
    let queries = json!({
        "q1": {"filters": [{"foo": "bar"}]}
    });
    let cf = json!({
        "cross_filters": [{"cross": "true"}]
    });

    let enriched = apply_cross_filter(queries, Some(&cf));

    assert_eq!(enriched["q1"]["filters"].as_array().unwrap().len(), 2);
    assert_eq!(enriched["q1"]["filters"][1]["cross"], "true"); // appended
}
