pub mod bulk;
pub mod discovery;
pub mod explore;
pub mod helpers;
pub mod match_rules;
pub mod query;
pub mod strategy;
pub mod transaction;

use self::helpers::ensure_status;
use self::strategy::NormalizerStrategy;
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseType {
    Query,
    Discovery,
    Explore,
    Transaction,
    Bulk,
    MatchRules,
    MatchRulesBatch,
    Default,
}

impl ResponseType {
    /// Infiere el tipo desde las claves del body.
    /// [PORTED_FROM: (cond (contains? body :data) :query-response ...)]
    pub fn infer(body: &Value) -> Self {
        let obj = match body.as_object() {
            Some(o) => o,
            None => return ResponseType::Default,
        };
        if obj.contains_key("ingested_count")
            || obj.contains_key("outbox_count")
            || obj
                .get("result")
                .and_then(|r| r.get("ingested_count"))
                .is_some()
        {
            ResponseType::Bulk
        } else if obj.contains_key("entity_id")
            || obj.contains_key("entity-id")
            || obj.get("result").and_then(|r| r.get("entity_id")).is_some()
            || obj.get("result").and_then(|r| r.get("entity-id")).is_some()
        {
            ResponseType::Transaction
        } else if obj.contains_key("data")
            || obj.contains_key("viz-ext")
            || obj.contains_key("query_key")
        {
            ResponseType::Query
        } else if obj.contains_key("schemas") {
            ResponseType::Discovery
        } else if obj.contains_key("values") {
            ResponseType::Explore
        } else if obj.contains_key("responses") {
            ResponseType::MatchRulesBatch
        } else if obj.contains_key("matched_rules") {
            ResponseType::MatchRules
        } else {
            ResponseType::Default
        }
    }
}

/// Normaliza una respuesta al 100% de cobertura del contrato.
/// Dispatch por ResponseType — equivalente al defmulti de Clojure.
/// [PORTED_FROM: (normalize-response [[_tag body]] dispatch)]
pub fn normalize_response(body: &Value, response_type: ResponseType) -> Value {
    let mut body = body.clone();

    // Garantizar que body sea un objeto
    if !body.is_object() {
        body = json!({"raw": body});
    }

    let success = !body
        .as_object()
        .map(|o| o.contains_key("code"))
        .unwrap_or(false);

    // SOLID: Dispatch to dedicated strategies
    match response_type {
        ResponseType::Query => {
            ensure_status(&mut body, success);
            query::QueryNormalizer.normalize(&mut body, success);
        }
        ResponseType::Discovery => {
            ensure_status(&mut body, success);
            discovery::DiscoveryNormalizer.normalize(&mut body, success);
        }
        ResponseType::Explore => {
            ensure_status(&mut body, success);
            explore::ExploreNormalizer.normalize(&mut body, success);
        }
        ResponseType::Transaction => {
            ensure_status(&mut body, success);
            transaction::TransactionNormalizer.normalize(&mut body, success);
        }
        ResponseType::Bulk => {
            ensure_status(&mut body, success);
            bulk::BulkNormalizer.normalize(&mut body, success);
        }
        ResponseType::MatchRules => {
            ensure_status(&mut body, success);
            match_rules::MatchRulesNormalizer.normalize(&mut body, success);
        }
        ResponseType::MatchRulesBatch => {
            ensure_status(&mut body, success);
            match_rules::MatchRulesBatchNormalizer.normalize(&mut body, success);
        }
        ResponseType::Default => {
            ensure_status(&mut body, success);
        }
    }

    body
}

/// Alias para normalizar chunks de QueryResponse (streaming, Paso 7).
/// [PORTED_FROM: (normalize-chunk chunk)]
pub fn normalize_chunk(body: &Value) -> Value {
    let mut body = body.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.insert("response_type".to_string(), json!("query_response"));
    }
    normalize_response(&body, ResponseType::Query)
}

/// Alias para normalizar respuestas unarias (Discovery, Explore, Match).
/// [PORTED_FROM: (normalize-unary chunk)]
pub fn normalize_unary(body: &Value) -> Value {
    let response_type = ResponseType::infer(body);
    normalize_response(body, response_type)
}
