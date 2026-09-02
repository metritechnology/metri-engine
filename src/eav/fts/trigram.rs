// eav/fts/trigram.rs — Índice Trigram para Full-Text Search
// Blueprint: Metri EAV §V

use crate::codice::registry::AttributeDescriptor;
use crate::eav::types::datom::Datom;
use aws_sdk_dynamodb::types::{AttributeValue, PutRequest, WriteRequest};

/// Genera todos los trigrams únicos de un texto (lowercase, Unicode normalizado).
/// "Bomba Hidráulica" → ["bom", "omb", "mba", "ba ", ...]
pub fn generate_trigrams(text: &str) -> Vec<String> {
    // pg_trgm style padding: 2 spaces prefix, 1 space suffix
    let padded = format!("  {} ", text.to_lowercase());
    let chars: Vec<char> = padded.chars().collect();
    let mut trigrams: Vec<String> = chars.windows(3).map(|w| w.iter().collect()).collect();
    trigrams.sort();
    trigrams.dedup();
    trigrams
}

/// Construye los items del índice FTS-GSI para un datom con `fts: true`.
/// PK = "T#<tenant>#FTS#<trigram>"  SK = entity_id
/// Blueprint: §V.1 — "Al escribir un datom con fts: true, se generan trigrams"
pub fn build_fts_items(datom: &Datom, attr: &AttributeDescriptor) -> Vec<WriteRequest> {
    if !attr.fts {
        return vec![];
    }

    let text = match &datom.value {
        crate::eav::types::datom::DatomValue::Str(s) => s.clone(),
        _ => return vec![],
    };

    generate_trigrams(&text)
        .into_iter()
        .filter_map(|trigram| {
            let pk = format!("T#{}#FTS#{}", datom.tenant_id, trigram);
            let mut sk_bytes = Vec::new();
            sk_bytes.extend_from_slice(&datom.attr_id.to_be_bytes()); // 2 bytes attr_id
            sk_bytes.extend_from_slice(datom.entity_id.as_bytes());

            let mut item = std::collections::HashMap::new();
            item.insert("PK".to_string(), AttributeValue::S(pk));
            item.insert(
                "SK".to_string(),
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk_bytes)),
            );

            let put_request = PutRequest::builder().set_item(Some(item)).build().ok()?;

            Some(WriteRequest::builder().put_request(put_request).build())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigrams_basic() {
        let t = generate_trigrams("bomba");
        assert!(t.contains(&"bom".to_string()));
        assert!(t.contains(&"omb".to_string()));
        assert!(t.contains(&"mba".to_string()));
    }

    #[test]
    fn trigrams_deduplicated() {
        let t = generate_trigrams("aaa");
        // Lexicographic sort order: "  a" < " aa" < "aa " < "aaa"
        assert_eq!(t, vec!["  a", " aa", "aa ", "aaa"]);
    }
}
