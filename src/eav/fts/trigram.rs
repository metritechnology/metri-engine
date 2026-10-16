//! Trigram and romance phonetic index builder for `.mbf` full-text search (Zone 5B).
//!
//! Índice Trigram y Fonético para Full-Text Search sobre `.mbf`.
//! Blueprint: Metri EAV §V & `09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md` §5

use crate::codice::registry::AttributeDescriptor;
use crate::eav::types::datom::Datom;
use crate::mbf::storage::{AttributeValue, Blob, PutRequest, WriteRequest};

/// Genera todos los trigrams únicos de un texto (lowercase, Unicode normalizado).
/// `"Nodo Primario"` → `["  n", " no", "nod", "odo", ...]`
pub fn generate_trigrams(text: &str) -> Vec<String> {
    // pg_trgm style padding: 2 spaces prefix, 1 space suffix
    let padded = format!("  {} ", text.to_lowercase());
    let chars: Vec<char> = padded.chars().collect();
    let mut trigrams: Vec<String> = chars.windows(3).map(|w| w.iter().collect()).collect();
    trigrams.sort();
    trigrams.dedup();
    trigrams
}

/// Construye los items del índice FTS para un datom con `fts: true` y sincroniza el documento en el índice híbrido MBF Zona 5B.
/// Incluye tanto trigrams (`T#<tenant>#FTS#<trigram>`) como códigos fonéticos bilingües (`T#<tenant>#FTS#PHON#<code_fonetico>`).
pub fn build_fts_items(datom: &Datom, attr: &AttributeDescriptor) -> Vec<WriteRequest> {
    if !attr.fts {
        return vec![];
    }

    let text = match &datom.value {
        crate::eav::types::datom::DatomValue::Str(s) => s.clone(),
        _ => return vec![],
    };

    // Sincronizar automáticamente con el índice híbrido MBF Zona 5B por tenant
    crate::eav::fts::searcher::register_fts_document(&datom.tenant_id, &datom.entity_id, &text);

    let mut keys: Vec<String> = generate_trigrams(&text)
        .into_iter()
        .map(|trigram| format!("T#{}#FTS#{}", datom.tenant_id, trigram))
        .collect();

    for word in text.split(|c: char| !c.is_alphanumeric()) {
        if word.len() >= 3 {
            let phon =
                crate::mbf::zones::hybrid_search::MbfHybridSearchIndex::encode_romance_phonetic(
                    word,
                );
            if !phon.is_empty() {
                keys.push(format!("T#{}#FTS#PHON#{}", datom.tenant_id, phon));
            }
        }
    }
    keys.sort();
    keys.dedup();

    keys.into_iter()
        .filter_map(|pk| {
            let mut sk_bytes = Vec::new();
            sk_bytes.extend_from_slice(&datom.attr_id.to_be_bytes()); // 2 bytes attr_id
            sk_bytes.extend_from_slice(datom.entity_id.as_bytes());

            let mut item = std::collections::HashMap::new();
            item.insert("PK".to_string(), AttributeValue::S(pk));
            item.insert("SK".to_string(), AttributeValue::B(Blob::new(sk_bytes)));

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
        let t = generate_trigrams("motor");
        assert!(t.contains(&"mot".to_string()));
        assert!(t.contains(&"oto".to_string()));
        assert!(t.contains(&"tor".to_string()));
    }

    #[test]
    fn trigrams_deduplicated() {
        let t = generate_trigrams("aaa");
        // Lexicographic sort order: "  a" < " aa" < "aa " < "aaa"
        assert_eq!(t, vec!["  a", " aa", "aa ", "aaa"]);
    }
}
