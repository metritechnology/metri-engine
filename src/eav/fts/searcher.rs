//! Domain-agnostic full-text search via trigram intersection, romance/double phonetics, alphanumeric code normalization, and Damerau-Levenshtein post-filtering.
//!
//! Blueprint: Metri EAV §V.2 & 09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md §5

use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use tracing::debug;

use crate::domain::errors::DomainError;
use crate::eav::fts::trigram::generate_trigrams;
use crate::mbf::zones::hybrid_search::{IndexedEntityDocument, MbfHybridSearchIndex};

/// Score de un resultado de búsqueda FTS.
#[derive(Debug, Clone, PartialEq)]
pub struct FtsResult {
    pub entity_id: String,
    pub score: f32, // combined Zone 5B RRF + trigram + phonetic + Damerau-Levenshtein similarity score
}

#[derive(Default)]
struct TenantFtsState {
    docs: Vec<(String, String)>,
    zone5b: MbfHybridSearchIndex,
}

static IN_MEMORY_FTS_CORPUS: OnceLock<RwLock<HashMap<String, TenantFtsState>>> = OnceLock::new();

fn fts_corpus() -> &'static RwLock<HashMap<String, TenantFtsState>> {
    IN_MEMORY_FTS_CORPUS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Registers or updates an entity text document in the local FTS corpus and Zone 5B `MbfHybridSearchIndex` for `tenant_id`.
pub fn register_fts_document(tenant_id: &str, entity_id: &str, text: &str) {
    if let Ok(mut guard) = fts_corpus().write() {
        let state = guard
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantFtsState {
                docs: Vec::new(),
                zone5b: MbfHybridSearchIndex::new(),
            });
        if let Some(existing) = state.docs.iter_mut().find(|(id, _)| id == entity_id) {
            if !existing.1.contains(text) {
                existing.1.push(' ');
                existing.1.push_str(text);
            }
        } else {
            state.docs.push((entity_id.to_string(), text.to_string()));
        }
        let row_id = state.zone5b.docs.len() as u32;
        let canonical_code = canonicalize_alphanumeric_code(text);
        state.zone5b.index_document(IndexedEntityDocument {
            entity_id: entity_id.to_string(),
            row_id,
            code: canonical_code,
            title: text.to_string(),
            description: text.to_string(),
            embedding: None,
            coordinates_wgs84: None,
        });
    }
}

/// Serializes the tenant's Zone 5B `MbfHybridSearchIndex` into 64-byte aligned binary bytes (`"MBFZ5BHS"`) for `.mbf.idx` mirror files.
pub fn export_tenant_zone5b_bytes(tenant_id: &str) -> Vec<u8> {
    fts_corpus()
        .read()
        .ok()
        .and_then(|guard| guard.get(tenant_id).map(|s| s.zone5b.encode_zone_binary()))
        .unwrap_or_default()
}

/// Canonicalizes alphanumeric codes by stripping non-alphanumeric separators (`-`, `_`, `/`, `.`, spaces)
/// and normalizing optical/human homoglyphs (`O` -> `0`, `I`/`l` -> `1`) inside mixed alphanumeric strings (§5.3.B).
pub fn canonicalize_alphanumeric_code(raw: &str) -> String {
    let trimmed = raw.trim();
    let has_digit = trimmed.chars().any(|c| c.is_ascii_digit());
    let mut out = String::with_capacity(trimmed.len());
    for (idx, c) in trimmed.chars().enumerate() {
        if !c.is_ascii_alphanumeric() {
            continue;
        }
        let upper = c.to_ascii_uppercase();
        if has_digit && idx >= 2 {
            match upper {
                'O' => out.push('0'),
                'I' | 'L' => out.push('1'),
                other => out.push(other),
            }
        } else {
            out.push(upper);
        }
    }
    out
}

/// Executes trigram intersection + romance phonetic + alphanumeric code + Damerau-Levenshtein scoring over `(entity_id, text)` candidates.
pub fn fts_search_candidates(
    term: &str,
    candidates: &[(String, String)],
    max_edit_distance: usize,
) -> Vec<FtsResult> {
    let query_trigrams: HashSet<String> = generate_trigrams(term).into_iter().collect();
    if query_trigrams.is_empty() {
        return Vec::new();
    }
    let query_phonetic = MbfHybridSearchIndex::encode_romance_phonetic(term);
    let query_code = canonicalize_alphanumeric_code(term);
    let query_tokens: Vec<String> = term
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();

    let mut results = Vec::new();
    for (entity_id, text) in candidates {
        let doc_trigrams = generate_trigrams(text);
        let matched_trigrams = doc_trigrams
            .iter()
            .filter(|t| query_trigrams.contains(*t))
            .count();
        let trigram_score = (matched_trigrams as f32) / (query_trigrams.len().max(1) as f32);

        let doc_code = canonicalize_alphanumeric_code(text);
        let code_bonus = if !query_code.is_empty() && doc_code.contains(&query_code) {
            0.65_f32
        } else {
            0.0_f32
        };

        let mut best_dl_bonus = 0.0_f32;
        let mut phonetic_bonus = 0.0_f32;
        for word in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
        {
            let word_lower = word.to_lowercase();
            if !query_phonetic.is_empty()
                && MbfHybridSearchIndex::encode_romance_phonetic(&word_lower) == query_phonetic
            {
                phonetic_bonus = 0.50_f32;
            }
            for q_tok in &query_tokens {
                let dl = damerau_levenshtein(q_tok, &word_lower);
                if dl <= max_edit_distance {
                    let bonus = 1.0_f32 / (1.0_f32 + (dl as f32));
                    if bonus > best_dl_bonus {
                        best_dl_bonus = bonus;
                    }
                }
            }
        }

        let total_score = trigram_score + code_bonus + phonetic_bonus + best_dl_bonus;
        if total_score >= 0.25 {
            results.push(FtsResult {
                entity_id: entity_id.clone(),
                score: total_score,
            });
        }
    }

    results.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.entity_id.cmp(&b.entity_id))
    });
    results
}

/// Busca entidades por texto unificando el motor híbrido MBF Zona 5B (`MbfHybridSearchIndex`) y el clasificador de códigos/trigrams/Damerau-Levenshtein.
pub async fn fts_search_entity_ids(
    term: &str,
    tenant_id: &str,
) -> Result<Vec<FtsResult>, DomainError> {
    let trigrams = generate_trigrams(term);
    if trigrams.is_empty() {
        return Ok(vec![]);
    }

    debug!(
        "[FTS] Buscando '{}' → {} trigrams para tenant {}",
        term,
        trigrams.len(),
        tenant_id
    );

    let (candidates, zone5b_hits) = fts_corpus()
        .read()
        .ok()
        .and_then(|guard| {
            guard.get(tenant_id).map(|state| {
                let z_hits = state.zone5b.search_hybrid(term, None, 32);
                (state.docs.clone(), z_hits)
            })
        })
        .unwrap_or_default();

    let mut merged: HashMap<String, f32> = HashMap::new();
    for hit in fts_search_candidates(term, &candidates, 2) {
        merged.insert(hit.entity_id, hit.score);
    }
    for zhit in zone5b_hits {
        let entry = merged.entry(zhit.entity_id).or_insert(0.0);
        *entry += zhit.total_score as f32;
    }

    let mut out: Vec<FtsResult> = merged
        .into_iter()
        .map(|(entity_id, score)| FtsResult { entity_id, score })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.entity_id.cmp(&b.entity_id))
    });
    Ok(out)
}

/// Calcula la distancia Damerau-Levenshtein entre dos strings.
/// Usado para post-filtrar candidatos del FTS.
/// [Blueprint: §V.2 — "Post-filter: Damerau-Levenshtein distance ≤ 1"]
pub fn damerau_levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    let mut d = vec![vec![0usize; n + 1]; m + 1];
    for (i, row) in d.iter_mut().enumerate().take(m + 1) {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate().take(n + 1) {
        *cell = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);

            // Transposición (Damerau)
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + cost);
            }
        }
    }
    d[m][n]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dl_exact_match() {
        assert_eq!(damerau_levenshtein("vector", "vector"), 0);
    }

    #[test]
    fn dl_one_deletion() {
        assert_eq!(damerau_levenshtein("vector", "vetor"), 1);
    }

    #[test]
    fn dl_transposition() {
        assert_eq!(damerau_levenshtein("vector", "evctor"), 1);
    }

    #[tokio::test]
    async fn fts_search_entity_ids_executes_real_fuzzy_and_alphanumeric_code_search() {
        register_fts_document(
            "tnt_fts_test",
            "ENT-01",
            "Módulo transaccional de alta precisión REF-6205 / 2RS para válvula",
        );
        register_fts_document(
            "tnt_fts_test",
            "ENT-02",
            "Nodo secundario de procesamiento distribuido",
        );

        let hits = fts_search_entity_ids("balbula REF 62O5 2RS", "tnt_fts_test")
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].entity_id, "ENT-01");
    }
}
