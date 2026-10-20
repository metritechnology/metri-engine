//! Motor de búsqueda de texto completo (FTS) agnóstico al dominio mediante intersección de trigramas,
//! fonética romance/doble, normalización de códigos alfanuméricos y post-filtrado Damerau-Levenshtein sobre la Zona 5B (`.mbf`).

use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use tracing::debug;

use crate::domain::errors::DomainError;
use crate::eav::fts::trigram::generate_trigrams;
use crate::mbf::zones::hybrid_search::{IndexedEntityDocument, MbfHybridSearchIndex};

/// Resultado individual de una búsqueda de texto completo (FTS) con su puntaje combinado.
#[derive(Debug, Clone, PartialEq)]
pub struct FtsResult {
    /// Identificador único de la entidad coincidente.
    pub entity_id: String,
    /// Puntaje de similitud combinado (Zona 5B RRF, trigramas, fonética y distancia Damerau-Levenshtein).
    pub score: f32,
}

#[derive(Default)]
struct TenantFtsState {
    docs: Vec<(String, String)>,
    doc_positions: HashMap<String, usize>,
    partial_coords: HashMap<String, (Option<f64>, Option<f64>)>,
    zone5b: MbfHybridSearchIndex,
}

static IN_MEMORY_FTS_CORPUS: OnceLock<RwLock<HashMap<String, TenantFtsState>>> = OnceLock::new();

fn fts_corpus() -> &'static RwLock<HashMap<String, TenantFtsState>> {
    IN_MEMORY_FTS_CORPUS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Registra o actualiza el documento textual de una entidad en el corpus FTS local y en el índice `MbfHybridSearchIndex` (Zona 5B) del `tenant_id`.
pub fn register_fts_document(tenant_id: &str, entity_id: &str, text: &str) {
    register_fts_document_attr(tenant_id, entity_id, None, text);
}

/// Registra o consolida un atributo textual específico (`attr_name`) de una entidad dentro del índice `MbfHybridSearchIndex` (Zona 5B) del `tenant_id`.
pub fn register_fts_document_attr(
    tenant_id: &str,
    entity_id: &str,
    attr_name: Option<&str>,
    text: &str,
) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    if let Ok(mut guard) = fts_corpus().write() {
        if !guard.contains_key(tenant_id) {
            guard.insert(
                tenant_id.to_string(),
                TenantFtsState {
                    docs: Vec::new(),
                    doc_positions: HashMap::new(),
                    partial_coords: HashMap::new(),
                    zone5b: MbfHybridSearchIndex::new(),
                },
            );
        }
        let Some(state) = guard.get_mut(tenant_id) else {
            return;
        };
        let mut is_new_segment = true;
        if let Some(&pos) = state.doc_positions.get(entity_id) {
            let existing = &mut state.docs[pos];
            if !existing.1.contains(trimmed) {
                existing.1.push(' ');
                existing.1.push_str(trimmed);
            } else {
                is_new_segment = false;
            }
        } else {
            let pos = state.docs.len();
            state.doc_positions.insert(entity_id.to_string(), pos);
            state
                .docs
                .push((entity_id.to_string(), trimmed.to_string()));
        }
        let is_title_attr = matches!(
            attr_name,
            Some("name" | "title" | "label" | "full_name" | "username")
        );
        let is_code_attr = matches!(
            attr_name,
            Some(
                "tag"
                    | "code"
                    | "serial_number"
                    | "barcode_qr_code"
                    | "functional_location_code"
                    | "sku"
            )
        );
        if is_new_segment || is_title_attr || is_code_attr {
            let coords = state
                .partial_coords
                .get(entity_id)
                .and_then(|&(lat, lon)| lat.zip(lon));
            let row_id = state.zone5b.docs.len() as u32;
            let code_field = if is_code_attr || attr_name.is_none() {
                canonicalize_alphanumeric_code(trimmed)
            } else {
                String::new()
            };
            let title_field = if is_title_attr || attr_name.is_none() {
                trimmed.to_string()
            } else {
                String::new()
            };
            state.zone5b.index_document(IndexedEntityDocument {
                entity_id: entity_id.to_string(),
                row_id,
                code: code_field,
                title: title_field,
                description: trimmed.to_string(),
                embedding: None,
                coordinates_wgs84: coords,
            });
            if is_title_attr {
                state
                    .zone5b
                    .set_document_title(entity_id, trimmed.to_string());
            }
        }
    }
}

/// Actualiza el eje de latitud o longitud WGS-84 de una entidad en el índice espacial de la Zona 5B del tenant.
pub fn register_fts_entity_coordinate_axis(
    tenant_id: &str,
    entity_id: &str,
    is_latitude: bool,
    coord_deg: f64,
) {
    if !coord_deg.is_finite() {
        return;
    }
    if let Ok(mut guard) = fts_corpus().write() {
        if !guard.contains_key(tenant_id) {
            guard.insert(
                tenant_id.to_string(),
                TenantFtsState {
                    docs: Vec::new(),
                    doc_positions: HashMap::new(),
                    partial_coords: HashMap::new(),
                    zone5b: MbfHybridSearchIndex::new(),
                },
            );
        }
        let Some(state) = guard.get_mut(tenant_id) else {
            return;
        };
        let entry = state
            .partial_coords
            .entry(entity_id.to_string())
            .or_insert((None, None));
        if is_latitude {
            entry.0 = Some(coord_deg);
        } else {
            entry.1 = Some(coord_deg);
        }
        if let (Some(lat), Some(lon)) = *entry {
            if lat < state.zone5b.spatial_mbr[0] {
                state.zone5b.spatial_mbr[0] = lat;
            }
            if lon < state.zone5b.spatial_mbr[1] {
                state.zone5b.spatial_mbr[1] = lon;
            }
            if lat > state.zone5b.spatial_mbr[2] {
                state.zone5b.spatial_mbr[2] = lat;
            }
            if lon > state.zone5b.spatial_mbr[3] {
                state.zone5b.spatial_mbr[3] = lon;
            }
            let h3_morton = MbfHybridSearchIndex::encode_spatial_cell_h3_morton(lat, lon);
            if !state
                .zone5b
                .set_document_coordinates(entity_id, (lat, lon), h3_morton)
            {
                let row_id = state.zone5b.docs.len() as u32;
                state.zone5b.index_document(IndexedEntityDocument {
                    entity_id: entity_id.to_string(),
                    row_id,
                    code: entity_id.to_string(),
                    title: String::new(),
                    description: String::new(),
                    embedding: None,
                    coordinates_wgs84: Some((lat, lon)),
                });
            }
        }
    }
}

/// Asocia un vector de incrustación semántica (`Vec<f32>`) a una entidad dentro del índice vectorial `RaBitQ` de la Zona 5B del tenant.
pub fn register_fts_entity_embedding(tenant_id: &str, entity_id: &str, embedding: Vec<f32>) {
    if embedding.is_empty() {
        return;
    }
    if let Ok(mut guard) = fts_corpus().write() {
        if !guard.contains_key(tenant_id) {
            guard.insert(
                tenant_id.to_string(),
                TenantFtsState {
                    docs: Vec::new(),
                    doc_positions: HashMap::new(),
                    partial_coords: HashMap::new(),
                    zone5b: MbfHybridSearchIndex::new(),
                },
            );
        }
        let Some(state) = guard.get_mut(tenant_id) else {
            return;
        };
        let qv = crate::mbf::zones::hybrid_search::RaBitQVector::from_f32_slice(&embedding);
        if !state
            .zone5b
            .set_document_embedding(entity_id, embedding.clone(), qv)
        {
            let row_id = state.zone5b.docs.len() as u32;
            state.zone5b.index_document(IndexedEntityDocument {
                entity_id: entity_id.to_string(),
                row_id,
                code: entity_id.to_string(),
                title: String::new(),
                description: String::new(),
                embedding: Some(embedding),
                coordinates_wgs84: None,
            });
        }
    }
}

/// Registra un grupo declarativo de equivalencia de sinónimos en el `SynonymThesaurus` de la Zona 5B del tenant.
pub fn register_tenant_synonym_group(tenant_id: &str, group: &[&str]) {
    if group.len() < 2 {
        return;
    }
    if let Ok(mut guard) = fts_corpus().write() {
        if !guard.contains_key(tenant_id) {
            guard.insert(
                tenant_id.to_string(),
                TenantFtsState {
                    docs: Vec::new(),
                    doc_positions: HashMap::new(),
                    partial_coords: HashMap::new(),
                    zone5b: MbfHybridSearchIndex::new(),
                },
            );
        }
        let Some(state) = guard.get_mut(tenant_id) else {
            return;
        };
        state.zone5b.thesaurus.register_equivalence_group(group);
    }
}

/// Ejecuta una consulta espacial de geocerca circular Haversine (WGS-84) sobre el índice activo de la Zona 5B del tenant.
pub fn fts_search_geofence_meters(
    tenant_id: &str,
    center_lat: f64,
    center_lon: f64,
    radius_meters: f64,
) -> Vec<(String, f64)> {
    fts_corpus()
        .read()
        .ok()
        .and_then(|guard| {
            guard.get(tenant_id).map(|s| {
                s.zone5b
                    .search_within_geofence_meters(center_lat, center_lon, radius_meters)
            })
        })
        .unwrap_or_default()
}

/// Serializa el índice `MbfHybridSearchIndex` (Zona 5B) del tenant en un bloque binario alineado a 64 bytes (`"MBFZ5BHS"`) para archivos espejo `.mbf.idx`.
pub fn export_tenant_zone5b_bytes(tenant_id: &str) -> Vec<u8> {
    fts_corpus()
        .read()
        .ok()
        .and_then(|guard| guard.get(tenant_id).map(|s| s.zone5b.encode_zone_binary()))
        .unwrap_or_default()
}

/// Serializa un índice compacto `MbfHybridSearchIndex` (Zona 5B, `"MBFZ5BHS"`) de una única entidad para archivos `<entity_id>.mbf.idx`,
/// evitando la duplicación cuadrática del índice completo del tenant en cada archivo individual.
pub fn export_single_entity_zone5b_bytes(
    tenant_id: &str,
    entity_id: &str,
    fallback_description: &str,
) -> Vec<u8> {
    let mut single_idx = MbfHybridSearchIndex::new();
    let existing_doc = fts_corpus().read().ok().and_then(|guard| {
        guard
            .get(tenant_id)
            .and_then(|s| s.zone5b.get_document(entity_id).cloned())
    });
    if let Some(mut doc) = existing_doc {
        doc.row_id = 0;
        let fb = fallback_description.trim();
        if !fb.is_empty() && doc.description.len() < fb.len() {
            doc.description = fb.to_string();
        }
        if doc.title.is_empty() && !fb.is_empty() {
            doc.title = fb.split_whitespace().take(8).collect::<Vec<_>>().join(" ");
        }
        single_idx.index_document(doc);
    } else if !fallback_description.trim().is_empty() {
        single_idx.index_document(IndexedEntityDocument {
            entity_id: entity_id.to_string(),
            row_id: 0,
            code: entity_id.to_string(),
            title: fallback_description
                .split_whitespace()
                .take(8)
                .collect::<Vec<_>>()
                .join(" "),
            description: fallback_description.to_string(),
            embedding: None,
            coordinates_wgs84: None,
        });
    }
    single_idx.encode_zone_binary()
}

/// Canonicaliza códigos alfanuméricos eliminando separadores no alfanuméricos (`-`, `_`, `/`, `.`, espacios)
/// y normalizando homóglifos ópticos/humanos (`O` -> `0`, `I`/`l` -> `1`) en tokens alfanuméricos mixtos.
pub fn canonicalize_alphanumeric_code(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let any_digit_in_raw = raw.chars().any(|c| c.is_ascii_digit());
    for word in raw.split_whitespace() {
        let word_has_digit = word.chars().any(|c| c.is_ascii_digit());
        let alnum_chars: Vec<char> = word.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
        for (idx, &c) in alnum_chars.iter().enumerate() {
            let upper = c.to_ascii_uppercase();
            let prev_is_digit = idx > 0 && alnum_chars[idx - 1].is_ascii_digit();
            let next_is_digit =
                idx + 1 < alnum_chars.len() && alnum_chars[idx + 1].is_ascii_digit();
            if word_has_digit && (idx >= 2 || prev_is_digit || next_is_digit) {
                match upper {
                    'O' => out.push('0'),
                    'I' | 'L' => out.push('1'),
                    other => out.push(other),
                }
            } else if any_digit_in_raw && alnum_chars.len() <= 4 && (prev_is_digit || next_is_digit)
            {
                match upper {
                    'O' => out.push('0'),
                    'I' | 'L' => out.push('1'),
                    other => out.push(other),
                }
            } else {
                out.push(upper);
            }
        }
    }
    out
}

/// Ejecuta la clasificación por intersección de trigramas, fonética romance, código alfanumérico y distancia Damerau-Levenshtein sobre pares `(entity_id, texto)`.
pub fn fts_search_candidates(
    term: &str,
    candidates: &[(String, String)],
    max_edit_distance: usize,
) -> Vec<FtsResult> {
    let query_trigrams: HashSet<String> = generate_trigrams(term).into_iter().collect();
    if query_trigrams.is_empty() {
        return Vec::new();
    }
    let term_norm = crate::aegis::oltp::fuzzy::remove_accents(&term.trim().to_lowercase());
    let query_phonetic = MbfHybridSearchIndex::encode_romance_phonetic(&term_norm);
    let query_code = canonicalize_alphanumeric_code(term);
    let query_tokens: Vec<String> = term_norm
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();

    let mut results = Vec::new();
    for (entity_id, text) in candidates {
        let doc_norm = crate::aegis::oltp::fuzzy::remove_accents(&text.to_lowercase());
        let substring_bonus = if !term_norm.is_empty() && doc_norm.contains(&term_norm) {
            1.0_f32
        } else if query_tokens.len() > 1 && query_tokens.iter().all(|qt| doc_norm.contains(qt)) {
            0.85_f32
        } else {
            0.0_f32
        };

        let doc_trigrams = generate_trigrams(text);
        let matched_trigrams = doc_trigrams
            .iter()
            .filter(|t| query_trigrams.contains(*t))
            .count();
        let trigram_score = (matched_trigrams as f32) / (query_trigrams.len().max(1) as f32);

        let doc_code = canonicalize_alphanumeric_code(text);
        let code_bonus = if query_code.len() >= 2 && doc_code.contains(&query_code) {
            0.65_f32
        } else {
            0.0_f32
        };

        let mut best_dl_bonus = 0.0_f32;
        let mut phonetic_bonus = 0.0_f32;
        for word in doc_norm
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
        {
            if !query_phonetic.is_empty()
                && MbfHybridSearchIndex::encode_romance_phonetic(word) == query_phonetic
            {
                phonetic_bonus = 0.50_f32;
            }
            for q_tok in &query_tokens {
                if q_tok.len() >= 3 {
                    let q_phon = MbfHybridSearchIndex::encode_romance_phonetic(q_tok);
                    if !q_phon.is_empty()
                        && MbfHybridSearchIndex::encode_romance_phonetic(word) == q_phon
                    {
                        phonetic_bonus = 0.50_f32;
                    }
                }
                let dl = damerau_levenshtein(q_tok, word);
                if dl <= max_edit_distance {
                    let bonus = 1.0_f32 / (1.0_f32 + (dl as f32));
                    if bonus > best_dl_bonus {
                        best_dl_bonus = bonus;
                    }
                }
            }
        }

        let total_score =
            substring_bonus + trigram_score + code_bonus + phonetic_bonus + best_dl_bonus;
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
                let top_k = state.zone5b.docs.len().max(256);
                let z_hits = state.zone5b.search_hybrid(term, None, top_k);
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

/// Calcula la distancia de edición Damerau-Levenshtein (inserción, eliminación, sustitución y transposición adyacente) entre dos cadenas.
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
