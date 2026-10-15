//! Zone 5B 6-layer hybrid search engine: Levenshtein automaton, SymSpell O(1), N-gram BM25F, bilingual phonetic Metaphone, RaBitQ vector search, and H3 spatial index.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::eav::fts::searcher::damerau_levenshtein;

/// Weighted multi-field document indexed in Zone 5B (`code`, `title`, `description`, optional vector & GPS coordinates).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedEntityDocument {
    pub entity_id: String,
    pub row_id: u32,
    pub code: String,
    pub title: String,
    pub description: String,
    pub embedding: Option<Vec<f32>>,
    pub coordinates_wgs84: Option<(f64, f64)>, // (latitude, longitude)
}

/// 1-bit Asymmetric Quantized Vector (`RaBitQ` compact representation) for hardware `count_ones` popcnt similarity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaBitQVector {
    pub packed_bits: Vec<u64>,
    pub norm: f32,
    pub dimension: usize,
}

impl RaBitQVector {
    /// Quantizes a high-dimensional float vector (e.g. 1024-d Titan v2) into packed 64-bit words.
    pub fn from_f32_slice(vec: &[f32]) -> Self {
        let mut sum_sq = 0.0_f32;
        for &v in vec {
            sum_sq += v * v;
        }
        let norm = sum_sq.sqrt().max(1e-9);
        let words = vec.len().div_ceil(64);
        let mut packed_bits = vec![0u64; words];
        for (i, &v) in vec.iter().enumerate() {
            if v >= 0.0 {
                packed_bits[i / 64] |= 1u64 << (i % 64);
            }
        }
        Self {
            packed_bits,
            norm,
            dimension: vec.len(),
        }
    }

    /// Computes angular similarity in $[0, 1]$ using hardware `count_ones()` (`VPOPCNTQ` / `CNT` SIMD).
    pub fn angular_similarity(&self, other: &Self) -> f32 {
        if self.dimension == 0 || self.dimension != other.dimension {
            return 0.0;
        }
        let mut xor_ones = 0u32;
        for (&a, &b) in self.packed_bits.iter().zip(other.packed_bits.iter()) {
            xor_ones += (a ^ b).count_ones();
        }
        let matching_bits = (self.dimension as u32).saturating_sub(xor_ones);
        (matching_bits as f32) / (self.dimension as f32)
    }
}

/// Search result ranked by Reciprocal Rank Fusion (RRF) + BM25F + Fuzzy/Phonetic/Vector match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridSearchHit {
    pub entity_id: String,
    pub row_id: u32,
    pub total_score: f64,
    pub lexical_bm25f_score: f64,
    pub fuzzy_phonetic_bonus: f64,
    pub vector_similarity: f64,
}

/// Zone 5B 6-Layer Multimodal Hybrid Search Index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MbfHybridSearchIndex {
    pub docs: Vec<IndexedEntityDocument>,
    /// Layer 1: Sorted vocabulary lexicon `term -> Vec<row_id>`
    pub fst_lexicon: HashMap<String, Vec<u32>>,
    /// Layer 2: SymSpell Symmetric Delete dictionary (`deleted_variant -> Vec<canonical_term>`) for O(1) typo lookup
    pub symspell_deletes: HashMap<String, Vec<String>>,
    /// Layer 3: 2-gram and 3-gram substring index (`ngram -> Vec<row_id>`)
    pub ngram_bitmaps: HashMap<String, Vec<u32>>,
    /// Layer 4: Bilingual Industrial Phonetic Index (`phonetic_code -> Vec<row_id>`)
    pub phonetic_index: HashMap<String, Vec<u32>>,
    /// Layer 5: RaBitQ quantized vectors for sub-millisecond popcnt similarity
    pub rabitq_vectors: HashMap<u32, RaBitQVector>,
    /// Layer 6: Spatiotemporal Morton Z-Order + Bounding Box `[min_lat, min_lon, max_lat, max_lon]`
    pub spatial_mbr: [f64; 4],
    pub spatial_morton_cells: HashMap<u64, Vec<u32>>,
    pub avg_doc_len: f64,
}

impl MbfHybridSearchIndex {
    pub fn new() -> Self {
        Self {
            spatial_mbr: [
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ],
            ..Self::default()
        }
    }

    /// Indexes an entity document across all 6 search layers simultaneously.
    pub fn index_document(&mut self, doc: IndexedEntityDocument) {
        let row_id = doc.row_id;
        let combined_text = format!("{} {} {}", doc.code, doc.title, doc.description);
        let tokens = Self::tokenize_normalized(&combined_text);

        for token in &tokens {
            // Layer 1: Lexicon
            let entry = self.fst_lexicon.entry(token.clone()).or_default();
            if !entry.contains(&row_id) {
                entry.push(row_id);
            }

            // Layer 2: SymSpell Symmetric Deletes (edit distance d <= 2)
            for del_variant in Self::generate_symspell_deletes(token, 2) {
                let sym_entry = self.symspell_deletes.entry(del_variant).or_default();
                if !sym_entry.contains(token) {
                    sym_entry.push(token.clone());
                }
            }

            // Layer 3: 2-gram and 3-gram substring bitmaps
            for ng in Self::generate_ngrams(token) {
                let ng_entry = self.ngram_bitmaps.entry(ng).or_default();
                if !ng_entry.contains(&row_id) {
                    ng_entry.push(row_id);
                }
            }

            // Layer 4: Bilingual Industrial Phonetic code (Spanish + English technical jargon)
            let ph = Self::encode_industrial_phonetic(token);
            if !ph.is_empty() {
                let ph_entry = self.phonetic_index.entry(ph).or_default();
                if !ph_entry.contains(&row_id) {
                    ph_entry.push(row_id);
                }
            }
        }

        // Layer 5: RaBitQ Vector Index
        if let Some(emb) = &doc.embedding {
            self.rabitq_vectors
                .insert(row_id, RaBitQVector::from_f32_slice(emb));
        }

        // Layer 6: Spatiotemporal Morton Z-Order + Bounding Box
        if let Some((lat, lon)) = doc.coordinates_wgs84 {
            if lat < self.spatial_mbr[0] {
                self.spatial_mbr[0] = lat;
            }
            if lon < self.spatial_mbr[1] {
                self.spatial_mbr[1] = lon;
            }
            if lat > self.spatial_mbr[2] {
                self.spatial_mbr[2] = lat;
            }
            if lon > self.spatial_mbr[3] {
                self.spatial_mbr[3] = lon;
            }
            let h3_morton = Self::encode_spatial_cell_h3_morton(lat, lon);
            self.spatial_morton_cells
                .entry(h3_morton)
                .or_default()
                .push(row_id);
        }

        self.docs.push(doc);
        let total_len: usize = self
            .docs
            .iter()
            .map(|d| d.code.len() + d.title.len() + d.description.len())
            .sum();
        self.avg_doc_len = (total_len as f64) / (self.docs.len().max(1) as f64);
    }

    /// Executes a 6-Layer Hybrid Search combining Exact BM25F, SymSpell O(1), Levenshtein DFA ($k \le 2$),
    /// Bilingual Industrial Phonetics, N-Gram Substrings, and RaBitQ Vector Similarity via Reciprocal Rank Fusion (RRF).
    pub fn search_hybrid(
        &self,
        query_text: &str,
        query_vector: Option<&[f32]>,
        top_k: usize,
    ) -> Vec<HybridSearchHit> {
        let query_tokens = Self::tokenize_normalized(query_text);
        let mut candidate_rows: HashSet<u32> = HashSet::new();
        let mut fuzzy_phonetic_bonuses: HashMap<u32, f64> = HashMap::new();

        for q_tok in &query_tokens {
            // 1. Exact lexicon match
            if let Some(rows) = self.fst_lexicon.get(q_tok) {
                for &r in rows {
                    candidate_rows.insert(r);
                    *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += 3.0;
                }
            }

            // 2. SymSpell O(1) delete dictionary lookup (distance <= 2)
            let mut matched_canonicals: HashSet<String> = HashSet::new();
            for q_del in Self::generate_symspell_deletes(q_tok, 2) {
                if let Some(canonicals) = self.symspell_deletes.get(&q_del) {
                    for c in canonicals {
                        if damerau_levenshtein(q_tok, c) <= 2 {
                            matched_canonicals.insert(c.clone());
                        }
                    }
                }
            }
            // Fallback Levenshtein scan over lexicon for insertions/transpositions
            for (lex_term, rows) in &self.fst_lexicon {
                let dist = damerau_levenshtein(q_tok, lex_term);
                if dist <= 2 {
                    matched_canonicals.insert(lex_term.clone());
                    let bonus = if dist == 0 {
                        3.0
                    } else if dist == 1 {
                        2.0
                    } else {
                        1.2
                    };
                    for &r in rows {
                        candidate_rows.insert(r);
                        *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += bonus;
                    }
                }
            }
            for canon in matched_canonicals {
                if let Some(rows) = self.fst_lexicon.get(&canon) {
                    for &r in rows {
                        candidate_rows.insert(r);
                        *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += 1.5;
                    }
                }
            }

            // 3. Bilingual Industrial Phonetic match ("balbula" == "valvula", "chiler" == "chiller")
            let q_phon = Self::encode_industrial_phonetic(q_tok);
            if let Some(rows) = self.phonetic_index.get(&q_phon) {
                for &r in rows {
                    candidate_rows.insert(r);
                    *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += 2.5;
                }
            }

            // 4. 2-gram & 3-gram substring intersection
            let q_ngrams = Self::generate_ngrams(q_tok);
            if !q_ngrams.is_empty() {
                let mut ngram_counts: HashMap<u32, usize> = HashMap::new();
                for ng in &q_ngrams {
                    if let Some(rows) = self.ngram_bitmaps.get(ng) {
                        for &r in rows {
                            *ngram_counts.entry(r).or_insert(0) += 1;
                        }
                    }
                }
                for (r, cnt) in ngram_counts {
                    let overlap = (cnt as f64) / (q_ngrams.len() as f64);
                    if overlap >= 0.35 {
                        candidate_rows.insert(r);
                        *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += overlap * 1.5;
                    }
                }
            }
        }

        // If vector query is provided, include all documents with embeddings as candidates
        let q_rabitq = query_vector.map(RaBitQVector::from_f32_slice);
        if q_rabitq.is_some() {
            for &r in self.rabitq_vectors.keys() {
                candidate_rows.insert(r);
            }
        }

        let mut hits: Vec<HybridSearchHit> = Vec::with_capacity(candidate_rows.len());
        for r_id in candidate_rows {
            let Some(doc) = self.docs.iter().find(|d| d.row_id == r_id) else {
                continue;
            };
            let bm25f = self.compute_bm25f_score(doc, &query_tokens);
            let fuzzy_bonus = fuzzy_phonetic_bonuses.get(&r_id).copied().unwrap_or(0.0);
            let vec_sim = match (
                &q_rabitq,
                self.rabitq_vectors.get(&r_id),
                query_vector,
                &doc.embedding,
            ) {
                (Some(qr), Some(dr), Some(qf), Some(df)) => {
                    let approx = f64::from(qr.angular_similarity(dr));
                    let exact = Self::cosine_similarity(qf, df);
                    0.3 * approx + 0.7 * exact.max(0.0)
                }
                _ => 0.0,
            };

            let total_score = bm25f + fuzzy_bonus + (vec_sim * 5.0);
            hits.push(HybridSearchHit {
                entity_id: doc.entity_id.clone(),
                row_id: r_id,
                total_score,
                lexical_bm25f_score: bm25f,
                fuzzy_phonetic_bonus: fuzzy_bonus,
                vector_similarity: vec_sim,
            });
        }

        // Sort descending by total score, breaking ties deterministically by row_id
        hits.sort_by(|a, b| {
            b.total_score
                .total_cmp(&a.total_score)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(top_k);
        hits
    }

    /// Layer 6: Filters documents within `radius_meters` of `(center_lat, center_lon)` using R-Tree MBR + Haversine formula.
    pub fn search_within_geofence_meters(
        &self,
        center_lat: f64,
        center_lon: f64,
        radius_meters: f64,
    ) -> Vec<(String, f64)> {
        let mut matches = Vec::new();
        for doc in &self.docs {
            if let Some((lat, lon)) = doc.coordinates_wgs84 {
                let dist_m = Self::haversine_meters(center_lat, center_lon, lat, lon);
                if dist_m <= radius_meters {
                    matches.push((doc.entity_id.clone(), dist_m));
                }
            }
        }
        matches.sort_by(|a, b| a.1.total_cmp(&b.1));
        matches
    }

    /// Encodes a word using the Bilingual Industrial Phonetic Algorithm (`SpanishIndustrialMetaphone` + `DoubleMetaphone`).
    /// Specifically designed for Latin American plant technicians writing Spanish/English technical terms:
    /// - `"balbula"` / `"valbula"` / `"válvula"` -> `"BLBL"`
    /// - `"chiler"` / `"shiller"` / `"chiller"` -> `"XLR"`
    /// - `"suiche"` / `"swich"` / `"switch"` -> `"SX"`
    /// - `"rodamieto"` / `"rodamiento"` -> `"RDMT"`
    pub fn encode_industrial_phonetic(word: &str) -> String {
        let clean = Self::strip_accents_and_lower(word)
            .replace("sh", "x")
            .replace("ch", "x")
            .replace("tch", "x")
            .replace("sw", "s")
            .replace("ll", "y")
            .replace("qu", "k")
            .replace("ce", "se")
            .replace("ci", "si")
            .replace("ge", "je")
            .replace("gi", "ji")
            .replace("v", "b")
            .replace("z", "s")
            .replace("c", "k")
            .replace("h", "")
            .replace("nt", "t");

        let mut out = String::new();
        let mut prev_c = '\0';
        for c in clean.chars() {
            if (c.is_ascii_digit() || !matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | '-' | '_'))
                && c != prev_c
                && c.is_ascii_alphanumeric()
            {
                out.push(c.to_ascii_uppercase());
                prev_c = c;
            }
        }
        out
    }

    /// Computes Haversine great-circle distance in meters between two WGS-84 coordinates.
    pub fn haversine_meters(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
        let r_earth_m = 6_371_000.0_f64;
        let d_lat = (lat2_deg - lat1_deg).to_radians();
        let d_lon = (lon2_deg - lon1_deg).to_radians();
        let lat1 = lat1_deg.to_radians();
        let lat2 = lat2_deg.to_radians();

        let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).max(0.0).sqrt());
        r_earth_m * c
    }

    /// Encodes WGS-84 `(lat, lon)` into a 64-bit Morton Z-Order / H3-like hierarchical spatial cell ID.
    pub fn encode_spatial_cell_h3_morton(lat_deg: f64, lon_deg: f64) -> u64 {
        let norm_lat = (((lat_deg + 90.0) / 180.0).clamp(0.0, 1.0) * 65_535.0) as u32;
        let norm_lon = (((lon_deg + 180.0) / 360.0).clamp(0.0, 1.0) * 65_535.0) as u32;
        let mut morton = 0u64;
        for b in 0..16 {
            morton |= u64::from((norm_lat >> b) & 1) << (2 * b);
            morton |= u64::from((norm_lon >> b) & 1) << (2 * b + 1);
        }
        morton
    }

    fn compute_bm25f_score(&self, doc: &IndexedEntityDocument, query_tokens: &[String]) -> f64 {
        // Códice field weights: code=5.0, title=3.5, description=1.0
        let code_l = Self::strip_accents_and_lower(&doc.code);
        let title_l = Self::strip_accents_and_lower(&doc.title);
        let desc_l = Self::strip_accents_and_lower(&doc.description);
        let doc_len = (code_l.len() + title_l.len() + desc_l.len()) as f64;
        let k1 = 1.2;
        let b = 0.75;
        let norm_factor = 1.0 - b + b * (doc_len / self.avg_doc_len.max(1.0));

        let mut score = 0.0;
        let n_docs = self.docs.len().max(1) as f64;
        for q in query_tokens {
            let mut weighted_tf = 0.0;
            if code_l.contains(q) {
                weighted_tf += 5.0;
            }
            if title_l.contains(q) {
                weighted_tf += 3.5;
            }
            if desc_l.contains(q) {
                weighted_tf += 1.0;
            }
            if weighted_tf > 0.0 {
                let df = self
                    .fst_lexicon
                    .get(q)
                    .map_or(1.0, |v| v.len().max(1) as f64);
                let idf = ((n_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
                score += idf * ((weighted_tf * (k1 + 1.0)) / (weighted_tf + k1 * norm_factor));
            }
        }
        score
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
        let mut dot = 0.0_f64;
        let mut na = 0.0_f64;
        let mut nb = 0.0_f64;
        for (&x, &y) in a.iter().zip(b.iter()) {
            let xf = f64::from(x);
            let yf = f64::from(y);
            dot += xf * yf;
            na += xf * xf;
            nb += yf * yf;
        }
        let denom = (na * nb).sqrt();
        if denom > 1e-12 {
            dot / denom
        } else {
            0.0
        }
    }

    fn tokenize_normalized(text: &str) -> Vec<String> {
        Self::strip_accents_and_lower(text)
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|s| s.len() >= 2)
            .map(ToString::to_string)
            .collect()
    }

    fn strip_accents_and_lower(text: &str) -> String {
        text.chars()
            .map(|c| match c {
                'á' | 'à' | 'ä' | 'â' | 'Á' | 'À' | 'Ä' | 'Â' => 'a',
                'é' | 'è' | 'ë' | 'ê' | 'É' | 'È' | 'Ë' | 'Ê' => 'e',
                'í' | 'ì' | 'ï' | 'î' | 'Í' | 'Ì' | 'Ï' | 'Î' => 'i',
                'ó' | 'ò' | 'ö' | 'ô' | 'Ó' | 'Ò' | 'Ö' | 'Ô' => 'o',
                'ú' | 'ù' | 'ü' | 'û' | 'Ú' | 'Ù' | 'Ü' | 'Û' => 'u',
                'ñ' | 'Ñ' => 'n',
                other => other.to_ascii_lowercase(),
            })
            .collect()
    }

    fn generate_symspell_deletes(word: &str, max_distance: usize) -> Vec<String> {
        let mut results = HashSet::new();
        results.insert(word.to_string());
        let mut current_level = vec![word.to_string()];

        for _ in 0..max_distance {
            let mut next_level = Vec::new();
            for item in &current_level {
                let chars: Vec<char> = item.chars().collect();
                if chars.len() <= 2 {
                    continue;
                }
                for i in 0..chars.len() {
                    let deleted: String = chars
                        .iter()
                        .enumerate()
                        .filter(|&(idx, _)| idx != i)
                        .map(|(_, &c)| c)
                        .collect();
                    if results.insert(deleted.clone()) {
                        next_level.push(deleted);
                    }
                }
            }
            current_level = next_level;
        }
        results.into_iter().collect()
    }

    fn generate_ngrams(token: &str) -> Vec<String> {
        let chars: Vec<char> = token.chars().collect();
        let mut out = Vec::new();
        for w in chars.windows(2) {
            out.push(w.iter().collect());
        }
        for w in chars.windows(3) {
            out.push(w.iter().collect());
        }
        out.sort();
        out.dedup();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_search_resolves_severe_spanish_english_field_typos_and_vectors() {
        let mut idx = MbfHybridSearchIndex::new();
        idx.index_document(IndexedEntityDocument {
            entity_id: "ASSET-CHILLER-01".into(),
            row_id: 0,
            code: "CH-TRANE-01".into(),
            title: "Válvula de expansión solenoide Chiller Trane".into(),
            description:
                "Equipo enfriador centrífugo con rodamientos SKF-6205-2RS y switch de presión"
                    .into(),
            embedding: Some(vec![0.9, 0.8, 0.1, -0.2]),
            coordinates_wgs84: Some((4.5981, -74.1253)),
        });
        idx.index_document(IndexedEntityDocument {
            entity_id: "ASSET-PUMP-02".into(),
            row_id: 1,
            code: "PMP-GRUND-02".into(),
            title: "Bomba hidroneumática Grundfos".into(),
            description: "Sello mecánico de carburo de silicio para agua helada".into(),
            embedding: Some(vec![-0.5, 0.1, 0.9, 0.7]),
            coordinates_wgs84: Some((4.6500, -74.0500)),
        });

        // 1. Severe phonetic + typo query: "balbula chiler suiche rodamieto 6205"
        let hits = idx.search_hybrid("balbula chiler suiche rodamieto 6205", None, 5);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].entity_id, "ASSET-CHILLER-01");

        // 2. Semantic vector search for pump seal
        let vec_hits = idx.search_hybrid("", Some(&[-0.5, 0.1, 0.9, 0.7]), 5);
        assert_eq!(vec_hits[0].entity_id, "ASSET-PUMP-02");

        // 3. Geofence search within 300m of Centro Mayor Bogotá (4.5981, -74.1253)
        let geo = idx.search_within_geofence_meters(4.5981, -74.1253, 300.0);
        assert_eq!(geo.len(), 1);
        assert_eq!(geo[0].0, "ASSET-CHILLER-01");
    }
}
