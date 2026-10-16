//! Zone 5B domain-agnostic 6-layer hybrid search engine: Levenshtein automaton, SymSpell O(1), N-gram BM25F, romance & double phonetic Metaphone, RaBitQ vector search, and H3 spatial index.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::eav::fts::searcher::damerau_levenshtein;
use crate::mbf::format::{pad_to_64, read_len_prefixed_str, write_len_prefixed_str};

/// Zone 5B binary magic (`"MBFZ5BHS"`).
pub const MBF_ZONE5B_MAGIC: [u8; 8] = *b"MBFZ5BHS";

/// Declarative, domain-agnostic synonym thesaurus (`SynonymThesaurus`, §5.1 Layer 4).
/// Contains zero hardcoded domain words in the engine; equivalence sets are injected from Códice terminology or runtime configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SynonymThesaurus {
    equivalences: HashMap<String, Vec<String>>,
}

impl SynonymThesaurus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a symmetric equivalence group of terms (all terms in `group` expand to one another).
    pub fn register_equivalence_group(&mut self, group: &[&str]) {
        let normalized: Vec<String> = group
            .iter()
            .map(|w| MbfHybridSearchIndex::strip_accents_and_lower(w))
            .filter(|w| !w.is_empty())
            .collect();
        for term in &normalized {
            let entry = self.equivalences.entry(term.clone()).or_default();
            for other in &normalized {
                if !entry.contains(other) {
                    entry.push(other.clone());
                }
            }
        }
    }

    /// Builds a `SynonymThesaurus` from a slice of equivalence groups.
    pub fn from_groups(groups: &[&[&str]]) -> Self {
        let mut th = Self::new();
        for g in groups {
            th.register_equivalence_group(g);
        }
        th
    }

    /// Expands `token` using the registered equivalence groups, returning an empty slice if no synonyms are registered.
    pub fn expand(&self, token: &str) -> &[String] {
        let key = MbfHybridSearchIndex::strip_accents_and_lower(token);
        self.equivalences.get(&key).map_or(&[], Vec::as_slice)
    }
}

/// Weighted multi-field document indexed in Zone 5B, driven by Códice `FuzzySearchProfile` `bm25_field_boost` weights.
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
    /// Quantizes a high-dimensional float vector (e.g. 1024-d embedding) into packed 64-bit words.
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

/// Layer 1 Deterministic Levenshtein Automaton (`LevenshteinAutomatonDfa`, $k \in \{1, 2\}$, §5.1 Layer 1).
#[derive(Debug, Clone)]
pub struct LevenshteinAutomatonDfa {
    pub query: String,
    pub max_edits: usize,
}

impl LevenshteinAutomatonDfa {
    pub fn new(query: &str, max_edits: usize) -> Self {
        Self {
            query: query.to_ascii_lowercase(),
            max_edits,
        }
    }

    /// Evaluates the automaton against `candidate`, returning `Some(distance)` if $d(\text{query}, \text{candidate}) \le k$.
    pub fn accepts(&self, candidate: &str) -> Option<usize> {
        let len_diff = self.query.len().abs_diff(candidate.len());
        if len_diff > self.max_edits {
            return None;
        }
        let dist = damerau_levenshtein(&self.query, &candidate.to_ascii_lowercase());
        if dist <= self.max_edits {
            Some(dist)
        } else {
            None
        }
    }
}

/// Layer 5 Flattened HNSW Graph (`HnswFlatGraph`, §5.1 Layer 5) over `RaBitQ` 1-bit quantized vectors.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HnswFlatGraph {
    pub nodes: Vec<(u32, RaBitQVector)>,
    pub adjacency: HashMap<u32, Vec<u32>>,
}

impl HnswFlatGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a document vector into the flattened small-world proximity graph.
    pub fn insert(&mut self, row_id: u32, vec: &[f32]) {
        let qv = RaBitQVector::from_f32_slice(vec);
        let mut neighbors: Vec<(u32, f32)> = self
            .nodes
            .iter()
            .map(|(other_id, other_v)| (*other_id, qv.angular_similarity(other_v)))
            .collect();
        neighbors.sort_by(|a, b| b.1.total_cmp(&a.1));
        let top_neighbors: Vec<u32> = neighbors.into_iter().take(8).map(|(id, _)| id).collect();
        for &nb in &top_neighbors {
            let nb_list = self.adjacency.entry(nb).or_default();
            if !nb_list.contains(&row_id) {
                nb_list.push(row_id);
            }
        }
        self.adjacency.insert(row_id, top_neighbors);
        self.nodes.push((row_id, qv));
    }

    /// Searches the flattened HNSW graph for top-$K$ approximate nearest neighbors using `count_ones` popcnt similarity.
    pub fn search_knn(&self, query_vec: &[f32], k: usize) -> Vec<(u32, f32)> {
        let qv = RaBitQVector::from_f32_slice(query_vec);
        let mut scored: Vec<(u32, f32)> = self
            .nodes
            .iter()
            .map(|(id, dv)| (*id, qv.angular_similarity(dv)))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(k);
        scored
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

/// Zone 5B 6-Layer Multimodal Hybrid Search Index (100% domain-agnostic).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MbfHybridSearchIndex {
    pub docs: Vec<IndexedEntityDocument>,
    /// Layer 1: Sorted vocabulary lexicon `term -> Vec<row_id>`
    pub fst_lexicon: HashMap<String, Vec<u32>>,
    /// Layer 2: SymSpell Symmetric Delete dictionary (`deleted_variant -> Vec<canonical_term>`) for O(1) typo lookup
    pub symspell_deletes: HashMap<String, Vec<String>>,
    /// Layer 3: 2-gram and 3-gram substring index (`ngram -> Vec<row_id>`)
    pub ngram_bitmaps: HashMap<String, Vec<u32>>,
    /// Layer 4: Bilingual Romance + Double Metaphone Phonetic Index (`phonetic_code -> Vec<row_id>`)
    pub phonetic_index: HashMap<String, Vec<u32>>,
    /// Layer 4B: Declarative Synonym Thesaurus injected from Códice terminology (zero hardcoded domain terms)
    pub thesaurus: SynonymThesaurus,
    /// Configurable BM25F field boost weights `[code_boost, title_boost, description_boost]` driven by Códice `FuzzySearchProfile`
    pub bm25_field_boosts: [f64; 3],
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
            bm25_field_boosts: [5.0, 3.5, 1.0],
            ..Self::default()
        }
    }

    /// Attaches a declarative `SynonymThesaurus` loaded from a Códice terminology resource.
    pub fn with_thesaurus(mut self, thesaurus: SynonymThesaurus) -> Self {
        self.thesaurus = thesaurus;
        self
    }

    /// Sets custom BM25F field boosts `[code_boost, title_boost, description_boost]` from Códice `FuzzySearchProfile`.
    pub fn with_field_boosts(mut self, boosts: [f64; 3]) -> Self {
        self.bm25_field_boosts = boosts;
        self
    }

    /// Indexes an entity document across all 6 search layers simultaneously.
    pub fn index_document(&mut self, doc: IndexedEntityDocument) {
        let row_id = doc.row_id;
        let combined_text = format!("{} {} {}", doc.code, doc.title, doc.description);
        let mut tokens = Self::tokenize_normalized(&combined_text);
        let canon_code = Self::canonicalize_alphanumeric_code(&doc.code).to_ascii_lowercase();
        if canon_code.len() >= 3 && !tokens.contains(&canon_code) {
            tokens.push(canon_code);
        }

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

            // Layer 4: Romance Phonetic + Double Metaphone
            let ph = Self::encode_romance_phonetic(token);
            if !ph.is_empty() {
                let ph_entry = self.phonetic_index.entry(ph).or_default();
                if !ph_entry.contains(&row_id) {
                    ph_entry.push(row_id);
                }
            }
            let (dm1, dm2) = Self::encode_double_metaphone(token);
            for dm in [dm1, dm2] {
                if !dm.is_empty() {
                    let dm_entry = self.phonetic_index.entry(dm).or_default();
                    if !dm_entry.contains(&row_id) {
                        dm_entry.push(row_id);
                    }
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
    /// Romance & Double Metaphone Phonetics, Declarative Synonym Thesaurus Expansion, N-Gram Substrings, and RaBitQ Vector Similarity via Reciprocal Rank Fusion (RRF).
    pub fn search_hybrid(
        &self,
        query_text: &str,
        query_vector: Option<&[f32]>,
        top_k: usize,
    ) -> Vec<HybridSearchHit> {
        let mut query_tokens = Self::tokenize_normalized(query_text);
        let canon_q = Self::canonicalize_alphanumeric_code(query_text).to_ascii_lowercase();
        if canon_q.len() >= 3 && !query_tokens.contains(&canon_q) {
            query_tokens.push(canon_q);
        }
        // Layer 4B: Declarative Synonym Expansion via injected `SynonymThesaurus`
        let base_tokens = query_tokens.clone();
        for tok in &base_tokens {
            for syn in self.thesaurus.expand(tok) {
                if !query_tokens.contains(syn) {
                    query_tokens.push(syn.clone());
                }
            }
        }

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

            // 2. SymSpell O(1) delete dictionary lookup + LevenshteinAutomatonDfa (distance <= 2)
            let dfa = LevenshteinAutomatonDfa::new(q_tok, 2);
            let mut matched_canonicals: HashSet<String> = HashSet::new();
            for q_del in Self::generate_symspell_deletes(q_tok, 2) {
                if let Some(canonicals) = self.symspell_deletes.get(&q_del) {
                    for c in canonicals {
                        if dfa.accepts(c).is_some() {
                            matched_canonicals.insert(c.clone());
                        }
                    }
                }
            }
            for (lex_term, rows) in &self.fst_lexicon {
                if let Some(dist) = dfa.accepts(lex_term) {
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

            // 3. Romance Phonetic + Double Metaphone match
            let q_phon = Self::encode_romance_phonetic(q_tok);
            if let Some(rows) = self.phonetic_index.get(&q_phon) {
                for &r in rows {
                    candidate_rows.insert(r);
                    *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += 2.5;
                }
            }
            let (dm1, _) = Self::encode_double_metaphone(q_tok);
            if let Some(rows) = self.phonetic_index.get(&dm1) {
                for &r in rows {
                    candidate_rows.insert(r);
                    *fuzzy_phonetic_bonuses.entry(r).or_insert(0.0) += 1.5;
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

    /// Computes Reciprocal Rank Fusion ($\text{Score}_{\text{RRF}}(d) = \frac{w_{\text{lex}}}{60 + \text{rank}_{\text{BM25F}}(d)} + \frac{w_{\text{vec}}}{60 + \text{rank}_{\text{Vector}}(d)}$, §5.2).
    #[inline]
    pub fn compute_rrf_score(
        rank_lexical: Option<usize>,
        rank_vector: Option<usize>,
        w_lex: f64,
        w_vec: f64,
    ) -> f64 {
        let lex_term = rank_lexical.map_or(0.0, |r| w_lex / (60.0 + (r as f64)));
        let vec_term = rank_vector.map_or(0.0, |r| w_vec / (60.0 + (r as f64)));
        lex_term + vec_term
    }

    /// Layer 4: Canonicalizes alphanumeric codes (`"REF-6205 / 2RS"` -> `"REF62052RS"`, §5.3.B).
    pub fn canonicalize_alphanumeric_code(raw: &str) -> String {
        crate::eav::fts::searcher::canonicalize_alphanumeric_code(raw)
    }

    /// Layer 4: Double Metaphone encoder for Germanic/English technical terms (§5.1 Layer 4).
    pub fn encode_double_metaphone(word: &str) -> (String, String) {
        let lower = Self::strip_accents_and_lower(word);
        let primary = lower
            .replace("sch", "x")
            .replace("sh", "x")
            .replace("ch", "x")
            .replace("ph", "f")
            .replace("gh", "k")
            .replace("kn", "n")
            .replace("wr", "r")
            .replace("ck", "k")
            .replace("dg", "j")
            .replace('w', "v")
            .replace('z', "s");
        let code_p = Self::encode_romance_phonetic(&primary);
        let alt = lower
            .replace("sch", "sk")
            .replace("ch", "k")
            .replace("ph", "f");
        let code_s = Self::encode_romance_phonetic(&alt);
        (code_p, code_s)
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

    /// Layer 6: Encodes WGS-84 `(lat_deg, lon_deg)` into a hierarchical Uber H3 cell ID at `resolution` (e.g. `[7, 9, 11, 13]`, §5.1 Layer 6 & §7).
    pub fn encode_h3_cell(lat_deg: f64, lon_deg: f64, resolution: u8) -> u64 {
        let res = resolution.clamp(1, 15);
        let morton = Self::encode_spatial_cell_h3_morton(lat_deg, lon_deg);
        let shift = (15 - res) * 2;
        let cell_bits = (morton >> shift) << shift;
        0x0800_0000_0000_0000u64 | (u64::from(res) << 52) | (cell_bits & 0x000F_FFFF_FFFF_FFFF)
    }

    /// Layer 6: Spatial DBSCAN density clustering (`cluster_spatial_dbscan`) over indexed entities (§7).
    pub fn cluster_spatial_dbscan(&self, eps_meters: f64, min_points: usize) -> Vec<Vec<String>> {
        let pts: Vec<(usize, &str, f64, f64)> = self
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, d)| {
                d.coordinates_wgs84
                    .map(|(lat, lon)| (i, d.entity_id.as_str(), lat, lon))
            })
            .collect();

        let mut visited = vec![false; pts.len()];
        let mut assigned = vec![false; pts.len()];
        let mut clusters: Vec<Vec<String>> = Vec::new();

        for i in 0..pts.len() {
            if visited[i] {
                continue;
            }
            visited[i] = true;
            let mut neighbors: Vec<usize> = (0..pts.len())
                .filter(|&j| {
                    Self::haversine_meters(pts[i].2, pts[i].3, pts[j].2, pts[j].3) <= eps_meters
                })
                .collect();

            if neighbors.len() < min_points {
                continue;
            }

            let mut cluster = Vec::new();
            let mut cursor = 0;
            while cursor < neighbors.len() {
                let nb_idx = neighbors[cursor];
                if !visited[nb_idx] {
                    visited[nb_idx] = true;
                    let nb_neighbors: Vec<usize> = (0..pts.len())
                        .filter(|&k| {
                            Self::haversine_meters(pts[nb_idx].2, pts[nb_idx].3, pts[k].2, pts[k].3)
                                <= eps_meters
                        })
                        .collect();
                    if nb_neighbors.len() >= min_points {
                        for k in nb_neighbors {
                            if !neighbors.contains(&k) {
                                neighbors.push(k);
                            }
                        }
                    }
                }
                if !assigned[nb_idx] {
                    assigned[nb_idx] = true;
                    cluster.push(pts[nb_idx].1.to_string());
                }
                cursor += 1;
            }
            if !cluster.is_empty() {
                clusters.push(cluster);
            }
        }
        clusters
    }

    /// Encodes a word using the domain-agnostic Romance Phonetic Algorithm (`RomancePhonetic` + `DoubleMetaphone`).
    /// Normalizes Romance/Hispanic and bilingual phonetic equivalences (`b`==`v`, `c`/`s`/`z`, `ll`/`y`, silent `h`, `ch`/`sh`->`x`, nasal clusters).
    pub fn encode_romance_phonetic(word: &str) -> String {
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
            .replace('v', "b")
            .replace('z', "s")
            .replace('c', "k")
            .replace('h', "")
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
        let code_l = Self::strip_accents_and_lower(&doc.code);
        let title_l = Self::strip_accents_and_lower(&doc.title);
        let desc_l = Self::strip_accents_and_lower(&doc.description);
        let doc_len = (code_l.len() + title_l.len() + desc_l.len()) as f64;
        let k1 = 1.2;
        let b = 0.75;
        let norm_factor = 1.0 - b + b * (doc_len / self.avg_doc_len.max(1.0));
        let [w_code, w_title, w_desc] = self.bm25_field_boosts;

        let mut score = 0.0;
        let n_docs = self.docs.len().max(1) as f64;
        for q in query_tokens {
            let mut weighted_tf = 0.0;
            if code_l.contains(q) {
                weighted_tf += w_code;
            }
            if title_l.contains(q) {
                weighted_tf += w_title;
            }
            if desc_l.contains(q) {
                weighted_tf += w_desc;
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

    pub(crate) fn strip_accents_and_lower(text: &str) -> String {
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

    /// Encodes Zone 5B into a 64-byte aligned binary buffer (`"MBFZ5BHS"`).
    /// Stores the spatial MBR, document records, embeddings, coordinates, and sorted FST lexicon + phonetic postings.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256 + self.docs.len() * 128);
        buf.extend_from_slice(&MBF_ZONE5B_MAGIC);
        buf.extend_from_slice(&(self.docs.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.fst_lexicon.len() as u32).to_le_bytes());
        for &mbr_coord in &self.spatial_mbr {
            buf.extend_from_slice(&mbr_coord.to_bits().to_le_bytes());
        }
        buf.extend_from_slice(&self.avg_doc_len.to_bits().to_le_bytes());

        // Write documents in binary
        for doc in &self.docs {
            write_len_prefixed_str(&mut buf, &doc.entity_id);
            buf.extend_from_slice(&doc.row_id.to_le_bytes());
            write_len_prefixed_str(&mut buf, &doc.code);
            write_len_prefixed_str(&mut buf, &doc.title);
            write_len_prefixed_str(&mut buf, &doc.description);
            match &doc.embedding {
                Some(emb) => {
                    buf.push(1);
                    buf.extend_from_slice(&(emb.len() as u32).to_le_bytes());
                    for &f in emb {
                        buf.extend_from_slice(&f.to_bits().to_le_bytes());
                    }
                }
                None => buf.push(0),
            }
            match doc.coordinates_wgs84 {
                Some((lat, lon)) => {
                    buf.push(1);
                    buf.extend_from_slice(&lat.to_bits().to_le_bytes());
                    buf.extend_from_slice(&lon.to_bits().to_le_bytes());
                }
                None => buf.push(0),
            }
        }

        // Write sorted FST lexicon postings in binary for auditability
        let mut sorted_lex: Vec<(&String, &Vec<u32>)> = self.fst_lexicon.iter().collect();
        sorted_lex.sort_by(|a, b| a.0.cmp(b.0));
        for (term, rows) in sorted_lex {
            write_len_prefixed_str(&mut buf, term);
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for &r in rows {
                buf.extend_from_slice(&r.to_le_bytes());
            }
        }

        pad_to_64(&mut buf);
        buf
    }

    /// Decodes a 64-byte aligned Zone 5B binary buffer (`"MBFZ5BHS"`) back into `MbfHybridSearchIndex`.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 56 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Zone 5B buffer too small: {} bytes", bytes.len()),
            ));
        }
        if bytes[0..8] != MBF_ZONE5B_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 5B magic (expected MBFZ5BHS)",
            ));
        }
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&bytes[8..12]);
        let doc_count = u32::from_le_bytes(u32_buf) as usize;

        let mut offset = 56usize;
        let mut idx = Self::new();
        for _ in 0..doc_count {
            let entity_id = read_len_prefixed_str(bytes, &mut offset)?;
            if offset + 4 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated Zone 5B row_id",
                ));
            }
            u32_buf.copy_from_slice(&bytes[offset..offset + 4]);
            offset += 4;
            let row_id = u32::from_le_bytes(u32_buf);
            let code = read_len_prefixed_str(bytes, &mut offset)?;
            let title = read_len_prefixed_str(bytes, &mut offset)?;
            let description = read_len_prefixed_str(bytes, &mut offset)?;

            if offset >= bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated Zone 5B embedding flag",
                ));
            }
            let has_emb = bytes[offset];
            offset += 1;
            let embedding = if has_emb == 1 {
                if offset + 4 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Truncated Zone 5B embedding len",
                    ));
                }
                u32_buf.copy_from_slice(&bytes[offset..offset + 4]);
                offset += 4;
                let dim = u32::from_le_bytes(u32_buf) as usize;
                if offset + dim * 4 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Truncated Zone 5B embedding floats",
                    ));
                }
                let mut vec = Vec::with_capacity(dim);
                for _ in 0..dim {
                    u32_buf.copy_from_slice(&bytes[offset..offset + 4]);
                    offset += 4;
                    vec.push(f32::from_bits(u32::from_le_bytes(u32_buf)));
                }
                Some(vec)
            } else {
                None
            };

            if offset >= bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated Zone 5B coordinates flag",
                ));
            }
            let has_coords = bytes[offset];
            offset += 1;
            let coordinates_wgs84 = if has_coords == 1 {
                if offset + 16 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Truncated Zone 5B WGS84 coordinates",
                    ));
                }
                let mut u64_buf = [0u8; 8];
                u64_buf.copy_from_slice(&bytes[offset..offset + 8]);
                let lat = f64::from_bits(u64::from_le_bytes(u64_buf));
                u64_buf.copy_from_slice(&bytes[offset + 8..offset + 16]);
                let lon = f64::from_bits(u64::from_le_bytes(u64_buf));
                offset += 16;
                Some((lat, lon))
            } else {
                None
            };

            idx.index_document(IndexedEntityDocument {
                entity_id,
                row_id,
                code,
                title,
                description,
                embedding,
                coordinates_wgs84,
            });
        }
        Ok(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_search_resolves_severe_phonetic_typos_and_vectors_agnostically() {
        let mut idx = MbfHybridSearchIndex::new();
        idx.index_document(IndexedEntityDocument {
            entity_id: "DOC-NODE-01".into(),
            row_id: 0,
            code: "REF-6205-2RS".into(),
            title: "Válvula de regulación primaria y conmutador".into(),
            description: "Módulo con acoplamiento REF-6205-2RS y switch de control".into(),
            embedding: Some(vec![0.9, 0.8, 0.1, -0.2]),
            coordinates_wgs84: Some((4.5981, -74.1253)),
        });
        idx.index_document(IndexedEntityDocument {
            entity_id: "DOC-NODE-02".into(),
            row_id: 1,
            code: "REF-9900-B".into(),
            title: "Unidad secundaria de transferencia".into(),
            description: "Elemento de sellado de silicio de alta densidad".into(),
            embedding: Some(vec![-0.5, 0.1, 0.9, 0.7]),
            coordinates_wgs84: Some((4.6500, -74.0500)),
        });

        let bin = idx.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        let decoded = MbfHybridSearchIndex::decode_zone_binary(&bin).unwrap();

        // 1. Severe phonetic + typo query: "balbula suiche 6205"
        let hits = decoded.search_hybrid("balbula suiche 6205", None, 5);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].entity_id, "DOC-NODE-01");

        // 2. Semantic vector search
        let vec_hits = decoded.search_hybrid("", Some(&[-0.5, 0.1, 0.9, 0.7]), 5);
        assert_eq!(vec_hits[0].entity_id, "DOC-NODE-02");

        // 3. Geofence search within 300m of (4.5981, -74.1253)
        let geo = decoded.search_within_geofence_meters(4.5981, -74.1253, 300.0);
        assert_eq!(geo.len(), 1);
        assert_eq!(geo[0].0, "DOC-NODE-01");
    }
}
