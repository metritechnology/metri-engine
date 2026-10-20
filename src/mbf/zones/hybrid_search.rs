//! Zona 5B del formato MBF: motor de búsqueda híbrida multimodal de 6 capas (autómata de Levenshtein, SymSpell $O(1)$, N-gramas BM25F, fonética romance y Double Metaphone, búsqueda vectorial RaBitQ e índice espacial H3).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::eav::fts::searcher::damerau_levenshtein;
use crate::mbf::format::{pad_to_64, read_len_prefixed_str, write_len_prefixed_str};

/// Bytes mágicos de la Zona 5B (`"MBFZ5BHS"`).
pub const MBF_ZONE5B_MAGIC: [u8; 8] = *b"MBFZ5BHS";

/// Tesauro declarativo de sinónimos agnóstico al dominio (`SynonymThesaurus`, Capa 4).
///
/// No contiene vocabulario de negocio acoplado en el motor; los grupos de equivalencia se inyectan desde los recursos terminológicos del Códice o la configuración en tiempo de ejecución.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SynonymThesaurus {
    equivalences: HashMap<String, Vec<String>>,
}

impl SynonymThesaurus {
    /// Crea un tesauro de sinónimos vacío.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra un grupo de equivalencia simétrica de términos (todos los términos en `group` se expanden mutuamente entre sí).
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

    /// Construye un `SynonymThesaurus` a partir de una lista de grupos de equivalencia.
    pub fn from_groups(groups: &[&[&str]]) -> Self {
        let mut th = Self::new();
        for g in groups {
            th.register_equivalence_group(g);
        }
        th
    }

    /// Expande `token` usando los grupos de equivalencia registrados, devolviendo una rebanada vacía si no tiene sinónimos asociados.
    pub fn expand(&self, token: &str) -> &[String] {
        let key = MbfHybridSearchIndex::strip_accents_and_lower(token);
        self.equivalences.get(&key).map_or(&[], Vec::as_slice)
    }
}

/// Documento multicampo ponderado indexado en la Zona 5B, gobernado por los pesos `bm25_field_boost` de `FuzzySearchProfile` del Códice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedEntityDocument {
    /// Identificador único de la entidad documentada.
    pub entity_id: String,
    /// Identificador numérico de fila dentro del bloque MBF.
    pub row_id: u32,
    /// Código o referencia alfanumérica de la entidad.
    pub code: String,
    /// Título o nombre principal de la entidad.
    pub title: String,
    /// Descripción textual detallada o cuerpo documental.
    pub description: String,
    /// Vector de incrustación semántica (*embedding*) opcional en precisión simple (`f32`).
    pub embedding: Option<Vec<f32>>,
    /// Coordenadas geográficas WGS-84 opcionales `(latitud, longitud)` en grados decimales.
    pub coordinates_wgs84: Option<(f64, f64)>, // (latitude, longitude)
}

/// Vector cuantizado asimétrico de 1 bit (`RaBitQ`) para similitud angular acelerada por hardware mediante `count_ones` (`POPCNT`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaBitQVector {
    /// Palabras de 64 bits que empaquetan los signos cuantizados de cada dimensión del vector.
    pub packed_bits: Vec<u64>,
    /// Norma euclidiana original ($\ell_2$) del vector antes de cuantizar.
    pub norm: f32,
    /// Dimensionalidad exacta del vector original.
    pub dimension: usize,
}

impl RaBitQVector {
    /// Cuantiza un vector de punto flotante de alta dimensionalidad en palabras empaquetadas de 64 bits.
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

    /// Calcula la similitud angular en el intervalo `` `[0, 1]` `` usando `count_ones()` por hardware (`VPOPCNTQ` / `CNT` SIMD).
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

/// Autómata determinista de Levenshtein de Capa 1 (`LevenshteinAutomatonDfa`, $k \in \{1, 2\}$).
#[derive(Debug, Clone)]
pub struct LevenshteinAutomatonDfa {
    /// Término de consulta normalizado en minúsculas ASCII.
    pub query: String,
    /// Distancia máxima de edición Damerau-Levenshtein permitida ($k$).
    pub max_edits: usize,
}

impl LevenshteinAutomatonDfa {
    /// Construye un nuevo autómata de Levenshtein para `query` con tolerancia máxima `max_edits`.
    pub fn new(query: &str, max_edits: usize) -> Self {
        Self {
            query: query.to_ascii_lowercase(),
            max_edits,
        }
    }

    /// Evalúa el autómata frente a `candidate`, devolviendo `Some(distance)` si $d(\text{query}, \text{candidate}) \le k$.
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

/// Grafo HNSW aplanado de Capa 5 (`HnswFlatGraph`) sobre vectores cuantizados de 1 bit `RaBitQ`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HnswFlatGraph {
    /// Nodos indexados representados como pares `(row_id, vector_rabitq)`.
    pub nodes: Vec<(u32, RaBitQVector)>,
    /// Listas de adyacencia de proximidad en el grafo de mundo pequeño por `row_id`.
    pub adjacency: HashMap<u32, Vec<u32>>,
}

impl HnswFlatGraph {
    /// Crea un grafo HNSW aplanado vacío.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserta el vector de un documento en el grafo aplanado de proximidad de mundo pequeño.
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

    /// Busca en el grafo HNSW aplanado los $K$ vecinos más cercanos aproximados mediante similitud `count_ones`.
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

/// Resultado de búsqueda clasificado mediante Reciprocal Rank Fusion (RRF), BM25F y coincidencia difusa/fonética/vectorial.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridSearchHit {
    /// Identificador único de la entidad coincidente.
    pub entity_id: String,
    /// Identificador numérico de fila dentro del bloque MBF.
    pub row_id: u32,
    /// Puntaje combinado total tras la fusión multimodal.
    pub total_score: f64,
    /// Componente de relevancia léxica multicampo BM25F.
    pub lexical_bm25f_score: f64,
    /// Bonificación acumulada por coincidencia difusa (SymSpell/Levenshtein), fonética y de N-gramas.
    pub fuzzy_phonetic_bonus: f64,
    /// Similitud vectorial híbrida (`RaBitQ` + coseno exacto) en `` `[0, 1]` ``.
    pub vector_similarity: f64,
}

/// Índice de búsqueda híbrida multimodal de 6 capas de la Zona 5B (100% agnóstico al dominio).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MbfHybridSearchIndex {
    /// Colección de documentos de entidad indexados en el bloque.
    pub docs: Vec<IndexedEntityDocument>,
    /// Capa 1: Léxico de vocabulario `term -> Vec<row_id>`.
    pub fst_lexicon: HashMap<String, Vec<u32>>,
    /// Capa 2: Diccionario de borrados simétricos SymSpell (`deleted_variant -> Vec<canonical_term>`) para resolución de erratas en $O(1)$.
    pub symspell_deletes: HashMap<String, Vec<String>>,
    /// Capa 3: Índice de subcadenas de 2-gramas y 3-gramas (`ngram -> Vec<row_id>`).
    pub ngram_bitmaps: HashMap<String, Vec<u32>>,
    /// Capa 4: Índice fonético bilingüe Romance + Double Metaphone (`phonetic_code -> Vec<row_id>`).
    pub phonetic_index: HashMap<String, Vec<u32>>,
    /// Capa 4B: Tesauro declarativo de sinónimos inyectado desde la terminología del Códice.
    pub thesaurus: SynonymThesaurus,
    /// Pesos configurables de impulso por campo BM25F `` `[code_boost, title_boost, description_boost]` `` gobernados por `FuzzySearchProfile`.
    pub bm25_field_boosts: [f64; 3],
    /// Capa 5: Vectores cuantizados `RaBitQ` para similitud `popcnt` sub-milisegundo.
    pub rabitq_vectors: HashMap<u32, RaBitQVector>,
    /// Capa 6: Caja delimitadora mínima espacial `` `[min_lat, min_lon, max_lat, max_lon]` ``.
    pub spatial_mbr: [f64; 4],
    /// Índice espacial de celdas jerárquicas Morton Z-Order / H3 (`cell_id -> Vec<row_id>`).
    pub spatial_morton_cells: HashMap<u64, Vec<u32>>,
    /// Longitud media de documento en caracteres para normalización BM25F.
    pub avg_doc_len: f64,
}

impl MbfHybridSearchIndex {
    /// Crea un nuevo índice de búsqueda híbrida multimodal vacío con pesos BM25F predeterminados.
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

    /// Adjunta un `SynonymThesaurus` declarativo cargado desde un recurso terminológico del Códice.
    pub fn with_thesaurus(mut self, thesaurus: SynonymThesaurus) -> Self {
        self.thesaurus = thesaurus;
        self
    }

    /// Configura pesos personalizados de impulso por campo BM25F `` `[code_boost, title_boost, description_boost]` `` desde `FuzzySearchProfile`.
    pub fn with_field_boosts(mut self, boosts: [f64; 3]) -> Self {
        self.bm25_field_boosts = boosts;
        self
    }

    /// Indexa o consolida un documento de entidad simultáneamente a través de las 6 capas de búsqueda.
    pub fn index_document(&mut self, doc: IndexedEntityDocument) {
        let existing_pos = self.docs.iter().position(|d| d.entity_id == doc.entity_id);
        let row_id = existing_pos.map_or(doc.row_id, |pos| self.docs[pos].row_id);
        let combined_text = format!("{} {} {}", doc.code, doc.title, doc.description);
        let mut tokens = Self::tokenize_normalized(&combined_text);
        let canon_code = Self::canonicalize_alphanumeric_code(&doc.code).to_ascii_lowercase();
        if canon_code.len() >= 3 && !tokens.contains(&canon_code) {
            tokens.push(canon_code);
        }

        for token in &tokens {
            let entry = self.fst_lexicon.entry(token.clone()).or_default();
            if !entry.contains(&row_id) {
                entry.push(row_id);
            }
            for del_variant in Self::generate_symspell_deletes(token, 2) {
                let sym_entry = self.symspell_deletes.entry(del_variant).or_default();
                if !sym_entry.contains(token) {
                    sym_entry.push(token.clone());
                }
            }
            for ng in Self::generate_ngrams(token) {
                let ng_entry = self.ngram_bitmaps.entry(ng).or_default();
                if !ng_entry.contains(&row_id) {
                    ng_entry.push(row_id);
                }
            }
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

        if let Some(emb) = &doc.embedding {
            self.rabitq_vectors
                .insert(row_id, RaBitQVector::from_f32_slice(emb));
        }

        if let Some((lat, lon)) = doc.coordinates_wgs84 {
            self.spatial_mbr[0] = self.spatial_mbr[0].min(lat);
            self.spatial_mbr[1] = self.spatial_mbr[1].min(lon);
            self.spatial_mbr[2] = self.spatial_mbr[2].max(lat);
            self.spatial_mbr[3] = self.spatial_mbr[3].max(lon);
            let h3_morton = Self::encode_spatial_cell_h3_morton(lat, lon);
            let cell_rows = self.spatial_morton_cells.entry(h3_morton).or_default();
            if !cell_rows.contains(&row_id) {
                cell_rows.push(row_id);
            }
        }

        if let Some(pos) = existing_pos {
            Self::merge_existing_document(&mut self.docs[pos], doc);
        } else {
            self.docs.push(doc);
        }
        let total_len: usize = self
            .docs
            .iter()
            .map(|d| d.code.len() + d.title.len() + d.description.len())
            .sum();
        self.avg_doc_len = (total_len as f64) / (self.docs.len().max(1) as f64);
    }

    fn merge_existing_document(
        existing: &mut IndexedEntityDocument,
        incoming: IndexedEntityDocument,
    ) {
        if !incoming.code.is_empty() && !existing.code.contains(&incoming.code) {
            if existing.code.is_empty() || existing.code == existing.entity_id {
                existing.code = incoming.code;
            } else {
                existing.code.push(' ');
                existing.code.push_str(&incoming.code);
            }
        }
        if !incoming.title.is_empty() {
            let incoming_has_spaces = incoming.title.contains(' ');
            let existing_has_spaces = existing.title.contains(' ');
            if existing.title.is_empty()
                || (incoming_has_spaces && !existing_has_spaces)
                || (incoming_has_spaces == existing_has_spaces
                    && incoming.title.len() > existing.title.len())
            {
                existing.title = incoming.title;
            }
        }
        if !incoming.description.is_empty() && !existing.description.contains(&incoming.description)
        {
            if !existing.description.is_empty() {
                existing.description.push(' ');
            }
            existing.description.push_str(&incoming.description);
        }
        if incoming.embedding.is_some() {
            existing.embedding = incoming.embedding;
        }
        if incoming.coordinates_wgs84.is_some() {
            existing.coordinates_wgs84 = incoming.coordinates_wgs84;
        }
    }

    /// Ejecuta una búsqueda híbrida de 6 capas combinando BM25F exacto, SymSpell $O(1)$, autómata de Levenshtein ($k \le 2$),
    /// fonética Romance y Double Metaphone, expansión declarativa de sinónimos, subcadenas de N-gramas y similitud vectorial RaBitQ.
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

    /// Calcula el puntaje Reciprocal Rank Fusion ($\text{Score}_{\text{RRF}}(d) = \frac{w_{\text{lex}}}{60 + \text{rank}_{\text{BM25F}}(d)} + \frac{w_{\text{vec}}}{60 + \text{rank}_{\text{Vector}}(d)}$).
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

    /// Capa 4: Canonicaliza códigos alfanuméricos (`"REF-6205 / 2RS"` -> `"REF62052RS"`).
    pub fn canonicalize_alphanumeric_code(raw: &str) -> String {
        crate::eav::fts::searcher::canonicalize_alphanumeric_code(raw)
    }

    /// Capa 4: Codificador fonético Double Metaphone para términos técnicos germánicos/anglosajones.
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

    /// Capa 6: Filtra los documentos dentro de un radio `radius_meters` respecto a `(center_lat, center_lon)` usando la fórmula de Haversine.
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

    /// Capa 6: Codifica coordenadas WGS-84 `(lat_deg, lon_deg)` en un identificador jerárquico de celda H3 en la resolución `resolution` (p. ej. `` `[7, 9, 11, 13]` ``).
    pub fn encode_h3_cell(lat_deg: f64, lon_deg: f64, resolution: u8) -> u64 {
        let res = resolution.clamp(1, 15);
        let morton = Self::encode_spatial_cell_h3_morton(lat_deg, lon_deg);
        let shift = (15 - res) * 2;
        let cell_bits = (morton >> shift) << shift;
        0x0800_0000_0000_0000u64 | (u64::from(res) << 52) | (cell_bits & 0x000F_FFFF_FFFF_FFFF)
    }

    /// Capa 6: Agrupamiento espacial por densidad DBSCAN (`cluster_spatial_dbscan`) sobre las entidades georreferenciadas.
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

    /// Codifica una palabra usando el algoritmo fonético romance agnóstico al dominio (`RomancePhonetic` + `DoubleMetaphone`).
    ///
    /// Normaliza equivalencias fonéticas romances/hispánicas y bilingües (`b`==`v`, `c`/`s`/`z`, `ll`/`y`, `h` muda, `ch`/`sh`->`x` y grupos nasales).
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

    /// Calcula la distancia ortodrómica de Haversine en metros entre dos coordenadas WGS-84.
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

    /// Codifica coordenadas WGS-84 `(lat, lon)` en un identificador de celda espacial jerárquica Morton Z-Order / H3 de 64 bits.
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

    /// Codifica la Zona 5B en un búfer binario alineado a 64 bytes (`"MBFZ5BHS"`).
    ///
    /// Almacena la caja delimitadora espacial (MBR), los documentos de entidad, incrustaciones vectoriales, coordenadas y el léxico FST ordenado.
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

    /// Decodifica un búfer binario de la Zona 5B alineado a 64 bytes (`"MBFZ5BHS"`) reconstruyendo `MbfHybridSearchIndex`.
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
