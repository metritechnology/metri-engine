//! Full-text search via trigram intersection.
//!
//! Full-Text Search via trigram intersection
//! Blueprint: Metri EAV §V.2

use tracing::debug;

use crate::domain::errors::DomainError;
use crate::eav::fts::trigram::generate_trigrams;

/// Score de un resultado de búsqueda FTS.
#[derive(Debug, Clone)]
pub struct FtsResult {
    pub entity_id: String,
    pub score: f32, // trigrams matched / total trigrams del query
}

/// Busca entidades por texto usando intersección de trigrams.
/// Pasos:
///   1. Genera trigrams del término buscado.
///   2. Consulta el GSI-FTS por cada trigram en paralelo.
///   3. Intersección ponderada: score = trigrams_matched / total.
///   4. Post-filter DL≤1 sobre el valor real (Damerau-Levenshtein).
///
/// [Blueprint: §V.2 — "Búsqueda Fuzzy (Damerau-Levenshtein ≤ 1)"]
pub async fn fts_search_entity_ids(
    term: &str,
    tenant_id: &str,
    // DynamoDB client sería inyectado en una implementación completa
    // Por ahora retorna Vec vacío (stub para integración futura)
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

    // FASE 1 STUB: en producción, lanzamos N queries DynamoDB en paralelo
    // (una por trigram) y hacemos intersección en memoria.
    // Aquí retornamos vacío hasta que el DynamoClient esté inyectado.
    Ok(vec![])
}

/// Calcula la distancia Damerau-Levenshtein entre dos strings.
/// Usado para post-filtrar candidatos del FTS.
/// [Blueprint: §V.2 — "Post-filter: Damerau-Levenshtein distance ≤ 1"]
pub fn damerau_levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    let mut d = vec![vec![0usize; n + 1]; m + 1];
    for i in 0..=m {
        d[i][0] = i;
    }
    for j in 0..=n {
        d[0][j] = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
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
        assert_eq!(damerau_levenshtein("bomba", "bomba"), 0);
    }

    #[test]
    fn dl_one_deletion() {
        assert_eq!(damerau_levenshtein("bomba", "boma"), 1);
    }

    #[test]
    fn dl_transposition() {
        assert_eq!(damerau_levenshtein("bomba", "obmba"), 1);
    }
}
