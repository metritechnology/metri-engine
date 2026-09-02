// [PORTED_FROM: src/metri/aegis/sql/fuzzy_sql.clj]
// [PORTED_FROM: src/metri/aegis/datalog/fuzzy.clj]
// aegis/sql/fuzzy.rs — Expansor de términos fuzzy para Athena/OLAP y OLTP.
// SRP: genera expresiones regex exactas de Damerau-Levenshtein 1 y evalúa distancias.

use std::cmp::min;

// ── Constantes ───────────────────────────────────────────────────────────────

const FUZZY_MIN_LEN: usize = 3;
const FUZZY_MAX_LEN: usize = 8;

// ── Funciones compartidas (OLTP / Datalog) ───────────────────────────────────

/// Distancia de edición mínima entre dos strings (Wagner-Fischer, O(n) espacio).
/// [PORTED_FROM: (levenshtein-distance a b)]
pub fn levenshtein_distance(a: &str, b: &str) -> usize {
    let la = a.chars().count();
    let lb = b.chars().count();

    if a == b {
        return 0;
    }
    if la == 0 {
        return lb;
    }
    if lb == 0 {
        return la;
    }

    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=lb).collect();
    let mut curr = vec![0; lb + 1];

    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b_chars.iter().enumerate() {
            let cost = if ca == *cb { 0 } else { 1 };
            curr[j + 1] = min(curr[j] + 1, min(prev[j + 1] + 1, prev[j] + cost));
        }
        prev.copy_from_slice(&curr);
    }
    prev[lb]
}

/// Threshold adaptativo según la longitud del término.
/// [PORTED_FROM: (fuzzy-threshold term)]
pub fn fuzzy_threshold(term: &str) -> usize {
    let n = term.chars().count();
    if n <= 2 {
        0
    } else if n <= 8 {
        1
    } else {
        2
    }
}

/// Tokeniza dividiendo por espacios, guiones y puntos.
/// [PORTED_FROM: (tokenize s)]
fn tokenize(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| c.is_whitespace() || c == '-' || c == '.' || c == '_')
        .filter(|part| !part.is_empty())
        .map(String::from)
        .collect()
}

/// Evalúa si `value` contiene `term` de forma aproximada.
/// [PORTED_FROM: (fuzzy-match? value term)]
pub fn fuzzy_match(value: &str, term: &str) -> bool {
    if value.is_empty() || term.is_empty() {
        return false;
    }
    let v_lower = value.to_lowercase();
    let t_lower = term.to_lowercase();

    // Fast path: substring exacto
    if v_lower.contains(&t_lower) {
        return true;
    }

    let thresh = fuzzy_threshold(&t_lower);
    if thresh > 0 {
        let tokens = tokenize(value);
        for token in tokens {
            if levenshtein_distance(&token, &t_lower) <= thresh {
                return true;
            }
        }
    }
    false
}

// ── Funciones OLAP (Athena SQL) ──────────────────────────────────────────────

/// Escapa caracteres literales para Presto/Athena Regex (Java-like).
/// [PORTED_FROM: (escape-regex-literal s)]
fn escape_regex_literal(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' => {
                escaped.push('\\');
                escaped.push(c);
            }
            _ => escaped.push(c),
        }
    }
    escaped
}

/// Genera un patrón RE2/Athena estricto con distancia Damerau-Levenshtein 1.
/// [PORTED_FROM: (generate-lev1-regex t)]
fn generate_lev1_regex(t: &str) -> String {
    let chars: Vec<char> = t.chars().collect();
    let n = chars.len();
    let mut alts = Vec::new();

    let to_str = |slice: &[char]| -> String { slice.iter().collect() };

    // Palabra exacta (se asume ya en lowercase)
    alts.push(escape_regex_literal(&to_str(&chars)));

    // Omisiones (length n-1)
    for i in 0..n {
        let mut del = chars[..i].to_vec();
        del.extend_from_slice(&chars[i + 1..]);
        alts.push(escape_regex_literal(&to_str(&del)));
    }

    // Sustituciones (length n)
    for i in 0..n {
        let left = escape_regex_literal(&to_str(&chars[..i]));
        let right = escape_regex_literal(&to_str(&chars[i + 1..]));
        alts.push(format!("{left}.{right}"));
    }

    // Inserciones (length n+1)
    for i in 0..=n {
        let left = escape_regex_literal(&to_str(&chars[..i]));
        let right = escape_regex_literal(&to_str(&chars[i..]));
        alts.push(format!("{left}.{right}"));
    }

    // Transposiciones (length n)
    for i in 0..n.saturating_sub(1) {
        let left = escape_regex_literal(&to_str(&chars[..i]));
        let c1 = escape_regex_literal(&chars[i + 1].to_string());
        let c2 = escape_regex_literal(&chars[i].to_string());
        let right = escape_regex_literal(&to_str(&chars[i + 2..]));
        alts.push(format!("{left}{c1}{c2}{right}"));
    }

    // Desduplicar manteniendo el orden no es estrictamente necesario, pero lo hacemos para reducir el patrón
    let mut unique_alts = Vec::new();
    for alt in alts {
        if !unique_alts.contains(&alt) {
            unique_alts.push(alt);
        }
    }

    format!("\\b({})\\b", unique_alts.join("|"))
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExpandedFuzzyTerm {
    pub like_pat: String,
    pub regex_pat: Option<String>,
}

/// Expande un término de búsqueda en patrones SQL cost-safe para Athena.
/// [PORTED_FROM: (expand-term term)]
pub fn expand_term(term: &str) -> Option<ExpandedFuzzyTerm> {
    if term.is_empty() {
        return None;
    }

    let t = term.to_lowercase();
    let n = t.chars().count();

    let mut esc_t = String::new();
    for c in t.chars() {
        match c {
            '\\' => esc_t.push_str("\\\\"),
            '%' => esc_t.push_str("\\%"),
            '_' => esc_t.push_str("\\_"),
            _ => esc_t.push(c),
        }
    }
    let like_pat = format!("%{esc_t}%");

    let regex_pat = if n >= FUZZY_MIN_LEN && n <= FUZZY_MAX_LEN {
        Some(generate_lev1_regex(&t))
    } else {
        None
    };

    Some(ExpandedFuzzyTerm {
        like_pat,
        regex_pat,
    })
}
