use crate::aegis::sql::fuzzy::*;

#[test]
fn test_levenshtein() {
    assert_eq!(levenshtein_distance("rack", "rak"), 1);
    assert_eq!(levenshtein_distance("chiller", "chiler"), 1);
    assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
}

#[test]
fn test_fuzzy_match() {
    assert!(fuzzy_match("Chiller A-01", "Chiller")); // exacto
    assert!(fuzzy_match("Chiller A-01", "chiler")); // fuzzy (thresh 1)
    assert!(!fuzzy_match("Chiller A-01", "rak")); // mismatch
    assert!(fuzzy_match("Rack Server", "rak")); // token 'rack' thresh 1
    assert!(fuzzy_match("UPS B-12", "up")); // substring exacto
    assert!(!fuzzy_match("UPS B-12", "xk")); // no match
}

#[test]
fn test_expand_term() {
    let expanded = expand_term("ID").unwrap();
    assert_eq!(expanded.like_pat, "%id%");
    assert_eq!(expanded.regex_pat, None); // < 3 chars

    let expanded = expand_term("Chiler").unwrap();
    assert_eq!(expanded.like_pat, "%chiler%");
    assert!(expanded.regex_pat.is_some());
    assert!(expanded.regex_pat.unwrap().starts_with("\\b("));
}
