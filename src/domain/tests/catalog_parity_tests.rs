// domain/tests/catalog_parity_tests.rs — Guards C1–C4 del catálogo de errores.
// PLAN_PATRON_RESULT.md §4.3: segunda cerradura junto al auditor Python —
// estos guards corren dentro de `cargo test` y no pueden desincronizarse del
// binario real.

use crate::domain::error_catalog::ErrorCatalog;
use crate::domain::errors::ErrorCode;
use std::collections::HashSet;

fn catalog() -> ErrorCatalog {
    ErrorCatalog::load("config/errors/error_catalog.toml").expect("catálogo de test")
}

/// C1 — zero-drop: `ALL` no repite variantes.
///
/// (La detección de "variante del enum ausente en `ALL`" es del auditor Python,
/// que parsea el enum textualmente; el match exhaustivo de `canonical_code`
/// obliga a que toda variante nueva tenga su brazo y su entrada TOML.)
#[test]
fn all_sin_variantes_duplicadas() {
    let names: HashSet<String> = ErrorCode::ALL.iter().map(|c| format!("{c:?}")).collect();
    assert_eq!(names.len(), ErrorCode::ALL.len());
}

/// C2 — todo código canónico existe como entrada del TOML.
#[test]
fn todo_codigo_canonico_esta_en_el_catalogo() {
    let catalog = catalog();
    for code in ErrorCode::ALL {
        let canon = code.canonical_code();
        assert!(
            catalog.get(canon).is_some(),
            "falta la entrada '{canon}' (ErrorCode::{code:?}) en error_catalog.toml"
        );
    }
}

/// C3 — 1 variante = 1 código canónico (sin compartidos).
#[test]
fn codigos_canonicos_son_unicos() {
    let codes: HashSet<&str> = ErrorCode::ALL.iter().map(|c| c.canonical_code()).collect();
    assert_eq!(
        codes.len(),
        ErrorCode::ALL.len(),
        "dos variantes comparten código canónico — ver PLAN_PATRON_RESULT.md §3.2"
    );
}

/// C4 — el fallback estático previo al bootstrap coincide con el TOML.
#[test]
fn fallback_retryable_coincide_con_el_catalogo() {
    let catalog = catalog();
    for code in ErrorCode::ALL {
        assert_eq!(
            code.fallback_is_retryable(),
            catalog.is_retryable(code.canonical_code()),
            "divergencia retryable en {code:?}"
        );
    }
}
