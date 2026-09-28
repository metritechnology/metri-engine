//! Guardián de vocabulario de dominio (PLAN_DESCACOLE_PM_TOTAL.md F2).
//!
//! Regla que conserva el desacople: **esquema = dato del motor
//! (config/models); comportamiento = plugin**. El motor puede declarar cómo
//! se storea una entidad de mantenimiento; jamás qué significa mantener.
//! Este test recorre `src/` y rompe el build si el vocabulario de
//! mantenimientos preventivos reaparece en código de plataforma (los
//! modelos como config y los fixtures bajo `*/tests/` quedan fuera: uno es
//! dato, el otro caracteriza).
//!
//! Si este guardián te mordió: la lógica que estás escribiendo pertenece a
//! metri-cmms-plugin (p. ej. el ciclo completo de las pautas vive en su
//! orquestador Go), o al modelo JSON de la entidad si es sólo esquema.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    "preventive_maintenance",
    "meter_based_trigger",
    "advance_notice",
    "inactive_periods",
    "recurrence_start",
    "recurrence_end",
    "recurrence_interval",
    "recurrence_unit",
    "recurrence_at_time",
    "recurrence_basis",
    "pm_orchestrator",
    "pmTrap",
];

fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // Fixtures de caracterización, no código de plataforma.
                if path.to_string_lossy().contains("/tests") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out
}

#[test]
fn el_motor_no_habla_vocabulario_de_mantenimiento_preventivo() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(src.is_dir(), "src/ no encontrado desde {}", src.display());

    let mut violaciones = Vec::new();
    for file in rust_files_under(&src) {
        let content = fs::read_to_string(&file).unwrap_or_default();
        for word in FORBIDDEN {
            if content.contains(word) {
                violaciones.push(format!("{} contiene '{}'", file.display(), word));
            }
        }
    }

    assert!(
        violaciones.is_empty(),
        "Regla (PLAN_DESCACOLE_PM_TOTAL): esquema = dato del motor (config/models), \
         comportamiento = plugin. Vocabulario de PM hallado en src/:\n  - {}",
        violaciones.join("\n  - ")
    );
}
