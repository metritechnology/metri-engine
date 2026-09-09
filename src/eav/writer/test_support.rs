//! Test support — one-shot Codice initialization for pure writer tests.
//!
//! Inicialización del Códice para tests puros.
//!
//! Los módulos nuevos del writer (outbox, datom_plan) son puros, pero validan
//! contra el registry global; estos tests necesitan config/models cargado una
//! sola vez por proceso. Los tests de integración (tests/writer_tests.rs) tienen
//! su propia copia local — no se tocan.

/// Dos cuidados, ya aprendidos en saga_tests: `Once` serializa nuestros tests,
/// y `catch_unwind` DENTRO del `Once` tolera la carrera con otros módulos de
/// test que inicializan el registry por su cuenta (si ganan, `init_global`
/// entra en pánico y envenenaría el Once).
pub fn init_codice() {
    static CODICE: std::sync::Once = std::sync::Once::new();
    CODICE.call_once(|| {
        if crate::codice::registry::global_opt().is_none() {
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {})); // silencia el ruido del intento fallido
            let _ = std::panic::catch_unwind(|| {
                let models = std::path::Path::new("config/models");
                let (registry, _) = crate::codice::CodeRegistry::build(models)
                    .expect("Códice: registry de config/models");
                crate::codice::init_global(registry);
            });
            std::panic::set_hook(prev);
        }
    });
    // Sea quien sea el que ganó la carrera, el registry debe estar disponible.
    crate::codice::global();
}
