//! Soporte de pruebas — inicialización única (*one-shot*) del Códice para pruebas unitarias puras del escritor EAV.
//!
//! Los módulos puros del escritor (`outbox`, `datom_plan`) validan contra el registro global
//! del Códice; esta utilidad garantiza que `config/models` se cargue una sola vez por proceso de prueba.

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
