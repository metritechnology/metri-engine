//! Tipos fundamentales del modelo EAV: `Datom`, `DatomValue`, `ValueType` y codificación binaria de claves de ordenamiento (`SK`).
/// Unidad atómica inmutable `Datom` y su enumeración de valores tipados `DatomValue`.
pub mod datom;
/// Constructores y decodificadores binarios `memcmp` para los cuatro índices EAV (`EAVT`, `AEVT`, `AVET`, `VAET`).
pub mod encoding;
/// Enumeración `ValueType` con los doce tipos de valor y sus etiquetas binarias (`tag`).
pub mod value_type;

pub use datom::{Datom, DatomValue};
pub use value_type::ValueType;
