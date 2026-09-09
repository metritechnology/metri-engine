//! The four EAV indexes — EAVT, AEVT, AVET and VAET.
//!
//! Cada índice sirve un patrón de acceso distinto sobre la misma tabla/GSIs:
//! EAVT (tabla principal) para el estado de una entidad, AEVT para
//! "qué entidades tienen este atributo", AVET para valores exactos/orden y
//! VAET para referencias inversas (grafo).
pub mod aevt;
pub mod avet;
pub mod eavt;
pub mod vaet;
