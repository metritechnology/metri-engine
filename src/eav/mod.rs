//! Motor de almacenamiento EAV — datoms inmutables sobre el motor binario unificado Metri Vortex (`.mbf`).
//!
//! # Garantías
//!
//! - Inmutabilidad: un datom persistido jamás se actualiza ni borra en sitio
//!   (modelo *retract + assert*). No existen sentencias `UPDATE` en el motor.
//! - Atomicidad ACID dentro de una transacción (`TransactWriteItems` sobre `MbfStorageClient`),
//!   con reclamos de unicidad optimistas para resolver colisiones concurrentes.
//! - Aislamiento estricto por tenant: `tenant_id` forma parte del prefijo de cada clave de partición (`PK`).
//!
//! # Submódulos
//!
//! - `types` — estructuras `Datom`, `DatomValue`, `ValueType` y codificación binaria de claves (`SK`).
//! - `writer` — ruta de escritura ACID y componentes satélites (outbox transaccional, FTS, invalidación de cachés y secuencias).
//! - `reader` — ruta de lectura selectiva (`pull` de estado de entidad y `query` de planes físicos sobre `.mbf`).
//! - `fts` — búsqueda de texto completo e índice híbrido por trigramas y fonética en Zona 5B.
/// Subsistema de búsqueda de texto completo (FTS) e índice híbrido en Zona 5B.
pub mod fts;
/// Ruta de lectura EAV (`pull` selectivo y `query` de planes físicos sobre `.mbf`).
pub mod reader;
/// Tipos atómicos (`Datom`, `DatomValue`, `ValueType`) y codificación binaria de claves (`SK`).
pub mod types;
/// Ruta de escritura ACID de datoms, outbox transaccional, restricciones y cachés.
pub mod writer;
