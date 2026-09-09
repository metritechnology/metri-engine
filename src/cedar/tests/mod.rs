//! Test suite entry point for `cedar::mod.rs`.
// cedar/tests — Suite del módulo cedar, dividida por área.
//
// Cada submodule agrupa los tests de su módulo de producción; los lectores
// EAV en memoria compartidos viven en `fakes`.

mod authn;
mod evaluator;
mod fakes;
mod principal_graph;
mod rules;
