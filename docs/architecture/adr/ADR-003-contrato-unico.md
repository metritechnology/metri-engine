# ADR-003 — Una sola fuente por contrato

**Estado:** aceptada · 2026

## Contexto
El contrato gRPC llegó a existir en tres copias (raíz, `proto/`, código generado commiteado) y una de ellas fue editada a mano sin absorber `ListEntities`. El CI de contratos validaba la copia muerta.

## Decisión
1. El API vive solo en `proto/metri.proto`; `build.rs` compila desde ahí (`include_proto!` desde `OUT_DIR` — el código generado jamás se commitea).
2. `tests/contract_conformance.rs` (Rust, nativo en Rust) verifica la superficie del contrato y falla si existe más de una `metri.proto` en el árbol.
3. Documentación de referencia **generada** desde las fuentes únicas (`scripts/docs/gen_reference.py`); el CI falla si el generado está desactualizado.
4. Lo que el código ya sabe (errores, modelos, API) no se documenta a mano.

## Consecuencias
- "El contrato" vuelve a significar algo. La sincronización manual — que ya falló una vez en la práctica — deja de ser posible.
