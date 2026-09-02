pub mod ast_ir;
pub mod formula;
pub mod label_template;
pub mod oltp;
pub mod pagination; // Paginación cursor Base64(offset:limit)
pub mod sql;
pub mod temporal_bridge; // Bridge proto/FBS → temporal canónico

#[cfg(test)]
mod testing;
