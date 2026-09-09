//! Transactional chunking — works around the 100-item DynamoDB limit.
//!
//! eav/writer/chunker.rs
//! Chunking transaccional — mitiga el límite físico de 100 items/TX de DynamoDB.
//! Blueprint: Metri EAV §XII.1 + §MÓDULO 3 writer/chunker.rs

use aws_sdk_dynamodb::types::TransactWriteItem;

const DYNAMO_TX_LIMIT: usize = 100;

/// Estrategia de chunking para la escritura transaccional.
#[derive(Debug)]
pub enum ChunkStrategy {
    /// Todos los items entran en una sola TX ACID — el caso feliz.
    SingleAtomic(Vec<TransactWriteItem>),
    /// Más de 100 items — separamos core (ACID) de secundarios (eventual).
    DegradedConsistency {
        /// Items críticos: datoms EAVT + checks de unicidad + outbox event.
        core_items: Vec<TransactWriteItem>,
        /// Items eventualmente consistentes: FTS trigrams + VAET masivos.
        secondary_items: Vec<TransactWriteItem>,
    },
}

/// Planifica el chunking de una lista de TransactWriteItems.
///
/// Si total <= 100 → SingleAtomic.
/// Si total >  100 → DegradedConsistency, priorizando los primeros items (EAVT+Outbox).
///
/// [Blueprint: §XII.1 — "Chunking Transaccional Degradado"]
pub fn plan_chunks(items: Vec<TransactWriteItem>) -> ChunkStrategy {
    if items.len() <= DYNAMO_TX_LIMIT {
        return ChunkStrategy::SingleAtomic(items);
    }

    // Los primeros 100 son los items CORE (EAVT + Outbox + AVET uniqueness checks).
    // El resto son índices secundarios (FTS trigrams, VAET de cardinality:many).
    let (core, secondary) = items.into_iter().enumerate().fold(
        (Vec::new(), Vec::new()),
        |(mut core, mut secondary), (i, item)| {
            if i < DYNAMO_TX_LIMIT {
                core.push(item);
            } else {
                secondary.push(item);
            }
            (core, secondary)
        },
    );

    ChunkStrategy::DegradedConsistency {
        core_items: core,
        secondary_items: secondary,
    }
}
