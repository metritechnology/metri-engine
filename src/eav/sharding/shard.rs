// eav/sharding/shard.rs — Write Sharding para hot partitions
// Blueprint: Metri EAV §XI.4 + §XII.2

/// Calcula el índice de shard para un entity_id dado.
/// Distribución determinista: hash(entity_id) % total_shards.
///
/// [Blueprint: §XII.2 — "sufijo de shard al PK: T#tnt#A#entity_type#N"]
pub fn shard_key(entity_id: &str, total_shards: u8) -> u8 {
    if total_shards <= 1 {
        return 0;
    }

    // FNV-1a hash — extremadamente rápido, sin dependencias externas
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in entity_id.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % total_shards as u64) as u8
}

/// Genera los PKs de scatter-gather para un AEVT scan con sharding.
/// [Blueprint: §XII.2 — "lanza S tareas paralelas asíncronas (tokio::spawn)"]
pub fn scatter_pks(tenant_id: &str, entity_type: &str, total_shards: u8) -> Vec<String> {
    (0..total_shards)
        .map(|shard| format!("T#{}#A#{}#{}", tenant_id, entity_type, shard))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_key_no_sharding() {
        assert_eq!(shard_key("any-id", 1), 0);
        assert_eq!(shard_key("any-id", 0), 0);
    }

    #[test]
    fn shard_key_in_range() {
        let shards = 10u8;
        let key = shard_key("01J_ENTITY_001", shards);
        assert!(key < shards);
    }

    #[test]
    fn shard_key_deterministic() {
        let id = "01JXYZABC";
        assert_eq!(shard_key(id, 8), shard_key(id, 8));
    }
}
