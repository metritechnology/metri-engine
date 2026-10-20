//! Gestor de copias de seguridad inmutables, verificación forense de integridad y restauración granular/completa para archivos `.mbf`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::{MbfTenantCryptoSeal, MBF_FLAG_CRYPTO_SEALED};
use crate::mbf::storage::{
    av_string, decode_mbf_envelope, encode_mbf_envelope, evict_all_read_caches_on_remote_sync,
    partition_for, AttributeValue, MbfPartitionStore, MbfPartitionWireSnapshot, MbfStorageClient,
};

/// Metadatos descriptivos de una copia de seguridad `.mbf` (partición completa o inquilino aislado).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfBackupMetadata {
    /// Identificador del tenant propietario (`"system"` para copias completas de partición).
    pub tenant_id: String,
    /// Nombre de la tabla o partición `.mbf` de origen.
    pub partition_table: String,
    /// Época transaccional (`epoch_id`) capturada en la copia.
    pub epoch_id: u64,
    /// Cantidad de registros/datoms incluidos en la instantánea.
    pub item_count: usize,
    /// Cantidad de contadores de cuota incluidos en la instantánea.
    pub quota_count: usize,
    /// Tamaño total del archivo `.mbf` comprimido y alineado a 64 bytes.
    pub byte_size: usize,
    /// Huella criptográfica SHA-256 en hexadecimal sobre la totalidad del archivo `.mbf`.
    pub sha256_hex: String,
    /// Marca temporal UTC en milisegundos de creación del respaldo.
    pub created_at_ms: i64,
}

/// Reporte forense de verificación física, criptográfica y estructural de un archivo `.mbf`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfIntegrityReport {
    /// Época (`epoch_id`) declarada en la cabecera y verificada en el payload.
    pub epoch_id: u64,
    /// Identificador de celda (`cell_id`) extraído de `MbfHeader`.
    pub cell_id: u32,
    /// Cantidad de registros verificados tras descomprimir el stream LZ4.
    pub verified_items: usize,
    /// Cantidad de contadores de cuota verificados en el snapshot.
    pub verified_quotas: usize,
    /// Indica si el sello criptográfico `MbfTenantCryptoSeal` fue validado con éxito.
    pub crypto_seal_verified: bool,
    /// Suma de verificación SHA-256 en hexadecimal del contenedor `.mbf`.
    pub sha256_hex: String,
}

/// Orquestador sin estado para generación de copias `.mbf`, auditoría de integridad y restauración atómica.
pub struct MbfBackupManager;

impl MbfBackupManager {
    /// Genera una copia de seguridad binaria `.mbf` completa de la partición `table`,
    /// sellada criptográficamente con la clave activa del `MbfSealKeyRing` y alineada a 64 bytes.
    pub fn create_partition_backup(table: &str) -> DomainResult<(MbfBackupMetadata, Vec<u8>)> {
        let part = partition_for(table);
        let snap = {
            let guard = part.read().map_err(|_| {
                DomainError::mbf(ErrorCode::MbfCorrupt001, "Poisoned partition lock")
            })?;
            guard.to_wire_snapshot()
        };
        Self::pack_snapshot("system", table, &snap)
    }

    /// Genera una copia de seguridad `.mbf` granular y aislada exclusivamente para `tenant_id`,
    /// extrayendo sus datoms (`T#<tenant_id>#*`), secuencias y cuotas sin bloquear otros tenants.
    pub fn create_tenant_backup(
        table: &str,
        tenant_id: &str,
    ) -> DomainResult<(MbfBackupMetadata, Vec<u8>)> {
        if tenant_id.trim().is_empty() {
            return Err(DomainError::mbf(
                ErrorCode::MbfTenantSeal001,
                "Tenant ID cannot be empty for tenant-granular backup",
            ));
        }
        let part = partition_for(table);
        let snap = {
            let guard = part.read().map_err(|_| {
                DomainError::mbf(ErrorCode::MbfCorrupt001, "Poisoned partition lock")
            })?;
            Self::extract_tenant_wire_snapshot(&guard, tenant_id)
        };
        Self::pack_snapshot(tenant_id, table, &snap)
    }

    /// Audita bit a bit un archivo `.mbf` verificando alineación a 64 bytes, `MbfHeader`, `MbfFooterTrailer`,
    /// `CRC32C` de cabecera y directorio, sello `MbfTenantCryptoSeal` (`KeyRing`) y descompresión LZ4.
    pub fn verify_backup_bytes(
        backup_bytes: &[u8],
        expected_tenant: Option<&str>,
        expected_table: Option<&str>,
    ) -> DomainResult<MbfIntegrityReport> {
        let (header, raw_payload) = decode_mbf_envelope(backup_bytes)?;
        let mut crypto_seal_verified = false;

        if (header.flags & MBF_FLAG_CRYPTO_SEALED) != 0 {
            let seal = MbfTenantCryptoSeal::from_bytes(&backup_bytes[64..128])?;
            if let (Some(tid), Some(tbl)) = (expected_tenant, expected_table) {
                seal.verify_with_keyring(tid, tbl, raw_payload)?;
                crypto_seal_verified = true;
            }
        }

        let snap = MbfPartitionWireSnapshot::decode_binary(raw_payload)?;
        if let Some(tid) = expected_tenant {
            if tid != "system" && !Self::all_items_belong_to_tenant(&snap, tid) {
                return Err(DomainError::mbf(
                    ErrorCode::MbfTenantSeal001,
                    format!("Backup payload contains records outside tenant '{tid}'"),
                ));
            }
        }

        Ok(MbfIntegrityReport {
            epoch_id: snap.epoch_id,
            cell_id: header.cell_id(),
            verified_items: snap.items.len(),
            verified_quotas: snap.quota_counters.len(),
            crypto_seal_verified,
            sha256_hex: Self::compute_sha256_hex(backup_bytes),
        })
    }

    /// Restaura atómicamente una partición completa desde `backup_bytes`, verificando primero el sello criptográfico,
    /// avanzando `epoch_id` monótonamente para invalidar cachés L1/L2 y reconstruyendo los índices BTree en memoria.
    pub async fn restore_partition_from_bytes(
        client: &MbfStorageClient,
        table: &str,
        backup_bytes: &[u8],
    ) -> DomainResult<u64> {
        let _report = Self::verify_backup_bytes(backup_bytes, Some("system"), Some(table))?;
        let (_hdr, raw_payload) = decode_mbf_envelope(backup_bytes)?;
        let snap = MbfPartitionWireSnapshot::decode_binary(raw_payload)?;

        let new_epoch = {
            let part = partition_for(table);
            let mut guard = part.write().map_err(|_| {
                DomainError::mbf(ErrorCode::MbfCorrupt001, "Poisoned partition lock")
            })?;
            Self::replace_partition_state(&mut guard, snap)
        };

        evict_all_read_caches_on_remote_sync();
        client.flush_to_s3(table, Vec::new()).await?;
        Ok(new_epoch)
    }

    /// Restaura de forma quirúrgica únicamente los datos, secuencias y cuotas de `tenant_id` a partir de `backup_bytes`,
    /// preservando intactos a todos los demás tenants de la partición y avanzando `epoch_id` monótonamente.
    pub async fn restore_tenant_from_bytes(
        client: &MbfStorageClient,
        table: &str,
        tenant_id: &str,
        backup_bytes: &[u8],
    ) -> DomainResult<u64> {
        let _report = Self::verify_backup_bytes(backup_bytes, Some(tenant_id), Some(table))?;
        let (_hdr, raw_payload) = decode_mbf_envelope(backup_bytes)?;
        let snap = MbfPartitionWireSnapshot::decode_binary(raw_payload)?;

        let new_epoch = {
            let part = partition_for(table);
            let mut guard = part.write().map_err(|_| {
                DomainError::mbf(ErrorCode::MbfCorrupt001, "Poisoned partition lock")
            })?;
            Self::replace_single_tenant_state(&mut guard, tenant_id, snap)
        };

        evict_all_read_caches_on_remote_sync();
        client.flush_to_s3(table, Vec::new()).await?;
        Ok(new_epoch)
    }

    fn pack_snapshot(
        tenant_id: &str,
        table: &str,
        snap: &MbfPartitionWireSnapshot,
    ) -> DomainResult<(MbfBackupMetadata, Vec<u8>)> {
        let raw_bin = snap.encode_binary();
        let mbf_bytes = encode_mbf_envelope(
            0,
            snap.items.len() as u64,
            snap.epoch_id,
            tenant_id,
            table,
            &raw_bin,
        )?;
        let sha256_hex = Self::compute_sha256_hex(&mbf_bytes);
        let meta = MbfBackupMetadata {
            tenant_id: tenant_id.to_string(),
            partition_table: table.to_string(),
            epoch_id: snap.epoch_id,
            item_count: snap.items.len(),
            quota_count: snap.quota_counters.len(),
            byte_size: mbf_bytes.len(),
            sha256_hex,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        Ok((meta, mbf_bytes))
    }

    fn extract_tenant_wire_snapshot(
        guard: &MbfPartitionStore,
        tenant_id: &str,
    ) -> MbfPartitionWireSnapshot {
        let pk_prefix = format!("T#{tenant_id}#");
        let items = guard
            .items
            .iter()
            .filter(|((pk, _), item)| {
                !pk.contains("#FTS#")
                    && Self::record_belongs_to_tenant(pk, item, tenant_id, &pk_prefix)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let quota_counters = guard
            .quota_counters
            .iter()
            .filter(|((t, _), _)| t == tenant_id)
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        let quota_idem = guard
            .quota_idem
            .iter()
            .filter(|((t, _), _)| t == tenant_id)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        MbfPartitionWireSnapshot {
            epoch_id: guard.epoch_id,
            items,
            quota_counters,
            quota_idem,
        }
    }

    fn record_belongs_to_tenant(
        pk: &str,
        item: &std::collections::HashMap<String, AttributeValue>,
        tenant_id: &str,
        pk_prefix: &str,
    ) -> bool {
        if pk.starts_with(pk_prefix) {
            return true;
        }
        item.get("tenant_id").and_then(av_string) == Some(tenant_id)
    }

    fn all_items_belong_to_tenant(snap: &MbfPartitionWireSnapshot, tenant_id: &str) -> bool {
        let pk_prefix = format!("T#{tenant_id}#");
        snap.items
            .iter()
            .all(|((pk, _), item)| Self::record_belongs_to_tenant(pk, item, tenant_id, &pk_prefix))
            && snap.quota_counters.iter().all(|((t, _), _)| t == tenant_id)
            && snap.quota_idem.iter().all(|((t, _), _)| t == tenant_id)
    }

    fn replace_partition_state(
        guard: &mut MbfPartitionStore,
        snap: MbfPartitionWireSnapshot,
    ) -> u64 {
        let next_epoch = guard.epoch_id.max(snap.epoch_id) + 1;
        guard.items.clear();
        guard.aevt_index.clear();
        guard.avet_index.clear();
        guard.vaet_index.clear();
        guard.dirty_entities.clear();
        guard.transient_entities.clear();
        guard.quota_counters.clear();
        guard.quota_idem.clear();

        for (key, item) in snap.items {
            if !key.0.contains("#FTS#") {
                guard.insert_indexed(key, item);
            }
        }
        for (k, v) in snap.quota_counters {
            guard.quota_counters.insert(k, v);
        }
        for (k, v) in snap.quota_idem {
            guard.quota_idem.insert(k, v);
        }
        guard.epoch_id = next_epoch;
        next_epoch
    }

    fn replace_single_tenant_state(
        guard: &mut MbfPartitionStore,
        tenant_id: &str,
        snap: MbfPartitionWireSnapshot,
    ) -> u64 {
        let next_epoch = guard.epoch_id.max(snap.epoch_id) + 1;
        let pk_prefix = format!("T#{tenant_id}#");
        let keys_to_remove: Vec<(String, Vec<u8>)> = guard
            .items
            .iter()
            .filter(|((pk, _), item)| {
                Self::record_belongs_to_tenant(pk, item, tenant_id, &pk_prefix)
            })
            .map(|(k, _)| k.clone())
            .collect();

        for key in &keys_to_remove {
            guard.remove_indexed(key);
        }
        guard.quota_counters.retain(|(t, _), _| t != tenant_id);
        guard.quota_idem.retain(|(t, _), _| t != tenant_id);
        guard.transient_entities.retain(|(t, _)| t != tenant_id);

        for (key, item) in snap.items {
            if !key.0.contains("#FTS#") {
                guard.insert_indexed(key, item);
            }
        }
        for (k, v) in snap.quota_counters {
            guard.quota_counters.insert(k, v);
        }
        for (k, v) in snap.quota_idem {
            guard.quota_idem.insert(k, v);
        }
        guard.epoch_id = next_epoch;
        next_epoch
    }

    fn compute_sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mbf::storage::S3MbfObjectStore;
    use std::collections::HashMap;

    fn sample_entity_item(tenant: &str, eid: &str, title: &str) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert(
            "PK".to_string(),
            AttributeValue::S(format!("T#{tenant}#E#{eid}")),
        );
        m.insert(
            "SK".to_string(),
            AttributeValue::B(crate::mbf::storage::Blob::new(vec![
                0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1,
            ])),
        );
        m.insert(
            "ap".to_string(),
            AttributeValue::S(format!("T#{tenant}#A#title")),
        );
        m.insert(
            "as".to_string(),
            AttributeValue::B(crate::mbf::storage::Blob::new(vec![1])),
        );
        m.insert("v".to_string(), AttributeValue::S(title.to_string()));
        m
    }

    #[tokio::test]
    async fn tenant_granular_backup_and_restore_preserves_other_tenants_and_advances_epoch() {
        let table = "test-mbf-backup-granular";
        let client = MbfStorageClient::new_stub();

        // 1. Seed tenant_mall_a and tenant_mall_b
        client
            .put_item(
                table,
                sample_entity_item("mall_a", "chiller_01", "Chiller York 500TR - Original"),
            )
            .await
            .unwrap();
        client
            .put_item(
                table,
                sample_entity_item("mall_b", "escalator_01", "Escalera Otis Norte"),
            )
            .await
            .unwrap();
        client
            .quota_settle(table, "mall_a", "work_order", 15)
            .await
            .unwrap();
        client
            .quota_settle(table, "mall_b", "work_order", 40)
            .await
            .unwrap();

        // 2. Take a tenant-granular backup of mall_a
        let (meta_a, backup_a_bytes) =
            MbfBackupManager::create_tenant_backup(table, "mall_a").unwrap();
        assert_eq!(meta_a.tenant_id, "mall_a");
        assert_eq!(meta_a.item_count, 1);
        assert_eq!(meta_a.quota_count, 1);
        assert_eq!(backup_a_bytes.len() % 64, 0);

        // Verify forensic integrity report
        let report =
            MbfBackupManager::verify_backup_bytes(&backup_a_bytes, Some("mall_a"), Some(table))
                .unwrap();
        assert!(report.crypto_seal_verified);
        assert_eq!(report.verified_items, 1);

        // Cross-tenant restore attempt (restoring mall_a backup into mall_b) must fail with MbfTenantSeal001!
        let cross_err =
            MbfBackupManager::restore_tenant_from_bytes(&client, table, "mall_b", &backup_a_bytes)
                .await
                .unwrap_err();
        assert_eq!(cross_err.code, ErrorCode::MbfTenantSeal001);

        // 3. Mutate both mall_a (accidental corruption/change) and mall_b (legitimate new work)
        client
            .put_item(
                table,
                sample_entity_item("mall_a", "chiller_01", "CORRUPTED_NAME"),
            )
            .await
            .unwrap();
        client
            .put_item(
                table,
                sample_entity_item("mall_a", "rogue_asset", "Should disappear on restore"),
            )
            .await
            .unwrap();
        client
            .quota_settle(table, "mall_a", "work_order", 100)
            .await
            .unwrap();
        client
            .put_item(
                table,
                sample_entity_item("mall_b", "escalator_02", "Escalera Otis Sur"),
            )
            .await
            .unwrap();

        // 4. Restore ONLY mall_a from its backup!
        let restored_epoch =
            MbfBackupManager::restore_tenant_from_bytes(&client, table, "mall_a", &backup_a_bytes)
                .await
                .unwrap();
        assert!(restored_epoch > meta_a.epoch_id);

        // Verify mall_a is restored to exact checkpoint state
        let sk = [0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1];
        let chiller = client
            .get_item(table, "T#mall_a#E#chiller_01", Some(&sk))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            chiller.get("v").and_then(av_string),
            Some("Chiller York 500TR - Original")
        );
        let rogue = client
            .get_item(table, "T#mall_a#E#rogue_asset", Some(&sk))
            .await
            .unwrap();
        assert!(rogue.is_none());
        let quotas_a = client
            .quota_read_many(table, "mall_a", &["work_order".to_string()])
            .await
            .unwrap();
        assert_eq!(quotas_a.get("work_order"), Some(&15));

        // Verify mall_b was NOT rolled back and retains escalator_02 + quota 40!
        let esc2 = client
            .get_item(table, "T#mall_b#E#escalator_02", Some(&sk))
            .await
            .unwrap();
        assert!(esc2.is_some());
        let quotas_b = client
            .quota_read_many(table, "mall_b", &["work_order".to_string()])
            .await
            .unwrap();
        assert_eq!(quotas_b.get("work_order"), Some(&40));
    }

    #[tokio::test]
    async fn full_partition_backup_and_restore_and_nvme_cache_validation() {
        let table = "test-mbf-backup-partition";
        let client = MbfStorageClient::new_stub();
        client
            .put_item(
                table,
                sample_entity_item("system", "cfg_01", "Master Config v1"),
            )
            .await
            .unwrap();

        let (meta, full_bytes) = MbfBackupManager::create_partition_backup(table).unwrap();
        assert!(S3MbfObjectStore::is_valid_cached_mbf(
            "vortex/test.mbf",
            &full_bytes
        ));

        // Tamper 1 byte in cached MBF header -> NVMe cache validator must reject it
        let mut tampered = full_bytes.clone();
        tampered[12] ^= 0xFF;
        assert!(!S3MbfObjectStore::is_valid_cached_mbf(
            "vortex/test.mbf",
            &tampered
        ));
        assert!(
            MbfBackupManager::verify_backup_bytes(&tampered, Some("system"), Some(table)).is_err()
        );

        // Delete item and restore full partition
        let sk = [0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1];
        client
            .delete_item(table, "T#system#E#cfg_01", Some(&sk))
            .await
            .unwrap();
        assert!(client
            .get_item(table, "T#system#E#cfg_01", Some(&sk))
            .await
            .unwrap()
            .is_none());

        let new_epoch = MbfBackupManager::restore_partition_from_bytes(&client, table, &full_bytes)
            .await
            .unwrap();
        assert!(new_epoch > meta.epoch_id);
        let restored = client
            .get_item(table, "T#system#E#cfg_01", Some(&sk))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            restored.get("v").and_then(av_string),
            Some("Master Config v1")
        );
    }
}
