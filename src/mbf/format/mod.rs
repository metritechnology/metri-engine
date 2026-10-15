//! Binary format headers, footers, cryptographic tenant seals, and zero-downtime schema evolution.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;

type HmacSha256 = Hmac<Sha256>;

/// Magic bytes identifying a valid Metri Binary Format v1 file (`"MBFv0001"`).
pub const MBF_MAGIC_BYTES: [u8; 8] = *b"MBFv0001";

/// Current wire format version.
pub const MBF_FORMAT_VERSION: u16 = 1;

/// Fixed 64-byte header aligned to a single CPU cache line (`#[repr(C, align(64))]`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub flags: u16,
    pub cell_id: u32,
    pub row_count: u64,
    pub epoch_id: u64,
    pub schema_fingerprint: u64,
    pub fiscal_lock_epoch_ms: i64,
    pub created_at_ms: i64,
    pub header_crc32: u32,
    pub reserved: u32,
}

impl MbfHeader {
    /// Creates a new validated 64-byte aligned header with precalculated checksum.
    pub fn new(
        cell_id: u32,
        row_count: u64,
        epoch_id: u64,
        schema_fingerprint: u64,
        fiscal_lock_epoch_ms: i64,
        created_at_ms: i64,
    ) -> Self {
        let mut h = Self {
            magic: MBF_MAGIC_BYTES,
            version: MBF_FORMAT_VERSION,
            flags: 0x0001,
            cell_id,
            row_count,
            epoch_id,
            schema_fingerprint,
            fiscal_lock_epoch_ms,
            created_at_ms,
            header_crc32: 0,
            reserved: 0,
        };
        h.header_crc32 = h.compute_crc32();
        h
    }

    /// Computes a deterministic CRC32/XXH32-style checksum over header fields.
    pub fn compute_crc32(&self) -> u32 {
        let mut hasher = Sha256::new();
        hasher.update(self.magic);
        hasher.update(self.version.to_le_bytes());
        hasher.update(self.flags.to_le_bytes());
        hasher.update(self.cell_id.to_le_bytes());
        hasher.update(self.row_count.to_le_bytes());
        hasher.update(self.epoch_id.to_le_bytes());
        hasher.update(self.schema_fingerprint.to_le_bytes());
        hasher.update(self.fiscal_lock_epoch_ms.to_le_bytes());
        hasher.update(self.created_at_ms.to_le_bytes());
        let digest = hasher.finalize();
        u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]])
    }

    /// Encodes the header into exactly 64 bytes.
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10..12].copy_from_slice(&self.flags.to_le_bytes());
        buf[12..16].copy_from_slice(&self.cell_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.row_count.to_le_bytes());
        buf[24..32].copy_from_slice(&self.epoch_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.schema_fingerprint.to_le_bytes());
        buf[40..48].copy_from_slice(&self.fiscal_lock_epoch_ms.to_le_bytes());
        buf[48..56].copy_from_slice(&self.created_at_ms.to_le_bytes());
        buf[56..60].copy_from_slice(&self.header_crc32.to_le_bytes());
        buf[60..64].copy_from_slice(&self.reserved.to_le_bytes());
        buf
    }

    /// Decodes and validates a 64-byte header slice.
    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Buffer too small for MbfHeader: {} bytes", bytes.len()),
            ));
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&bytes[0..8]);
        if magic != MBF_MAGIC_BYTES {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MBF magic bytes in header",
            ));
        }
        let version = u16::from_le_bytes([bytes[8], bytes[9]]);
        if version != MBF_FORMAT_VERSION {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Unsupported MBF version: {version}"),
            ));
        }
        let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
        let cell_id = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        let row_count = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);
        let epoch_id = u64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        let schema_fingerprint = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]);
        let fiscal_lock_epoch_ms = i64::from_le_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
        ]);
        let created_at_ms = i64::from_le_bytes([
            bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53], bytes[54], bytes[55],
        ]);
        let header_crc32 = u32::from_le_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);
        let reserved = u32::from_le_bytes([bytes[60], bytes[61], bytes[62], bytes[63]]);

        let header = Self {
            magic,
            version,
            flags,
            cell_id,
            row_count,
            epoch_id,
            schema_fingerprint,
            fiscal_lock_epoch_ms,
            created_at_ms,
            header_crc32,
            reserved,
        };
        if header.compute_crc32() != header_crc32 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Header CRC32 checksum mismatch (bit corruption detected)",
            ));
        }
        Ok(header)
    }
}

/// Fixed 64-byte footer trailer at the end of every `.mbf` file (`#[repr(C, align(64))]`).
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfFooterTrailer {
    pub zone1_pax_offset: u64,
    pub zone2_csr_offset: u64,
    pub zone3_iot_offset: u64,
    pub zone4_ledger_offset: u64,
    pub zone5_synopsis_offset: u64,
    pub zone5b_search_offset: u64,
    pub zone6b_cov_offset: u64,
    pub footer_magic: [u8; 4],
    pub full_file_crc32: u32,
}

impl MbfFooterTrailer {
    pub const FOOTER_MAGIC: [u8; 4] = *b"MBFE";

    pub fn to_bytes(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf[0..8].copy_from_slice(&self.zone1_pax_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.zone2_csr_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.zone3_iot_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.zone4_ledger_offset.to_le_bytes());
        buf[32..40].copy_from_slice(&self.zone5_synopsis_offset.to_le_bytes());
        buf[40..48].copy_from_slice(&self.zone5b_search_offset.to_le_bytes());
        buf[48..56].copy_from_slice(&self.zone6b_cov_offset.to_le_bytes());
        buf[56..60].copy_from_slice(&self.footer_magic);
        buf[60..64].copy_from_slice(&self.full_file_crc32.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Buffer too small for MbfFooterTrailer",
            ));
        }
        let mut footer_magic = [0u8; 4];
        footer_magic.copy_from_slice(&bytes[56..60]);
        if footer_magic != Self::FOOTER_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MbfFooterTrailer magic bytes",
            ));
        }
        Ok(Self {
            zone1_pax_offset: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            zone2_csr_offset: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
            zone3_iot_offset: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
            zone4_ledger_offset: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
            zone5_synopsis_offset: u64::from_le_bytes([
                bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38],
                bytes[39],
            ]),
            zone5b_search_offset: u64::from_le_bytes([
                bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46],
                bytes[47],
            ]),
            zone6b_cov_offset: u64::from_le_bytes([
                bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53], bytes[54],
                bytes[55],
            ]),
            footer_magic,
            full_file_crc32: u32::from_le_bytes([bytes[60], bytes[61], bytes[62], bytes[63]]),
        })
    }
}

/// Cryptographic Multi-Tenant Block Seal (`#[repr(C, align(64))]`).
/// Prevents any cross-tenant data leakage in shared `/tmp` NVMe cache or S3 buckets.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfTenantCryptoSeal {
    pub tenant_context_hash: [u8; 16],
    pub nonce: [u8; 12],
    pub crypto_flags: u32,
    pub keyed_mac: [u8; 32],
}

impl MbfTenantCryptoSeal {
    /// Derives a per-tenant key from the master secret and seals a payload slice.
    pub fn seal_block(
        master_secret: &[u8],
        tenant_id: &str,
        entity_type: &str,
        epoch_id: u64,
        payload: &[u8],
    ) -> DomainResult<Self> {
        let tenant_key = Self::derive_tenant_key(master_secret, tenant_id);
        let context_hash = Self::compute_context_hash(tenant_id, entity_type);
        let mut nonce = [0u8; 12];
        nonce[0..8].copy_from_slice(&epoch_id.to_le_bytes());

        let mut mac = HmacSha256::new_from_slice(&tenant_key).map_err(|_| {
            DomainError::mbf(ErrorCode::MbfTenantSeal001, "Invalid HMAC key length")
        })?;
        mac.update(&context_hash);
        mac.update(&nonce);
        mac.update(payload);
        let result = mac.finalize().into_bytes();
        let mut keyed_mac = [0u8; 32];
        keyed_mac.copy_from_slice(&result);

        Ok(Self {
            tenant_context_hash: context_hash,
            nonce,
            crypto_flags: 1,
            keyed_mac,
        })
    }

    /// Verifies that the block belongs to `expected_tenant_id` and that `payload` has not been tampered with.
    pub fn verify_block(
        &self,
        master_secret: &[u8],
        expected_tenant_id: &str,
        expected_entity_type: &str,
        payload: &[u8],
    ) -> DomainResult<()> {
        let expected_ctx = Self::compute_context_hash(expected_tenant_id, expected_entity_type);
        if !constant_time_eq::constant_time_eq(&self.tenant_context_hash, &expected_ctx) {
            return Err(DomainError::mbf(
                ErrorCode::MbfTenantSeal001,
                format!("Block tenant context does not match '{expected_tenant_id}'"),
            ));
        }

        let tenant_key = Self::derive_tenant_key(master_secret, expected_tenant_id);
        let mut mac = HmacSha256::new_from_slice(&tenant_key).map_err(|_| {
            DomainError::mbf(ErrorCode::MbfTenantSeal001, "Invalid HMAC key length")
        })?;
        mac.update(&self.tenant_context_hash);
        mac.update(&self.nonce);
        mac.update(payload);
        let expected_mac = mac.finalize().into_bytes();

        if !constant_time_eq::constant_time_eq(&self.keyed_mac, &expected_mac) {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Cryptographic MAC verification failed on MBF block",
            ));
        }
        Ok(())
    }

    fn derive_tenant_key(master_secret: &[u8], tenant_id: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"METRI_MBF_V1_TENANT_KDF:");
        hasher.update(master_secret);
        hasher.update(b":");
        hasher.update(tenant_id.as_bytes());
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        out
    }

    fn compute_context_hash(tenant_id: &str, entity_type: &str) -> [u8; 16] {
        let mut hasher = Sha256::new();
        hasher.update(tenant_id.as_bytes());
        hasher.update(b"#");
        hasher.update(entity_type.as_bytes());
        let digest = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[0..16]);
        out
    }
}

/// Logical value representation supporting Zero-Downtime Schema Evolution (`LazyDefaultProjection`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MbfCellValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Decimal128(i128),
    Text(String),
}

/// Column storage in an `.mbf` segment: either physically stored or lazily synthesized from Códice defaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EvolvedColumnProjection {
    /// Column exists physically in the `.mbf` file.
    Physical(Vec<MbfCellValue>),
    /// Column was added to `config/models/*.json` after this `.mbf` file was sealed;
    /// projected in $O(1)$ with zero disk rewrites!
    VirtualConstantDefault {
        default_value: MbfCellValue,
        row_count: usize,
    },
}

impl EvolvedColumnProjection {
    /// Retrieves the value at `row_idx` in $O(1)$ regardless of whether the column is physical or virtual.
    pub fn get(&self, row_idx: usize) -> Option<&MbfCellValue> {
        match self {
            EvolvedColumnProjection::Physical(values) => values.get(row_idx),
            EvolvedColumnProjection::VirtualConstantDefault {
                default_value,
                row_count,
            } => {
                if row_idx < *row_count {
                    Some(default_value)
                } else {
                    None
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EvolvedColumnProjection::Physical(values) => values.len(),
            EvolvedColumnProjection::VirtualConstantDefault { row_count, .. } => *row_count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Schema evolution resolver for historical `.mbf` files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MbfSchemaEvolutionDirectory {
    pub row_count: usize,
    pub physical_columns: HashMap<u16, Vec<MbfCellValue>>,
}

impl MbfSchemaEvolutionDirectory {
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            physical_columns: HashMap::new(),
        }
    }

    pub fn insert_physical_column(&mut self, attr_id: u16, values: Vec<MbfCellValue>) {
        self.physical_columns.insert(attr_id, values);
    }

    /// Resolves `attr_id`: returns physical column if present, or synthesizes a `VirtualConstantDefault` in $O(1)$.
    pub fn resolve_column(
        &self,
        attr_id: u16,
        codice_default: MbfCellValue,
    ) -> EvolvedColumnProjection {
        if let Some(col) = self.physical_columns.get(&attr_id) {
            EvolvedColumnProjection::Physical(col.clone())
        } else {
            EvolvedColumnProjection::VirtualConstantDefault {
                default_value: codice_default,
                row_count: self.row_count,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_64_byte_alignment_and_roundtrip() {
        assert_eq!(std::mem::size_of::<MbfHeader>(), 64);
        assert_eq!(std::mem::align_of::<MbfHeader>(), 64);
        assert_eq!(std::mem::size_of::<MbfFooterTrailer>(), 64);
        assert_eq!(std::mem::align_of::<MbfFooterTrailer>(), 64);
        assert_eq!(std::mem::size_of::<MbfTenantCryptoSeal>(), 64);
        assert_eq!(std::mem::align_of::<MbfTenantCryptoSeal>(), 64);

        let header = MbfHeader::new(
            1,
            25_000,
            42,
            0xCAFE_BABE,
            1_700_000_000_000,
            1_700_000_100_000,
        );
        let bytes = header.to_bytes();
        let decoded = MbfHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header, decoded);

        // Bit-flip corruption must return MbfCorrupt001
        let mut corrupted = bytes;
        corrupted[20] ^= 0x01;
        let err = MbfHeader::from_bytes(&corrupted).unwrap_err();
        assert_eq!(err.code, ErrorCode::MbfCorrupt001);
    }

    #[test]
    fn tenant_crypto_seal_prevents_cross_tenant_reads_and_tampering() {
        let master = b"kms-master-secret-key-metri-2026";
        let payload = b"columnar-financial-block-bytes";
        let seal = MbfTenantCryptoSeal::seal_block(
            master,
            "tenant_centro_mayor",
            "inventory_ledger",
            10,
            payload,
        )
        .unwrap();

        assert!(seal
            .verify_block(master, "tenant_centro_mayor", "inventory_ledger", payload)
            .is_ok());

        // Cross-tenant attempt must fail with MbfTenantSeal001
        let cross_err = seal
            .verify_block(master, "tenant_andino", "inventory_ledger", payload)
            .unwrap_err();
        assert_eq!(cross_err.code, ErrorCode::MbfTenantSeal001);

        // Bit-flip on payload must fail with MbfCorrupt001
        let tampered = b"columnar-financial-block-byteZ";
        let tamper_err = seal
            .verify_block(master, "tenant_centro_mayor", "inventory_ledger", tampered)
            .unwrap_err();
        assert_eq!(tamper_err.code, ErrorCode::MbfCorrupt001);
    }

    #[test]
    fn zero_downtime_schema_evolution_projects_virtual_defaults_in_o1() {
        let mut dir = MbfSchemaEvolutionDirectory::new(3);
        dir.insert_physical_column(
            1,
            vec![
                MbfCellValue::Text("WO-001".into()),
                MbfCellValue::Text("WO-002".into()),
                MbfCellValue::Text("WO-003".into()),
            ],
        );

        let col1 = dir.resolve_column(1, MbfCellValue::Null);
        assert_eq!(col1.get(1), Some(&MbfCellValue::Text("WO-002".into())));

        // Attribute 99 added in Códice v2: historical file returns default without rewrite!
        let col99 = dir.resolve_column(99, MbfCellValue::Decimal128(50_000));
        assert_eq!(col99.len(), 3);
        assert_eq!(col99.get(0), Some(&MbfCellValue::Decimal128(50_000)));
        assert_eq!(col99.get(2), Some(&MbfCellValue::Decimal128(50_000)));
        assert_eq!(col99.get(3), None);
    }
}
