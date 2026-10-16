//! Binary format headers, footers, directory index, .mbf.idx mirror, cryptographic tenant seals, and zero-downtime schema evolution.

use bytemuck::{Pod, Zeroable};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;

type HmacSha256 = Hmac<Sha256>;

/// Canonical 8-byte magic header identifying a Metri Binary Format v1 file (`"METRIBIN"`, §4.1).
pub const MBF_MAGIC: [u8; 8] = *b"METRIBIN";

/// Alias preserved for module compatibility.
pub const MBF_MAGIC_BYTES: [u8; 8] = MBF_MAGIC;

/// Canonical 8-byte magic footer identifying the trailer of a `.mbf` file (`"MBFFOOTR"`, §4.1).
pub const MBF_FOOTER_MAGIC: [u8; 8] = *b"MBFFOOTR";

/// Canonical 8-byte magic identifying a `.mbf.idx` synopsis mirror file (`"MBFINDEX"`).
pub const MBF_IDX_MAGIC: [u8; 8] = *b"MBFINDEX";

/// Current wire format version (`1`).
pub const MBF_FORMAT_VERSION: u16 = 1;

/// Canonical micro-block row group size ($2^{12} = 4096$ rows, §4.2).
pub const MBF_ROW_GROUP_SIZE: usize = 4096;

/// Header flag: file is a transactional `HOT_HEAD.mbf` frame.
pub const MBF_FLAG_HOT_HEAD: u16 = 1 << 0;
/// Header flag: file includes a 64-byte `MbfTenantCryptoSeal`.
pub const MBF_FLAG_CRYPTO_SEALED: u16 = 1 << 1;
/// Header flag: Zone 1 (PAX-MVCC) present.
pub const MBF_FLAG_HAS_PAX: u16 = 1 << 2;
/// Header flag: Zone 2 (CSR Graph) present.
pub const MBF_FLAG_HAS_CSR: u16 = 1 << 3;
/// Header flag: Zone 3 (IoT Time-Series) present.
pub const MBF_FLAG_HAS_IOT: u16 = 1 << 4;
/// Header flag: Zone 4 (Financial Ledger) present.
pub const MBF_FLAG_HAS_LEDGER: u16 = 1 << 5;
/// Header flag: Zone 5 (BI Synopses) present.
pub const MBF_FLAG_HAS_SYNOPSES: u16 = 1 << 6;
/// Header flag: Zone 5B (Hybrid Search) present.
pub const MBF_FLAG_HAS_SEARCH: u16 = 1 << 7;
/// Header flag: Zone 6B (Cross Covariance) present.
pub const MBF_FLAG_HAS_COVARIANCE: u16 = 1 << 8;

/// Pads `buf` with zeroes up to the next exact multiple of 64 bytes (L1 CPU cache line size).
#[inline]
pub fn pad_to_64(buf: &mut Vec<u8>) {
    let rem = buf.len() % 64;
    if rem != 0 {
        buf.resize(buf.len() + (64 - rem), 0u8);
    }
}

/// Writes a 4-byte little-endian length-prefixed UTF-8 string into `buf`.
#[inline]
pub fn write_len_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

/// Reads a 4-byte little-endian length-prefixed UTF-8 string from `bytes` at `*cursor`.
#[inline]
pub fn read_len_prefixed_str(bytes: &[u8], cursor: &mut usize) -> DomainResult<String> {
    if *cursor + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading length-prefixed string header",
        ));
    }
    let len = u32::from_le_bytes([
        bytes[*cursor],
        bytes[*cursor + 1],
        bytes[*cursor + 2],
        bytes[*cursor + 3],
    ]) as usize;
    *cursor += 4;
    if *cursor + len > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading length-prefixed string body",
        ));
    }
    let s = String::from_utf8_lossy(&bytes[*cursor..*cursor + len]).into_owned();
    *cursor += len;
    Ok(s)
}

/// Computes a hardware-accelerated CRC32 checksum over `bytes`.
#[inline]
pub fn compute_crc32c(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// Computes a deterministic 128-bit truncated hash of `tenant_id` (§4.1 `tenant_hash`).
#[inline]
pub fn compute_tenant_hash(tenant_id: &str) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(b"MBF_TENANT_ID_V1:");
    hasher.update(tenant_id.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[0..16]);
    out
}

/// Fixed 64-byte header aligned to a single CPU cache line (`#[repr(C, align(64))]`, §4.1).
///
/// Byte layout:
/// - `0..8`:   `magic` (`"METRIBIN"`)
/// - `8..10`:  `version` (`1`)
/// - `10..12`: `flags` (Bit 0: HotHead, Bit 1: Encrypted, Bits 2..9: ZoneMask, Bits 10..15: CellId)
/// - `12..16`: `row_count` (`u32`)
/// - `16..32`: `tenant_hash` (`[u8; 16]`)
/// - `32..40`: `min_tx_id` (`u64`)
/// - `40..48`: `max_tx_id` (`u64`, also serves as committed `epoch_id`)
/// - `48..56`: `min_timestamp_ms` (`i64`)
/// - `56..64`: `max_timestamp_ms` (`i64`)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct MbfHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub flags: u16,
    pub row_count: u32,
    pub tenant_hash: [u8; 16],
    pub min_tx_id: u64,
    pub max_tx_id: u64,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
}

impl MbfHeader {
    pub const FLAG_HOT_HEAD: u16 = MBF_FLAG_HOT_HEAD;
    pub const FLAG_ENCRYPTED: u16 = MBF_FLAG_CRYPTO_SEALED;
    pub const FLAG_ZONE_ALL: u16 = 0b0111_1111 << 2;

    /// Creates a new spec-compliant 64-byte `MbfHeader`.
    pub fn new(
        cell_id: u32,
        row_count: u64,
        epoch_id: u64,
        schema_fingerprint: u64,
        fiscal_lock_epoch_ms: i64,
        created_at_ms: i64,
    ) -> Self {
        let cell_bits = ((cell_id & 0x3F) as u16) << 10;
        let mut tenant_hash = [0u8; 16];
        tenant_hash[0..8].copy_from_slice(&schema_fingerprint.to_le_bytes());
        tenant_hash[8..16].copy_from_slice(&(fiscal_lock_epoch_ms as u64).to_le_bytes());

        Self {
            magic: MBF_MAGIC,
            version: MBF_FORMAT_VERSION,
            flags: Self::FLAG_ZONE_ALL | cell_bits,
            row_count: row_count.min(u64::from(u32::MAX)) as u32,
            tenant_hash,
            min_tx_id: if epoch_id > 0 { 1 } else { 0 },
            max_tx_id: epoch_id,
            min_timestamp_ms: fiscal_lock_epoch_ms,
            max_timestamp_ms: created_at_ms,
        }
    }

    /// Creates a 64-byte header for a `.mbf.idx` synopsis mirror file (`"MBFINDEX"`).
    pub fn new_idx_mirror(
        cell_id: u32,
        row_count: u64,
        epoch_id: u64,
        schema_fingerprint: u64,
        fiscal_lock_epoch_ms: i64,
        created_at_ms: i64,
    ) -> Self {
        let mut hdr = Self::new(
            cell_id,
            row_count,
            epoch_id,
            schema_fingerprint,
            fiscal_lock_epoch_ms,
            created_at_ms,
        );
        hdr.magic = MBF_IDX_MAGIC;
        hdr
    }

    /// Sets additional bitflags on `flags` while preserving cell_id bits.
    #[inline]
    pub fn with_flags(mut self, extra_flags: u16) -> Self {
        let cell_bits = self.flags & (0x3F << 10);
        self.flags = cell_bits | extra_flags;
        self
    }

    /// Creates a new `MbfHeader` with explicit tenant hash and transaction/timestamp bounds (§4.1).
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tenant(
        tenant_id: &str,
        cell_id: u8,
        is_hot_head: bool,
        zone_mask: u8,
        row_count: u32,
        min_tx_id: u64,
        max_tx_id: u64,
        min_timestamp_ms: i64,
        max_timestamp_ms: i64,
    ) -> Self {
        let mut flags = (u16::from(zone_mask & 0x7F)) << 2;
        if is_hot_head {
            flags |= Self::FLAG_HOT_HEAD;
        }
        flags |= u16::from(cell_id & 0x3F) << 10;

        Self {
            magic: MBF_MAGIC,
            version: MBF_FORMAT_VERSION,
            flags,
            row_count,
            tenant_hash: compute_tenant_hash(tenant_id),
            min_tx_id,
            max_tx_id,
            min_timestamp_ms,
            max_timestamp_ms,
        }
    }

    /// Extracts the 6-bit domain cell ID (`0..=7`) encoded in `flags`.
    #[inline]
    pub fn cell_id(&self) -> u32 {
        u32::from((self.flags >> 10) & 0x3F)
    }

    /// Returns the highest transaction/epoch ID in this `.mbf` file.
    #[inline]
    pub fn epoch_id(&self) -> u64 {
        self.max_tx_id
    }

    /// Returns true if this file is a `HOT_HEAD.mbf` transactional manifest.
    #[inline]
    pub fn is_hot_head(&self) -> bool {
        (self.flags & Self::FLAG_HOT_HEAD) != 0
    }

    /// Computes the CRC32C checksum of the 64-byte header.
    #[inline]
    pub fn compute_crc32(&self) -> u32 {
        compute_crc32c(bytemuck::bytes_of(self))
    }

    /// Zero-copy cast from a 64-byte aligned slice (`0 ns` via `bytemuck`).
    pub fn try_zero_copy_ref(bytes: &[u8]) -> DomainResult<&Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Buffer too small for MbfHeader: {} bytes", bytes.len()),
            ));
        }
        let header: &Self = bytemuck::try_from_bytes(&bytes[0..64]).map_err(|_| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Slice is not 64-byte aligned for zero-copy MbfHeader cast",
            )
        })?;
        header.validate()?;
        Ok(header)
    }

    /// Validates magic bytes and format version.
    pub fn validate(&self) -> DomainResult<()> {
        if self.magic != MBF_MAGIC && self.magic != MBF_IDX_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MBF magic bytes in header (expected METRIBIN or MBFINDEX)",
            ));
        }
        if self.version != MBF_FORMAT_VERSION {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Unsupported MBF version: {}", self.version),
            ));
        }
        Ok(())
    }

    /// Encodes the header into exactly 64 bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decodes and validates a 64-byte header slice (works on both aligned and unaligned buffers).
    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Buffer too small for MbfHeader: {} bytes", bytes.len()),
            ));
        }
        let header: Self = bytemuck::pod_read_unaligned(&bytes[0..64]);
        header.validate()?;
        Ok(header)
    }
}

/// 128-byte aligned table of physical zone offsets and byte lengths inside the `.mbf` file (§3.7.1).
/// Every offset and length is a multiple of 64 bytes.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct ZoneOffsetsTable {
    pub zone1_pax_offset: u64,
    pub zone1_pax_len: u64,
    pub zone2_csr_offset: u64,
    pub zone2_csr_len: u64,
    pub zone3_iot_offset: u64,
    pub zone3_iot_len: u64,
    pub zone4_ledger_offset: u64,
    pub zone4_ledger_len: u64,
    pub zone5_synopsis_offset: u64,
    pub zone5_synopsis_len: u64,
    pub zone5b_search_offset: u64,
    pub zone5b_search_len: u64,
    pub zone6b_cov_offset: u64,
    pub zone6b_cov_len: u64,
    pub state_overlay_offset: u64,
    pub state_overlay_len: u64,
}

impl ZoneOffsetsTable {
    pub fn to_bytes(&self) -> [u8; 128] {
        let mut out = [0u8; 128];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 128 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Buffer too small for ZoneOffsetsTable",
            ));
        }
        Ok(bytemuck::pod_read_unaligned(&bytes[0..128]))
    }
}

/// 32-byte aligned column chunk descriptor inside the Directory Index (§3.7.1).
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct ColumnChunkDescriptor {
    pub attr_id: u16,
    pub zone_id: u8,
    pub value_tag: u8,
    pub row_count: u32,
    pub byte_offset: u64,
    pub byte_len: u64,
    pub synopsis_offset: u64,
}

/// Complete binary Directory Index written at `directory_offset` before `MbfFooterTrailer` (§3.7).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfDirectoryIndex {
    pub zones: ZoneOffsetsTable,
    pub columns: Vec<ColumnChunkDescriptor>,
}

impl MbfDirectoryIndex {
    /// Encodes the directory index into a 64-byte aligned binary buffer.
    pub fn to_aligned_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128 + 8 + self.columns.len() * 32 + 64);
        buf.extend_from_slice(&self.zones.to_bytes());
        buf.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved padding
        for col in &self.columns {
            buf.extend_from_slice(bytemuck::bytes_of(col));
        }
        pad_to_64(&mut buf);
        buf
    }

    /// Alias for `to_aligned_bytes`.
    #[inline]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_aligned_bytes()
    }

    /// Decodes a `MbfDirectoryIndex` from its binary slice.
    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 136 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("DirectoryIndex buffer too small: {} bytes", bytes.len()),
            ));
        }
        let zones = ZoneOffsetsTable::from_bytes(&bytes[0..128])?;
        let col_count =
            u32::from_le_bytes([bytes[128], bytes[129], bytes[130], bytes[131]]) as usize;
        let needed = 136usize.saturating_add(col_count.saturating_mul(32));
        if bytes.len() < needed {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "DirectoryIndex truncated column descriptors",
            ));
        }
        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let start = 136 + i * 32;
            let desc: ColumnChunkDescriptor =
                bytemuck::pod_read_unaligned(&bytes[start..start + 32]);
            columns.push(desc);
        }
        Ok(Self { zones, columns })
    }
}

/// Fixed 64-byte footer trailer at the end of every `.mbf` file (`#[repr(C, align(64))]`, §4.1).
///
/// Byte layout:
/// - `0..8`:   `directory_offset` (`u64`)
/// - `8..12`:  `directory_len` (`u32`)
/// - `12..16`: `header_crc32c` (`u32`)
/// - `16..20`: `directory_crc32c` (`u32`)
/// - `20..24`: `schema_fingerprint_hi` (`u32`)
/// - `24..32`: `fiscal_lock_epoch_ms` (`i64`)
/// - `32..56`: `hmac_tag_truncated` (`[u8; 24]`)
/// - `56..64`: `footer_magic` (`"MBFFOOTR"`)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct MbfFooterTrailer {
    pub directory_offset: u64,
    pub directory_len: u32,
    pub header_crc32c: u32,
    pub directory_crc32c: u32,
    pub schema_fingerprint_hi: u32,
    pub fiscal_lock_epoch_ms: i64,
    pub hmac_tag_truncated: [u8; 24],
    pub footer_magic: [u8; 8],
}

impl MbfFooterTrailer {
    pub const FOOTER_MAGIC: [u8; 8] = MBF_FOOTER_MAGIC;

    /// Constructs a new `MbfFooterTrailer` with computed CRC32C and 192-bit truncated HMAC tag.
    pub fn new(
        directory_offset: u64,
        directory_len: u32,
        header_crc32c: u32,
        directory_crc32c: u32,
        schema_fingerprint_hi: u32,
        fiscal_lock_epoch_ms: i64,
        hmac_tag_truncated: [u8; 24],
    ) -> Self {
        Self {
            directory_offset,
            directory_len,
            header_crc32c,
            directory_crc32c,
            schema_fingerprint_hi,
            fiscal_lock_epoch_ms,
            hmac_tag_truncated,
            footer_magic: MBF_FOOTER_MAGIC,
        }
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf.copy_from_slice(bytemuck::bytes_of(self));
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Buffer too small for MbfFooterTrailer",
            ));
        }
        let trailer: Self = bytemuck::pod_read_unaligned(&bytes[0..64]);
        if trailer.footer_magic != MBF_FOOTER_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MbfFooterTrailer magic bytes (expected MBFFOOTR)",
            ));
        }
        Ok(trailer)
    }
}

/// Cryptographic Multi-Tenant Block Seal (`#[repr(C, align(64))]`).
/// Prevents any cross-tenant data leakage in shared `/tmp` NVMe cache or S3 buckets.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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

    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Buffer too small for MbfTenantCryptoSeal",
            ));
        }
        Ok(bytemuck::pod_read_unaligned(&bytes[0..64]))
    }

    /// Returns the first 24 bytes of `keyed_mac` for `MbfFooterTrailer.hmac_tag_truncated`.
    #[inline]
    pub fn truncated_hmac_24(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out.copy_from_slice(&self.keyed_mac[0..24]);
        out
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

/// Logical value representation supporting Zero-Downtime Schema Evolution (`LazyDefaultProjection`)
/// and pure binary encoding (Zero-JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MbfCellValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Decimal128(i128),
    Text(String),
    Bytes(Vec<u8>),
}

impl MbfCellValue {
    /// Appends the compact binary representation of this cell value to `buf`.
    pub fn write_binary(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Null => buf.push(0),
            Self::Bool(b) => {
                buf.push(1);
                buf.push(u8::from(*b));
            }
            Self::Int64(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Self::Float64(v) => {
                buf.push(3);
                buf.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            Self::Decimal128(v) => {
                buf.push(4);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Self::Text(s) => {
                buf.push(5);
                let b = s.as_bytes();
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Self::Bytes(b) => {
                buf.push(6);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
    }

    /// Reads a binary `MbfCellValue` from `bytes` starting at `*cursor`.
    pub fn read_binary(bytes: &[u8], cursor: &mut usize) -> DomainResult<Self> {
        if *cursor >= bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF reading MbfCellValue tag",
            ));
        }
        let tag = bytes[*cursor];
        *cursor += 1;
        match tag {
            0 => Ok(Self::Null),
            1 => {
                if *cursor >= bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Bool cell",
                    ));
                }
                let b = bytes[*cursor] != 0;
                *cursor += 1;
                Ok(Self::Bool(b))
            }
            2 => {
                if *cursor + 8 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Int64 cell",
                    ));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[*cursor..*cursor + 8]);
                *cursor += 8;
                Ok(Self::Int64(i64::from_le_bytes(arr)))
            }
            3 => {
                if *cursor + 8 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Float64 cell",
                    ));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[*cursor..*cursor + 8]);
                *cursor += 8;
                Ok(Self::Float64(f64::from_bits(u64::from_le_bytes(arr))))
            }
            4 => {
                if *cursor + 16 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Decimal128 cell",
                    ));
                }
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&bytes[*cursor..*cursor + 16]);
                *cursor += 16;
                Ok(Self::Decimal128(i128::from_le_bytes(arr)))
            }
            5 => {
                if *cursor + 4 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Text length",
                    ));
                }
                let len = u32::from_le_bytes([
                    bytes[*cursor],
                    bytes[*cursor + 1],
                    bytes[*cursor + 2],
                    bytes[*cursor + 3],
                ]) as usize;
                *cursor += 4;
                if *cursor + len > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Text payload",
                    ));
                }
                let s = String::from_utf8_lossy(&bytes[*cursor..*cursor + len]).into_owned();
                *cursor += len;
                Ok(Self::Text(s))
            }
            6 => {
                if *cursor + 4 > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Bytes length",
                    ));
                }
                let len = u32::from_le_bytes([
                    bytes[*cursor],
                    bytes[*cursor + 1],
                    bytes[*cursor + 2],
                    bytes[*cursor + 3],
                ]) as usize;
                *cursor += 4;
                if *cursor + len > bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF reading Bytes payload",
                    ));
                }
                let out = bytes[*cursor..*cursor + len].to_vec();
                *cursor += len;
                Ok(Self::Bytes(out))
            }
            other => Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Invalid MbfCellValue binary tag: {other}"),
            )),
        }
    }
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
    fn verify_64_byte_alignment_zero_copy_and_roundtrip() {
        assert_eq!(std::mem::size_of::<MbfHeader>(), 64);
        assert_eq!(std::mem::align_of::<MbfHeader>(), 64);
        assert_eq!(std::mem::size_of::<MbfFooterTrailer>(), 64);
        assert_eq!(std::mem::align_of::<MbfFooterTrailer>(), 64);
        assert_eq!(std::mem::size_of::<MbfTenantCryptoSeal>(), 64);
        assert_eq!(std::mem::align_of::<MbfTenantCryptoSeal>(), 64);
        assert_eq!(std::mem::size_of::<ZoneOffsetsTable>(), 128);
        assert_eq!(std::mem::align_of::<ZoneOffsetsTable>(), 64);
        assert_eq!(std::mem::size_of::<ColumnChunkDescriptor>(), 32);
        assert_eq!(std::mem::align_of::<ColumnChunkDescriptor>(), 32);

        let header = MbfHeader::new(
            2,
            25_000,
            42,
            0xCAFE_BABE,
            1_700_000_000_000,
            1_700_000_100_000,
        );
        assert_eq!(&header.magic, b"METRIBIN");
        assert_eq!(header.cell_id(), 2);
        assert_eq!(header.epoch_id(), 42);

        let bytes = header.to_bytes();
        let decoded = MbfHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header, decoded);

        // Zero-copy reference from aligned struct bytes
        let zc_ref = MbfHeader::try_zero_copy_ref(bytemuck::bytes_of(&header)).unwrap();
        assert_eq!(zc_ref, &header);

        // Magic corruption must return MbfCorrupt001
        let mut corrupted = bytes;
        corrupted[0] ^= 0xFF;
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
    fn directory_index_and_cell_value_binary_roundtrip() {
        let dir = MbfDirectoryIndex {
            zones: ZoneOffsetsTable {
                zone1_pax_offset: 128,
                zone1_pax_len: 4096,
                zone2_csr_offset: 4224,
                zone2_csr_len: 512,
                ..Default::default()
            },
            columns: vec![ColumnChunkDescriptor {
                attr_id: 7,
                zone_id: 1,
                value_tag: 4,
                row_count: 100,
                byte_offset: 128,
                byte_len: 1600,
                synopsis_offset: 4736,
            }],
        };
        let encoded = dir.to_aligned_bytes();
        assert_eq!(encoded.len() % 64, 0);
        let decoded = MbfDirectoryIndex::from_bytes(&encoded).unwrap();
        assert_eq!(dir, decoded);
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
