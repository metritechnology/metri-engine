//! Especificación binaria de cabeceras, pies de página, índice de directorio, espejo `.mbf.idx`, sellos criptográficos por tenant y evolución de esquema sin tiempo de inactividad.

use bytemuck::{Pod, Zeroable};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;

type HmacSha256 = Hmac<Sha256>;

/// Firma mágica canónica de 8 bytes que identifica un archivo Metri Binary Format v1 (`"METRIBIN"`).
pub const MBF_MAGIC: [u8; 8] = *b"METRIBIN";

/// Alias de `MBF_MAGIC` preservado por compatibilidad entre módulos.
pub const MBF_MAGIC_BYTES: [u8; 8] = MBF_MAGIC;

/// Firma mágica canónica de 8 bytes que identifica el pie de página (`trailer`) de un archivo `.mbf` (`"MBFFOOTR"`).
pub const MBF_FOOTER_MAGIC: [u8; 8] = *b"MBFFOOTR";

/// Firma mágica canónica de 8 bytes que identifica un archivo espejo de sinopsis `.mbf.idx` (`"MBFINDEX"`).
pub const MBF_IDX_MAGIC: [u8; 8] = *b"MBFINDEX";

/// Versión vigente del formato binario en disco (`1`).
pub const MBF_FORMAT_VERSION: u16 = 1;

/// Tamaño canónico de un grupo de filas (*micro-block row group* de $2^{12} = 4096$ filas).
pub const MBF_ROW_GROUP_SIZE: usize = 4096;

/// Bandera de cabecera: el archivo es una trama transaccional `HOT_HEAD.mbf`.
pub const MBF_FLAG_HOT_HEAD: u16 = 1 << 0;
/// Bandera de cabecera: el archivo incluye un sello criptográfico `MbfTenantCryptoSeal` de 64 bytes.
pub const MBF_FLAG_CRYPTO_SEALED: u16 = 1 << 1;
/// Bandera de cabecera: Zona 1 (`PAX-MVCC`) presente.
pub const MBF_FLAG_HAS_PAX: u16 = 1 << 2;
/// Bandera de cabecera: Zona 2 (`CSR Graph`) presente.
pub const MBF_FLAG_HAS_CSR: u16 = 1 << 3;
/// Bandera de cabecera: Zona 3 (`IoT Time-Series`) presente.
pub const MBF_FLAG_HAS_IOT: u16 = 1 << 4;
/// Bandera de cabecera: Zona 4 (`Financial Ledger`) presente.
pub const MBF_FLAG_HAS_LEDGER: u16 = 1 << 5;
/// Bandera de cabecera: Zona 5 (`BI Synopses`) presente.
pub const MBF_FLAG_HAS_SYNOPSES: u16 = 1 << 6;
/// Bandera de cabecera: Zona 5B (`Hybrid Search`) presente.
pub const MBF_FLAG_HAS_SEARCH: u16 = 1 << 7;
/// Bandera de cabecera: Zona 6B (`Cross Covariance`) presente.
pub const MBF_FLAG_HAS_COVARIANCE: u16 = 1 << 8;

/// Rellena `buf` con ceros hasta alcanzar el siguiente múltiplo exacto de 64 bytes (tamaño de línea de caché L1).
#[inline]
pub fn pad_to_64(buf: &mut Vec<u8>) {
    let rem = buf.len() % 64;
    if rem != 0 {
        buf.resize(buf.len() + (64 - rem), 0u8);
    }
}

/// Escribe una cadena UTF-8 precedida por su longitud de 4 bytes en formato *little-endian* dentro de `buf`.
#[inline]
pub fn write_len_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

/// Lee una cadena UTF-8 precedida por su longitud de 4 bytes en *little-endian* desde `bytes` a partir de `*cursor`.
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

/// Calcula la suma de verificación CRC32 acelerada por hardware sobre `bytes`.
#[inline]
pub fn compute_crc32c(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// Calcula un hash determinista truncado a 128 bits (`[u8; 16]`) del `tenant_id`.
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

/// Cabecera fija de 64 bytes alineada a una línea de caché de CPU (`#[repr(C, align(64))]`).
///
/// Disposición de bytes:
/// - `0..8`:   `magic` (`"METRIBIN"` o `"MBFINDEX"`)
/// - `8..10`:  `version` (`1`)
/// - `10..12`: `flags` (Bit 0: HotHead, Bit 1: Encrypted, Bits 2..9: ZoneMask, Bits 10..15: CellId)
/// - `12..16`: `row_count` (`u32`)
/// - `16..32`: `tenant_hash` (`[u8; 16]`)
/// - `32..40`: `min_tx_id` (`u64`)
/// - `40..48`: `max_tx_id` (`u64`, también actúa como `epoch_id` confirmado)
/// - `48..56`: `min_timestamp_ms` (`i64`)
/// - `56..64`: `max_timestamp_ms` (`i64`)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct MbfHeader {
    /// Bytes mágicos de identificación del formato (`"METRIBIN"` o `"MBFINDEX"`).
    pub magic: [u8; 8],
    /// Versión del formato binario (`1`).
    pub version: u16,
    /// Máscara de bits con banderas de zonas presentes, modo `HOT_HEAD`, sello criptográfico y `cell_id`.
    pub flags: u16,
    /// Número total de filas o registros contenidos en el archivo.
    pub row_count: u32,
    /// Huella de 128 bits del tenant o metadatos de esquema/bloqueo fiscal.
    pub tenant_hash: [u8; 16],
    /// Identificador mínimo de transacción (`tx_id`) incluido en el archivo.
    pub min_tx_id: u64,
    /// Identificador máximo de transacción (`tx_id` / `epoch_id`) incluido en el archivo.
    pub max_tx_id: u64,
    /// Marca temporal mínima en milisegundos (o época de bloqueo fiscal).
    pub min_timestamp_ms: i64,
    /// Marca temporal máxima en milisegundos (o instante de creación del bloque).
    pub max_timestamp_ms: i64,
}

impl MbfHeader {
    /// Bandera que indica que el archivo es un manifiesto `HOT_HEAD.mbf`.
    pub const FLAG_HOT_HEAD: u16 = MBF_FLAG_HOT_HEAD;
    /// Bandera que indica que el bloque cuenta con sello criptográfico por tenant.
    pub const FLAG_ENCRYPTED: u16 = MBF_FLAG_CRYPTO_SEALED;
    /// Máscara de bits que habilita todas las zonas físicas en `flags`.
    pub const FLAG_ZONE_ALL: u16 = 0b0111_1111 << 2;

    /// Crea una nueva cabecera `MbfHeader` de 64 bytes conforme a la especificación.
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

    /// Crea una cabecera de 64 bytes para un archivo espejo de sinopsis `.mbf.idx` (`"MBFINDEX"`).
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

    /// Asigna banderas adicionales en `flags` preservando los bits del `cell_id`.
    #[inline]
    pub fn with_flags(mut self, extra_flags: u16) -> Self {
        let cell_bits = self.flags & (0x3F << 10);
        self.flags = cell_bits | extra_flags;
        self
    }

    /// Crea un `MbfHeader` con hash explícito de tenant y cotas de transacciones y marcas de tiempo.
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

    /// Extrae el identificador de celda de dominio de 6 bits (`0..=7`) codificado en `flags`.
    #[inline]
    pub fn cell_id(&self) -> u32 {
        u32::from((self.flags >> 10) & 0x3F)
    }

    /// Retorna el identificador de época/transacción más alto en este archivo `.mbf`.
    #[inline]
    pub fn epoch_id(&self) -> u64 {
        self.max_tx_id
    }

    /// Retorna `true` si este archivo corresponde a un manifiesto transaccional `HOT_HEAD.mbf`.
    #[inline]
    pub fn is_hot_head(&self) -> bool {
        (self.flags & Self::FLAG_HOT_HEAD) != 0
    }

    /// Calcula el checksum CRC32C de los 64 bytes de la cabecera.
    #[inline]
    pub fn compute_crc32(&self) -> u32 {
        compute_crc32c(bytemuck::bytes_of(self))
    }

    /// Conversión *zero-copy* desde un slice alineado a 64 bytes (`0 ns` vía `bytemuck`).
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

    /// Valida los bytes mágicos y la versión del formato.
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

    /// Codifica la cabecera en un arreglo exacto de 64 bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decodifica y valida un slice de cabecera de 64 bytes (soporta búferes alineados y no alineados).
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

/// Tabla de 128 bytes alineada a 64 bytes con los desplazamientos físicos y longitudes de cada zona dentro del archivo `.mbf`.
/// Todo desplazamiento y longitud es múltiplo exacto de 64 bytes.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct ZoneOffsetsTable {
    /// Desplazamiento en bytes hacia la Zona 1 (`PAX-MVCC`).
    pub zone1_pax_offset: u64,
    /// Longitud en bytes de la Zona 1 (`PAX-MVCC`).
    pub zone1_pax_len: u64,
    /// Desplazamiento en bytes hacia la Zona 2 (`CSR Graph`).
    pub zone2_csr_offset: u64,
    /// Longitud en bytes de la Zona 2 (`CSR Graph`).
    pub zone2_csr_len: u64,
    /// Desplazamiento en bytes hacia la Zona 3 (`IoT Time-Series`).
    pub zone3_iot_offset: u64,
    /// Longitud en bytes de la Zona 3 (`IoT Time-Series`).
    pub zone3_iot_len: u64,
    /// Desplazamiento en bytes hacia la Zona 4 (`Financial Ledger`).
    pub zone4_ledger_offset: u64,
    /// Longitud en bytes de la Zona 4 (`Financial Ledger`).
    pub zone4_ledger_len: u64,
    /// Desplazamiento en bytes hacia la Zona 5 (`BI Synopses`).
    pub zone5_synopsis_offset: u64,
    /// Longitud en bytes de la Zona 5 (`BI Synopses`).
    pub zone5_synopsis_len: u64,
    /// Desplazamiento en bytes hacia la Zona 5B (`Hybrid Search`).
    pub zone5b_search_offset: u64,
    /// Longitud en bytes de la Zona 5B (`Hybrid Search`).
    pub zone5b_search_len: u64,
    /// Desplazamiento en bytes hacia la Zona 6B (`Cross Covariance`).
    pub zone6b_cov_offset: u64,
    /// Longitud en bytes de la Zona 6B (`Cross Covariance`).
    pub zone6b_cov_len: u64,
    /// Desplazamiento en bytes hacia el *overlay* de estado transaccional.
    pub state_overlay_offset: u64,
    /// Longitud en bytes del *overlay* de estado transaccional.
    pub state_overlay_len: u64,
}

impl ZoneOffsetsTable {
    /// Serializa la tabla de desplazamientos de zonas en 128 bytes exactos.
    pub fn to_bytes(&self) -> [u8; 128] {
        let mut out = [0u8; 128];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decodifica una `ZoneOffsetsTable` a partir de un slice de al menos 128 bytes.
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

/// Descriptor de bloque de columna de 32 bytes alineado (`#[repr(C, align(32))]`) dentro del `MbfDirectoryIndex`.
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct ColumnChunkDescriptor {
    /// Identificador numérico del atributo (`attr_id`).
    pub attr_id: u16,
    /// Identificador de la zona física donde reside el bloque.
    pub zone_id: u8,
    /// Etiqueta del tipo de valor o compresión (`value_tag`).
    pub value_tag: u8,
    /// Cantidad de filas almacenadas en este bloque de columna.
    pub row_count: u32,
    /// Desplazamiento físico en bytes desde el inicio del archivo `.mbf`.
    pub byte_offset: u64,
    /// Longitud física en bytes del bloque de columna.
    pub byte_len: u64,
    /// Desplazamiento físico hacia la sinopsis estadística de esta columna en la Zona 5.
    pub synopsis_offset: u64,
}

/// Índice de directorio binario completo escrito en `directory_offset` justo antes de `MbfFooterTrailer`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfDirectoryIndex {
    /// Tabla de desplazamientos y longitudes de las zonas físicas.
    pub zones: ZoneOffsetsTable,
    /// Lista de descriptores de bloques de columnas presentes en el archivo.
    pub columns: Vec<ColumnChunkDescriptor>,
}

impl MbfDirectoryIndex {
    /// Codifica el índice de directorio en un búfer binario alineado a 64 bytes.
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

    /// Alias de `to_aligned_bytes`.
    #[inline]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_aligned_bytes()
    }

    /// Decodifica un `MbfDirectoryIndex` desde su representación binaria.
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

/// Pie de página fijo (`trailer`) de 64 bytes al final de todo archivo `.mbf` (`#[repr(C, align(64))]`).
///
/// Disposición de bytes:
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
    /// Desplazamiento en bytes donde inicia el `MbfDirectoryIndex`.
    pub directory_offset: u64,
    /// Longitud en bytes del `MbfDirectoryIndex`.
    pub directory_len: u32,
    /// Suma de verificación CRC32C de los 64 bytes de `MbfHeader`.
    pub header_crc32c: u32,
    /// Suma de verificación CRC32C de los bytes del `MbfDirectoryIndex`.
    pub directory_crc32c: u32,
    /// Parte alta (32 bits) de la huella de esquema del modelo.
    pub schema_fingerprint_hi: u32,
    /// Marca temporal (epoch ms) de bloqueo contable/fiscal del bloque.
    pub fiscal_lock_epoch_ms: i64,
    /// Etiqueta HMAC-SHA256 truncada a 192 bits (24 bytes) del sello criptográfico del bloque.
    pub hmac_tag_truncated: [u8; 24],
    /// Firma mágica de cierre de 8 bytes (`"MBFFOOTR"`).
    pub footer_magic: [u8; 8],
}

impl MbfFooterTrailer {
    /// Bytes mágicos canónicos del pie de página (`"MBFFOOTR"`).
    pub const FOOTER_MAGIC: [u8; 8] = MBF_FOOTER_MAGIC;

    /// Construye un nuevo `MbfFooterTrailer` con los CRC32C calculados y la etiqueta HMAC truncada a 192 bits.
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

    /// Serializa el pie de página en 64 bytes exactos.
    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf.copy_from_slice(bytemuck::bytes_of(self));
        buf
    }

    /// Decodifica y valida el pie de página de 64 bytes verificando la firma `"MBFFOOTR"`.
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

/// Sello criptográfico multitenant de bloque de 64 bytes (`#[repr(C, align(64))]`).
/// Impide cualquier fuga de datos entre tenants en la caché NVMe compartida `/tmp` o en S3.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct MbfTenantCryptoSeal {
    /// Hash de contexto de 128 bits derivado de `tenant_id` y `entity_type`.
    pub tenant_context_hash: [u8; 16],
    /// Nonce de 96 bits que incorpora el `epoch_id` del bloque.
    pub nonce: [u8; 12],
    /// Banderas de configuración criptográfica del sello.
    pub crypto_flags: u32,
    /// Código de autenticación de mensaje HMAC-SHA256 de 256 bits sobre el contexto, nonce y carga útil.
    pub keyed_mac: [u8; 32],
}

impl MbfTenantCryptoSeal {
    /// Deriva una clave específica por tenant a partir del secreto maestro y sella criptográficamente `payload`.
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

    /// Verifica en tiempo constante que el bloque pertenezca a `expected_tenant_id` y que `payload` no haya sido alterado.
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

    /// Serializa el sello criptográfico en 64 bytes exactos.
    #[inline]
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decodifica un `MbfTenantCryptoSeal` desde un slice de al menos 64 bytes.
    pub fn from_bytes(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Buffer too small for MbfTenantCryptoSeal",
            ));
        }
        Ok(bytemuck::pod_read_unaligned(&bytes[0..64]))
    }

    /// Retorna los primeros 24 bytes de `keyed_mac` para poblar `MbfFooterTrailer.hmac_tag_truncated`.
    #[inline]
    pub fn truncated_hmac_24(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out.copy_from_slice(&self.keyed_mac[0..24]);
        out
    }

    /// Sella un bloque `.mbf` utilizando la clave maestra activa del `MbfSealKeyRing` global (`crypto_flags = 2`).
    pub fn seal_active(
        tenant_id: &str,
        entity_type: &str,
        epoch_id: u64,
        payload: &[u8],
    ) -> DomainResult<Self> {
        let active_key = crate::domain::config::engine_config().active_mbf_seal_key();
        let mut seal = Self::seal_block(active_key, tenant_id, entity_type, epoch_id, payload)?;
        seal.crypto_flags = if active_key == crate::domain::config::LEGACY_MBF_SEAL_KEY {
            1
        } else {
            2
        };
        Ok(seal)
    }

    /// Verifica en tiempo constante el sello criptográfico probando primero la clave activa del `EngineConfig`
    /// y, como respaldo de rotación sin tiempo de inactividad, la clave legada `LEGACY_MBF_SEAL_KEY`.
    pub fn verify_with_keyring(
        &self,
        expected_tenant_id: &str,
        expected_entity_type: &str,
        payload: &[u8],
    ) -> DomainResult<()> {
        let cfg = crate::domain::config::engine_config();
        let active_key = cfg.active_mbf_seal_key();
        match self.verify_block(
            active_key,
            expected_tenant_id,
            expected_entity_type,
            payload,
        ) {
            Ok(()) => Ok(()),
            Err(err) if err.code == ErrorCode::MbfTenantSeal001 => Err(err),
            Err(_) => {
                if let Some(ref raw_hmac) = cfg.hmac_secret {
                    if !raw_hmac.is_empty() && raw_hmac.as_bytes() != active_key {
                        if self
                            .verify_block(
                                raw_hmac.as_bytes(),
                                expected_tenant_id,
                                expected_entity_type,
                                payload,
                            )
                            .is_ok()
                        {
                            return Ok(());
                        }
                    }
                }
                if active_key != crate::domain::config::LEGACY_MBF_SEAL_KEY {
                    return self.verify_block(
                        crate::domain::config::LEGACY_MBF_SEAL_KEY,
                        expected_tenant_id,
                        expected_entity_type,
                        payload,
                    );
                }
                Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "MbfTenantCryptoSeal keyed_mac mismatch (tampering or wrong seal key)",
                ))
            }
        }
    }

    /// Verifica en tiempo constante que la etiqueta HMAC truncada de 192 bits en `MbfFooterTrailer`
    /// coincida exactamente con los primeros 24 bytes de `self.keyed_mac`.
    pub fn verify_footer_tag(&self, footer: &MbfFooterTrailer) -> DomainResult<()> {
        if !constant_time_eq::constant_time_eq(&self.keyed_mac[0..24], &footer.hmac_tag_truncated) {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Footer truncated HMAC-192 tag does not match MbfTenantCryptoSeal",
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

/// Representación lógica de un valor de celda columnar con soporte de evolución de esquema sin tiempo de inactividad
/// y codificación binaria pura (*Zero-JSON*).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MbfCellValue {
    /// Celda con valor nulo.
    Null,
    /// Valor booleano (`true` o `false`).
    Bool(bool),
    /// Entero con signo de 64 bits (`i64`).
    Int64(i64),
    /// Punto flotante de doble precisión IEEE 754 (`f64`).
    Float64(f64),
    /// Decimal de punto fijo de 128 bits (`i128`).
    Decimal128(i128),
    /// Cadena de texto UTF-8 (`String`).
    Text(String),
    /// Secuencia de bytes binarios (`Vec<u8>`).
    Bytes(Vec<u8>),
}

impl MbfCellValue {
    /// Añade la representación binaria compacta de este valor de celda en `buf`.
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

    /// Lee un `MbfCellValue` binario desde `bytes` avanzando `*cursor`.
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

/// Proyección de columna en un segmento `.mbf`: almacenada físicamente o sintetizada perezosamente desde valores por defecto del Códice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EvolvedColumnProjection {
    /// La columna existe físicamente en el archivo `.mbf`.
    Physical(Vec<MbfCellValue>),
    /// La columna fue añadida en `config/models/*.json` después de haberse sellado el archivo `.mbf`;
    /// se proyecta en $O(1)$ sin reescribir el archivo en disco.
    VirtualConstantDefault {
        /// Valor constante por defecto definido en el Códice.
        default_value: MbfCellValue,
        /// Número total de filas lógicas proyectadas.
        row_count: usize,
    },
}

impl EvolvedColumnProjection {
    /// Obtiene el valor en `row_idx` en tiempo $O(1)$ tanto para columnas físicas como virtuales.
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

    /// Retorna la cantidad de filas de la columna proyectada.
    pub fn len(&self) -> usize {
        match self {
            EvolvedColumnProjection::Physical(values) => values.len(),
            EvolvedColumnProjection::VirtualConstantDefault { row_count, .. } => *row_count,
        }
    }

    /// Retorna `true` si la columna proyectada no contiene filas.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Directorio de resolución de evolución de esquema para archivos `.mbf` históricos.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MbfSchemaEvolutionDirectory {
    /// Cantidad total de filas del bloque o segmento `.mbf`.
    pub row_count: usize,
    /// Mapa de columnas físicas presentes en el archivo indexadas por `attr_id`.
    pub physical_columns: HashMap<u16, Vec<MbfCellValue>>,
}

impl MbfSchemaEvolutionDirectory {
    /// Crea un nuevo directorio de evolución de esquema para un segmento con `row_count` filas.
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            physical_columns: HashMap::new(),
        }
    }

    /// Registra una columna física decodificada asociada a `attr_id`.
    pub fn insert_physical_column(&mut self, attr_id: u16, values: Vec<MbfCellValue>) {
        self.physical_columns.insert(attr_id, values);
    }

    /// Resuelve `attr_id`: retorna la columna física si existe en el segmento, o sintetiza un `VirtualConstantDefault` en $O(1)$.
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
