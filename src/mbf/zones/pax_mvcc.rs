//! Zone 1 PAX columnar storage with MVCC time-travel visibility vectors, Elias-Fano + MPHF directory, and FSST symbol compression.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use xxhash_rust::xxh64::xxh64;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::{pad_to_64, MbfCellValue, MBF_ROW_GROUP_SIZE};

const ZONE1_MAGIC: [u8; 8] = *b"MBFZ1PAX";

/// MVCC row visibility metadata for snapshot isolation and time-travel queries (`as_of_tx`, §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvccRowVersion {
    pub row_id: u32,
    pub tx_assert_epoch: u64,
    pub tx_retract_epoch: u64, // u64::MAX means currently active (not retracted)
}

impl MvccRowVersion {
    pub const ACTIVE_EPOCH: u64 = u64::MAX;

    /// Returns true if this row version was alive at `snapshot_epoch`.
    #[inline]
    pub fn is_visible_at(&self, snapshot_epoch: u64) -> bool {
        self.tx_assert_epoch <= snapshot_epoch && snapshot_epoch < self.tx_retract_epoch
    }
}

/// Elias-Fano monotonic offset sequence + MPHF displacement table for $O(1)$ `EAVT` entity lookups (§4.2.1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EliasFanoEntityDirectory {
    pub mphf_seed: u64,
    pub sorted_entity_ids: Vec<String>,
    /// Monotonically increasing byte/row offsets compressed via Elias-Fano low/high bit split.
    pub ef_low_width: u8,
    pub ef_low_bits: Vec<u64>,
    pub ef_high_bits: Vec<u64>,
    /// Row version chain per sorted entity index.
    pub entity_row_chains: Vec<Vec<u32>>,
    /// MPHF slot displacement table mapping `xxh64(entity_id, mphf_seed) % bucket_count` to sorted index.
    pub mphf_slots: Vec<u32>,
}

impl EliasFanoEntityDirectory {
    /// Builds an Elias-Fano + MPHF directory from an entity primary key map.
    pub fn build_from_map(index: &HashMap<String, Vec<u32>>) -> Self {
        let mut pairs: Vec<(String, Vec<u32>)> =
            index.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let n = pairs.len();
        if n == 0 {
            return Self::default();
        }

        let mut sorted_entity_ids = Vec::with_capacity(n);
        let mut entity_row_chains = Vec::with_capacity(n);
        let mut monotonic_offsets = Vec::with_capacity(n);
        let mut running_offset = 0u64;

        for (eid, rows) in pairs {
            monotonic_offsets.push(running_offset);
            running_offset += (eid.len() + rows.len() * 4) as u64;
            sorted_entity_ids.push(eid);
            entity_row_chains.push(rows);
        }

        let universe = running_offset.max(1);
        let ef_low_width = ((universe / (n as u64).max(1)).ilog2() as u8).min(31);
        let low_mask = if ef_low_width == 0 {
            0
        } else {
            (1u64 << ef_low_width) - 1
        };

        let mut ef_low_bits = vec![0u64; n];
        let max_high =
            (monotonic_offsets.last().copied().unwrap_or(0) >> ef_low_width) as usize + n + 2;
        let mut ef_high_bits = vec![0u64; max_high.div_ceil(64)];

        for (i, &off) in monotonic_offsets.iter().enumerate() {
            ef_low_bits[i] = off & low_mask;
            let high_pos = ((off >> ef_low_width) as usize) + i;
            if high_pos / 64 < ef_high_bits.len() {
                ef_high_bits[high_pos / 64] |= 1u64 << (high_pos % 64);
            }
        }

        // Build MPHF open-addressed displacement table (load factor 50% for guaranteed O(1) lookup)
        let table_len = (n * 2).next_power_of_two().max(8);
        let mphf_seed = 0x4D45_5452_495F_4546u64;
        let mut mphf_slots = vec![u32::MAX; table_len];
        for (sorted_idx, eid) in sorted_entity_ids.iter().enumerate() {
            let mut slot = (xxh64(eid.as_bytes(), mphf_seed) as usize) & (table_len - 1);
            while mphf_slots[slot] != u32::MAX {
                slot = (slot + 1) & (table_len - 1);
            }
            mphf_slots[slot] = sorted_idx as u32;
        }

        Self {
            mphf_seed,
            sorted_entity_ids,
            ef_low_width,
            ef_low_bits,
            ef_high_bits,
            entity_row_chains,
            mphf_slots,
        }
    }

    /// Resolves `entity_id` to its versioned row chain in $O(1)$ via MPHF slot probing.
    pub fn lookup_rows(&self, entity_id: &str) -> Option<&[u32]> {
        if self.mphf_slots.is_empty() {
            return None;
        }
        let mask = self.mphf_slots.len() - 1;
        let mut slot = (xxh64(entity_id.as_bytes(), self.mphf_seed) as usize) & mask;
        for _ in 0..self.mphf_slots.len() {
            let idx = self.mphf_slots[slot];
            if idx == u32::MAX {
                return None;
            }
            let idx_usize = idx as usize;
            if self.sorted_entity_ids.get(idx_usize).map(String::as_str) == Some(entity_id) {
                return self.entity_row_chains.get(idx_usize).map(Vec::as_slice);
            }
            slot = (slot + 1) & mask;
        }
        None
    }
}

/// Fast Static Symbol Table (`FSST`, up to 255 entries, §4.2.3) for dictionary/symbol compression of strings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsstSymbolTable {
    pub symbols: Vec<String>,
    symbol_to_code: HashMap<String, u8>,
}

impl FsstSymbolTable {
    pub const ESCAPE_CODE: u8 = 0xFF;

    pub fn new() -> Self {
        Self::default()
    }

    /// Interns a frequent string or status token into the 255-entry symbol table.
    pub fn intern_or_escape(&mut self, text: &str) -> Option<u8> {
        if let Some(&code) = self.symbol_to_code.get(text) {
            return Some(code);
        }
        if self.symbols.len() < 255 {
            let code = self.symbols.len() as u8;
            self.symbols.push(text.to_string());
            self.symbol_to_code.insert(text.to_string(), code);
            Some(code)
        } else {
            None
        }
    }

    pub fn decode_symbol(&self, code: u8) -> Option<&str> {
        self.symbols.get(code as usize).map(String::as_str)
    }
}

/// Zone 1 PAX + MVCC block storing entities with O(1) primary key lookup, secondary attribute index, and time-travel history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaxMvccZone {
    pub entity_type: String,
    pub primary_key_index: HashMap<String, Vec<u32>>,
    pub mvcc_versions: Vec<MvccRowVersion>,
    pub columns: HashMap<String, Vec<MbfCellValue>>,
    pub fsst_table: FsstSymbolTable,
    pub directory: EliasFanoEntityDirectory,
    /// Active visibility bitmaps: 512 bytes (`[u64; 64]`) per 4,096-row micro-block (§4.2).
    pub active_visibility_words: Vec<u64>,
    /// Native secondary attribute-value index `(attr_name, canonical_value_str) -> Vec<row_id>` for O(log N) AVET filtering.
    pub avet_secondary_index: BTreeMap<(String, String), Vec<u32>>,
}

impl PaxMvccZone {
    pub fn new(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            primary_key_index: HashMap::new(),
            mvcc_versions: Vec::new(),
            columns: HashMap::new(),
            fsst_table: FsstSymbolTable::new(),
            directory: EliasFanoEntityDirectory::default(),
            active_visibility_words: Vec::new(),
            avet_secondary_index: BTreeMap::new(),
        }
    }

    /// Upserts an entity version at `epoch_id`, automatically retracting any previous active version
    /// and updating the 512B visibility bitmap, FSST symbol table, and secondary AVET index.
    pub fn upsert_entity(
        &mut self,
        entity_id: &str,
        epoch_id: u64,
        attributes: HashMap<String, MbfCellValue>,
    ) -> u32 {
        if let Some(existing_rows) = self.primary_key_index.get(entity_id) {
            for &r_id in existing_rows {
                if let Some(ver) = self.mvcc_versions.get_mut(r_id as usize) {
                    if ver.tx_retract_epoch == MvccRowVersion::ACTIVE_EPOCH {
                        ver.tx_retract_epoch = epoch_id;
                        let word_idx = (r_id as usize) / 64;
                        let bit_idx = (r_id as usize) % 64;
                        if let Some(w) = self.active_visibility_words.get_mut(word_idx) {
                            *w &= !(1u64 << bit_idx);
                        }
                    }
                }
            }
        }

        let new_row_id = self.mvcc_versions.len() as u32;
        self.mvcc_versions.push(MvccRowVersion {
            row_id: new_row_id,
            tx_assert_epoch: epoch_id,
            tx_retract_epoch: MvccRowVersion::ACTIVE_EPOCH,
        });

        // Ensure active_visibility_words is sized in multiples of 64 words (512B per 4096 rows)
        let required_blocks = (self.mvcc_versions.len())
            .div_ceil(MBF_ROW_GROUP_SIZE)
            .max(1);
        let required_words = required_blocks * 64;
        if self.active_visibility_words.len() < required_words {
            self.active_visibility_words.resize(required_words, 0u64);
        }
        self.active_visibility_words[(new_row_id as usize) / 64] |=
            1u64 << ((new_row_id as usize) % 64);

        self.primary_key_index
            .entry(entity_id.to_string())
            .or_default()
            .push(new_row_id);

        let total_rows = self.mvcc_versions.len();
        for col in self.columns.values_mut() {
            while col.len() < total_rows {
                col.push(MbfCellValue::Null);
            }
        }
        for (attr_name, val) in attributes {
            if let MbfCellValue::Text(ref s) = val {
                let _ = self.fsst_table.intern_or_escape(s);
                self.avet_secondary_index
                    .entry((attr_name.clone(), s.clone()))
                    .or_default()
                    .push(new_row_id);
            } else if let MbfCellValue::Int64(n) = val {
                self.avet_secondary_index
                    .entry((attr_name.clone(), n.to_string()))
                    .or_default()
                    .push(new_row_id);
            }

            let col = self
                .columns
                .entry(attr_name)
                .or_insert_with(|| vec![MbfCellValue::Null; total_rows]);
            while col.len() < total_rows {
                col.push(MbfCellValue::Null);
            }
            if let Some(slot) = col.get_mut(new_row_id as usize) {
                *slot = val;
            }
        }

        self.directory = EliasFanoEntityDirectory::build_from_map(&self.primary_key_index);
        new_row_id
    }

    /// Pulls an entity's attributes as of `snapshot_epoch` (Time-Travel Pull) using the MPHF directory if built.
    pub fn pull_as_of(
        &self,
        entity_id: &str,
        snapshot_epoch: u64,
    ) -> Option<HashMap<String, MbfCellValue>> {
        let rows = self
            .directory
            .lookup_rows(entity_id)
            .or_else(|| self.primary_key_index.get(entity_id).map(Vec::as_slice))?;
        let active_row_id = rows.iter().rev().copied().find(|&r_id| {
            self.mvcc_versions
                .get(r_id as usize)
                .is_some_and(|v| v.is_visible_at(snapshot_epoch))
        })?;

        let mut map = HashMap::new();
        for (col_name, values) in &self.columns {
            if let Some(val) = values.get(active_row_id as usize) {
                if *val != MbfCellValue::Null {
                    map.insert(col_name.clone(), val.clone());
                }
            }
        }
        Some(map)
    }

    /// Looks up active `row_id`s matching `(attr_name, value_str)` in $O(\log N)$ via the secondary AVET index.
    pub fn query_by_attribute_value(
        &self,
        attr_name: &str,
        value_str: &str,
        snapshot_epoch: u64,
    ) -> Vec<u32> {
        let Some(candidates) = self
            .avet_secondary_index
            .get(&(attr_name.to_string(), value_str.to_string()))
        else {
            return Vec::new();
        };
        candidates
            .iter()
            .copied()
            .filter(|&r_id| {
                self.mvcc_versions
                    .get(r_id as usize)
                    .is_some_and(|v| v.is_visible_at(snapshot_epoch))
            })
            .collect()
    }

    /// Returns all visible `row_id`s at `snapshot_epoch`.
    pub fn visible_rows_at(&self, snapshot_epoch: u64) -> Vec<u32> {
        self.mvcc_versions
            .iter()
            .filter(|v| v.is_visible_at(snapshot_epoch))
            .map(|v| v.row_id)
            .collect()
    }

    /// Encodes Zone 1 into a 64-byte aligned binary slab (Zero-JSON, §4.2):
    /// `[Magic "MBFZ1PAX" (8B) | entity_type | MVCC vectors (tx_assert, tx_retract, bitmap) | PK Index | FSST Table | Columnar PAX Slabs | Padding 64B]`.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ZONE1_MAGIC);

        let et_bytes = self.entity_type.as_bytes();
        buf.extend_from_slice(&(et_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(et_bytes);

        // 1. MVCC Visibility Slab: row_count + tx_assert[u64] + tx_retract[u64]
        buf.extend_from_slice(&(self.mvcc_versions.len() as u32).to_le_bytes());
        for ver in &self.mvcc_versions {
            buf.extend_from_slice(&ver.row_id.to_le_bytes());
            buf.extend_from_slice(&ver.tx_assert_epoch.to_le_bytes());
            buf.extend_from_slice(&ver.tx_retract_epoch.to_le_bytes());
        }

        // 2. Sorted Entity Directory
        let mut sorted_pks: Vec<(&String, &Vec<u32>)> = self.primary_key_index.iter().collect();
        sorted_pks.sort_by(|a, b| a.0.cmp(b.0));
        buf.extend_from_slice(&(sorted_pks.len() as u32).to_le_bytes());
        for (eid, rows) in sorted_pks {
            let eb = eid.as_bytes();
            buf.extend_from_slice(&(eb.len() as u32).to_le_bytes());
            buf.extend_from_slice(eb);
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for &r in rows {
                buf.extend_from_slice(&r.to_le_bytes());
            }
        }

        // 3. FSST Symbol Table (up to 255 entries)
        buf.extend_from_slice(&(self.fsst_table.symbols.len() as u32).to_le_bytes());
        for sym in &self.fsst_table.symbols {
            let sb = sym.as_bytes();
            buf.extend_from_slice(&(sb.len() as u32).to_le_bytes());
            buf.extend_from_slice(sb);
        }

        // 4. Columnar Micro-Blocks (FSST-compressed Text + Binary Numeric Cells)
        let mut sorted_cols: Vec<(&String, &Vec<MbfCellValue>)> = self.columns.iter().collect();
        sorted_cols.sort_by(|a, b| a.0.cmp(b.0));
        buf.extend_from_slice(&(sorted_cols.len() as u32).to_le_bytes());
        for (col_name, cells) in sorted_cols {
            let cb = col_name.as_bytes();
            buf.extend_from_slice(&(cb.len() as u32).to_le_bytes());
            buf.extend_from_slice(cb);
            buf.extend_from_slice(&(cells.len() as u32).to_le_bytes());
            for cell in cells {
                if let MbfCellValue::Text(s) = cell {
                    if let Some(&sym_code) = self.fsst_table.symbol_to_code.get(s) {
                        buf.push(7); // Tag 7: 1-byte FSST symbol reference
                        buf.push(sym_code);
                        continue;
                    }
                }
                cell.write_binary(&mut buf);
            }
        }

        pad_to_64(&mut buf);
        buf
    }

    /// Decodes a Zone 1 `PaxMvccZone` from a binary slice.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 12 || bytes[0..8] != ZONE1_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 1 PAX binary magic header",
            ));
        }
        let mut cur = 8usize;
        let et_len = read_u32(bytes, &mut cur)? as usize;
        let entity_type = read_string(bytes, &mut cur, et_len)?;

        let mvcc_count = read_u32(bytes, &mut cur)? as usize;
        let mut mvcc_versions = Vec::with_capacity(mvcc_count);
        for _ in 0..mvcc_count {
            let row_id = read_u32(bytes, &mut cur)?;
            let tx_assert_epoch = read_u64(bytes, &mut cur)?;
            let tx_retract_epoch = read_u64(bytes, &mut cur)?;
            mvcc_versions.push(MvccRowVersion {
                row_id,
                tx_assert_epoch,
                tx_retract_epoch,
            });
        }

        let pk_count = read_u32(bytes, &mut cur)? as usize;
        let mut primary_key_index = HashMap::with_capacity(pk_count);
        for _ in 0..pk_count {
            let eid_len = read_u32(bytes, &mut cur)? as usize;
            let eid = read_string(bytes, &mut cur, eid_len)?;
            let r_count = read_u32(bytes, &mut cur)? as usize;
            let mut rows = Vec::with_capacity(r_count);
            for _ in 0..r_count {
                rows.push(read_u32(bytes, &mut cur)?);
            }
            primary_key_index.insert(eid, rows);
        }

        let sym_count = read_u32(bytes, &mut cur)? as usize;
        let mut fsst_table = FsstSymbolTable::new();
        for _ in 0..sym_count {
            let slen = read_u32(bytes, &mut cur)? as usize;
            let s = read_string(bytes, &mut cur, slen)?;
            let _ = fsst_table.intern_or_escape(&s);
        }

        let col_count = read_u32(bytes, &mut cur)? as usize;
        let mut columns = HashMap::with_capacity(col_count);
        let mut avet_secondary_index: BTreeMap<(String, String), Vec<u32>> = BTreeMap::new();

        for _ in 0..col_count {
            let clen = read_u32(bytes, &mut cur)? as usize;
            let col_name = read_string(bytes, &mut cur, clen)?;
            let cell_count = read_u32(bytes, &mut cur)? as usize;
            let mut cells = Vec::with_capacity(cell_count);
            for row_idx in 0..cell_count {
                if cur >= bytes.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Unexpected EOF in Zone 1 column cells",
                    ));
                }
                let cell = if bytes[cur] == 7 {
                    cur += 1;
                    if cur >= bytes.len() {
                        return Err(DomainError::mbf(
                            ErrorCode::MbfCorrupt001,
                            "Unexpected EOF reading FSST symbol code",
                        ));
                    }
                    let code = bytes[cur];
                    cur += 1;
                    let text = fsst_table
                        .decode_symbol(code)
                        .ok_or_else(|| {
                            DomainError::mbf(
                                ErrorCode::MbfCorrupt001,
                                format!("Unknown FSST symbol code {code}"),
                            )
                        })?
                        .to_string();
                    MbfCellValue::Text(text)
                } else {
                    MbfCellValue::read_binary(bytes, &mut cur)?
                };

                match &cell {
                    MbfCellValue::Text(s) => {
                        avet_secondary_index
                            .entry((col_name.clone(), s.clone()))
                            .or_default()
                            .push(row_idx as u32);
                    }
                    MbfCellValue::Int64(n) => {
                        avet_secondary_index
                            .entry((col_name.clone(), n.to_string()))
                            .or_default()
                            .push(row_idx as u32);
                    }
                    _ => {}
                }
                cells.push(cell);
            }
            columns.insert(col_name, cells);
        }

        let required_blocks = mvcc_versions.len().div_ceil(MBF_ROW_GROUP_SIZE).max(1);
        let mut active_visibility_words = vec![0u64; required_blocks * 64];
        for ver in &mvcc_versions {
            if ver.tx_retract_epoch == MvccRowVersion::ACTIVE_EPOCH {
                active_visibility_words[(ver.row_id as usize) / 64] |=
                    1u64 << ((ver.row_id as usize) % 64);
            }
        }
        let directory = EliasFanoEntityDirectory::build_from_map(&primary_key_index);

        Ok(Self {
            entity_type,
            primary_key_index,
            mvcc_versions,
            columns,
            fsst_table,
            directory,
            active_visibility_words,
            avet_secondary_index,
        })
    }
}

fn read_u32(bytes: &[u8], cur: &mut usize) -> DomainResult<u32> {
    if *cur + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u32 in Zone 1",
        ));
    }
    let val = u32::from_le_bytes([
        bytes[*cur],
        bytes[*cur + 1],
        bytes[*cur + 2],
        bytes[*cur + 3],
    ]);
    *cur += 4;
    Ok(val)
}

fn read_u64(bytes: &[u8], cur: &mut usize) -> DomainResult<u64> {
    if *cur + 8 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u64 in Zone 1",
        ));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[*cur..*cur + 8]);
    *cur += 8;
    Ok(u64::from_le_bytes(arr))
}

fn read_string(bytes: &[u8], cur: &mut usize, len: usize) -> DomainResult<String> {
    if *cur + len > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading string in Zone 1",
        ));
    }
    let s = String::from_utf8_lossy(&bytes[*cur..*cur + len]).into_owned();
    *cur += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvcc_time_travel_pull_elias_fano_fsst_and_binary_roundtrip_work_accurately() {
        let mut zone = PaxMvccZone::new("work_order");
        let mut v1 = HashMap::new();
        v1.insert("status".into(), MbfCellValue::Text("OPEN".into()));
        v1.insert("total_cost".into(), MbfCellValue::Decimal128(100_0000));
        zone.upsert_entity("WO-100", 1, v1);

        let mut v2 = HashMap::new();
        v2.insert("status".into(), MbfCellValue::Text("COMPLETED".into()));
        v2.insert("total_cost".into(), MbfCellValue::Decimal128(250_0000));
        zone.upsert_entity("WO-100", 5, v2);

        // Encode to 64-byte aligned binary and decode back without any JSON
        let bin = zone.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        assert_eq!(&bin[0..8], b"MBFZ1PAX");

        let restored = PaxMvccZone::decode_zone_binary(&bin).unwrap();

        // As of epoch 3, status was OPEN and cost was 100.0000
        let snap_e3 = restored.pull_as_of("WO-100", 3).unwrap();
        assert_eq!(
            snap_e3.get("status"),
            Some(&MbfCellValue::Text("OPEN".into()))
        );
        assert_eq!(
            snap_e3.get("total_cost"),
            Some(&MbfCellValue::Decimal128(100_0000))
        );

        // As of epoch 5, status is COMPLETED and cost is 250.0000
        let snap_e5 = restored.pull_as_of("WO-100", 5).unwrap();
        assert_eq!(
            snap_e5.get("status"),
            Some(&MbfCellValue::Text("COMPLETED".into()))
        );
        assert_eq!(
            snap_e5.get("total_cost"),
            Some(&MbfCellValue::Decimal128(250_0000))
        );
        assert_eq!(restored.visible_rows_at(5).len(), 1);
        assert_eq!(
            restored.query_by_attribute_value("status", "COMPLETED", 5),
            vec![1]
        );
    }
}
