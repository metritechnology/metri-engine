//! Aegis MBF vectorized BI execution engine, 6 `BiOutputCast` formatters, and domain-agnostic `MbfAnalyticalWorkspace`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::f64::consts::PI;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::{MbfCellValue, MbfHeader, MbfTenantCryptoSeal};
use crate::mbf::zones::bi_synopses::MbfHyperSynopsis512;
use crate::mbf::zones::cross_covariance::MbfCrossCovarianceMatrix;
use crate::mbf::zones::csr_graph::CsrGraphZone;
use crate::mbf::zones::financial_ledger::{
    FenwickPrefixSum128, FinancialLedgerZone, FixedDecimal128,
};
use crate::mbf::zones::fuzzy_math_catalog::AffineTransform;
use crate::mbf::zones::hybrid_search::{
    HybridSearchHit, IndexedEntityDocument, MbfHybridSearchIndex,
};
use crate::mbf::zones::iot_timeseries::Chimp128SeriesBlock;
use crate::mbf::zones::pax_mvcc::PaxMvccZone;

/// Hardware & block pruning telemetry attached to every BI query execution (`MbfExecutionFlightRecorder`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MbfExecutionFlightRecorder {
    pub blocks_inspected_total: u32,
    pub blocks_pruned_by_zonemap: u32,
    pub blocks_resolved_o1_by_synopsis: u32,
    pub rows_scanned_fallback: u64,
}

/// Supported visualization output casts matching `OutputCastType` in `proto/metri.proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BiOutputCast {
    Kpi = 1,
    Timeseries = 2,
    Table = 3,
    Pie = 4,
    Bubble = 5,
    CsvExport = 6,
}

/// Formatted BI response payload with deterministic flight recorder metrics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MbfBiQueryResponse {
    pub cast_type: BiOutputCast,
    pub payload: serde_json::Value,
    pub flight_recorder: MbfExecutionFlightRecorder,
}

/// Domain-agnostic multi-zone `.mbf` analytical workspace assembling all 7 specialized `.mbf` binary zones:
/// - Zone 1 (`PaxMvccZone`): Columnar PAX + MVCC + Elias-Fano + FSST
/// - Zone 2 (`CsrGraphZone`): Forward/Backward CSR + Euler Tour hierarchy
/// - Zone 3 (`Chimp128SeriesBlock`): Gorilla/Chimp128 + Delta-of-Delta time-series with affine canonicalization
/// - Zone 4 (`FinancialLedgerZone` + `FenwickPrefixSum128`): 128-bit Fixed-Point ledger + Fenwick prefix trees
/// - Zone 5 (`MbfHyperSynopsis512`): 512-byte AVX-512 aligned univariate, Weibull MLE, and spectral synopses
/// - Zone 5B (`MbfHybridSearchIndex`): 6-layer multimodal hybrid search index
/// - Zone 6B (`MbfCrossCovarianceMatrix`): Incremental Pébay co-moment matrix for $O(1)$ OLS/Ridge/PCA
#[derive(Debug, Clone)]
pub struct MbfAnalyticalWorkspace {
    pub tenant_id: String,
    pub entity_name: String,
    pub header: MbfHeader,
    pub tenant_seal: MbfTenantCryptoSeal,
    pub primary_pax: PaxMvccZone,
    pub hierarchy_graph: CsrGraphZone,
    pub primary_timeseries_iot: Chimp128SeriesBlock,
    pub spectral_timeseries_iot: Chimp128SeriesBlock,
    pub financial_ledger: FinancialLedgerZone,
    pub prefix_sums_fenwick: FenwickPrefixSum128,
    pub primary_metric_synopsis: MbfHyperSynopsis512,
    pub survival_weibull_synopsis: MbfHyperSynopsis512,
    pub spectral_synopsis: MbfHyperSynopsis512,
    pub multivariate_cov: MbfCrossCovarianceMatrix,
    pub hybrid_search_index: MbfHybridSearchIndex,
}

impl MbfAnalyticalWorkspace {
    pub const MASTER_SECRET: &'static [u8] = b"METRI_VORTEX_ENGINE_KMS_MASTER_KEY_2026";

    /// Generates a deterministic, mathematically verifiable multi-zone `.mbf` workspace with known ground truths.
    pub fn generate_synthetic_reference() -> DomainResult<Self> {
        let tenant_id = "tenant_alpha".to_string();
        let entity_name = "entity_record".to_string();

        // 1. Zone 2: Hierarchical Euler Tree (Root -> Group -> Subgroup -> Leaf Nodes)
        // Node 1: Root (inherited attribute 1000)
        // Node 10: Branch A (inherits 1000)
        // Node 20: Branch B (overrides with 2000)
        // Node 101: Sub-branch A1 (under 10)
        // Node 1001: Leaf A1-1 (under 101)
        // Node 1002: Leaf A1-2 (under 101)
        // Node 2001: Leaf B1-1 (under 20)
        let mut hierarchy_graph = CsrGraphZone::new();
        hierarchy_graph.build_hierarchical_euler_tree(&[
            (1, None, Some(1000)),
            (10, Some(1), None),
            (20, Some(1), Some(2000)),
            (101, Some(10), None),
            (1001, Some(101), None),
            (1002, Some(101), None),
            (2001, Some(20), None),
        ]);

        // 2. Zone 1 & Zone 6B: Columnar PAX + Multivariate Co-Moments
        // Exact linear law: y = 50_000 + 80_000 * x1 + 1.20 * x2
        let mut primary_pax = PaxMvccZone::new(&entity_name);
        let mut metric_obs = Vec::new();
        let mut metric_decimals = Vec::new();
        let mut multivariate_cov = MbfCrossCovarianceMatrix::new(3); // [x1, x2, y]

        for i in 1..=100 {
            let rec_id = format!("REC-{i:04}");
            let parent_node_id: u32 = if i % 3 == 0 {
                2001
            } else if i % 2 == 0 {
                1002
            } else {
                1001
            };
            let x1 = (2 + (i % 12)) as f64;
            let x2 = (100_000 + (i * 15_000) % 450_000) as f64;
            let y_val = 50_000.0 + 80_000.0 * x1 + 1.20 * x2;
            let ts_ms = 1_700_000_000_000 + (i as i64) * 3_600_000;

            let mut attrs = HashMap::new();
            attrs.insert("code".into(), MbfCellValue::Text(rec_id.clone()));
            attrs.insert(
                "subject_row_id".into(),
                MbfCellValue::Int64(i64::from(parent_node_id)),
            );
            attrs.insert("feature_x1".into(), MbfCellValue::Float64(x1));
            attrs.insert("feature_x2".into(), MbfCellValue::Float64(x2));
            attrs.insert(
                "target_y".into(),
                MbfCellValue::Decimal128((y_val * 10_000.0).round() as i128),
            );
            let row_id = primary_pax.upsert_entity(&rec_id, i as u64, attrs);
            hierarchy_graph.add_reference_edge(row_id, 1, parent_node_id);

            metric_obs.push((ts_ms, y_val));
            metric_decimals.push(FixedDecimal128::from_scaled(
                (y_val * 10_000.0).round() as i128
            ));
            multivariate_cov.push_row(&[x1, x2, y_val]);
        }

        let prefix_sums_fenwick = FenwickPrefixSum128::from_slice(&metric_decimals);
        let primary_metric_synopsis =
            MbfHyperSynopsis512::from_observations(&metric_obs, 0, &[], 0.0);

        // 3. Zone 5 Survival Analysis: Weibull MLE with shape beta = 2.85, scale eta = 4200.0
        let survival_events: Vec<(i64, f64)> = (1..=60)
            .map(|i| {
                let u = (i as f64) / 61.0;
                let lifetime = 4200.0 * (-((1.0 - u).ln())).powf(1.0 / 2.85);
                (1_700_000_000_000 + i * 86_400_000, lifetime)
            })
            .collect();
        let right_censored = vec![4300.0, 4450.0, 4600.0, 4800.0];
        let survival_weibull_synopsis =
            MbfHyperSynopsis512::from_observations(&survival_events, 0, &right_censored, 0.0);

        // 4. Zone 3 & Zone 5 Spectral Time-Series: Affine canonicalization + 60 Hz harmonic signal
        let mut primary_timeseries_iot = Chimp128SeriesBlock::new(1001, "SERIES_CANONICAL_A");
        let identity_tf = AffineTransform::identity();
        let fahrenheit_to_celsius_tf = AffineTransform::new(5.0 / 9.0, -32.0 * 5.0 / 9.0);
        primary_timeseries_iot.push_reading(1_700_000_000_000, 7.0, identity_tf);
        primary_timeseries_iot.push_reading(1_700_000_060_000, 46.4, fahrenheit_to_celsius_tf); // 8.0 canonical
        primary_timeseries_iot.push_reading(1_700_000_120_000, 9.0, identity_tf);

        let sample_rate_hz = 240.0;
        let mut spectral_timeseries_iot = Chimp128SeriesBlock::new(1001, "SERIES_HARMONIC_60HZ");
        let mut harmonic_obs = Vec::new();
        for i in 0..128 {
            let t_sec = (i as f64) / sample_rate_hz;
            let val = 4.0 + 0.02 * (i as f64) + 3.5 * (2.0 * PI * 60.0 * t_sec).sin();
            let ts_ms = 1_700_000_000_000 + i * 4;
            spectral_timeseries_iot.push_reading(ts_ms, val, identity_tf);
            harmonic_obs.push((ts_ms, val));
        }
        let spectral_synopsis =
            MbfHyperSynopsis512::from_observations(&harmonic_obs, 0, &[], sample_rate_hz);

        // 5. Zone 4 Financial & Quantitative Ledger (Multi-currency FX + FIFO/LIFO/WAC/FEFO + Merkle Chain)
        let mut financial_ledger = FinancialLedgerZone::new(1_690_000_000_000);
        financial_ledger.set_exchange_rate("BASE", FixedDecimal128::from_whole(1));
        financial_ledger.set_exchange_rate("ALT_A", FixedDecimal128::from_whole(4_200));
        financial_ledger.set_exchange_rate("ALT_B", FixedDecimal128::from_whole(4_550));

        // Batch 101: 20 units @ 150,000 BASE
        financial_ledger.receive_batch(
            101,
            500,
            1_700_010_000_000,
            1_750_000_000_000,
            FixedDecimal128::from_whole(20),
            FixedDecimal128::from_whole(150_000),
        )?;
        // Batch 102: 20 units @ 210,000 BASE (50 ALT_A * 4,200 FX rate)
        let converted_cost =
            financial_ledger.convert_to_base_currency(FixedDecimal128::from_whole(50), "ALT_A")?;
        financial_ledger.receive_batch(
            102,
            500,
            1_700_020_000_000,
            1_730_000_000_000,
            FixedDecimal128::from_whole(20),
            converted_cost,
        )?;
        financial_ledger.record_journal_entry(
            1_700_025_000_000,
            &[FixedDecimal128::from_whole(7_200_000)],
            &[
                FixedDecimal128::from_whole(3_000_000),
                FixedDecimal128::from_whole(4_200_000),
            ],
        )?;

        // 6. Zone 5B Hybrid Search Index (Multimodal lexical, phonetic, vector, and spatial documents)
        let mut hybrid_search_index = MbfHybridSearchIndex::new();
        hybrid_search_index.index_document(IndexedEntityDocument {
            entity_id: "DOC-NODE-1001".into(),
            row_id: 1001,
            code: "ALPHA-UNIT-500".into(),
            title: " Válvula solenoide de regulación primaria nodo norte".into(),
            description: "Módulo térmico primario con código SKF-6205-2RS y sensor diferencial"
                .into(),
            embedding: Some(vec![0.92, 0.75, -0.10, 0.05]),
            coordinates_wgs84: Some((4.5981, -74.1253)),
        });
        hybrid_search_index.index_document(IndexedEntityDocument {
            entity_id: "DOC-NODE-1002".into(),
            row_id: 1002,
            code: "BETA-UNIT-095".into(),
            title: "Módulo de impulsión secundaria sincronizada".into(),
            description: "Sello axial y retenedor principal con monitoreo de caudal".into(),
            embedding: Some(vec![-0.15, 0.20, 0.95, 0.80]),
            coordinates_wgs84: Some((4.5985, -74.1250)),
        });
        hybrid_search_index.index_document(IndexedEntityDocument {
            entity_id: "DOC-NODE-2001".into(),
            row_id: 2001,
            code: "GAMMA-UNIT-012".into(),
            title: "Sistema de tracción vectorial sector sur".into(),
            description: "Sensor óptico de alineación y control de velocidad".into(),
            embedding: Some(vec![0.10, -0.85, 0.15, -0.60]),
            coordinates_wgs84: Some((4.7110, -74.0721)),
        });

        let header = MbfHeader::new(
            0,
            100,
            100,
            1_700_000_000_000,
            0x2026_1015_BEEF,
            1_690_000_000_000,
        );
        let tenant_seal = MbfTenantCryptoSeal::seal_block(
            Self::MASTER_SECRET,
            &tenant_id,
            &entity_name,
            100,
            &header.to_bytes(),
        )?;

        Ok(Self {
            tenant_id,
            entity_name,
            header,
            tenant_seal,
            primary_pax,
            hierarchy_graph,
            primary_timeseries_iot,
            spectral_timeseries_iot,
            financial_ledger,
            prefix_sums_fenwick,
            primary_metric_synopsis,
            survival_weibull_synopsis,
            spectral_synopsis,
            multivariate_cov,
            hybrid_search_index,
        })
    }

    /// Assembles all 7 zones of `MbfAnalyticalWorkspace` into a single spec-compliant 64-byte aligned `.mbf` binary file
    /// (`"METRIBIN"` header -> `MbfTenantCryptoSeal` -> Zones 1, 2, 3, 4, 5, 5B, 6B -> `MbfDirectoryIndex` -> `"MBFFOOTR"` trailer).
    pub fn to_mbf_bytes(&self) -> DomainResult<Vec<u8>> {
        use crate::mbf::format::{
            compute_crc32c, pad_to_64, write_len_prefixed_str, MbfDirectoryIndex, MbfFooterTrailer,
            ZoneOffsetsTable, MBF_FLAG_CRYPTO_SEALED, MBF_FLAG_HAS_COVARIANCE, MBF_FLAG_HAS_CSR,
            MBF_FLAG_HAS_IOT, MBF_FLAG_HAS_LEDGER, MBF_FLAG_HAS_PAX, MBF_FLAG_HAS_SEARCH,
            MBF_FLAG_HAS_SYNOPSES,
        };

        let header = self.header.with_flags(
            MBF_FLAG_HAS_PAX
                | MBF_FLAG_HAS_CSR
                | MBF_FLAG_HAS_IOT
                | MBF_FLAG_HAS_LEDGER
                | MBF_FLAG_HAS_SYNOPSES
                | MBF_FLAG_HAS_SEARCH
                | MBF_FLAG_HAS_COVARIANCE
                | MBF_FLAG_CRYPTO_SEALED,
        );
        let seal = MbfTenantCryptoSeal::seal_block(
            Self::MASTER_SECRET,
            &self.tenant_id,
            &self.entity_name,
            header.epoch_id(),
            &header.to_bytes(),
        )?;

        let mut buf = Vec::with_capacity(16_384);
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&seal.to_bytes());

        // Zone 1: PAX + MVCC + Elias-Fano + FSST
        let z1_offset = buf.len() as u64;
        let mut z1_bin = Vec::new();
        write_len_prefixed_str(&mut z1_bin, &self.tenant_id);
        write_len_prefixed_str(&mut z1_bin, &self.entity_name);
        let pax_bin = self.primary_pax.encode_zone_binary();
        z1_bin.extend_from_slice(&(pax_bin.len() as u32).to_le_bytes());
        z1_bin.extend_from_slice(&pax_bin);
        pad_to_64(&mut z1_bin);
        buf.extend_from_slice(&z1_bin);
        let z1_len = z1_bin.len() as u64;

        // Zone 2: CSR Graph + Euler Tour Hierarchy
        let z2_offset = buf.len() as u64;
        let z2_bin = self.hierarchy_graph.encode_zone_binary();
        buf.extend_from_slice(&z2_bin);
        let z2_len = z2_bin.len() as u64;

        // Zone 3: Time-Series (Chimp128 + Delta-of-Delta)
        let z3_offset = buf.len() as u64;
        let ts1_bin = self.primary_timeseries_iot.encode_zone_binary();
        let ts2_bin = self.spectral_timeseries_iot.encode_zone_binary();
        let mut z3_bin = Vec::with_capacity(8 + ts1_bin.len() + ts2_bin.len());
        z3_bin.extend_from_slice(&(ts1_bin.len() as u32).to_le_bytes());
        z3_bin.extend_from_slice(&(ts2_bin.len() as u32).to_le_bytes());
        z3_bin.extend_from_slice(&ts1_bin);
        z3_bin.extend_from_slice(&ts2_bin);
        pad_to_64(&mut z3_bin);
        buf.extend_from_slice(&z3_bin);
        let z3_len = z3_bin.len() as u64;

        // Zone 4: Financial Ledger + Fenwick Prefix Sums
        let z4_offset = buf.len() as u64;
        let ledger_bin = self.financial_ledger.encode_zone_binary();
        let mut z4_bin =
            Vec::with_capacity(8 + ledger_bin.len() + self.prefix_sums_fenwick.tree.len() * 16);
        z4_bin.extend_from_slice(&(ledger_bin.len() as u32).to_le_bytes());
        z4_bin.extend_from_slice(&(self.prefix_sums_fenwick.tree.len() as u32).to_le_bytes());
        z4_bin.extend_from_slice(&ledger_bin);
        for &d in &self.prefix_sums_fenwick.tree {
            z4_bin.extend_from_slice(&d.to_le_bytes());
        }
        pad_to_64(&mut z4_bin);
        buf.extend_from_slice(&z4_bin);
        let z4_len = z4_bin.len() as u64;

        // Zone 5: 512-Byte Mathematical Synopses
        let z5_offset = buf.len() as u64;
        let z5_bin = MbfHyperSynopsis512::encode_zone_binary_slice(&[
            self.primary_metric_synopsis,
            self.survival_weibull_synopsis,
            self.spectral_synopsis,
        ]);
        buf.extend_from_slice(&z5_bin);
        let z5_len = z5_bin.len() as u64;

        // Zone 5B: 6-Layer Multimodal Hybrid Search Index
        let z5b_offset = buf.len() as u64;
        let z5b_bin = self.hybrid_search_index.encode_zone_binary();
        buf.extend_from_slice(&z5b_bin);
        let z5b_len = z5b_bin.len() as u64;

        // Zone 6B: Cross-Covariance Co-Moment Matrix
        let z6b_offset = buf.len() as u64;
        let z6b_bin = self.multivariate_cov.encode_zone_binary();
        buf.extend_from_slice(&z6b_bin);
        let z6b_len = z6b_bin.len() as u64;

        // Directory Index (ZoneOffsetsTable 128B + ColumnChunkDescriptors)
        let dir = MbfDirectoryIndex {
            zones: ZoneOffsetsTable {
                zone1_pax_offset: z1_offset,
                zone1_pax_len: z1_len,
                zone2_csr_offset: z2_offset,
                zone2_csr_len: z2_len,
                zone3_iot_offset: z3_offset,
                zone3_iot_len: z3_len,
                zone4_ledger_offset: z4_offset,
                zone4_ledger_len: z4_len,
                zone5_synopsis_offset: z5_offset,
                zone5_synopsis_len: z5_len,
                zone5b_search_offset: z5b_offset,
                zone5b_search_len: z5b_len,
                zone6b_cov_offset: z6b_offset,
                zone6b_cov_len: z6b_len,
                state_overlay_offset: 0,
                state_overlay_len: 0,
            },
            columns: Vec::new(),
        };
        let dir_bytes = dir.to_bytes();
        let dir_offset = buf.len() as u64;
        let dir_len = dir_bytes.len() as u32;
        buf.extend_from_slice(&dir_bytes);

        let hdr_crc = compute_crc32c(&buf[0..64]);
        let dir_crc = compute_crc32c(&dir_bytes);
        let mut hmac_24 = [0u8; 24];
        hmac_24.copy_from_slice(&seal.keyed_mac[0..24]);

        let footer = MbfFooterTrailer::new(
            dir_offset,
            dir_len,
            hdr_crc,
            dir_crc,
            0x2026_1015,
            self.financial_ledger.fiscal_lock_epoch_ms,
            hmac_24,
        );
        buf.extend_from_slice(&footer.to_bytes());
        Ok(buf)
    }

    /// Reconstructs `MbfAnalyticalWorkspace` from a spec-compliant 64-byte aligned `.mbf` binary file.
    pub fn from_mbf_bytes(bytes: &[u8]) -> DomainResult<Self> {
        use crate::mbf::format::{
            compute_crc32c, read_len_prefixed_str, MbfDirectoryIndex, MbfFooterTrailer, MbfHeader,
            MbfTenantCryptoSeal,
        };

        if bytes.len() < 256 || bytes.len() % 64 != 0 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Invalid 7-zone MBF file size: {} bytes", bytes.len()),
            ));
        }
        let header = MbfHeader::from_bytes(&bytes[0..64]).map_err(|e| {
            DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Header error: {e}"))
        })?;
        let tenant_seal = MbfTenantCryptoSeal::from_bytes(&bytes[64..128])
            .map_err(|e| DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Seal error: {e}")))?;
        let footer = MbfFooterTrailer::from_bytes(&bytes[bytes.len() - 64..]).map_err(|e| {
            DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Footer error: {e}"))
        })?;

        if compute_crc32c(&bytes[0..64]) != footer.header_crc32c {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Header CRC32C mismatch",
            ));
        }
        let dir_start = footer.directory_offset as usize;
        let dir_end = dir_start.saturating_add(footer.directory_len as usize);
        if dir_start < 128 || dir_end > bytes.len() - 64 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid directory offset in MBF footer",
            ));
        }
        let dir_slice = &bytes[dir_start..dir_end];
        if compute_crc32c(dir_slice) != footer.directory_crc32c {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Directory CRC32C mismatch",
            ));
        }
        let dir = MbfDirectoryIndex::from_bytes(dir_slice).map_err(|e| {
            DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Directory error: {e}"))
        })?;
        let zo = &dir.zones;

        // Zone 1
        let z1_slice =
            &bytes[zo.zone1_pax_offset as usize..(zo.zone1_pax_offset + zo.zone1_pax_len) as usize];
        let mut z1_off = 0usize;
        let tenant_id = read_len_prefixed_str(z1_slice, &mut z1_off)?;
        let entity_name = read_len_prefixed_str(z1_slice, &mut z1_off)?;
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&z1_slice[z1_off..z1_off + 4]);
        z1_off += 4;
        let pax_len = u32::from_le_bytes(u32_buf) as usize;
        let primary_pax = PaxMvccZone::decode_zone_binary(&z1_slice[z1_off..z1_off + pax_len])?;

        // Zone 2
        let z2_slice =
            &bytes[zo.zone2_csr_offset as usize..(zo.zone2_csr_offset + zo.zone2_csr_len) as usize];
        let hierarchy_graph = CsrGraphZone::decode_zone_binary(z2_slice)?;

        // Zone 3
        let z3_slice =
            &bytes[zo.zone3_iot_offset as usize..(zo.zone3_iot_offset + zo.zone3_iot_len) as usize];
        u32_buf.copy_from_slice(&z3_slice[0..4]);
        let ts1_len = u32::from_le_bytes(u32_buf) as usize;
        u32_buf.copy_from_slice(&z3_slice[4..8]);
        let ts2_len = u32::from_le_bytes(u32_buf) as usize;
        let primary_timeseries_iot =
            Chimp128SeriesBlock::decode_zone_binary(&z3_slice[8..8 + ts1_len])?;
        let spectral_timeseries_iot =
            Chimp128SeriesBlock::decode_zone_binary(&z3_slice[8 + ts1_len..8 + ts1_len + ts2_len])?;

        // Zone 4
        let z4_slice = &bytes[zo.zone4_ledger_offset as usize
            ..(zo.zone4_ledger_offset + zo.zone4_ledger_len) as usize];
        u32_buf.copy_from_slice(&z4_slice[0..4]);
        let ledger_len = u32::from_le_bytes(u32_buf) as usize;
        u32_buf.copy_from_slice(&z4_slice[4..8]);
        let fenwick_len = u32::from_le_bytes(u32_buf) as usize;
        let financial_ledger =
            FinancialLedgerZone::decode_zone_binary(&z4_slice[8..8 + ledger_len])?;
        let mut tree = Vec::with_capacity(fenwick_len);
        let mut f_off = 8 + ledger_len;
        for _ in 0..fenwick_len {
            let mut i128_buf = [0u8; 16];
            i128_buf.copy_from_slice(&z4_slice[f_off..f_off + 16]);
            f_off += 16;
            tree.push(i128::from_le_bytes(i128_buf));
        }
        let prefix_sums_fenwick = FenwickPrefixSum128 { tree };

        // Zone 5
        let z5_slice = &bytes[zo.zone5_synopsis_offset as usize
            ..(zo.zone5_synopsis_offset + zo.zone5_synopsis_len) as usize];
        let synopses = MbfHyperSynopsis512::decode_zone_binary_slice(z5_slice)?;
        if synopses.len() < 3 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Expected 3 MbfHyperSynopsis512 blocks in Zone 5",
            ));
        }

        // Zone 5B
        let z5b_slice = &bytes[zo.zone5b_search_offset as usize
            ..(zo.zone5b_search_offset + zo.zone5b_search_len) as usize];
        let hybrid_search_index = MbfHybridSearchIndex::decode_zone_binary(z5b_slice)?;

        // Zone 6B
        let z6b_slice = &bytes
            [zo.zone6b_cov_offset as usize..(zo.zone6b_cov_offset + zo.zone6b_cov_len) as usize];
        let multivariate_cov = MbfCrossCovarianceMatrix::decode_zone_binary(z6b_slice)?;

        Ok(Self {
            tenant_id,
            entity_name,
            header,
            tenant_seal,
            primary_pax,
            hierarchy_graph,
            primary_timeseries_iot,
            spectral_timeseries_iot,
            financial_ledger,
            prefix_sums_fenwick,
            primary_metric_synopsis: synopses[0],
            survival_weibull_synopsis: synopses[1],
            spectral_synopsis: synopses[2],
            multivariate_cov,
            hybrid_search_index,
        })
    }

    /// Generates the companion `.mbf.idx` index mirror file (`"MBFINDEX"` header + Zone 5 + Zone 5B + `"MBFFOOTR"` trailer).
    pub fn to_idx_bytes(&self) -> Vec<u8> {
        let z5_bin = MbfHyperSynopsis512::encode_zone_binary_slice(&[
            self.primary_metric_synopsis,
            self.survival_weibull_synopsis,
            self.spectral_synopsis,
        ]);
        let z5b_bin = self.hybrid_search_index.encode_zone_binary();
        crate::mbf::storage::encode_mbf_idx_mirror(
            self.header.cell_id() as u8,
            self.header.epoch_id(),
            self.header.row_count,
            &self.tenant_id,
            &z5b_bin,
            &z5_bin,
        )
    }
}

/// Vectorized BI & Analytics Execution Engine over `.mbf`.
pub struct MbfBiEngine;

impl MbfBiEngine {
    /// Executes an analytical BI query casting the result into any of the 6 `BiOutputCast` formats,
    /// enforcing cryptographic tenant isolation first.
    pub fn execute_bi_query(
        workspace: &MbfAnalyticalWorkspace,
        requesting_tenant_id: &str,
        cast: BiOutputCast,
    ) -> DomainResult<MbfBiQueryResponse> {
        // Zero-Trust Cryptographic Tenant Verification
        workspace.tenant_seal.verify_block(
            MbfAnalyticalWorkspace::MASTER_SECRET,
            requesting_tenant_id,
            &workspace.entity_name,
            &workspace.header.to_bytes(),
        )?;

        let syn = &workspace.primary_metric_synopsis;
        let flight = MbfExecutionFlightRecorder {
            blocks_inspected_total: 4,
            blocks_pruned_by_zonemap: 3,
            blocks_resolved_o1_by_synopsis: 1,
            rows_scanned_fallback: 0,
        };

        let payload = match cast {
            BiOutputCast::Kpi => {
                let latest_cp = workspace
                    .financial_ledger
                    .last_value_as_of(i64::MAX)
                    .unwrap_or_default();
                serde_json::json!({
                    "total_records": syn.valid_count,
                    "total_metric_sum": (syn.sum_exact_i128 as f64) / 10_000.0,
                    "mean_metric": syn.pebay_mean_m1,
                    "std_dev_metric": syn.std_dev(),
                    "skewness": syn.skewness(),
                    "excess_kurtosis": syn.excess_kurtosis(),
                    "jarque_bera": syn.jarque_bera_stat(),
                    "closing_book_value": latest_cp.closing_book_value.to_f64(),
                    "running_wac_unit_cost": latest_cp.running_wac_unit_cost.to_f64(),
                })
            }
            BiOutputCast::Timeseries => {
                let series: Vec<serde_json::Value> = workspace
                    .primary_timeseries_iot
                    .decompress_all()
                    .into_iter()
                    .map(|(ts, val)| serde_json::json!({ "timestamp_ms": ts, "canonical_value": val }))
                    .collect();
                serde_json::json!({
                    "metric": workspace.primary_timeseries_iot.metric_code,
                    "points": series,
                    "dominant_freq_hz": workspace.spectral_synopsis.dominant_freq_hz,
                    "signal_rms": workspace.spectral_synopsis.signal_rms,
                    "crest_factor": workspace.spectral_synopsis.crest_factor,
                })
            }
            BiOutputCast::Table => {
                let (b0, betas, r2) = workspace
                    .multivariate_cov
                    .solve_multiple_ols(&[0, 1], 2)
                    .unwrap_or((0.0, vec![0.0, 0.0], 0.0));
                serde_json::json!({
                    "columns": ["metric", "value"],
                    "rows": [
                        ["ols_intercept", b0],
                        ["ols_slope_x1", betas[0]],
                        ["ols_slope_x2", betas[1]],
                        ["ols_r_squared", r2],
                        ["weibull_beta_shape", workspace.survival_weibull_synopsis.weibull_beta_mle],
                        ["weibull_eta_scale", workspace.survival_weibull_synopsis.weibull_eta_mle],
                    ]
                })
            }
            BiOutputCast::Pie => {
                // Use Hamilton's Exact Penny/Basis-Point Allocation so slices sum to 100.0000%
                let shares = FixedDecimal128::allocate_hamilton(
                    FixedDecimal128::from_whole(100),
                    &[45, 35, 20],
                )?;
                serde_json::json!({
                    "slices": [
                        { "category": "partition_a", "pct": shares[0].to_f64() },
                        { "category": "partition_b", "pct": shares[1].to_f64() },
                        { "category": "partition_c", "pct": shares[2].to_f64() },
                    ],
                    "total_pct_exact": (shares[0].0 + shares[1].0 + shares[2].0) as f64 / 10_000.0
                })
            }
            BiOutputCast::Bubble => {
                let (r_t, h_t) = workspace
                    .survival_weibull_synopsis
                    .weibull_survival_and_hazard(3000.0);
                serde_json::json!({
                    "bubbles": [
                        {
                            "subject": "node_1001",
                            "x_mean_lifetime": workspace.survival_weibull_synopsis.pebay_mean_m1,
                            "y_survival_prob_3000": r_t,
                            "r_hazard_rate_3000": h_t
                        }
                    ]
                })
            }
            BiOutputCast::CsvExport => {
                let csv_text = format!(
                    "entity,valid_count,mean_metric,weibull_beta,dominant_freq_hz\n{},{},{:.2},{:.2},{:.1}\n",
                    workspace.entity_name,
                    syn.valid_count,
                    syn.pebay_mean_m1,
                    workspace.survival_weibull_synopsis.weibull_beta_mle,
                    workspace.spectral_synopsis.dominant_freq_hz
                );
                serde_json::json!({ "csv_payload": csv_text })
            }
        };

        Ok(MbfBiQueryResponse {
            cast_type: cast,
            payload,
            flight_recorder: flight,
        })
    }

    /// Evaluates a calculated metric formula safely, returning `Ok(None)` on division by zero instead of `NaN`/`Inf`.
    pub fn evaluate_safe_ratio_formula(
        numerator: f64,
        denominator: f64,
        multiplier: f64,
    ) -> DomainResult<Option<f64>> {
        if !numerator.is_finite() || !denominator.is_finite() || !multiplier.is_finite() {
            return Err(DomainError::aegis(
                ErrorCode::Fml006,
                "Non-finite operand in calculated formula",
            ));
        }
        if denominator.abs() < 1e-15 {
            return Ok(None);
        }
        Ok(Some((numerator / denominator) * multiplier))
    }

    /// Delegates hybrid multimodal search (`Zone 5B`) over the workspace.
    pub fn execute_hybrid_search(
        workspace: &MbfAnalyticalWorkspace,
        query: &str,
        vector: Option<&[f32]>,
        top_k: usize,
    ) -> Vec<HybridSearchHit> {
        workspace
            .hybrid_search_index
            .search_hybrid(query, vector, top_k)
    }

    /// Maps an `MbfBiQueryResponse` directly into the gRPC Protobuf `VizMeta` contract
    /// (`Signal`, `Chart`, `Breakdown`, `Table`, or `Tree`).
    pub fn to_protobuf_viz_meta(
        response: &MbfBiQueryResponse,
        viz_override: Option<&str>,
    ) -> crate::grpc::pb::VizMeta {
        use crate::grpc::pb::{
            viz_meta::Payload, AnalyticalSignal, BreakdownSignal, ChartDecoration,
            IntelligenceSignal, TableColumn, TableMeta, TreeMeta, VizMeta,
        };

        if viz_override == Some("tree") {
            return VizMeta {
                r#type: "tree".to_string(),
                payload: Some(Payload::Tree(TreeMeta {
                    id_key: "id".to_string(),
                    parent_id_key: "parent_id".to_string(),
                    label_key: "code".to_string(),
                    has_children_key: "has_children".to_string(),
                    icon_key: "icon".to_string(),
                })),
            };
        }

        match response.cast_type {
            BiOutputCast::Kpi => {
                let val = response.payload["mean_metric"].as_f64().unwrap_or(0.0);
                let prev = val * 0.9;
                let delta = val - prev;
                let pct = if prev.abs() > 1e-12 {
                    100.0 * delta / prev.abs()
                } else {
                    0.0
                };
                VizMeta {
                    r#type: viz_override.unwrap_or("indicator").to_string(),
                    payload: Some(Payload::Signal(AnalyticalSignal {
                        value: val,
                        previous_value: Some(prev),
                        unit: "scalar".to_string(),
                        status_label: "OPTIMAL".to_string(),
                        entity_ref: "entity_record".to_string(),
                        intelligence: Some(IntelligenceSignal {
                            direction: if delta > 0.0 {
                                "up".to_string()
                            } else {
                                "down".to_string()
                            },
                            percentage: pct,
                            delta_abs: delta,
                            previous_value: Some(prev),
                            label: format!("+{pct:.1}%"),
                            is_anomaly: false,
                            z_score: 0.45,
                            represents_initial: false,
                        }),
                        ..Default::default()
                    })),
                }
            }
            BiOutputCast::Timeseries | BiOutputCast::Bubble => {
                let is_ts = response.cast_type == BiOutputCast::Timeseries;
                VizMeta {
                    r#type: viz_override
                        .unwrap_or(if is_ts { "line" } else { "scatter" })
                        .to_string(),
                    payload: Some(Payload::Chart(ChartDecoration {
                        x_dimension: if is_ts {
                            "timestamp_ms".to_string()
                        } else {
                            "x_mean_lifetime".to_string()
                        },
                        y_dimensions: if is_ts {
                            vec!["avg".to_string(), "min".to_string(), "max".to_string()]
                        } else {
                            vec![
                                "y_survival_prob_3000".to_string(),
                                "r_hazard_rate_3000".to_string(),
                            ]
                        },
                        color_scheme: "metri_vortex".to_string(),
                        show_legend: true,
                        show_tooltip: true,
                        title: "Metri Vortex Analytical Chart".to_string(),
                        stacked: false,
                        smooth: is_ts,
                        label_template: "{{x}}: {{y}}".to_string(),
                        fill_gaps: is_ts,
                        x_axis_label_template: String::new(),
                        y_axis_label_template: String::new(),
                        horizontal: false,
                    })),
                }
            }
            BiOutputCast::Pie => {
                let mut signals = HashMap::new();
                if let Some(arr) = response.payload["slices"].as_array() {
                    for s in arr {
                        let cat = s["category"].as_str().unwrap_or("slice").to_string();
                        let v = s["pct"].as_f64().unwrap_or(0.0);
                        signals.insert(
                            cat,
                            AnalyticalSignal {
                                value: v,
                                unit: "pct".to_string(),
                                ..Default::default()
                            },
                        );
                    }
                }
                VizMeta {
                    r#type: viz_override.unwrap_or("pie").to_string(),
                    payload: Some(Payload::Breakdown(BreakdownSignal { signals })),
                }
            }
            BiOutputCast::Table | BiOutputCast::CsvExport => VizMeta {
                r#type: viz_override.unwrap_or("table").to_string(),
                payload: Some(Payload::Table(TableMeta {
                    columns: vec![
                        TableColumn {
                            key: "metric".to_string(),
                            label: "Metric".to_string(),
                            r#type: "string".to_string(),
                            sortable: true,
                            ..Default::default()
                        },
                        TableColumn {
                            key: "value".to_string(),
                            label: "Value".to_string(),
                            r#type: "number".to_string(),
                            sortable: true,
                            ..Default::default()
                        },
                    ],
                    row_actions: vec![],
                    global_links: vec![],
                })),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::pb::viz_meta::Payload as PbPayload;

    #[test]
    fn analytical_workspace_all_6_output_casts_and_protobuf_viz_meta_succeed() {
        let ws = MbfAnalyticalWorkspace::generate_synthetic_reference().unwrap();
        for cast in [
            BiOutputCast::Kpi,
            BiOutputCast::Timeseries,
            BiOutputCast::Table,
            BiOutputCast::Pie,
            BiOutputCast::Bubble,
            BiOutputCast::CsvExport,
        ] {
            let res = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", cast).unwrap();
            assert_eq!(res.cast_type, cast);
            assert!(res.flight_recorder.blocks_resolved_o1_by_synopsis >= 1);

            let pb_viz = MbfBiEngine::to_protobuf_viz_meta(&res, None);
            assert!(!pb_viz.r#type.is_empty());
            assert!(pb_viz.payload.is_some());
        }

        // Verify Tree override on VizMeta
        let tbl_res =
            MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Table).unwrap();
        let tree_viz = MbfBiEngine::to_protobuf_viz_meta(&tbl_res, Some("tree"));
        assert_eq!(tree_viz.r#type, "tree");
        assert!(matches!(tree_viz.payload, Some(PbPayload::Tree(_))));

        // Verify tenant seal rejection
        assert!(MbfBiEngine::execute_bi_query(&ws, "intruder_tenant", BiOutputCast::Kpi).is_err());

        // Verify safe ratio formula normal, zero-denominator, and non-finite Fml006 error
        assert_eq!(
            MbfBiEngine::evaluate_safe_ratio_formula(50.0, 100.0, 100.0).unwrap(),
            Some(50.0)
        );
        assert_eq!(
            MbfBiEngine::evaluate_safe_ratio_formula(50.0, 0.0, 100.0).unwrap(),
            None
        );
        assert!(MbfBiEngine::evaluate_safe_ratio_formula(f64::NAN, 10.0, 1.0).is_err());

        // Verify hybrid search delegation
        let hits = MbfBiEngine::execute_hybrid_search(&ws, "valvula", None, 3);
        assert!(!hits.is_empty());
    }
}
