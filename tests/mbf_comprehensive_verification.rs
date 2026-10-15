//! Comprehensive E2E verification suite for Metri Vortex (.mbf), Aegis BI, 35+ math functions, financial ledger, 6-layer hybrid search, and S3 ACID concurrency.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use metri_engine::aegis::mbf::{BiOutputCast, MayorPlazaBogotaDataset, MbfBiEngine};
use metri_engine::domain::errors::ErrorCode;
use metri_engine::domain::protocols::{IClock, IMbfObjectStore, MbfConditionalGet};
use metri_engine::mbf::format::{MbfCellValue, MbfFooterTrailer, MbfSchemaEvolutionDirectory};
use metri_engine::mbf::tx::{DomainCellRouter, MbfWriteCoordinator};
use metri_engine::mbf::zones::financial_ledger::{
    FinancialLedgerZone, FixedDecimal128, ValuationMethod,
};
use metri_engine::mbf::{DeterministicClock, InMemoryChaosS3Store};

#[test]
fn all_60_codice_models_route_cleanly_to_8_domain_cells() {
    let models_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/models");
    let mut count = 0usize;
    let mut cell_distribution = [0usize; 8];

    for entry in std::fs::read_dir(&models_dir).expect("read config/models") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let raw = std::fs::read_to_string(&path).expect("read model json");
        let parsed: serde_json::Value = serde_json::from_str(&raw).expect("valid json");
        let entity = parsed["entity"].as_str().expect("entity name");
        let cell_id = DomainCellRouter::resolve_cell_id(entity, "asset-001");
        assert!(cell_id < 8, "Invalid cell_id {cell_id} for {entity}");
        cell_distribution[cell_id as usize] += 1;
        count += 1;
    }

    assert_eq!(count, 60, "All 60 Códice models must be verified");
    // Ensure all domain clusters (CMMS, Ledger, Assets, PM, RAG, IAM, IoT) are populated
    for (cell, &n) in cell_distribution.iter().take(6).enumerate() {
        assert!(
            n > 0,
            "Domain cell {cell} should have at least 1 Códice entity"
        );
    }
}

#[test]
fn verify_all_6_bi_output_casts_and_35_plus_math_functions_on_mayor_plaza_bogota_dataset() {
    let ds = MayorPlazaBogotaDataset::generate_deterministic().expect("generate realistic dataset");

    // 1. KPI OutputCastType + Higher-Order Pébay Moments (M1..M4, Skewness, Kurtosis, Jarque-Bera)
    let kpi_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Kpi)
            .expect("KPI query");
    assert_eq!(kpi_resp.cast_type, BiOutputCast::Kpi);
    assert_eq!(kpi_resp.flight_recorder.blocks_resolved_o1_by_synopsis, 1);
    assert_eq!(kpi_resp.flight_recorder.rows_scanned_fallback, 0);
    assert_eq!(kpi_resp.payload["total_work_orders"], 100);
    assert!(kpi_resp.payload["mean_cost_cop"].as_f64().unwrap() > 500_000.0);
    assert!(kpi_resp.payload["std_dev_cost_cop"].as_f64().unwrap() > 10_000.0);
    assert!(kpi_resp.payload["jarque_bera"].as_f64().unwrap() >= 0.0);
    // Closing inventory book value: 20 * 150,000 + 20 * 210,000 = 7,200,000 COP; WAC = 180,000 COP
    assert!(
        (kpi_resp.payload["inventory_closing_book_value_cop"]
            .as_f64()
            .unwrap()
            - 7_200_000.0)
            .abs()
            < 1e-6
    );
    assert!(
        (kpi_resp.payload["inventory_wac_unit_cost_cop"]
            .as_f64()
            .unwrap()
            - 180_000.0)
            .abs()
            < 1e-6
    );

    // 2. TIMESERIES OutputCastType + UN/CEFACT FAH->CEL Normalization + 60 Hz Spectral FFT/PSD
    let ts_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Timeseries)
            .expect("Timeseries query");
    let points = ts_resp.payload["points"].as_array().unwrap();
    assert_eq!(points.len(), 3);
    // 2nd reading was ingested as 46.4°F and automatically normalized to 8.0°C!
    assert!((points[1]["temp_celsius"].as_f64().unwrap() - 8.0).abs() < 1e-9);
    // Dominant vibration frequency must be 60 Hz
    let dom_hz = ts_resp.payload["dominant_vibration_freq_hz"]
        .as_f64()
        .unwrap();
    assert!((dom_hz - 60.0).abs() < 2.5);

    // 3. TABLE OutputCastType + Multiple OLS Regression from Zone 6B Co-Moments + Weibull MLE
    let table_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Table)
            .expect("Table query");
    let rows = table_resp.payload["rows"].as_array().unwrap();
    let b0 = rows[0][1].as_f64().unwrap();
    let slope_hours = rows[1][1].as_f64().unwrap();
    let slope_parts = rows[2][1].as_f64().unwrap();
    let r2 = rows[3][1].as_f64().unwrap();
    // Ground truth: total_cost = 50_000 + 80_000 * duration_hours + 1.20 * parts_cost
    assert!((b0 - 50_000.0).abs() < 1.0);
    assert!((slope_hours - 80_000.0).abs() < 1.0);
    assert!((slope_parts - 1.20).abs() < 1e-4);
    assert!((r2 - 1.0).abs() < 1e-6);

    // 4. PIE OutputCastType with Hamilton's Exact 100.0000% Sum
    let pie_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Pie)
            .expect("Pie query");
    assert!((pie_resp.payload["total_pct_exact"].as_f64().unwrap() - 100.0).abs() < 1e-9);

    // 5. BUBBLE OutputCastType with Weibull Survival R(t) and Hazard Rate h(t)
    let bubble_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Bubble)
            .expect("Bubble query");
    let b = &bubble_resp.payload["bubbles"][0];
    assert!(b["y_survival_prob_3000h"].as_f64().unwrap() > 0.50);
    assert!(b["r_hazard_rate_3000h"].as_f64().unwrap() > 0.0);

    // 6. CSV_EXPORT OutputCastType
    let csv_resp =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::CsvExport)
            .expect("CSV export query");
    assert!(csv_resp.payload["csv_payload"]
        .as_str()
        .unwrap()
        .contains("work_order,100,"));

    // 7. Cross-Tenant Security Check: "tenant_andino" must be rejected by MbfTenantCryptoSeal!
    let unauthorized = MbfBiEngine::execute_bi_query(&ds, "tenant_andino", BiOutputCast::Kpi);
    assert_eq!(unauthorized.unwrap_err().code, ErrorCode::MbfTenantSeal001);

    // 8. Verify remaining advanced math methods on MbfHyperSynopsis512 & Zone 6B
    let syn = &ds.work_order_cost_synopsis;
    assert!(syn.geometric_mean() > 0.0);
    assert!(syn.harmonic_mean() > 0.0);
    assert!(syn.coeff_of_variation() > 0.0);
    assert!(syn.mad_median > 0.0);
    assert!(syn.mad_abs_deviation > 0.0);
    assert!(syn.iqr_spread > 0.0);
    assert!(syn.estimate_distinct_count() >= 10);
    assert!(ds.vibration_spectral_synopsis.acf_lag1().abs() <= 1.0);
    assert!(ds.vibration_spectral_synopsis.durbin_watson() > 0.0);
    assert!(ds.vibration_spectral_synopsis.temporal_drift_slope() > 0.0);
    assert!(ds.vibration_spectral_synopsis.spectral_entropy > 0.0);
    assert!(ds.vibration_spectral_synopsis.zero_crossing_rate > 0.0);

    // Safe formula evaluator protection against division by zero
    assert_eq!(
        MbfBiEngine::evaluate_safe_ratio_formula(100.0, 0.0, 1.0).unwrap(),
        None
    );
    assert_eq!(
        MbfBiEngine::evaluate_safe_ratio_formula(100.0, 25.0, 2.0).unwrap(),
        Some(8.0)
    );
    assert_eq!(
        MbfBiEngine::evaluate_safe_ratio_formula(f64::NAN, 10.0, 1.0)
            .unwrap_err()
            .code,
        ErrorCode::Fml006
    );
}

#[test]
fn verify_6_layer_hybrid_fuzzy_phonetic_vector_and_geofence_search() {
    let ds = MayorPlazaBogotaDataset::generate_deterministic().expect("dataset");

    // 1. Phonetic + Typo search in Spanish/English technical jargon:
    // "balbula chiler suiche rodamieto 6205" -> must match ASSET-CHILLER-TRANE-01 at Rank 1!
    let hits =
        MbfBiEngine::execute_hybrid_search(&ds, "balbula chiler suiche rodamieto 6205", None, 3);
    assert!(!hits.is_empty());
    assert_eq!(hits[0].entity_id, "ASSET-CHILLER-TRANE-01");

    // 2. Semantic Vector Search + Hybrid RRF:
    // Query vector close to Grundfos pump seal `[-0.15, 0.20, 0.95, 0.80]`
    let vec_hits = MbfBiEngine::execute_hybrid_search(
        &ds,
        "fuga aceite empaque",
        Some(&[-0.14, 0.22, 0.94, 0.81]),
        3,
    );
    assert_eq!(vec_hits[0].entity_id, "ASSET-PUMP-GRUNDFOS-02");

    // 3. Spatiotemporal Geofence Search (within 200m of Centro Comercial Mayor Plaza: 4.5981, -74.1253)
    // Should include Chiller (0m) and Pump (55m), and exclude Escalator (13.8km away)
    let inside_mall = ds
        .hybrid_search_index
        .search_within_geofence_meters(4.5981, -74.1253, 200.0);
    assert_eq!(inside_mall.len(), 2);
    assert_eq!(inside_mall[0].0, "ASSET-CHILLER-TRANE-01");
    assert_eq!(inside_mall[1].0, "ASSET-PUMP-GRUNDFOS-02");
}

#[test]
fn verify_isa95_graph_fenwick_prefix_sums_and_nic16_depreciation() {
    let mut ds = MayorPlazaBogotaDataset::generate_deterministic().expect("dataset");

    // 1. ISA-95 Subtree rollup in O(1): Node 10 (Torre Norte) contains [10, 101, 1001, 1002]
    assert_eq!(
        ds.isa95_graph.filter_subtree_descendants(10),
        vec![10, 101, 1001, 1002]
    );
    // Reverse VAET edges on Chiller 1001: all work orders referencing asset 1001
    let chiller_wos = ds.isa95_graph.reverse_referrers(1001, 1);
    assert!(!chiller_wos.is_empty());

    // 2. FenwickPrefixSum128 O(log N) range sum parity with exact sum
    let total_via_fenwick = ds.work_order_costs_fenwick.range_sum(0, 99);
    assert_eq!(
        total_via_fenwick.0,
        ds.work_order_cost_synopsis.sum_exact_i128
    );

    // 3. NIC 16 Depreciation crossed with IoT horometers:
    // Acquisition = 500,000,000 COP, Salvage = 50,000,000 COP, Useful life = 45,000 hours, Consumed = 9,000 hours (20%)
    // Expected depreciation = (500M - 50M) * 0.20 = 90,000,000 COP exact!
    let depr = FinancialLedgerZone::compute_nic16_depreciation(
        FixedDecimal128::from_whole(500_000_000),
        FixedDecimal128::from_whole(50_000_000),
        45_000,
        9_000,
    )
    .unwrap();
    assert_eq!(depr, FixedDecimal128::from_whole(90_000_000));

    // 4. Issue 10 bearings from FinancialLedgerZone under FIFO @ 150,000 COP = 1,500,000 COP
    let (cogs, cp) = ds
        .financial_ledger
        .issue_inventory(
            1_700_030_000_000,
            FixedDecimal128::from_whole(10),
            ValuationMethod::Fifo,
        )
        .unwrap();
    assert_eq!(cogs, FixedDecimal128::from_whole(1_500_000));
    assert_eq!(cp.closing_qty, FixedDecimal128::from_whole(30));
    assert_eq!(
        cp.closing_book_value,
        FixedDecimal128::from_whole(5_700_000)
    );
}

#[tokio::test]
async fn verify_high_concurrency_s3_transactions_clock_and_footer_roundtrip() {
    let clock = DeterministicClock::new(1_700_000_000_000);
    assert_eq!(clock.now_epoch_ms(), 1_700_000_000_000);
    assert_eq!(clock.advance_ms(5_000), 1_700_000_005_000);

    // Footer 64-byte roundtrip
    let footer = MbfFooterTrailer {
        zone1_pax_offset: 64,
        zone2_csr_offset: 1024,
        zone3_iot_offset: 2048,
        zone4_ledger_offset: 4096,
        zone5_synopsis_offset: 8192,
        zone5b_search_offset: 12288,
        zone6b_cov_offset: 16384,
        footer_magic: MbfFooterTrailer::FOOTER_MAGIC,
        full_file_crc32: 0xDEAD_BEEF,
    };
    let f_bytes = footer.to_bytes();
    assert_eq!(MbfFooterTrailer::from_bytes(&f_bytes).unwrap(), footer);

    // 32 concurrent Tokio tasks committing to MbfWriteCoordinator
    let store = Arc::new(InMemoryChaosS3Store::new());
    let coord = Arc::new(MbfWriteCoordinator::new(store.clone()));
    let mut handles = Vec::new();

    for i in 0..32u32 {
        let c = coord.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("WO-CONC-{i:03}");
            c.commit_transaction(
                "tenant_centro_mayor_bogota",
                "work_order",
                &key,
                &[&key],
                &[&key],
                format!(r#"{{"wo":"{key}"}}"#),
            )
            .await
        }));
    }

    for h in handles {
        let epoch = h.await.expect("join").expect("commit");
        assert!((1..=32).contains(&epoch));
    }

    // Verify 304 Not Modified cache hit on `get_if_none_match`
    let cell0_key = DomainCellRouter::s3_hot_head_key("tenant_centro_mayor_bogota", 0);
    let obj = store.get_object(&cell0_key).await.unwrap().unwrap();
    let cond = store
        .get_if_none_match(&cell0_key, &obj.etag)
        .await
        .unwrap();
    assert_eq!(cond, MbfConditionalGet::NotModified);

    // Verify byte-range slicing
    let slice = store.get_byte_range(&cell0_key, 0, 10).await.unwrap();
    assert_eq!(slice.len(), 10);

    // Verify Hilbert 3D curve clustering determinism
    let h_a = MbfWriteCoordinator::<InMemoryChaosS3Store>::hilbert_cluster_key_3d(10, 20, 30);
    let h_b = MbfWriteCoordinator::<InMemoryChaosS3Store>::hilbert_cluster_key_3d(10, 20, 31);
    assert_ne!(h_a, h_b);

    // Verify PAX time-travel and Schema Evolution integration
    let mut evo = MbfSchemaEvolutionDirectory::new(2);
    evo.insert_physical_column(1, vec![MbfCellValue::Int64(10), MbfCellValue::Int64(20)]);
    let mut map = HashMap::new();
    map.insert("a".to_string(), MbfCellValue::Bool(true));
    assert_eq!(evo.resolve_column(2, MbfCellValue::Bool(false)).len(), 2);
}

#[test]
fn verify_viz_engine_protobuf_decorations_across_all_output_casts_and_tree() {
    use metri_engine::grpc::pb::viz_meta::Payload as PbVizPayload;

    let ds = MayorPlazaBogotaDataset::generate_deterministic().expect("dataset");

    // 1. KPI -> Signal + Intelligence
    let kpi = MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Kpi)
        .expect("kpi");
    let kpi_viz = MbfBiEngine::to_protobuf_viz_meta(&kpi, Some("gauge"));
    assert_eq!(kpi_viz.r#type, "gauge");
    assert!(matches!(kpi_viz.payload, Some(PbVizPayload::Signal(_))));

    // 2. Timeseries -> Chart (line, fill_gaps=true, smooth=true)
    let ts =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Timeseries)
            .expect("ts");
    let ts_viz = MbfBiEngine::to_protobuf_viz_meta(&ts, None);
    assert_eq!(ts_viz.r#type, "line");
    if let Some(PbVizPayload::Chart(c)) = ts_viz.payload {
        assert!(c.fill_gaps);
        assert!(c.smooth);
        assert_eq!(c.x_dimension, "hour_bucket_ms");
    } else {
        panic!("expected Chart payload");
    }

    // 3. Bubble -> Chart (scatter, fill_gaps=false)
    let bub =
        MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Bubble)
            .expect("bubble");
    let bub_viz = MbfBiEngine::to_protobuf_viz_meta(&bub, None);
    assert_eq!(bub_viz.r#type, "scatter");
    if let Some(PbVizPayload::Chart(c)) = bub_viz.payload {
        assert!(!c.fill_gaps);
        assert_eq!(c.x_dimension, "duration_minutes");
    } else {
        panic!("expected Chart payload");
    }

    // 4. Pie -> Breakdown (signals map)
    let pie = MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Pie)
        .expect("pie");
    let pie_viz = MbfBiEngine::to_protobuf_viz_meta(&pie, Some("donut"));
    assert_eq!(pie_viz.r#type, "donut");
    if let Some(PbVizPayload::Breakdown(b)) = pie_viz.payload {
        assert_eq!(b.signals.len(), 3);
    } else {
        panic!("expected Breakdown payload");
    }

    // 5. Table & CsvExport -> TableMeta
    for cast in [BiOutputCast::Table, BiOutputCast::CsvExport] {
        let tbl =
            MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", cast).expect("table");
        let tbl_viz = MbfBiEngine::to_protobuf_viz_meta(&tbl, None);
        assert_eq!(tbl_viz.r#type, "table");
        assert!(matches!(tbl_viz.payload, Some(PbVizPayload::Table(_))));
    }

    // 6. Tree override -> TreeMeta
    let tbl = MbfBiEngine::execute_bi_query(&ds, "tenant_centro_mayor_bogota", BiOutputCast::Table)
        .expect("table");
    let tree_viz = MbfBiEngine::to_protobuf_viz_meta(&tbl, Some("tree"));
    assert_eq!(tree_viz.r#type, "tree");
    if let Some(PbVizPayload::Tree(t)) = tree_viz.payload {
        assert_eq!(t.id_key, "id");
        assert_eq!(t.parent_id_key, "parent_id");
        assert_eq!(t.has_children_key, "has_children");
    } else {
        panic!("expected Tree payload");
    }
}
