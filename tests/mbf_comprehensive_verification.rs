//! Suite exhaustiva de verificación E2E del motor binario unificado Metri Vortex (`.mbf`), Aegis BI, funciones matemáticas, libro contable cuantitativo, búsqueda híbrida y concurrencia ACID en S3.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use metri_engine::aegis::mbf::{BiOutputCast, MbfAnalyticalWorkspace, MbfBiEngine};
use metri_engine::codice::CodeRegistry;
use metri_engine::domain::errors::ErrorCode;
use metri_engine::domain::protocols::{IClock, IMbfObjectStore, MbfConditionalGet};
use metri_engine::mbf::format::{MbfCellValue, MbfFooterTrailer, MbfSchemaEvolutionDirectory};
use metri_engine::mbf::tx::{DomainCellRouter, MbfWriteCoordinator};
use metri_engine::mbf::zones::financial_ledger::{
    FinancialLedgerZone, FixedDecimal128, ValuationMethod,
};
use metri_engine::mbf::{DeterministicClock, InMemoryChaosS3Store};

#[test]
fn domain_agnostic_engine_guard_forbids_domain_coupling_in_src() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let guarded_dirs = [
        manifest_dir.join("src/codice"),
        manifest_dir.join("src/mbf"),
        manifest_dir.join("src/eav"),
        manifest_dir.join("src/aegis/mbf"),
    ];

    let core_forbidden_tokens = [
        "unCefactUnit",
        "UnCefactUnit",
        "UncefactUnit",
        "AssetState",
        "AssetMarkovChain",
        "asset_ref_field",
        "expand_cmms_synonyms",
        "SpanishIndustrialMetaphone",
        "PartNumberCanonicalizer",
        "build_isa95_euler_tree",
        "effective_cost_center_id",
        "convert_to_cop",
        "compute_nic16_depreciation",
        "MayorPlazaBogotaDataset",
        "wilson_eoq",
        "safety_stock_gaussian",
        "dynamic_reorder_point",
        "fill_rate_and_stockout_prob",
        "inherent_availability",
        "optimal_pm_interval",
        "canonicalize_part_number",
        "encode_industrial_phonetic",
        "resource_domain",
        "domain_quota/resource_domain",
        "primary_phone",
        "Chiller",
        "Bomba",
        "to_dynamo_string",
        "to_dynamo_number",
        "pull_nocache",
    ];

    let eav_mbf_forbidden_tokens = [
        "work_order_procedure",
        "procedure_template",
        "technician_shift",
        "work_order_id",
    ];

    fn scan_dir(dir: &Path, forbidden: &[&str]) {
        for entry in std::fs::read_dir(dir).expect("read_dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                scan_dir(&path, forbidden);
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                let content = std::fs::read_to_string(&path).expect("read rs file");
                for &token in forbidden {
                    assert!(
                        !content.contains(token),
                        "Domain-coupled or obsolete token `{token}` found in engine source file `{}`! Engine core must remain 100% domain-agnostic and clean.",
                        path.display()
                    );
                }
            }
        }
    }

    for dir in &guarded_dirs {
        scan_dir(dir, &core_forbidden_tokens);
    }
    for dir in [
        manifest_dir.join("src/mbf"),
        manifest_dir.join("src/eav"),
        manifest_dir.join("src/aegis/mbf"),
    ] {
        scan_dir(&dir, &eav_mbf_forbidden_tokens);
    }
}

#[test]
fn all_60_codice_models_route_cleanly_via_structural_topology_classifier() {
    let models_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/models");
    let (registry, _rules) = CodeRegistry::build(&models_dir).expect("build CodeRegistry");
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
        let model = registry.get_model(entity).expect("registered EntityModel");
        let cell_id = DomainCellRouter::classify_model_topology(model, "partition-001");
        assert!(
            cell_id < 8,
            "Invalid cell_id {cell_id} for {}",
            model.entity
        );
        cell_distribution[cell_id as usize] += 1;
        count += 1;
    }

    assert_eq!(count, 60, "All 60 Códice models must be verified");
    let populated_cells = cell_distribution.iter().filter(|&&n| n > 0).count();
    assert!(
        populated_cells >= 4,
        "Structural topology classifier must distribute models across multiple physical cells"
    );
}

#[test]
fn verify_all_6_bi_output_casts_and_35_plus_math_functions_on_analytical_workspace() {
    let ws = MbfAnalyticalWorkspace::generate_synthetic_reference()
        .expect("generate synthetic workspace");

    // 1. KPI OutputCastType + Higher-Order Pébay Moments (M1..M4, Skewness, Kurtosis, Jarque-Bera)
    let kpi_resp =
        MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Kpi).expect("KPI query");
    assert_eq!(kpi_resp.cast_type, BiOutputCast::Kpi);
    assert_eq!(kpi_resp.flight_recorder.blocks_resolved_o1_by_synopsis, 1);
    assert_eq!(kpi_resp.flight_recorder.rows_scanned_fallback, 0);
    assert_eq!(kpi_resp.payload["total_records"], 100);
    assert!(kpi_resp.payload["mean_metric"].as_f64().unwrap() > 500_000.0);
    assert!(kpi_resp.payload["std_dev_metric"].as_f64().unwrap() > 10_000.0);
    assert!(kpi_resp.payload["jarque_bera"].as_f64().unwrap() >= 0.0);
    // Closing book value: 20 * 150,000 + 20 * 210,000 = 7,200,000; WAC = 180,000
    assert!((kpi_resp.payload["closing_book_value"].as_f64().unwrap() - 7_200_000.0).abs() < 1e-6);
    assert!((kpi_resp.payload["running_wac_unit_cost"].as_f64().unwrap() - 180_000.0).abs() < 1e-6);

    // 2. TIMESERIES OutputCastType + Affine Canonicalization + 60 Hz Spectral FFT/PSD
    let ts_resp = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Timeseries)
        .expect("Timeseries query");
    let points = ts_resp.payload["points"].as_array().unwrap();
    assert_eq!(points.len(), 3);
    // 2nd reading was ingested as 46.4 with affine transform (5/9, -160/9) -> 8.0 canonical!
    assert!((points[1]["canonical_value"].as_f64().unwrap() - 8.0).abs() < 1e-9);
    // Dominant harmonic frequency must be 60 Hz
    let dom_hz = ts_resp.payload["dominant_freq_hz"].as_f64().unwrap();
    assert!((dom_hz - 60.0).abs() < 2.5);

    // 3. TABLE OutputCastType + Multiple OLS Regression from Zone 6B Co-Moments + Weibull MLE
    let table_resp = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Table)
        .expect("Table query");
    let rows = table_resp.payload["rows"].as_array().unwrap();
    let b0 = rows[0][1].as_f64().unwrap();
    let slope_x1 = rows[1][1].as_f64().unwrap();
    let slope_x2 = rows[2][1].as_f64().unwrap();
    let r2 = rows[3][1].as_f64().unwrap();
    // Ground truth: y = 50_000 + 80_000 * x1 + 1.20 * x2
    assert!((b0 - 50_000.0).abs() < 1.0);
    assert!((slope_x1 - 80_000.0).abs() < 1.0);
    assert!((slope_x2 - 1.20).abs() < 1e-4);
    assert!((r2 - 1.0).abs() < 1e-6);

    // 4. PIE OutputCastType with Hamilton's Exact 100.0000% Sum
    let pie_resp =
        MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Pie).expect("Pie query");
    assert!((pie_resp.payload["total_pct_exact"].as_f64().unwrap() - 100.0).abs() < 1e-9);

    // 5. BUBBLE OutputCastType with Weibull Survival R(t) and Hazard Rate h(t)
    let bubble_resp = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Bubble)
        .expect("Bubble query");
    let b = &bubble_resp.payload["bubbles"][0];
    assert!(b["y_survival_prob_3000"].as_f64().unwrap() > 0.50);
    assert!(b["r_hazard_rate_3000"].as_f64().unwrap() > 0.0);

    // 6. CSV_EXPORT OutputCastType
    let csv_resp = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::CsvExport)
        .expect("CSV export query");
    assert!(csv_resp.payload["csv_payload"]
        .as_str()
        .unwrap()
        .contains("entity_record,100,"));

    // 7. Cross-Tenant Security Check: "tenant_beta" must be rejected by MbfTenantCryptoSeal!
    let unauthorized = MbfBiEngine::execute_bi_query(&ws, "tenant_beta", BiOutputCast::Kpi);
    assert_eq!(unauthorized.unwrap_err().code, ErrorCode::MbfTenantSeal001);

    // 8. Verify remaining advanced math methods on MbfHyperSynopsis512 & Zone 6B
    let syn = &ws.primary_metric_synopsis;
    assert!(syn.geometric_mean() > 0.0);
    assert!(syn.harmonic_mean() > 0.0);
    assert!(syn.coeff_of_variation() > 0.0);
    assert!(syn.mad_median > 0.0);
    assert!(syn.mad_abs_deviation > 0.0);
    assert!(syn.iqr_spread > 0.0);
    assert!(syn.estimate_distinct_count() >= 10);
    assert!(ws.spectral_synopsis.acf_lag1().abs() <= 1.0);
    assert!(ws.spectral_synopsis.durbin_watson() > 0.0);
    assert!(ws.spectral_synopsis.temporal_drift_slope() > 0.0);
    assert!(ws.spectral_synopsis.spectral_entropy > 0.0);
    assert!(ws.spectral_synopsis.zero_crossing_rate > 0.0);

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
    let ws = MbfAnalyticalWorkspace::generate_synthetic_reference().expect("workspace");

    // 1. Phonetic + Typo search in Romance/technical tokens:
    // "balbula solenoide skf 6205" -> must match DOC-NODE-1001 at Rank 1!
    let hits = MbfBiEngine::execute_hybrid_search(&ws, "balbula solenoide skf 6205", None, 3);
    assert!(!hits.is_empty());
    assert_eq!(hits[0].entity_id, "DOC-NODE-1001");

    // 2. Semantic Vector Search + Hybrid RRF:
    // Query vector close to DOC-NODE-1002 `[-0.15, 0.20, 0.95, 0.80]`
    let vec_hits = MbfBiEngine::execute_hybrid_search(
        &ws,
        "sello axial caudal",
        Some(&[-0.14, 0.22, 0.94, 0.81]),
        3,
    );
    assert_eq!(vec_hits[0].entity_id, "DOC-NODE-1002");

    // 3. Spatiotemporal Geofence Search (within 200m of reference point: 4.5981, -74.1253)
    // Should include DOC-NODE-1001 (0m) and DOC-NODE-1002 (55m), and exclude DOC-NODE-2001 (13.8km away)
    let inside_geofence = ws
        .hybrid_search_index
        .search_within_geofence_meters(4.5981, -74.1253, 200.0);
    assert_eq!(inside_geofence.len(), 2);
    assert_eq!(inside_geofence[0].0, "DOC-NODE-1001");
    assert_eq!(inside_geofence[1].0, "DOC-NODE-1002");
}

#[test]
fn verify_hierarchical_euler_graph_fenwick_prefix_sums_and_straight_line_amortization() {
    let mut ws = MbfAnalyticalWorkspace::generate_synthetic_reference().expect("workspace");

    // 1. Hierarchical Subtree rollup in O(1): Node 10 contains [10, 101, 1001, 1002]
    assert_eq!(
        ws.hierarchy_graph.filter_subtree_descendants(10),
        vec![10, 101, 1001, 1002]
    );
    // Reverse VAET edges on node 1001: all records referencing node 1001
    let referrers = ws.hierarchy_graph.reverse_referrers(1001, 1);
    assert!(!referrers.is_empty());

    // 2. FenwickPrefixSum128 O(log N) range sum parity with exact sum
    let total_via_fenwick = ws.prefix_sums_fenwick.range_sum(0, 99);
    assert_eq!(
        total_via_fenwick.0,
        ws.primary_metric_synopsis.sum_exact_i128
    );

    // 3. Straight-Line Amortization over lifetime units:
    // Acquisition = 500,000,000, Salvage = 50,000,000, Total capacity = 45,000 units, Consumed = 9,000 units (20%)
    // Expected amortization = (500M - 50M) * 0.20 = 90,000,000 exact!
    let amort = FinancialLedgerZone::compute_straight_line_amortization(
        FixedDecimal128::from_whole(500_000_000),
        FixedDecimal128::from_whole(50_000_000),
        45_000,
        9_000,
    )
    .unwrap();
    assert_eq!(amort, FixedDecimal128::from_whole(90_000_000));

    // 4. Issue 10 units from FinancialLedgerZone under FIFO @ 150,000 = 1,500,000
    let (cogs, cp) = ws
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
    let footer = MbfFooterTrailer::new(
        16384,
        128,
        0xCAFE_BABE,
        0xDEAD_BEEF,
        0x2026_1015,
        1_690_000_000_000,
        [0x5A; 24],
    );
    let f_bytes = footer.to_bytes();
    assert_eq!(f_bytes.len(), 64);
    assert_eq!(&f_bytes[56..64], b"MBFFOOTR");
    assert_eq!(MbfFooterTrailer::from_bytes(&f_bytes).unwrap(), footer);

    // 32 concurrent Tokio tasks committing to MbfWriteCoordinator
    let store = Arc::new(InMemoryChaosS3Store::new());
    let coord = Arc::new(MbfWriteCoordinator::new(store.clone()));
    let mut handles = Vec::new();

    for i in 0..32u32 {
        let c = coord.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("REC-CONC-{i:03}");
            c.commit_transaction(
                "tenant_alpha",
                "entity_alpha",
                &key,
                &[&key],
                &[&key],
                format!("rec={key}"),
            )
            .await
        }));
    }

    for h in handles {
        let epoch = h.await.expect("join").expect("commit");
        assert!((1..=32).contains(&epoch));
    }

    // Verify 304 Not Modified cache hit on `get_if_none_match` and pure 64B-aligned binary HOT_HEAD
    let cell_id = DomainCellRouter::resolve_cell_id("entity_alpha", "REC-CONC-000");
    let cell_key = DomainCellRouter::s3_hot_head_key("tenant_alpha", cell_id);
    let obj = store.get_object(&cell_key).await.unwrap().unwrap();
    assert_eq!(obj.bytes.len() % 64, 0);
    assert_eq!(&obj.bytes[0..8], b"METRIBIN");
    assert_eq!(&obj.bytes[obj.bytes.len() - 8..], b"MBFFOOTR");
    let cond = store.get_if_none_match(&cell_key, &obj.etag).await.unwrap();
    assert_eq!(cond, MbfConditionalGet::NotModified);

    // Verify byte-range slicing
    let slice = store.get_byte_range(&cell_key, 0, 10).await.unwrap();
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

#[tokio::test]
async fn verify_spec_03_mbf_binary_format_zero_json_and_indexed_queries() {
    use metri_engine::mbf::storage::{AttributeValue, Blob, MbfStorageClient};

    let ws = MbfAnalyticalWorkspace::generate_synthetic_reference().expect("workspace");

    // 1. Serialize all 7 zones into a single spec-compliant 64-byte aligned `.mbf` binary
    let mbf_bytes = ws.to_mbf_bytes().expect("to_mbf_bytes");
    assert_eq!(mbf_bytes.len() % 64, 0);
    assert_eq!(&mbf_bytes[0..8], b"METRIBIN");
    assert_eq!(&mbf_bytes[mbf_bytes.len() - 8..], b"MBFFOOTR");

    // Verify companion `.mbf.idx` index mirror binary
    let idx_bytes = ws.to_idx_bytes();
    assert_eq!(idx_bytes.len() % 64, 0);
    assert_eq!(&idx_bytes[0..8], b"MBFINDEX");
    assert_eq!(&idx_bytes[idx_bytes.len() - 8..], b"MBFFOOTR");

    // 2. Roundtrip decode all 7 zones from binary `.mbf` and verify exact parity
    let decoded = MbfAnalyticalWorkspace::from_mbf_bytes(&mbf_bytes).expect("from_mbf_bytes");
    assert_eq!(decoded.tenant_id, ws.tenant_id);
    assert_eq!(decoded.primary_pax.mvcc_versions.len(), 100);
    assert_eq!(
        decoded
            .primary_pax
            .directory
            .lookup_rows("REC-0042")
            .and_then(|r| r.first().copied()),
        Some(41)
    );
    assert_eq!(
        decoded.hierarchy_graph.filter_subtree_descendants(10),
        vec![10, 101, 1001, 1002]
    );
    assert_eq!(
        decoded.primary_timeseries_iot.decompress_all(),
        ws.primary_timeseries_iot.decompress_all()
    );
    assert_eq!(decoded.primary_metric_synopsis, ws.primary_metric_synopsis);
    assert_eq!(decoded.multivariate_cov, ws.multivariate_cov);
    let hits = decoded
        .hybrid_search_index
        .search_hybrid("balbula solenoide skf 6205", None, 3);
    assert_eq!(hits[0].entity_id, "DOC-NODE-1001");

    // 3. Verify native O(log N) secondary BTree index queries on MbfStorageClient
    let client = MbfStorageClient::new_stub();
    let table = "test_mbf_native_idx_table";
    for i in 1..=5u8 {
        let mut item = HashMap::new();
        item.insert(
            "PK".to_string(),
            AttributeValue::S(format!("T#t1#E#ent_{i}")),
        );
        item.insert("SK".to_string(), AttributeValue::B(Blob::new(vec![0, i])));
        item.insert(
            "ap".to_string(),
            AttributeValue::S("T#t1#A#entity/type#record_alpha".to_string()),
        );
        item.insert("as".to_string(), AttributeValue::B(Blob::new(vec![i])));
        item.insert(
            "vp".to_string(),
            AttributeValue::S("T#t1#AV#code".to_string()),
        );
        item.insert(
            "vs".to_string(),
            AttributeValue::B(Blob::new(format!("REC-00{i}").into_bytes())),
        );
        client.put_item(table, item).await.expect("put_item");
    }

    let mut expr_vals = HashMap::new();
    expr_vals.insert(
        ":pk".to_string(),
        AttributeValue::S("T#t1#AV#code".to_string()),
    );
    expr_vals.insert(
        ":sk_prefix".to_string(),
        AttributeValue::B(Blob::new(b"REC-003".to_vec())),
    );
    let avet_res = client
        .query(
            table,
            Some("GSI-AVET"),
            "vp = :pk AND begins_with(vs, :sk_prefix)",
            HashMap::new(),
            expr_vals,
            true,
            None,
        )
        .await
        .expect("avet query");
    assert_eq!(avet_res.len(), 1);
}

#[test]
fn verify_viz_engine_protobuf_decorations_across_all_output_casts_and_tree() {
    use metri_engine::grpc::pb::viz_meta::Payload as PbVizPayload;

    let ws = MbfAnalyticalWorkspace::generate_synthetic_reference().expect("workspace");

    // 1. KPI -> Signal + Intelligence
    let kpi = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Kpi).expect("kpi");
    let kpi_viz = MbfBiEngine::to_protobuf_viz_meta(&kpi, Some("gauge"));
    assert_eq!(kpi_viz.r#type, "gauge");
    assert!(matches!(kpi_viz.payload, Some(PbVizPayload::Signal(_))));

    // 2. Timeseries -> Chart (line, fill_gaps=true, smooth=true)
    let ts =
        MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Timeseries).expect("ts");
    let ts_viz = MbfBiEngine::to_protobuf_viz_meta(&ts, None);
    assert_eq!(ts_viz.r#type, "line");
    if let Some(PbVizPayload::Chart(c)) = ts_viz.payload {
        assert!(c.fill_gaps);
        assert!(c.smooth);
        assert_eq!(c.x_dimension, "timestamp_ms");
    } else {
        panic!("expected Chart payload");
    }

    // 3. Bubble -> Chart (scatter, fill_gaps=false)
    let bub =
        MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Bubble).expect("bubble");
    let bub_viz = MbfBiEngine::to_protobuf_viz_meta(&bub, None);
    assert_eq!(bub_viz.r#type, "scatter");
    if let Some(PbVizPayload::Chart(c)) = bub_viz.payload {
        assert!(!c.fill_gaps);
        assert_eq!(c.x_dimension, "x_mean_lifetime");
    } else {
        panic!("expected Chart payload");
    }

    // 4. Pie -> Breakdown (signals map)
    let pie = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Pie).expect("pie");
    let pie_viz = MbfBiEngine::to_protobuf_viz_meta(&pie, Some("donut"));
    assert_eq!(pie_viz.r#type, "donut");
    if let Some(PbVizPayload::Breakdown(b)) = pie_viz.payload {
        assert_eq!(b.signals.len(), 3);
    } else {
        panic!("expected Breakdown payload");
    }

    // 5. Table & CsvExport -> TableMeta
    for cast in [BiOutputCast::Table, BiOutputCast::CsvExport] {
        let tbl = MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", cast).expect("table");
        let tbl_viz = MbfBiEngine::to_protobuf_viz_meta(&tbl, None);
        assert_eq!(tbl_viz.r#type, "table");
        assert!(matches!(tbl_viz.payload, Some(PbVizPayload::Table(_))));
    }

    // 6. Tree override -> TreeMeta
    let tbl =
        MbfBiEngine::execute_bi_query(&ws, "tenant_alpha", BiOutputCast::Table).expect("table");
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

#[tokio::test]
async fn verify_spec_09_codice_metadata_and_fuzzy_math_engine_100_percent_coverage() {
    use metri_engine::codice::{CodeRegistry, DistributionFamily, PhoneticAlgorithm};
    use metri_engine::eav::fts::searcher::{
        canonicalize_alphanumeric_code, fts_search_entity_ids, register_fts_document,
    };
    use metri_engine::mbf::zones::bi_synopses::{
        AffineTransform, DimensionalUnitRegistry, DiscreteMarkovChain, MbfHyperSynopsis512,
    };
    use metri_engine::mbf::zones::cross_covariance::MbfCrossCovarianceMatrix;
    use metri_engine::mbf::zones::hybrid_search::{
        HnswFlatGraph, IndexedEntityDocument, LevenshteinAutomatonDfa, MbfHybridSearchIndex,
        SynonymThesaurus,
    };

    // 1. Verify §1 & §2: Códice Declarative Metadata Expansion across config/models/*.json
    let models_dir = std::path::Path::new("config/models");
    let (registry, _rules) =
        CodeRegistry::build(models_dir).expect("config/models must compile with Spec 09 profiles");

    let wo = registry.get_model("work_order").expect("work_order model");
    let wo_surv = wo
        .survival_profile
        .as_ref()
        .expect("work_order survival_profile");
    assert!(wo_surv.enabled && wo_surv.fit_weibull_3p && wo_surv.enable_kaplan_meier_censoring);
    assert_eq!(wo_surv.subject_ref_field, "asset_id");
    let wo_spatial = wo
        .spatial_profile
        .as_ref()
        .expect("work_order spatial_profile");
    assert_eq!(wo_spatial.h3_resolutions, vec![7, 9, 11, 13]);
    assert!(wo_spatial.morton_z_order);
    let wo_title = wo
        .attributes
        .iter()
        .find(|a| a.name == "title")
        .expect("wo.title");
    let title_fuzzy = wo_title
        .fuzzy_profile
        .as_ref()
        .expect("wo.title fuzzy_profile");
    assert_eq!(
        title_fuzzy.phonetic_algorithm,
        PhoneticAlgorithm::RomancePhonetic
    );
    assert!((title_fuzzy.bm25_field_boost - 3.5).abs() < 1e-6);

    let asset = registry.get_model("asset").expect("asset model");
    let asset_model_num = asset
        .attributes
        .iter()
        .find(|a| a.name == "model_number")
        .expect("asset.model_number");
    assert_eq!(
        asset_model_num
            .fuzzy_profile
            .as_ref()
            .expect("fuzzy")
            .phonetic_algorithm,
        PhoneticAlgorithm::DoubleMetaphone
    );
    let asset_cost = asset
        .attributes
        .iter()
        .find(|a| a.name == "purchase_cost_cents")
        .expect("asset.purchase_cost_cents");
    assert_eq!(
        asset_cost
            .math_profile
            .as_ref()
            .expect("math")
            .distribution_family,
        DistributionFamily::Pareto
    );

    let downtime = registry
        .get_model("downtime_log")
        .expect("downtime_log model");
    assert!(downtime.survival_profile.is_some());

    let meter = registry
        .get_model("meter_reading")
        .expect("meter_reading model");
    let reading_val = meter
        .attributes
        .iter()
        .find(|a| a.name == "reading_value")
        .expect("reading_value");
    let reading_math = reading_val.math_profile.as_ref().expect("reading math");
    assert_eq!(
        reading_math.unit_field_ref.as_deref(),
        Some("unit_of_measure")
    );
    let spec_prof = reading_math
        .spectral_profile
        .as_ref()
        .expect("spectral_profile");
    assert!(spec_prof.enable_fft);
    assert_eq!(spec_prof.psd_bands, 8);
    assert_eq!(spec_prof.goertzel_target_hz, vec![50.0, 60.0, 100.0, 120.0]);

    let inv_ledger = registry
        .get_model("inventory_ledger")
        .expect("inventory_ledger model");
    assert_eq!(
        inv_ledger
            .cross_covariance_matrix
            .as_ref()
            .expect("cov")
            .measure_fields
            .len(),
        3
    );

    // 2. Verify §6: Injectable Affine Dimensional Normalization (y = a * x + b)
    let mut unit_reg = DimensionalUnitRegistry::new();
    unit_reg.register_unit("CEL", AffineTransform::new(1.0, 273.15));
    unit_reg.register_unit("FAH", AffineTransform::new(5.0 / 9.0, 255.3722222222222));
    unit_reg.register_unit("KEL", AffineTransform::identity());
    unit_reg.register_unit("BAR", AffineTransform::linear(100_000.0));
    unit_reg.register_unit("KWH", AffineTransform::linear(3_600_000.0));

    let mixed_readings = vec![
        (1000, 100.0, "CEL"), // 373.15 K
        (2000, 212.0, "FAH"), // 373.15 K
        (3000, 373.15, "KEL"),
    ];
    let temp_syn = MbfHyperSynopsis512::from_observations_with_registry(
        &mixed_readings,
        &unit_reg,
        0,
        &[],
        0.0,
    );
    assert!((temp_syn.pebay_mean_m1 - 373.15).abs() < 1e-4);
    assert!(temp_syn.std_dev() < 1e-4);
    assert!((unit_reg.normalize(2.0, "BAR") - 200_000.0).abs() < 1e-6);
    assert!((unit_reg.normalize(1.0, "KWH") - 3_600_000.0).abs() < 1e-6);

    // 3. Verify §1.1 #1, §3, §4 & §7: Master Catalog of Mathematical, Survival, Discrete Markov & Operations-Research Functions
    let chain_a = DiscreteMarkovChain::<4>::from_state_sequence(&[0, 0, 1, 0, 2, 3, 0]);
    let chain_b = DiscreteMarkovChain::<4>::from_state_sequence(&[0, 2, 3, 0]);
    let chain = chain_a.merge(&chain_b);
    let steady = chain.steady_state_distribution();
    let sum_steady: f64 = steady.iter().sum();
    assert!((sum_steady - 1.0).abs() < 1e-6);
    let reach_prob = chain.n_step_target_probability(0, &[2], 3);
    assert!((0.0..=1.0).contains(&reach_prob));

    let obs: Vec<(i64, f64)> = (1..=64)
        .map(|i| {
            let t = i * 1000;
            let val = 100.0 + (i as f64) * 2.5 + ((i % 5) as f64);
            (t, val)
        })
        .collect();
    let syn = MbfHyperSynopsis512::from_observations(&obs, 0, &[280.0, 300.0], 64.0);
    assert!(syn.modified_z_score(10_000.0) > 3.5);
    assert!(syn.is_robust_outlier(10_000.0, 3.5));
    let (low_fence, high_fence) = syn.tukey_iqr_fences();
    assert!(low_fence < high_fence);
    assert!(syn.trimmed_mean_5pct() > 100.0);
    assert!(syn.quantile(0.50) > 100.0);
    let (beta, eta, gamma_loc) = syn.weibull_3p_params();
    assert!(beta > 0.0 && eta > 0.0 && gamma_loc >= 0.0);
    assert!(syn.kaplan_meier_survival(150.0) > 0.0);
    assert!(syn.mean_interarrival_time() > 0.0);

    let restoration_obs: Vec<(i64, f64)> =
        (1..=20).map(|i| (i * 1000, 4.0 + (i % 3) as f64)).collect();
    let restoration_syn = MbfHyperSynopsis512::from_observations(&restoration_obs, 0, &[], 0.0);
    let avail = syn.steady_state_availability(&restoration_syn);
    assert!(avail > 0.85 && avail <= 1.0);
    assert!(syn.optimal_renewal_interval(1_000.0, 5_000.0).unwrap() > 0.0);
    assert!((-1.0..=1.0).contains(&syn.acf_lag2()));
    let (ema_f, ema_s, macd_div) = syn.macd_state();
    assert!((ema_f - ema_s - macd_div).abs() < 1e-9);
    assert!(syn.cusum_regime_change_detected(2.0));
    assert!(syn.mann_kendall_drift_z() > 2.0);
    assert!(MbfHyperSynopsis512::goertzel_harmonic_power(&obs, 64.0, 10.0) >= 0.0);

    // Operations-Research Lot-Sizing & Buffer Optimization
    let lot_opt = syn.convex_lot_size_optimum(150.0, 5.0);
    let buf = syn.gaussian_safety_buffer(4.0, 0.5, 1.96);
    let thresh = syn.stochastic_threshold_point(4.0, 0.5, 1.96);
    let (fill_rate, tail_risk) = syn.service_fill_rate_and_tail_risk(thresh * 1.2, 4.0, 0.5);
    assert!(lot_opt > 0.0 && buf > 0.0 && thresh > buf);
    assert!(fill_rate > 0.80 && tail_risk < 0.20);

    // Cross-Covariance Matrix: Spearman & Cholesky OLS
    let rows = vec![
        vec![1.0, 10.0, 25.0],
        vec![2.0, 20.0, 45.0],
        vec![3.0, 30.0, 65.0],
        vec![4.0, 40.0, 85.0],
        vec![5.0, 50.0, 105.0],
    ];
    let mut cov_mat = MbfCrossCovarianceMatrix::new(3);
    for r in &rows {
        cov_mat.push_row(r);
    }
    let spearman_o1 = cov_mat.spearman_correlation_matrix();
    let spearman_exact = MbfCrossCovarianceMatrix::spearman_from_rows(&rows);
    assert!((spearman_o1[0][1] - 1.0).abs() < 1e-4);
    assert!((spearman_exact[0][1] - 1.0).abs() < 1e-6);

    // 4. Verify §1.1 #2 & §5: 6-Layer Hybrid Search, Injectable SynonymThesaurus, Alphanumeric Normalizer, H3 & DBSCAN
    assert_eq!(
        canonicalize_alphanumeric_code("SKF-6205 / 2RS"),
        canonicalize_alphanumeric_code("skf 62o5 2rs")
    );
    let dfa = LevenshteinAutomatonDfa::new("rodamiento", 2);
    assert_eq!(dfa.accepts("rodamieto"), Some(1));
    assert!(dfa.accepts("compresor").is_none());

    let mut hnsw = HnswFlatGraph::new();
    hnsw.insert(10, &[0.9, 0.8, 0.1, -0.2]);
    hnsw.insert(20, &[-0.8, -0.7, 0.5, 0.9]);
    let knn = hnsw.search_knn(&[0.9, 0.8, 0.1, -0.2], 1);
    assert_eq!(knn[0].0, 10);

    let rrf = MbfHybridSearchIndex::compute_rrf_score(Some(1), Some(1), 1.0, 1.0);
    assert!((rrf - (2.0 / 61.0)).abs() < 1e-9);

    for res in [7u8, 9, 11, 13] {
        let h3_cell = MbfHybridSearchIndex::encode_h3_cell(4.5981, -74.1253, res);
        assert_eq!((h3_cell >> 52) & 0xF, u64::from(res));
    }

    let mut thesaurus = SynonymThesaurus::new();
    thesaurus.register_equivalence_group(&["fuga", "goteo", "filtracion", "leak"]);
    thesaurus.register_equivalence_group(&["aceite", "lubricante", "oil"]);
    thesaurus.register_equivalence_group(&["sello", "empaque", "retenedor", "seal"]);

    let mut search_idx = MbfHybridSearchIndex::new().with_thesaurus(thesaurus);
    search_idx.index_document(IndexedEntityDocument {
        entity_id: "DOC-ALPHA-01".into(),
        row_id: 1,
        code: "SKF-6205 / 2RS".into(),
        title: "Fuga de aceite en sello mecánico primario".into(),
        description: "Reemplazo programado en sector norte".into(),
        embedding: Some(vec![0.8, 0.6, 0.0, 0.0]),
        coordinates_wgs84: Some((4.5981, -74.1253)),
    });
    search_idx.index_document(IndexedEntityDocument {
        entity_id: "DOC-ALPHA-02".into(),
        row_id: 2,
        code: "MOD-BETA-02".into(),
        title: "Inspección periódica de unidad secundaria".into(),
        description: "Verificación de parámetros nominales".into(),
        embedding: Some(vec![0.0, 0.0, 0.8, 0.6]),
        coordinates_wgs84: Some((4.5983, -74.1254)),
    });

    // Search using injected synonyms ("goteo lubricante empaque") + OCR typo alphanumeric code ("skf 62o5 2rs")
    let syn_hits = search_idx.search_hybrid("goteo lubricante empaque skf62052rs", None, 5);
    assert!(!syn_hits.is_empty());
    assert_eq!(syn_hits[0].entity_id, "DOC-ALPHA-01");

    // Spatial DBSCAN hotspot clustering
    let clusters = search_idx.cluster_spatial_dbscan(200.0, 2);
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0].len(), 2);

    // Non-stub EAV FTS searcher (Zone 5B + trigram + code + Damerau-Levenshtein)
    register_fts_document(
        "tenant_spec09",
        "DOC-ALPHA-01",
        "Fuga de aceite en sello mecánico SKF-6205 / 2RS",
    );
    let fts_hits = fts_search_entity_ids("SKF 62O5 2RS", "tenant_spec09")
        .await
        .expect("fts search");
    assert_eq!(fts_hits[0].entity_id, "DOC-ALPHA-01");
}

#[tokio::test]
async fn verify_eav_mbf_native_integration_and_binary_preservation() {
    use metri_engine::eav::fts::searcher::{export_tenant_zone5b_bytes, fts_search_entity_ids};
    use metri_engine::eav::fts::trigram::build_fts_items;
    use metri_engine::eav::types::datom::{Datom, DatomValue};
    use metri_engine::eav::types::encoding::{avet_sk_prefix, build_avet_sk};

    // 1. Verify Datom::avet_pk() is 100% tenant-isolated and domain-agnostic
    let datom = Datom::assert(
        "tnt_eav_mbf",
        "ent_001",
        "title",
        10,
        DatomValue::Str("Compresor Axial 7700".to_string()),
        100,
    );
    assert_eq!(datom.avet_pk(), "T#tnt_eav_mbf#AV#title");

    // 2. Verify avet_sk_prefix preserves 0x00 bytes inside Bool(false), Long(0), Double(0.0), and Ref(42)
    for val in [
        DatomValue::Bool(false),
        DatomValue::Long(0),
        DatomValue::Double(0.0),
        DatomValue::Ref(42),
    ] {
        let prefix = avet_sk_prefix(&val);
        let full_sk = build_avet_sk(&val, "ent_001");
        assert!(full_sk.starts_with(&prefix));
        assert!(prefix.len() >= 2);
    }

    // 3. Verify build_fts_items feeds Zone 5B MbfHybridSearchIndex and exports 64-byte aligned MBFZ5BHS binary
    let (registry, _) = CodeRegistry::build(Path::new("config/models")).expect("registry");
    let reminder_model = registry.get_model("reminder").expect("reminder model");
    let attr_desc = reminder_model
        .attributes
        .iter()
        .find(|a| a.name == "title")
        .expect("reminder.title attr");
    let fts_items = build_fts_items(&datom, attr_desc);
    assert!(!fts_items.is_empty());
    let hits = fts_search_entity_ids("Compresor Axial", "tnt_eav_mbf")
        .await
        .expect("zone5b + trigram fts search");
    assert_eq!(hits[0].entity_id, "ent_001");

    let zone5b_bin = export_tenant_zone5b_bytes("tnt_eav_mbf");
    assert!(!zone5b_bin.is_empty());
    assert_eq!(zone5b_bin.len() % 64, 0);
    assert_eq!(&zone5b_bin[0..8], b"MBFZ5BHS");
}

#[test]
fn verify_terabyte_scale_federated_catalog_and_hilbert_zonemap_pruning() {
    use metri_engine::aegis::mbf::{MbfTerabyteQueryFilter, MbfTieredTerabyteCatalog};

    // 1,024 L2 archives x 1 GiB = 1.0 TiB (1,099,511,627,776 physical bytes, 5.12 Billion rows)
    let catalog =
        MbfTieredTerabyteCatalog::generate_terabyte_reference_catalog(1024).expect("1 TB catalog");
    assert_eq!(catalog.total_physical_bytes, 1_099_511_627_776);
    assert_eq!(catalog.total_rows, 5_120_000_000);

    // 1. Full 1-TB aggregation across all 1,024 stripes via O(1) Pébay synopsis fusion
    let t0 = std::time::Instant::now();
    let kpi_resp = MbfBiEngine::execute_terabyte_federated_query(
        &catalog,
        "tenant_alpha",
        &MbfTerabyteQueryFilter::default(),
        BiOutputCast::Kpi,
    )
    .expect("1 TB KPI query");
    let full_elapsed = t0.elapsed();
    assert!(
        full_elapsed.as_millis() < 25,
        "Full 1 TB synopsis fusion took {:?} (must be < 25 ms)",
        full_elapsed
    );
    assert_eq!(kpi_resp.flight_recorder.blocks_inspected_total, 1024);
    assert_eq!(
        kpi_resp.flight_recorder.blocks_resolved_o1_by_synopsis,
        1024
    );
    assert_eq!(kpi_resp.flight_recorder.rows_scanned_fallback, 0);
    assert_eq!(kpi_resp.payload["total_records"], 5_120_000_000_u64);

    // 2. Pruned 1-TB query using 3D Hilbert + Temporal Zone Map (prunes 99.02% of stripes in < 5 ms)
    let filter = MbfTerabyteQueryFilter {
        min_timestamp_ms: 1_700_000_000_000,
        max_timestamp_ms: 1_700_000_000_000 + 10 * 86_400_000 - 1,
        min_hilbert_key: 0,
        max_hilbert_key: 10 * 1_000_000 - 1,
        ..Default::default()
    };
    let t1 = std::time::Instant::now();
    let table_resp = MbfBiEngine::execute_terabyte_federated_query(
        &catalog,
        "tenant_alpha",
        &filter,
        BiOutputCast::Table,
    )
    .expect("Pruned 1 TB Table query");
    let pruned_elapsed = t1.elapsed();
    assert!(
        pruned_elapsed.as_micros() < 5_000,
        "Pruned 1 TB query took {:?} (must be < 5 ms)",
        pruned_elapsed
    );
    assert_eq!(table_resp.flight_recorder.blocks_pruned_by_zonemap, 1014);
    assert_eq!(
        table_resp.flight_recorder.blocks_resolved_o1_by_synopsis,
        10
    );
}

#[test]
fn verify_petabyte_scale_hierarchical_l3_catalog_and_boundary_drilldown() {
    use metri_engine::aegis::mbf::{MbfPetabyteHierarchicalCatalog, MbfTerabyteQueryFilter};

    // 1,024 L3 Super-Manifests x 1,024 L2 Stripes = 1,048,576 Stripes (1.0 PiB = 1,125,899,906,842,624 bytes, 5.24288 Trillion rows)
    let pb_catalog = MbfPetabyteHierarchicalCatalog::generate_petabyte_reference_catalog(1024)
        .expect("1 PB catalog");
    assert_eq!(pb_catalog.total_l2_stripes, 1_048_576);
    assert_eq!(pb_catalog.total_physical_bytes, 1_125_899_906_842_624);
    assert_eq!(pb_catalog.total_rows, 5_242_880_000_000);

    // 1. Full 1.0-Petabyte aggregation via L3 Super-Manifest collapse (fuses 1,048,576 stripes in 1,024 O(1) steps)
    let t0 = std::time::Instant::now();
    let full_pb = MbfBiEngine::execute_petabyte_hierarchical_query(
        &pb_catalog,
        "tenant_alpha",
        &MbfTerabyteQueryFilter::default(),
        BiOutputCast::Kpi,
    )
    .expect("1 PB KPI query");
    let elapsed_full = t0.elapsed();
    assert!(
        elapsed_full.as_millis() < 25,
        "Full 1 PB synopsis collapse took {:?} (must be < 25 ms)",
        elapsed_full
    );
    assert_eq!(full_pb.flight_recorder.blocks_inspected_total, 1_048_576);
    assert_eq!(
        full_pb.flight_recorder.blocks_resolved_o1_by_synopsis,
        1_048_576
    );
    assert_eq!(full_pb.flight_recorder.rows_scanned_fallback, 0);
    assert_eq!(full_pb.payload["total_records"], 5_242_880_000_000_u64);

    // 2. Partial-boundary drill-down across 1.0 Petabyte (99.99905% pruned in < 5 ms!)
    let filter = MbfTerabyteQueryFilter {
        min_timestamp_ms: 1_700_000_000_000,
        max_timestamp_ms: 1_700_000_000_000 + 10 * 86_400_000 - 1,
        min_hilbert_key: 0,
        max_hilbert_key: 10 * 1_000_000 - 1,
        ..Default::default()
    };
    let t1 = std::time::Instant::now();
    let pruned_pb = MbfBiEngine::execute_petabyte_hierarchical_query(
        &pb_catalog,
        "tenant_alpha",
        &filter,
        BiOutputCast::Table,
    )
    .expect("Pruned 1 PB Table query");
    let elapsed_pruned = t1.elapsed();
    assert!(
        elapsed_pruned.as_micros() < 5_000,
        "Pruned 1 PB query took {:?} (must be < 5 ms)",
        elapsed_pruned
    );
    assert_eq!(pruned_pb.flight_recorder.blocks_inspected_total, 1_048_576);
    assert_eq!(
        pruned_pb.flight_recorder.blocks_pruned_by_zonemap,
        1_048_566
    );
    assert_eq!(pruned_pb.flight_recorder.blocks_resolved_o1_by_synopsis, 10);
}

#[tokio::test]
async fn verify_all_eight_mbf_and_idx_bug_fixes_end_to_end() {
    use metri_engine::codice::{global, init_global};
    use metri_engine::eav::reader::query::{EavQueryExecutor, NativeQueryPlan};
    use metri_engine::eav::types::DatomValue;
    use metri_engine::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use metri_engine::mbf::format::{MbfDirectoryIndex, MbfHeader, MBF_FLAG_HOT_HEAD};
    use metri_engine::mbf::storage::MbfStorageClient;
    use metri_engine::mbf::zones::bi_synopses::MbfHyperSynopsis512;
    use metri_engine::mbf::zones::hybrid_search::MbfHybridSearchIndex;

    let models_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/models");
    let (built_reg, _) = CodeRegistry::build(&models_dir).expect("build CodeRegistry");
    init_global(built_reg);

    let storage = Arc::new(MbfStorageClient::new_stub());
    let table = "metri_fixes_table";
    let writer = EavWriter::new(Arc::clone(&storage), table);
    let query_executor = EavQueryExecutor::new(Arc::clone(&storage), table);

    // 1. Insert first asset with unique tag, name, description, and numeric attributes
    //    (Note: normal `transact` automatically creates an `outbox_event` in the same ACID TX!)
    let asset1_id = "01J8Z000000000000000ASSET1";
    let mut asset1_attrs = HashMap::new();
    asset1_attrs.insert(
        "name".to_string(),
        DatomValue::Str("Turbina Principal Norte".to_string()),
    );
    asset1_attrs.insert(
        "tag".to_string(),
        DatomValue::Str("A-UNICO-001".to_string()),
    );
    asset1_attrs.insert(
        "description".to_string(),
        DatomValue::Str("Equipo critico de generacion".to_string()),
    );
    asset1_attrs.insert(
        "criticality".to_string(),
        DatomValue::Str("HIGH".to_string()),
    );
    asset1_attrs.insert("status".to_string(), DatomValue::Str("ACTIVE".to_string()));
    asset1_attrs.insert(
        "location_id".to_string(),
        DatomValue::Str("01J8Z0000000000000000LOC01".to_string()),
    );
    asset1_attrs.insert("purchase_cost_cents".to_string(), DatomValue::Long(120));

    let tx1_res = writer
        .transact(TransactPayload {
            tenant_id: "tenant_fixes".to_string(),
            entity_id: Some(asset1_id.to_string()),
            entity_type: "asset".to_string(),
            attrs: asset1_attrs.clone(),
            op: TransactOp::Create,
            suppress_events: false,
        })
        .await
        .expect("First asset transaction must succeed");
    assert_eq!(
        tx1_res.outbox_count, 1,
        "Transaction must also persist 1 outbox_event in the same ACID write"
    );

    // 2. Insert second asset in the same cell to verify HOT_HEAD_c{cell}.mbf aggregates multiple entities
    let asset2_id = "01J8Z000000000000000ASSET2";
    let mut asset2_attrs = asset1_attrs.clone();
    asset2_attrs.insert(
        "name".to_string(),
        DatomValue::Str("Turbina Secundaria Sur".to_string()),
    );
    asset2_attrs.insert(
        "tag".to_string(),
        DatomValue::Str("A-UNICO-002".to_string()),
    );
    writer
        .transact(TransactPayload {
            tenant_id: "tenant_fixes".to_string(),
            entity_id: Some(asset2_id.to_string()),
            entity_type: "asset".to_string(),
            attrs: asset2_attrs,
            op: TransactOp::Create,
            suppress_events: false,
        })
        .await
        .expect("Second asset transaction must succeed");

    // 3. Bug #6 verification: Duplicate unique attribute `tag` on a 3rd entity must fail with EavTx001
    let mut asset3_dup_attrs = asset1_attrs.clone();
    asset3_dup_attrs.insert(
        "name".to_string(),
        DatomValue::Str("Turbina Duplicada".to_string()),
    );
    let dup_err = writer
        .transact(TransactPayload {
            tenant_id: "tenant_fixes".to_string(),
            entity_id: Some("01J8Z000000000000000ASSET3".to_string()),
            entity_type: "asset".to_string(),
            attrs: asset3_dup_attrs,
            op: TransactOp::Create,
            suppress_events: false,
        })
        .await
        .expect_err("Duplicate asset/tag across different entities must fail");
    assert_eq!(dup_err.code, ErrorCode::Cod001);

    // Idempotent update on the SAME entity with the same unique tag must succeed
    writer
        .transact(TransactPayload {
            tenant_id: "tenant_fixes".to_string(),
            entity_id: Some(asset1_id.to_string()),
            entity_type: "asset".to_string(),
            attrs: asset1_attrs,
            op: TransactOp::Update,
            suppress_events: false,
        })
        .await
        .expect("Idempotent update of same entity with same unique tag must succeed");

    // 4. Bug #2 verification: `vortex/{table}.mbf` wire snapshot must NOT contain any `#FTS#` keys,
    //    and FTS search must still find the entity in memory.
    let fts_hits = query_executor
        .execute_native_plan(&NativeQueryPlan::FtsSearch {
            tenant_id: "tenant_fixes".to_string(),
            term: "Turbina".to_string(),
        })
        .await
        .expect("FTS search");
    assert!(
        fts_hits.contains(&asset1_id.to_string()),
        "FTS search must find asset1_id"
    );

    let (vortex_bytes, extra_files) = storage
        .export_flush_artifacts(table, "vortex")
        .expect("export_flush_artifacts");
    let extra_map: HashMap<String, Vec<u8>> = extra_files.into_iter().collect();

    let snapshot_str = String::from_utf8_lossy(&vortex_bytes);
    assert!(
        !snapshot_str.contains("#FTS#"),
        "Wire snapshot must not contain #FTS# trigram bloat"
    );

    // 5. Bug #1, #3, #5, #7 verification on `<asset1_id>.mbf.idx` and `HOT_HEAD.mbf.idx`
    let entity_idx_key = format!("vortex/tenants/tenant_fixes/entities/asset/{asset1_id}.mbf.idx");
    let entity_idx_bytes = extra_map
        .get(&entity_idx_key)
        .expect("entity .mbf.idx must be generated");

    let hot_head_idx_key = "vortex/tenants/tenant_fixes/head/HOT_HEAD.mbf.idx";
    let hot_head_idx_bytes = extra_map
        .get(hot_head_idx_key)
        .expect("HOT_HEAD.mbf.idx must be generated");

    for (label, bytes) in [
        ("entity.mbf.idx", entity_idx_bytes),
        ("HOT_HEAD.mbf.idx", hot_head_idx_bytes),
    ] {
        let header = MbfHeader::from_bytes(bytes).expect("valid MbfHeader");
        // Bug #7: non-zero max_timestamp_ms & non-zero footer HMAC
        assert!(
            header.max_timestamp_ms > 0,
            "{label} must have max_timestamp_ms > 0"
        );
        let footer = MbfFooterTrailer::from_bytes(&bytes[bytes.len() - 64..])
            .expect("valid MbfFooterTrailer");
        assert!(
            footer.hmac_tag_truncated.iter().any(|&b| b != 0),
            "{label} must have non-zero 24-byte hmac_tag_truncated"
        );

        let dir_off = footer.directory_offset as usize;
        let dir_len = footer.directory_len as usize;
        let dir = MbfDirectoryIndex::from_bytes(&bytes[dir_off..dir_off + dir_len])
            .expect("valid MbfDirectoryIndex");

        // Bug #5: Zone5aBiSynopses grouped by domain attribute without sys/created_at or sys/version pollution
        let z5a_off = dir.zones.zone5_synopsis_offset as usize;
        let z5a_len = dir.zones.zone5_synopsis_len as usize;
        let synopses =
            MbfHyperSynopsis512::decode_zone_binary_slice(&bytes[z5a_off..z5a_off + z5a_len])
                .expect("decode Zone5a synopses");
        assert_eq!(
            synopses.len(),
            1,
            "{label} must have 1 synopsis for the single numeric domain attribute (useful_life_months)"
        );
        assert_eq!(synopses[0].min_val, 120.0);
        assert_eq!(synopses[0].max_val, 120.0);
        assert_eq!(synopses[0].pebay_mean_m1, 120.0);
    }

    // Bug #1: Entity `.mbf.idx` Zone5b has exactly 1 consolidated document with real title and code
    let entity_footer =
        MbfFooterTrailer::from_bytes(&entity_idx_bytes[entity_idx_bytes.len() - 64..])
            .expect("entity footer");
    let entity_dir = MbfDirectoryIndex::from_bytes(
        &entity_idx_bytes[entity_footer.directory_offset as usize
            ..entity_footer.directory_offset as usize + entity_footer.directory_len as usize],
    )
    .expect("entity dir");
    let z5b_off = entity_dir.zones.zone5b_search_offset as usize;
    let z5b_len = entity_dir.zones.zone5b_search_len as usize;
    let entity_fts =
        MbfHybridSearchIndex::decode_zone_binary(&entity_idx_bytes[z5b_off..z5b_off + z5b_len])
            .expect("deserialize entity Zone5b");
    assert_eq!(
        entity_fts.docs.len(),
        1,
        "Entity .mbf.idx must have exactly 1 consolidated document"
    );
    assert_eq!(entity_fts.docs[0].entity_id, asset1_id);
    assert_eq!(entity_fts.docs[0].title, "Turbina Principal Norte");
    assert_eq!(
        entity_fts.docs[0].code,
        MbfHybridSearchIndex::canonicalize_alphanumeric_code("A-UNICO-001")
    );
    assert!(
        entity_fts.docs[0]
            .description
            .contains("Equipo critico de generacion"),
        "Consolidated description must include description text"
    );

    // Bug #3: Tenant `HOT_HEAD.mbf.idx` Zone5b must NOT contain outbox_event entities
    let hh_footer =
        MbfFooterTrailer::from_bytes(&hot_head_idx_bytes[hot_head_idx_bytes.len() - 64..])
            .expect("hh footer");
    let hh_dir = MbfDirectoryIndex::from_bytes(
        &hot_head_idx_bytes[hh_footer.directory_offset as usize
            ..hh_footer.directory_offset as usize + hh_footer.directory_len as usize],
    )
    .expect("hh dir");
    let hh_z5b_off = hh_dir.zones.zone5b_search_offset as usize;
    let hh_z5b_len = hh_dir.zones.zone5b_search_len as usize;
    let hh_fts = MbfHybridSearchIndex::decode_zone_binary(
        &hot_head_idx_bytes[hh_z5b_off..hh_z5b_off + hh_z5b_len],
    )
    .expect("deserialize HOT_HEAD Zone5b");
    assert_eq!(
        hh_fts.docs.len(),
        2,
        "HOT_HEAD.mbf.idx must contain only the 2 assets and 0 outbox_event documents"
    );
    assert!(
        hh_fts
            .docs
            .iter()
            .all(|d| d.entity_id != tx1_res.mutation_ulid),
        "outbox_event must never be indexed in Zone5b"
    );

    // 6. Bug #4 verification: `HOT_HEAD_c{cell_id}.mbf` has MBF_FLAG_HOT_HEAD and contains both assets
    let registry_ref = global();
    let asset_model = registry_ref.get_model("asset").expect("asset model");
    let cell_id = DomainCellRouter::classify_model_topology(asset_model, asset1_id);
    let cell_key = format!("vortex/tenants/tenant_fixes/head/HOT_HEAD_c{cell_id}.mbf");
    let cell_bytes = extra_map
        .get(&cell_key)
        .expect("HOT_HEAD_c{cell_id}.mbf must be generated");
    let cell_hdr = MbfHeader::from_bytes(cell_bytes).expect("valid cell MbfHeader");
    assert!(
        (cell_hdr.flags & MBF_FLAG_HOT_HEAD) != 0,
        "HOT_HEAD_c{{cell_id}}.mbf must have MBF_FLAG_HOT_HEAD set"
    );
    assert!(
        cell_hdr.row_count >= 2,
        "HOT_HEAD_c{{cell_id}}.mbf must aggregate rows from both assets in the cell, got {}",
        cell_hdr.row_count
    );

    // 7. Bug #8 verification: Quota counter decrement in a newer wire snapshot propagates on remote sync
    storage
        .quota_settle(table, "tenant_fixes", "asset", 2)
        .await
        .expect("decrement quota counter to 2 on remote");
    let (remote_vortex_bytes, _) = storage
        .export_flush_artifacts(table, "vortex")
        .expect("export remote snapshot with counter = 2");

    // Local instance temporarily has higher counter (5) at an older epoch, then syncs the newer remote snapshot (2)
    let local_table = "metri_fixes_local_table";
    storage
        .quota_settle(local_table, "tenant_fixes", "asset", 5)
        .await
        .expect("set local counter to 5");
    let before_dec = storage
        .quota_read_many(local_table, "tenant_fixes", &["asset".to_string()])
        .await
        .expect("read before sync");
    assert_eq!(before_dec.get("asset").copied(), Some(5));

    storage
        .apply_remote_vortex_envelope(local_table, &remote_vortex_bytes, "etag-newer")
        .expect("apply newer remote vortex envelope");
    let after_dec = storage
        .quota_read_many(local_table, "tenant_fixes", &["asset".to_string()])
        .await
        .expect("read after sync");
    assert_eq!(
        after_dec.get("asset").copied(),
        Some(2),
        "Quota counter must decrement to 2 when remote snapshot has a newer epoch_id"
    );
}

#[tokio::test]
async fn mbf_1000_assets_linear_scalability_and_outbox_exclusion() {
    use metri_engine::eav::fts::searcher::{
        export_tenant_zone5b_bytes, fts_search_entity_ids, register_fts_documents_batch,
    };
    use metri_engine::mbf::zones::hybrid_search::{IndexedEntityDocument, MbfHybridSearchIndex};
    use std::time::Instant;

    let t0 = Instant::now();
    let mut batch_docs = Vec::with_capacity(1_000);
    for i in 0..1_000u32 {
        batch_docs.push(IndexedEntityDocument {
            entity_id: format!("01J99999999999999999{:06}", i),
            row_id: i,
            code: format!("ACT10K{:05}", i),
            title: format!("Compresor Centrifugo Industrial {:05}", i),
            description: format!(
                "Equipo rotativo de alta eficiencia serie {:05} planta norte",
                i
            ),
            embedding: None,
            coordinates_wgs84: Some((4.60 + (f64::from(i) * 0.0001), -74.08)),
        });
    }
    register_fts_documents_batch("tenant_scale_1k", batch_docs);
    let elapsed_index = t0.elapsed();
    assert!(
        elapsed_index.as_millis() < 1_500,
        "Batch FTS indexing of 1,000 entities must be linear and fast, took {:?}",
        elapsed_index
    );

    let z5b_bytes = export_tenant_zone5b_bytes("tenant_scale_1k");
    let decoded = MbfHybridSearchIndex::decode_zone_binary(&z5b_bytes).expect("decode 1k Zone5b");
    assert_eq!(decoded.docs.len(), 1_000);

    let hits = fts_search_entity_ids("ACT10K00999", "tenant_scale_1k")
        .await
        .expect("fts search");
    assert!(!hits.is_empty());
    assert_eq!(hits[0].entity_id, "01J99999999999999999000999");

    // Verify outbox_event exclusion from vortex wire snapshot and cold-start FTS reconstruction
    use metri_engine::eav::types::datom::DatomValue;
    use metri_engine::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use metri_engine::mbf::storage::{
        decode_mbf_envelope, MbfPartitionWireSnapshot, MbfStorageClient,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    let storage = Arc::new(MbfStorageClient::new_stub());
    let src_table = "metri_scale_src_table";
    let writer = EavWriter::new(Arc::clone(&storage), src_table);

    let mut loc_attrs = HashMap::new();
    loc_attrs.insert(
        "name".to_string(),
        DatomValue::Str("Planta Sur".to_string()),
    );
    loc_attrs.insert(
        "status".to_string(),
        DatomValue::Str("OPERATIONAL".to_string()),
    );
    writer
        .transact(TransactPayload {
            tenant_id: "tenant_cold_sync".to_string(),
            entity_id: Some("01J99900000000000000LOC999".to_string()),
            entity_type: "location".to_string(),
            attrs: loc_attrs,
            op: TransactOp::Create,
            suppress_events: true,
        })
        .await
        .expect("create location");

    let mut asset_attrs = HashMap::new();
    asset_attrs.insert(
        "name".to_string(),
        DatomValue::Str("Generador Sincrono".to_string()),
    );
    asset_attrs.insert("tag".to_string(), DatomValue::Str("A-COLD-777".to_string()));
    asset_attrs.insert(
        "criticality".to_string(),
        DatomValue::Str("HIGH".to_string()),
    );
    asset_attrs.insert("status".to_string(), DatomValue::Str("ACTIVE".to_string()));
    asset_attrs.insert(
        "location_id".to_string(),
        DatomValue::Str("01J99900000000000000LOC999".to_string()),
    );
    asset_attrs.insert("purchase_cost_cents".to_string(), DatomValue::Long(500));

    let tx_res = writer
        .transact(TransactPayload {
            tenant_id: "tenant_cold_sync".to_string(),
            entity_id: Some("01J99900000000000000AST777".to_string()),
            entity_type: "asset".to_string(),
            attrs: asset_attrs,
            op: TransactOp::Create,
            suppress_events: false,
        })
        .await
        .expect("create asset with outbox_event");
    assert_eq!(tx_res.outbox_count, 1);

    let (vortex_bytes, _) = storage
        .export_flush_artifacts(src_table, "vortex")
        .expect("export flush artifacts");
    let (_, raw_snap) = decode_mbf_envelope(&vortex_bytes).expect("decode vortex envelope");
    let snap = MbfPartitionWireSnapshot::decode_binary(raw_snap).expect("decode wire snapshot");
    let outbox_pk = format!("T#tenant_cold_sync#E#{}", tx_res.mutation_ulid);
    assert!(
        snap.items.iter().all(|((pk, _), _)| pk != &outbox_pk),
        "to_wire_snapshot must exclude ephemeral outbox_event entities from vortex snapshot"
    );
}
