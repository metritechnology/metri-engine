use insta::assert_debug_snapshot;
use proptest::prelude::*;
use serde_json::json;
use crate::janus::fbs;
use crate::janus::ast_compiler::compile_ast_fbs;
use crate::janus::plan_selector::{select_plan_fbs, EavQueryPlan};
use crate::janus::router::CedarCtx;
use crate::aegis::oltp::compiler::compile_native_plan_fbs;

// --- SNAPSHOT TESTS (Golden Master) ---

#[test]
fn test_snapshot_point_lookup_contract() {
    let mut req = fbs::AnalyticsRequestT::default();
    req.entity = Some("asset".to_string());
    
    // Simular un request con entity/ulid explícito
    let filter = fbs::FilterNodeT {
        criteria: Some(Box::new(fbs::FilterCriteriaT {
            field: Some("entity/ulid".to_string()),
            op_ref: fbs::FilterOperator::EQ,
            value: Some(Box::new(fbs::FilterValueT {
                string_val: Some("01JTESTULID".to_string()),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    req.filters = Some(vec![filter]);

    let cedar_ctx = CedarCtx {
        tenant_id: "tenant_999".to_string(),
        user_id: "user_1".to_string(),
        roles: vec![],
        is_super_master: false,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };

    // 1. Compilar AST (Añade ABAC y Tenant Isolation)
    let compiled_ast = compile_ast_fbs(&req, &cedar_ctx, &json!({})).expect("Debe compilar");
    
    // 2. Select Plan
    let plan = select_plan_fbs(&compiled_ast);
    
    // 3. Native Plan Compile
    let native_plan = compile_native_plan_fbs(&compiled_ast, "tenant_999");

    assert_debug_snapshot!("point_lookup_plan", plan);
    assert_debug_snapshot!("point_lookup_native", native_plan);
}

#[test]
fn test_snapshot_fts_search_contract() {
    let mut req = fbs::AnalyticsRequestT::default();
    req.entity = Some("work_order".to_string());
    req.search = Some("bomba hidraulica".to_string());
    
    let cedar_ctx = CedarCtx {
        tenant_id: "tenant_999".to_string(),
        user_id: "user_1".to_string(),
        roles: vec![],
        is_super_master: false,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };

    let schema = json!({
        "attributes": [
            {"name": "description", "type": "string", "fts": true}
        ]
    });

    let compiled_ast = compile_ast_fbs(&req, &cedar_ctx, &schema).expect("Debe compilar");
    let plan = select_plan_fbs(&compiled_ast);
    let native_plan = compile_native_plan_fbs(&compiled_ast, "tenant_999");

    assert_debug_snapshot!("fts_search_plan", plan);
    assert_debug_snapshot!("fts_search_native", native_plan);
}

// --- PROPERTY-BASED TESTS ---

proptest! {
    #[test]
    fn doesnt_crash_on_random_filter_trees(
        field in "[a-z]+(/[a-z]+)?",
        value in "[0-9a-zA-Z]+"
    ) {
        let mut req = fbs::AnalyticsRequestT::default();
        req.entity = Some("test_entity".to_string());
        
        let filter = fbs::FilterNodeT {
            criteria: Some(Box::new(fbs::FilterCriteriaT {
                field: Some(field),
                op_ref: fbs::FilterOperator::EQ,
                value: Some(Box::new(fbs::FilterValueT {
                    string_val: Some(value),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        req.filters = Some(vec![filter]);

        let cedar_ctx = CedarCtx {
            tenant_id: "tnt_rand".to_string(),
            user_id: "usr".to_string(),
            roles: vec![],
            is_super_master: false,
            cross_tenant_scope: "".to_string(),
            domain_boundaries: serde_json::json!({}),
        };

        // Property: Never panic, always return a Result and a valid EavQueryPlan
        let compiled_result = compile_ast_fbs(&req, &cedar_ctx, &json!({}));
        prop_assert!(compiled_result.is_ok());
        
        let compiled = compiled_result.unwrap();
        let _plan = select_plan_fbs(&compiled);
    }
}

// --- MEGA-BATCH PERFORMANCE BENCHMARK ---
#[test]
fn test_mega_batch_performance() {
    use std::time::Instant;
    use crate::grpc::pb::QueryRequest;
    use crate::grpc::pb::AnalyticsRequest as PbAnalyticsRequest;
    use crate::grpc::pb::MetricDefinition;
    use crate::grpc::translator;

    // 1. Simular un dashboard Mega-Batch con 100 widgets
    let num_widgets = 100;
    let mut queries = std::collections::HashMap::new();

    for i in 0..num_widgets {
        let widget_req = PbAnalyticsRequest {
            tenant_id: "tnt_prod".to_string(),
            entity: "work_order".to_string(),
            metrics: vec![
                MetricDefinition {
                    attribute: "cost".to_string(),
                    aggregation: 2, // SUM
                    name: "total_cost".to_string(),
                    ..Default::default()
                }
            ],
            limit: 100,
            output_cast: 1, // KPI
            ..Default::default()
        };
        queries.insert(format!("widget_{}", i), widget_req);
    }

    let request = QueryRequest {
        tenant_id: "tnt_test".to_string(),
        queries,
        ..Default::default()
    };

    // 2. Medir tiempo de traducción gRPC -> FlatBuffers Object API
    let start_translation = Instant::now();
    let fbs_map = translator::query_request_to_queries_map(&request).expect("Translation failed");
    let translation_duration = start_translation.elapsed();

    assert_eq!(fbs_map.len(), num_widgets);

    // 3. Medir tiempo de compilación AST y Selección de Plan para 100 queries
    let cedar_ctx = CedarCtx {
        tenant_id: "tnt_test".to_string(),
        user_id: "user_1".to_string(),
        roles: vec![],
        is_super_master: false,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };
    let schema = json!({});

    let start_compilation = Instant::now();
    for (_key, fbs_req) in fbs_map.iter() {
        let compiled_ast = compile_ast_fbs(fbs_req, &cedar_ctx, &schema).expect("AST Compilation failed");
        let _plan = select_plan_fbs(&compiled_ast);
        let _native_plan = compile_native_plan_fbs(&compiled_ast, "tnt_test");
    }
    let compilation_duration = start_compilation.elapsed();

    println!("Mega-Batch ({} queries) Performance:", num_widgets);
    println!(" - Protobuf to FBS Translation: {:?}", translation_duration);
    println!(" - AST Compilation & Plan Selection: {:?}", compilation_duration);
    println!(" - Total CPU Overhead: {:?}", translation_duration + compilation_duration);
    
    // Assert que el overhead para 100 widgets sea sub-milisegundo (o muy bajo)
    // En una máquina normal, esto debería ser del orden de microsegundos a <10ms.
    assert!(translation_duration.as_millis() < 10, "Traducción demoró mucho");
    assert!(compilation_duration.as_millis() < 10, "Compilación demoró mucho");
}

// --- TEST 100 CASOS AISLADOS + 1 MEGA-BATCH ---
#[test]
fn test_100_isolated_cases_and_megabatch() {
    use crate::grpc::pb::{
        QueryRequest, AnalyticsRequest as PbAnalyticsRequest, MetricDefinition,
        TimeFrameContext, FilterNode, FilterCriteria, FilterOperator,
        FilterValue, DimensionDefinition
    };
    use crate::grpc::translator;

    // Generamos los 100 casos como entidades independientes
    let mut isolated_cases = Vec::new();

    let output_casts = [1, 2, 3, 4, 5, 6]; // 1:KPI, 2:TABLE, 3:TIMESERIES(Line/Area), 4:PIE, 5:BUBBLE/TREE, 6:CSV
    let aggregations = [1, 2, 3, 4, 5];    // 1:SUM, 2:AVG, 3:COUNT, 4:MIN, 5:MAX
    let time_filters = [1, 2, 5, 7];       // 1:CUSTOM, 2:TODAY, 5:LAST_MINUTES, 7:LAST_DAYS
    let filter_ops = [
        FilterOperator::Eq as i32,
        FilterOperator::Gt as i32,
        FilterOperator::Between as i32,
        FilterOperator::Like as i32,
    ];

    let mut id_counter = 0;

    for &cast in &output_casts {
        for &agg in &aggregations {
            for &tf in &time_filters {
                for &f_op in &filter_ops {
                    let mut req = PbAnalyticsRequest {
                        tenant_id: "tnt_prod".to_string(),
                        entity: "work_order".to_string(),
                        metrics: vec![MetricDefinition {
                            attribute: "total_amount".to_string(),
                            aggregation: agg,
                            name: format!("agg_{}", agg),
                            ..Default::default()
                        }],
                        output_cast: cast,
                        time_frame: Some(TimeFrameContext {
                            r#type: tf,
                            n_value: 7,
                            start_ts: 1600000000,
                            end_ts: 1700000000,
                            timezone: "UTC".to_string(),
                        }),
                        ..Default::default()
                    };

                    // Dimensions para Area/Pie/Table/Tree
                    if cast == 2 || cast == 3 || cast == 4 || cast == 5 {
                        req.dimensions = vec![DimensionDefinition {
                            attribute: "status".to_string(),
                            ..Default::default()
                        }];
                    }

                    // Filtros y Comparaciones
                    if f_op == FilterOperator::Eq as i32 || f_op == FilterOperator::Gt as i32 {
                        req.filters = vec![FilterNode {
                            node: Some(crate::grpc::pb::filter_node::Node::Criteria(FilterCriteria {
                                field: "category".to_string(),
                                op_ref: f_op,
                                value: Some(FilterValue {
                                    kind: Some(crate::grpc::pb::filter_value::Kind::StringVal("urgent".to_string())),
                                }),
                            })),
                        }];
                    }

                    isolated_cases.push(req);
                    id_counter += 1;

                    if id_counter >= 100 { break; }
                }
                if id_counter >= 100 { break; }
            }
            if id_counter >= 100 { break; }
        }
        if id_counter >= 100 { break; }
    }

    // Contexto y Esquema compartido
    let cedar_ctx = CedarCtx {
        tenant_id: "tnt_prod".to_string(),
        user_id: "admin".to_string(),
        roles: vec![],
        is_super_master: true,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };
    let schema = serde_json::json!({});

    // PARTE 1: Testear 100 Casos Aislados
    let mut success_count = 0;
    for (i, case) in isolated_cases.iter().enumerate() {
        let mut queries = std::collections::HashMap::new();
        queries.insert("isolated_widget".to_string(), case.clone());
        
        let request = QueryRequest {
            tenant_id: "tnt_prod".to_string(),
            queries,
            ..Default::default()
        };

        let fbs_map = translator::query_request_to_queries_map(&request).expect("Translation failed");
        let fbs_req = fbs_map.get("isolated_widget").unwrap();

        let compiled_ast = compile_ast_fbs(fbs_req, &cedar_ctx, &schema)
            .unwrap_or_else(|e| panic!("AST Compilation failed for isolated case {}: {:?}", i, e));
        
        let _plan = select_plan_fbs(&compiled_ast);
        success_count += 1;
    }

    assert_eq!(success_count, 100, "Debieron pasar 100 tests aislados.");

    // PARTE 2: Testear MegaBatch con los 100 casos (MultiQuery)
    let mut mega_queries = std::collections::HashMap::new();
    for (i, case) in isolated_cases.into_iter().enumerate() {
        mega_queries.insert(format!("widget_{}", i), case);
    }

    let request = QueryRequest {
        tenant_id: "tnt_prod".to_string(),
        queries: mega_queries,
        ..Default::default()
    };

    let fbs_map = translator::query_request_to_queries_map(&request).expect("Megabatch Translation failed");
    assert_eq!(fbs_map.len(), 100);

    for (widget_id, fbs_req) in fbs_map.iter() {
        let compiled_ast = compile_ast_fbs(fbs_req, &cedar_ctx, &schema)
            .unwrap_or_else(|e| panic!("Megabatch AST Compilation failed for {}: {:?}", widget_id, e));
        let _plan = select_plan_fbs(&compiled_ast);
    }

    println!("Successfully tested 100 isolated cases AND 1 megabatch multiquery without errors.");
}

#[test]
fn test_tenant_ast_compilation() {
    let mut req = fbs::AnalyticsRequestT::default();
    req.entity = Some("tenant".to_string());

    let schema = json!({
        "attributes": [
            {"name": "name", "type": "string"}
        ]
    });

    // Case 1: System Tenant user (should not have tenant_id filter, allowing all)
    let system_ctx = CedarCtx {
        tenant_id: "system".to_string(),
        user_id: "usr_sys".to_string(),
        roles: vec![],
        is_super_master: false,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };
    let compiled_sys = compile_ast_fbs(&req, &system_ctx, &schema).expect("Debe compilar");
    // Verify that tenant_id is NOT in the filters (no tenant isolation filter for system tenant querying tenant)
    let sys_has_tenant_filter = compiled_sys.filters.as_ref()
        .map(|filters| filters.iter().any(|f| f.criteria.as_ref().map(|c| c.field.as_deref() == Some("tenant_id")).unwrap_or(false)))
        .unwrap_or(false);
    assert!(!sys_has_tenant_filter);

    // Case 2: Regular Tenant user (should have entity/ulid = tenant_id filter)
    let regular_ctx = CedarCtx {
        tenant_id: "tnt_01".to_string(),
        user_id: "usr_normal".to_string(),
        roles: vec![],
        is_super_master: false,
        cross_tenant_scope: "".to_string(),
        domain_boundaries: serde_json::json!({}),
    };
    let compiled_reg = compile_ast_fbs(&req, &regular_ctx, &schema).expect("Debe compilar");
    // Verify that entity/ulid = "tnt_01" is in the filters
    let reg_tenant_filter = compiled_reg.filters.as_ref()
        .and_then(|filters| filters.iter().find(|f| f.criteria.as_ref().map(|c| c.field.as_deref() == Some("entity/ulid")).unwrap_or(false)))
        .expect("Debe existir el filtro por entity/ulid para no-system users");
    
    assert_eq!(
        reg_tenant_filter.criteria.as_ref().unwrap().value.as_ref().unwrap().string_val.as_deref().unwrap(),
        "tnt_01"
    );
}
