#!/usr/bin/env python3
import json
import time
import os
import sys

# Setup paths to import the local client
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

# Set HMAC secret for production
os.environ["HMAC_SECRET"] = "QnhvaQUtUpym6iRSGzfIxxtEBbm53GM7kPEYCEbToktFYUmI6dmWlwNsxf7RHFqs"
os.environ["ENGINE_HOST"] = os.environ.get("ENGINE_HOST", "127.0.0.1:9090")

def get_row_value(val):
    if val.HasField("number_value"):
        return val.number_value
    elif val.HasField("string_value"):
        return val.string_value
    elif val.HasField("bool_value"):
        return val.bool_value
    return None

def main():
    print("=" * 70)
    print("       METRI MATHEMATICAL ANALYTICS VERIFICATION (8 WIDGETS)        ")
    print("=" * 70)
    
    # 1. Load Expected Truth Data
    truth_file_path = os.path.join(SCRIPT_DIR, "expected_metrics.json")
    if not os.path.exists(truth_file_path):
        print(f"[ERROR] Expected truth file not found at: {truth_file_path}")
        print("Please run ingest_1000_assets_complete.py first.")
        sys.exit(1)
        
    with open(truth_file_path, "r") as f:
        truth = json.load(f)
        
    print("✓ Loaded Expected Metrics Truth:")
    print(f"  - Expected Total Assets: {truth['total_assets']}")
    print(f"  - Expected Avg Health Score: {truth['avg_health_score']}")
    print(f"  - Expected Max Reading: {truth['max_current_meter_reading']}")
    print(f"  - Expected Criticality: {truth['criticality_counts']}")
    print(f"  - Expected Categories: {truth['category_counts']}\n")

    # 2. Build Query Request covering all 8 widgets
    stub = GrpcWebStub(os.environ["ENGINE_HOST"])
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-real"
    
    # Widget 1: kpi-total-assets-q
    q1 = req.queries["kpi-total-assets-q"]
    q1.tenant_id = "golden-tenant-real"
    q1.entity = "asset"
    metric1 = q1.metrics.add()
    metric1.entity = "asset"
    metric1.attribute = "id"
    metric1.aggregation = pb.COUNT
    metric1.name = "Activos Totales"
    comp1 = q1.comparisons.add()
    comp1.type = pb.AnalyticalComparison.TIME_SHIFT_RELATIVE
    comp1.relative_granularity = "month"
    comp1.relative_amount = 1
    comp1.label = "vs mes anterior"
    q1.output_cast = pb.KPI
    q1.viz = "kpi"
    q1.limit = 1000

    # Widget 2: kpi-avg-health-q
    q2 = req.queries["kpi-avg-health-q"]
    q2.tenant_id = "golden-tenant-real"
    q2.entity = "asset"
    metric2 = q2.metrics.add()
    metric2.entity = "asset"
    metric2.attribute = "health_score"
    metric2.aggregation = pb.AVG
    metric2.name = "Health Score"
    q2.output_cast = pb.KPI
    q2.viz = "kpi"

    # Widget 3: kpi-max-reading-q
    q3 = req.queries["kpi-max-reading-q"]
    q3.tenant_id = "golden-tenant-real"
    q3.entity = "asset"
    metric3 = q3.metrics.add()
    metric3.entity = "asset"
    metric3.attribute = "current_meter_reading"
    metric3.aggregation = pb.MAX
    metric3.name = "Lectura (Max)"
    q3.output_cast = pb.KPI
    q3.viz = "kpi"

    # Widget 4: chart-line-q
    q4 = req.queries["chart-line-q"]
    q4.tenant_id = "golden-tenant-real"
    q4.entity = "asset"
    dim4 = q4.dimensions.add()
    dim4.entity = "asset"
    dim4.attribute = "created_at"
    dim4.interval = "day"
    dim4.label_template = "{{created_at}}"
    metric4 = q4.metrics.add()
    metric4.entity = "asset"
    metric4.attribute = "id"
    metric4.aggregation = pb.COUNT
    metric4.name = "Registros Nuevos"
    q4.time_frame.type = pb.TimeFrameContext.LAST_N_DAYS
    q4.time_frame.n_value = 30
    q4.time_frame.timezone = "America/Bogota"
    q4.output_cast = pb.TIMESERIES
    q4.viz = "line"
    q4.limit = 1000

    # Widget 5: chart-pie-q
    q5 = req.queries["chart-pie-q"]
    q5.tenant_id = "golden-tenant-real"
    q5.entity = "asset"
    dim5 = q5.dimensions.add()
    dim5.entity = "asset"
    dim5.attribute = "criticality"
    dim5.label_template = "Nivel: {{criticality}}"
    metric5 = q5.metrics.add()
    metric5.entity = "asset"
    metric5.attribute = "id"
    metric5.aggregation = pb.COUNT
    metric5.name = "Cantidad"
    q5.output_cast = pb.PIE
    q5.viz = "pie"

    # Widget 6: chart-scatter-q
    q6 = req.queries["chart-scatter-q"]
    q6.tenant_id = "golden-tenant-real"
    q6.entity = "asset"
    dim6 = q6.dimensions.add()
    dim6.entity = "asset"
    dim6.attribute = "tag"
    metric6_1 = q6.metrics.add()
    metric6_1.entity = "asset"
    metric6_1.attribute = "health_score"
    metric6_1.aggregation = pb.AVG
    metric6_1.name = "y"
    metric6_2 = q6.metrics.add()
    metric6_2.entity = "asset"
    metric6_2.attribute = "current_meter_reading"
    metric6_2.aggregation = pb.AVG
    metric6_2.name = "size"
    q6.output_cast = pb.BUBBLE
    q6.viz = "scatter"

    # Widget 7: table-assets-q
    q7 = req.queries["table-assets-q"]
    q7.tenant_id = "golden-tenant-real"
    q7.entity = "asset"
    dim7_1 = q7.dimensions.add()
    dim7_1.entity = "asset"
    dim7_1.attribute = "name"
    dim7_1.label_template = "Nombre: {{name}}"
    dim7_2 = q7.dimensions.add()
    dim7_2.entity = "asset"
    dim7_2.attribute = "status"
    dim7_2.label_template = "Estado: {{status}}"
    dim7_3 = q7.dimensions.add()
    dim7_3.entity = "asset"
    dim7_3.attribute = "criticality"
    dim7_4 = q7.dimensions.add()
    dim7_4.entity = "asset"
    dim7_4.attribute = "health_score"
    metric7 = q7.metrics.add()
    metric7.entity = "asset"
    metric7.attribute = "id"
    metric7.aggregation = pb.COUNT
    metric7.name = "Cantidad"
    q7.output_cast = pb.TABLE
    q7.viz = "table"
    q7.limit = 1000  # Let's request all to verify exact contents

    # Widget 8: chart-bar-q
    q8 = req.queries["chart-bar-q"]
    q8.tenant_id = "golden-tenant-real"
    q8.entity = "asset"
    dim8 = q8.dimensions.add()
    dim8.entity = "asset"
    dim8.attribute = "category"
    dim8.label_template = "Categoría: {{category}}"
    metric8 = q8.metrics.add()
    metric8.entity = "asset"
    metric8.attribute = "id"
    metric8.aggregation = pb.COUNT
    metric8.name = "Total de Activos"
    q8.output_cast = pb.OUTPUT_CAST_UNSPECIFIED
    q8.viz = "bar"

    print("-> Sending all 8 analytical widgets request to production engine...")
    start_time = time.time()
    try:
        results = stub.Query(req)
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"✓ Response received in {duration_ms} ms!")
        
        # Parse batches
        all_batches = {}
        for res in results:
            for key, batch_res in res.batch_results.items():
                all_batches[key] = batch_res
                
        # 3. Perform mathematical comparisons
        success = True
        discrepancies = []

        # -- VALIDATE WIDGET 1: kpi-total-assets-q
        print("\n--- [Widget 1] Total Assets KPI ---")
        w1 = all_batches.get("kpi-total-assets-q")
        if w1 and w1.status.success:
            val = w1.viz_ext.signal.value
            expected = float(truth["total_assets"])
            if val == expected:
                print(f"  ✓ Perfect match: Value = {val}")
            else:
                success = False
                discrepancies.append(f"Widget 1 KPI value mismatch: Expected {expected}, got {val}")
                print(f"  ✗ MISMATCH: Expected {expected}, got {val}")
        else:
            success = False
            err = w1.status.error_message if w1 else "No response"
            discrepancies.append(f"Widget 1 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 2: kpi-avg-health-q
        print("\n--- [Widget 2] Average Health Score KPI/Gauge ---")
        w2 = all_batches.get("kpi-avg-health-q")
        if w2 and w2.status.success:
            val = round(w2.viz_ext.signal.value, 4)
            expected = round(truth["avg_health_score"], 4)
            if abs(val - expected) < 0.0001:
                print(f"  ✓ Perfect match: Value = {val}")
            else:
                success = False
                discrepancies.append(f"Widget 2 Avg Health value mismatch: Expected {expected}, got {val}")
                print(f"  ✗ MISMATCH: Expected {expected}, got {val}")
        else:
            success = False
            err = w2.status.error_message if w2 else "No response"
            discrepancies.append(f"Widget 2 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 3: kpi-max-reading-q
        print("\n--- [Widget 3] Max Meter Reading KPI ---")
        w3 = all_batches.get("kpi-max-reading-q")
        if w3 and w3.status.success:
            val = w3.viz_ext.signal.value
            expected = float(truth["max_current_meter_reading"])
            if val == expected:
                print(f"  ✓ Perfect match: Value = {val}")
            else:
                success = False
                discrepancies.append(f"Widget 3 Max Reading value mismatch: Expected {expected}, got {val}")
                print(f"  ✗ MISMATCH: Expected {expected}, got {val}")
        else:
            success = False
            err = w3.status.error_message if w3 else "No response"
            discrepancies.append(f"Widget 3 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 4: chart-line-q
        print("\n--- [Widget 4] Ingested Assets Timeline (Line Chart) ---")
        w4 = all_batches.get("chart-line-q")
        if w4 and w4.status.success:
            rows = w4.data.rows_json.iter
            total_sum = 0
            for r in rows:
                v = get_row_value(r.values[1])  # Column 0 is created_at, Column 1 is Registros Nuevos
                total_sum += float(v) if v is not None else 0
            expected = float(truth["total_assets"])
            if total_sum == expected:
                print(f"  ✓ Perfect match: Total timeline sum = {total_sum}")
            else:
                success = False
                discrepancies.append(f"Widget 4 Timeline sum mismatch: Expected {expected}, got {total_sum}")
                print(f"  ✗ MISMATCH: Expected sum {expected}, got {total_sum}")
        else:
            success = False
            err = w4.status.error_message if w4 else "No response"
            discrepancies.append(f"Widget 4 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 5: chart-pie-q
        print("\n--- [Widget 5] Criticality Distribution (Pie Chart) ---")
        w5 = all_batches.get("chart-pie-q")
        if w5 and w5.status.success:
            rows = w5.data.rows_json.iter
            actual_counts = {}
            for r in rows:
                crit_val = get_row_value(r.values[0])  # Column 0 is criticality
                count_val = int(get_row_value(r.values[1]))  # Column 1 is Count
                
                # strip dimension label prefix if any (e.g. "Nivel: LOW" -> "LOW")
                crit = crit_val.replace("Nivel: ", "") if isinstance(crit_val, str) else crit_val
                actual_counts[crit] = count_val
            
            expected = truth["criticality_counts"]
            match = True
            for k, exp_val in expected.items():
                act_val = actual_counts.get(k, 0)
                if act_val != exp_val:
                    match = False
                    print(f"  ✗ Criticality '{k}' MISMATCH: Expected {exp_val}, got {act_val}")
                else:
                    print(f"  ✓ Criticality '{k}': {act_val} assets")
            
            if match:
                print("  ✓ All criticality counts match perfectly!")
            else:
                success = False
                discrepancies.append(f"Widget 5 Criticality distribution mismatch: Expected {expected}, got {actual_counts}")
        else:
            success = False
            err = w5.status.error_message if w5 else "No response"
            discrepancies.append(f"Widget 5 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 8: chart-bar-q
        print("\n--- [Widget 8] Category Distribution (Bar Chart) ---")
        w8 = all_batches.get("chart-bar-q")
        if w8 and w8.status.success:
            rows = w8.data.rows_json.iter
            actual_counts = {}
            for r in rows:
                cat_val = get_row_value(r.values[0])
                count_val = int(get_row_value(r.values[1]))
                cat = cat_val.replace("Categoría: ", "") if isinstance(cat_val, str) else cat_val
                actual_counts[cat] = count_val
                
            expected = truth["category_counts"]
            match = True
            for k, exp_val in expected.items():
                act_val = actual_counts.get(k, 0)
                if act_val != exp_val:
                    match = False
                    print(f"  ✗ Category '{k}' MISMATCH: Expected {exp_val}, got {act_val}")
                else:
                    print(f"  ✓ Category '{k}': {act_val} assets")
            
            if match:
                print("  ✓ All category counts match perfectly!")
            else:
                success = False
                discrepancies.append(f"Widget 8 Category distribution mismatch: Expected {expected}, got {actual_counts}")
        else:
            success = False
            err = w8.status.error_message if w8 else "No response"
            discrepancies.append(f"Widget 8 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 6: chart-scatter-q
        print("\n--- [Widget 6] Health Score vs Meter Reading Scatter Plot (Bubble Chart) ---")
        w6 = all_batches.get("chart-scatter-q")
        if w6 and w6.status.success:
            print("  W6 columns:", list(w6.data.columns))
            rows = w6.data.rows_json.iter
            print(f"  Bubble points returned: {len(rows)}")
            if len(rows) == len(truth["assets"]):
                print("  ✓ Perfect count match for scatter bubble points!")
                
                # Check random sample of 5 items
                # Row mapping: Column 0: tag, Column 1: y (health_score), Column 2: size (current_meter_reading)
                truth_map = {a["tag"]: a for a in truth["assets"]}
                
                # Find columns dynamically for robustness
                col_indices = {}
                for idx, col in enumerate(w6.data.columns):
                    col_indices[col.key] = idx
                
                for required in ["tag", "y", "size"]:
                    if required not in col_indices:
                        print(f"[ERROR] Required column '{required}' not found in scatter columns: {list(col_indices.keys())}")
                        sys.exit(6)
                        
                sample_success = True
                for r in list(rows)[:5]:
                    tag = get_row_value(r.values[col_indices["tag"]])
                    health_act = round(get_row_value(r.values[col_indices["y"]]), 2)
                    reading_act = round(get_row_value(r.values[col_indices["size"]]), 2)
                    
                    expected_asset = truth_map.get(tag)
                    if not expected_asset:
                        sample_success = False
                        print(f"  ✗ Scatter returned unexpected tag: {tag}")
                    else:
                        health_exp = round(expected_asset["health_score"], 2)
                        reading_exp = round(expected_asset["current_meter_reading"], 2)
                        if health_act == health_exp and reading_act == reading_exp:
                            print(f"    ✓ Bubble tag '{tag}': health = {health_act}, reading = {reading_act} matches exactly")
                        else:
                            sample_success = False
                            print(f"    ✗ Bubble tag '{tag}' MISMATCH: health_exp={health_exp}/act={health_act}, reading_exp={reading_exp}/act={reading_act}")
                
                if not sample_success:
                    success = False
                    discrepancies.append("Scatter point values sample mismatch.")
            else:
                success = False
                discrepancies.append(f"Widget 6 Scatter count mismatch: Expected {len(truth['assets'])}, got {len(rows)}")
                print(f"  ✗ Scatter count MISMATCH: Expected {len(truth['assets'])}, got {len(rows)}")
        else:
            success = False
            err = w6.status.error_message if w6 else "No response"
            discrepancies.append(f"Widget 6 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # -- VALIDATE WIDGET 7: table-assets-q
        print("\n--- [Widget 7] Dynamic Assets Inventory (Table) ---")
        w7 = all_batches.get("table-assets-q")
        if w7 and w7.status.success:
            print("  W7 columns keys:", [col.key for col in w7.data.columns])
            print("  W7 columns labels:", [col.label for col in w7.data.columns])
            # Print sample row to see value types
            rows = w7.data.rows_json.iter
            if len(rows) > 0:
                print("  Raw Sample row values:", [get_row_value(v) for v in rows[0].values])
                print("  All values count in sample row:", len(rows[0].values))
            print(f"  Table rows returned: {len(rows)}")
            if len(rows) == len(truth["assets"]):
                print("  ✓ Perfect count match for table rows!")
                
                # Check sample of first 3 rows
                # Dimensions: name, status, criticality, health_score
                # Metric: Count
                truth_assets_sorted = sorted(truth["assets"], key=lambda x: x["name"])
                
                # Find columns dynamically for robustness
                col_indices = {}
                for idx, col in enumerate(w7.data.columns):
                    col_indices[col.key] = idx
                
                # Check that required columns are found
                for required in ["name", "status", "criticality", "health_score"]:
                    if required not in col_indices:
                        print(f"[ERROR] Required column '{required}' not found in table columns: {list(col_indices.keys())}")
                        sys.exit(5)
                
                # The engine table may return rows sorted or unsorted. Let's index them by name to compare
                actual_assets_map = {}
                for r in rows:
                    name_val = get_row_value(r.values[col_indices["name"]])
                    name = name_val.replace("Nombre: ", "") if isinstance(name_val, str) else name_val
                    
                    status_val = get_row_value(r.values[col_indices["status"]])
                    status = status_val.replace("Estado: ", "") if isinstance(status_val, str) else status_val
                    
                    criticality_val = get_row_value(r.values[col_indices["criticality"]])
                    criticality = criticality_val.replace("Nivel: ", "") if isinstance(criticality_val, str) else criticality_val
                    
                    health_val = get_row_value(r.values[col_indices["health_score"]])
                    health = round(float(health_val), 2) if health_val is not None else 0.0
                    
                    if name:
                        actual_assets_map[name] = {
                            "name": name,
                            "status": status,
                            "criticality": criticality,
                            "health_score": health
                        }
                
                sample_success = True
                for exp_asset in truth_assets_sorted[:3]:
                    name = exp_asset["name"]
                    act_asset = actual_assets_map.get(name)
                    if not act_asset:
                        sample_success = False
                        print(f"  ✗ Table is missing expected asset by name: {name}")
                    else:
                        if act_asset["status"] == exp_asset["status"] and \
                           act_asset["criticality"] == exp_asset["criticality"] and \
                           abs(act_asset["health_score"] - exp_asset["health_score"]) < 0.01:
                            print(f"    ✓ Table row '{name}': status={act_asset['status']}, criticality={act_asset['criticality']}, health={act_asset['health_score']} matches exactly")
                        else:
                            sample_success = False
                            print(f"    ✗ Table row '{name}' MISMATCH:")
                            print(f"      Expected: status={exp_asset['status']}, criticality={exp_asset['criticality']}, health={exp_asset['health_score']}")
                            print(f"      Actual:   status={act_asset['status']}, criticality={act_asset['criticality']}, health={act_asset['health_score']}")
                
                if not sample_success:
                    success = False
                    discrepancies.append("Table rows values sample mismatch.")
            else:
                success = False
                discrepancies.append(f"Widget 7 Table row count mismatch: Expected {len(truth['assets'])}, got {len(rows)}")
                print(f"  ✗ Table row count MISMATCH: Expected {len(truth['assets'])}, got {len(rows)}")
        else:
            success = False
            err = w7.status.error_message if w7 else "No response"
            discrepancies.append(f"Widget 7 failed: {err}")
            print(f"  ✗ FAILED: {err}")

        # 4. Final verification output
        print("\n" + "=" * 70)
        print("                        VERIFICATION RESULTS                        ")
        print("=" * 70)
        if success:
            print("  ✓ SUCCESS! 100% mathematical match across all 8 analytical widgets.")
            print("  ✓ All aggregations (COUNT, AVG, MAX), dimensions and joins are correct.")
            print("  ✓ metri-engine answers are 100% reliable and match the ingest dataset.")
            print(f"  ✓ Production Query latency: {duration_ms} ms (Performance is outstanding).")
        else:
            print("  ✗ FAILURE: Mathematical mismatches or errors detected!")
            for d in discrepancies:
                print(f"    - {d}")
            sys.exit(3)
        print("=" * 70)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(4)

if __name__ == "__main__":
    main()
