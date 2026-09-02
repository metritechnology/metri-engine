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

def main():
    print("=" * 60)
    print("VERIFYING ANALYTICS WIDGETS DATA FROM RUST ENGINE")
    print("=" * 60)
    stub = GrpcWebStub("127.0.0.1:9090")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    
    # 1. kpi-total-assets-q
    q1 = req.queries["kpi-total-assets-q"]
    q1.tenant_id = "golden-tenant-benchmark"
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

    # 2. kpi-avg-health-q
    q2 = req.queries["kpi-avg-health-q"]
    q2.tenant_id = "golden-tenant-benchmark"
    q2.entity = "asset"
    metric2 = q2.metrics.add()
    metric2.entity = "asset"
    metric2.attribute = "health_score"
    metric2.aggregation = pb.AVG
    metric2.name = "Health Score"
    q2.output_cast = pb.KPI
    q2.viz = "kpi"

    # 3. kpi-max-reading-q
    q3 = req.queries["kpi-max-reading-q"]
    q3.tenant_id = "golden-tenant-benchmark"
    q3.entity = "asset"
    metric3 = q3.metrics.add()
    metric3.entity = "asset"
    metric3.attribute = "current_meter_reading"
    metric3.aggregation = pb.MAX
    metric3.name = "Lectura (Max)"
    q3.output_cast = pb.KPI
    q3.viz = "kpi"

    # 4. chart-pie-q (criticality distribution)
    q4 = req.queries["chart-pie-q"]
    q4.tenant_id = "golden-tenant-benchmark"
    q4.entity = "asset"
    dim4 = q4.dimensions.add()
    dim4.entity = "asset"
    dim4.attribute = "criticality"
    dim4.label_template = "Nivel: {{criticality}}"
    metric4 = q4.metrics.add()
    metric4.entity = "asset"
    metric4.attribute = "id"
    metric4.aggregation = pb.COUNT
    metric4.name = "Cantidad"
    q4.output_cast = pb.PIE
    q4.viz = "pie"

    # 5. chart-bar-q (category distribution)
    q5 = req.queries["chart-bar-q"]
    q5.tenant_id = "golden-tenant-benchmark"
    q5.entity = "asset"
    dim5 = q5.dimensions.add()
    dim5.entity = "asset"
    dim5.attribute = "category"
    dim5.label_template = "Categoría: {{category}}"
    metric5 = q5.metrics.add()
    metric5.entity = "asset"
    metric5.attribute = "id"
    metric5.aggregation = pb.COUNT
    metric5.name = "Total de Activos"
    q5.output_cast = pb.OUTPUT_CAST_UNSPECIFIED
    q5.viz = "bar"

    print("-> Sending multi-widget query request to engine...")
    try:
        results = stub.Query(req)
        print(f"<- Received {len(results)} chunks.")
        for res in results:
            for key, batch_res in res.batch_results.items():
                print(f"\n--- WIDGET: {key} (Success: {batch_res.status.success}) ---")
                if batch_res.status.success:
                    if batch_res.viz_ext and batch_res.viz_ext.HasField("signal"):
                        sig = batch_res.viz_ext.signal
                        change_str = f"{sig.intelligence.percentage}%" if sig.HasField("intelligence") else "N/A"
                        print(f"  [KPI/Gauge Signal] Value: {sig.value}, Previous: {sig.previous_value}, Change: {change_str}")
                    if batch_res.HasField("data"):
                        data = batch_res.data
                        if data.columns:
                            print(f"  [Table Columns]: {[c.label for c in data.columns]}")
                        
                        strategy = data.WhichOneof("payload_strategy")
                        if strategy == "rows_json":
                            rows = data.rows_json.iter
                            print(f"  [Rows count]: {len(rows)}")
                            for i, r in enumerate(rows[:5]):
                                row_vals = []
                                for val in r.values:
                                    if val.HasField("number_value"):
                                        row_vals.append(val.number_value)
                                    elif val.HasField("string_value"):
                                        row_vals.append(val.string_value)
                                    elif val.HasField("bool_value"):
                                        row_vals.append(val.bool_value)
                                    else:
                                        row_vals.append(None)
                                print(f"    Row {i+1}: {row_vals}")
                else:
                    print(f"  [Error]: {batch_res.status.error_message}")
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
