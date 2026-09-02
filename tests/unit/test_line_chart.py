#!/usr/bin/env python3
import sys, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

from grpc_web_client import GrpcWebStub

PROTO_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

def main():
    print("=" * 60)
    print("TESTING LINE CHART: chart-line-q")
    print("=" * 60)
    stub = GrpcWebStub("127.0.0.1:9090")
    
    req = pb.QueryRequest()
    req.tenant_id = "golden-tenant-benchmark"
    
    q = req.queries["chart-line-q"]
    q.tenant_id = "golden-tenant-benchmark"
    q.entity = "asset"
    q.output_cast = pb.TIMESERIES
    q.viz = "line"
    
    # Dimension: created_at, daily interval
    d = q.dimensions.add()
    d.entity = "asset"
    d.attribute = "created_at"
    d.interval = "day"
    d.label_template = "{{created_at}}"
    
    # Metric: count(id)
    m = q.metrics.add()
    m.entity = "asset"
    m.attribute = "id"
    m.aggregation = pb.COUNT
    m.name = "Registros Nuevos"
    
    # Timeframe: Last 30 days, Bogota timezone
    q.time_frame.type = pb.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    q.time_frame.timezone = "America/Bogota"
    
    print("-> Sending query request...")
    try:
        results = stub.Query(req)
        print(f"<- Received {len(results)} chunks.")
        for res in results:
            for key, batch_res in res.batch_results.items():
                print(f"\n--- WIDGET: {key} (Success: {batch_res.status.success}) ---")
                if batch_res.status.success:
                    if batch_res.viz_ext:
                        ve = batch_res.viz_ext
                        print(f"  [VizMeta Type]: {ve.type}")
                        if ve.HasField("chart"):
                            chart = ve.chart
                            print(f"    - xDimension: {chart.x_dimension}")
                            print(f"    - yDimensions: {list(chart.y_dimensions)}")
                            print(f"    - fillGaps: {chart.fill_gaps}")
                    if batch_res.HasField("data"):
                        data = batch_res.data
                        if data.columns:
                            print(f"  [Table Columns]: {[{'key': c.key, 'label': c.label, 'type': c.type} for c in data.columns]}")
                        
                        strategy = data.WhichOneof("payload_strategy")
                        if strategy == "rows_json":
                            rows = data.rows_json.iter
                            print(f"  [Rows count]: {len(rows)}")
                            for i, r in enumerate(rows[:10]):
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
