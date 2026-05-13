import grpc
import time
import json
import logging
import google.protobuf.json_format as json_format
from metri_pb2_grpc import MetriServiceStub
import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def send_query(req, q_name, results_map):
    try:
        channel = grpc.insecure_channel('localhost:9090')
        stub = MetriServiceStub(channel)
        
        responses = stub.Query(req)
        for resp in responses:
            json_dict = json.loads(json_format.MessageToJson(resp, preserving_proto_field_name=True))
            if "batch_results" in json_dict:
                for k, v in json_dict["batch_results"].items():
                    results_map[k] = v
    except Exception as e:
        logging.error(f"Error executing query {q_name}: {e}")

def run_tests():
    logging.info("Construyendo Suite de 50 Tests OLAP (TimeFrame & Comparison)...")
    
    aggregations = [
        ("COUNT", metri_pb2.COUNT), ("SUM", metri_pb2.SUM), ("AVG", metri_pb2.AVG),
        ("MIN", metri_pb2.MIN), ("MAX", metri_pb2.MAX), ("MEDIAN", metri_pb2.MEDIAN),
        ("STD_DEV", metri_pb2.STD_DEV), ("VARIANCE", metri_pb2.VARIANCE),
        ("PERCENTILE_90", metri_pb2.PERCENTILE_90), ("PERCENTILE_95", metri_pb2.PERCENTILE_95)
    ]
    
    results_map = {}
    test_id = 1
    
    # 1. TimeFrame Relative (LAST_N_DAYS)
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Last30Days"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        tf.n_value = 30
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1

    # 2. TimeFrame This Month
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_ThisMonth"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.THIS_MONTH
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1

    # 3. TimeFrame Custom Range
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_CustomRange"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.CUSTOM_RANGE
        now_ms = int(time.time() * 1000)
        tf.start_ts = now_ms - 86400000 * 10
        tf.end_ts = now_ms + 86400000 * 10
        tf.timezone = "America/Bogota"
        
        send_query(req, q_name, results_map)
        test_id += 1
        
    # 4. Comparisons (Benchmark vs Average)
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_Benchmark"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
        tf.n_value = 15
        tf.timezone = "America/Bogota"
        
        comp = req.queries[q_name].comparisons.add()
        comp.type = metri_pb2.AnalyticalComparison.BENCHMARK
        comp.label = "vs Threshold 50.0"
        comp.benchmark_value = 50.0
        
        send_query(req, q_name, results_map)
        test_id += 1
        
    # 5. Comparisons (Time Shift Shortcut)
    for name, agg_enum in aggregations:
        req = metri_pb2.QueryRequest()
        req.tenant_id = "golden-tenant"
        q_name = f"Test_{test_id:02d}_{name}_CompareLastYear"
        q = req.queries[q_name].CopyFrom(metri_pb2.AnalyticsRequest(tenant_id="golden-tenant", entity="meter_reading", output_cast=metri_pb2.TABLE))
        
        m = req.queries[q_name].metrics.add()
        m.attribute = "reading_value"
        m.aggregation = agg_enum
        
        tf = req.queries[q_name].time_frame
        tf.type = metri_pb2.TimeFrameContext.THIS_MONTH
        tf.timezone = "America/Bogota"
        
        comp = req.queries[q_name].comparisons.add()
        comp.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
        comp.label = "vs Same Period Last Year"
        comp.shortcut = metri_pb2.AnalyticalComparison.SAME_PERIOD_LAST_YEAR
        
        send_query(req, q_name, results_map)
        test_id += 1

    print(f"# Reporte de Suite de Pruebas: 50 TimeFrames & Comparativas\n")
    print(f"Total de tests ejecutados: {len(results_map)}\n")
    success_count = sum(1 for v in results_map.values() if v.get("status", {}).get("success") == True)
    print(f"**Éxitos:** {success_count} / {test_id - 1}")
    print(f"**Fallos:** {test_id - 1 - success_count}\n")
    
    with open("olap_50_timeframe_results.json", "w") as f:
        json.dump(results_map, f, indent=2)
    print("Reporte JSON generado exitosamente en olap_50_timeframe_results.json")
    
if __name__ == "__main__":
    run_tests()
