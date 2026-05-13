import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    cases = []
    
    # RELATIVE
    req_rel = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q1 = req_rel.queries["q1"]
    q1.tenant_id = TENANT_ID
    q1.entity = "meter_reading"
    m1 = q1.metrics.add()
    m1.attribute = "reading_value"
    m1.aggregation = metri_pb2.AggregationFunction.SUM
    c1 = q1.comparisons.add()
    c1.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_RELATIVE
    c1.relative_granularity = "day"
    c1.relative_amount = -1
    
    cases.append(TestCase(
        id="TC-F6-RELATIVE",
        phase=6,
        rpc="Query",
        description="Comparison RELATIVE",
        requests=lambda: [req_rel],
        expected={},
        tolerance_type="none",
        tags=["comparisons"]
    ))
    
    # SHORTCUT
    req_short = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q2 = req_short.queries["q1"]
    q2.tenant_id = TENANT_ID
    q2.entity = "meter_reading"
    m2 = q2.metrics.add()
    m2.attribute = "reading_value"
    m2.aggregation = metri_pb2.AggregationFunction.SUM
    c2 = q2.comparisons.add()
    c2.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    c2.shortcut = metri_pb2.AnalyticalComparison.SAME_PERIOD_LAST_YEAR
    
    cases.append(TestCase(
        id="TC-F6-SHORTCUT",
        phase=6,
        rpc="Query",
        description="Comparison SHORTCUT",
        requests=lambda: [req_short],
        expected={},
        tolerance_type="none",
        tags=["comparisons"]
    ))
    
    # BENCHMARK
    req_bench = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q3 = req_bench.queries["q1"]
    q3.tenant_id = TENANT_ID
    q3.entity = "meter_reading"
    m3 = q3.metrics.add()
    m3.attribute = "reading_value"
    m3.aggregation = metri_pb2.AggregationFunction.SUM
    c3 = q3.comparisons.add()
    c3.type = metri_pb2.AnalyticalComparison.BENCHMARK
    c3.benchmark_value = 100.0
    
    cases.append(TestCase(
        id="TC-F6-BENCHMARK",
        phase=6,
        rpc="Query",
        description="Comparison BENCHMARK",
        requests=lambda: [req_bench],
        expected={},
        tolerance_type="none",
        tags=["comparisons"]
    ))

    return cases

def setup(client):
    pass
