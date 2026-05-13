import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def build_agg_query(agg_str, field="reading_value", entity="meter_reading"):
    req = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = entity
    m = q.metrics.add()
    m.entity = entity
    m.attribute = field
    m.aggregation = getattr(metri_pb2, agg_str)
    return req

def get_cases() -> list[TestCase]:
    # COUNT, SUM, AVG, MIN, MAX
    # MEDIAN, STD_DEV, VARIANCE, PERCENTILE_90, 95, 99
    
    aggs = ["COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "STD_DEV", "VARIANCE", 
            "PERCENTILE_90", "PERCENTILE_95", "PERCENTILE_99"]
            
    cases = []
    for agg in aggs:
        cases.append(TestCase(
            id=f"TC-F4-{agg}",
            phase=4,
            rpc="Query",
            description=f"Aggregation {agg}",
            requests=lambda a=agg: [build_agg_query(a)],
            expected={},
            tolerance_type="none",
            tags=["aggregation", agg.lower()]
        ))
        
    return cases

def setup(client):
    pass
