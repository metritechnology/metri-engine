import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def build_viz_query(viz_type):
    req = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = "asset"
    q.viz = viz_type
    
    # We must add an aggregation for visualizations like pie, bar, kpi to work properly
    m = q.metrics.add()
    m.entity = "asset"
    m.attribute = "purchase_cost"
    m.aggregation = metri_pb2.SUM
    
    # Add a dimension for pie/bar
    if viz_type in ["pie", "bar", "line", "area", "table"]:
        d = q.dimensions.add()
        d.entity = "asset"
        d.attribute = "status"
        
    return req

def get_cases() -> list[TestCase]:
    viz_types = ["kpi", "table", "pie", "donut", "bar", "line", "area"]
    cases = []
    
    for v in viz_types:
        cases.append(TestCase(
            id=f"TC-F7-{v.upper()}",
            phase=7,
            rpc="Query",
            description=f"Viz {v}",
            requests=lambda vt=v: [build_viz_query(vt)],
            expected={},
            tolerance_type="none",
            tags=["viz", v]
        ))
        
    return cases

def setup(client):
    pass
