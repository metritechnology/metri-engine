import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    req = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = "asset"
    
    m = q.metrics.add()
    m.entity = "asset"
    m.attribute = "purchase_cost"
    m.aggregation = metri_pb2.SUM
    
    d1 = q.dimensions.add()
    d1.entity = "asset"
    d1.attribute = "status"
    
    d2 = q.dimensions.add()
    d2.entity = "asset"
    d2.attribute = "type"
    
    return [
        TestCase(
            id="TC-F8-DIMENSIONS",
            phase=8,
            rpc="Query",
            description="Multi dimensions grouping",
            requests=lambda: [req],
            expected={},
            tolerance_type="none",
            tags=["dimensions"]
        )
    ]

def setup(client):
    pass
