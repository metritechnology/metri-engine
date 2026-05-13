import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    req = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = "asset"
    q.limit = 10
    
    return [
        TestCase(
            id="TC-F9-PAGINATION",
            phase=9,
            rpc="Query",
            description="Pagination limit test",
            requests=lambda: [req],
            expected={},
            tolerance_type="none",
            tags=["pagination"]
        )
    ]

def setup(client):
    pass
