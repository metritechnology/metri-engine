import metri_pb2
from tests.core.test_case import TestCase

def get_cases() -> list[TestCase]:
    # TENANT MISSING
    req_no_tenant = metri_pb2.QueryRequest()
    # No tenant_id set
    req_no_tenant.queries["q1"].entity = "asset"
    
    # TENANT INVALID
    req_invalid_tenant = metri_pb2.QueryRequest()
    req_invalid_tenant.tenant_id = "evil-tenant"
    req_invalid_tenant.queries["q1"].tenant_id = "evil-tenant"
    req_invalid_tenant.queries["q1"].entity = "asset"
    
    return [
        TestCase(
            id="TC-F11-NO-TENANT",
            phase=11,
            rpc="Query",
            description="Missing tenant ID",
            requests=lambda: [req_no_tenant],
            expected={"error_code": "JANUS_400"},
            tolerance_type="error",
            tags=["zero_trust", "tenant_missing"]
        ),
        TestCase(
            id="TC-F11-EVIL-TENANT",
            phase=11,
            rpc="Query",
            description="Invalid tenant ID",
            requests=lambda: [req_invalid_tenant],
            expected={"error_code": "JANUS_403"},
            tolerance_type="error",
            tags=["zero_trust", "tenant_invalid"]
        )
    ]

def setup(client):
    pass
