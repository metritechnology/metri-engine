import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    # OLAP ENGINE
    req_olap = metri_pb2.QueryRequest()
    req_olap.tenant_id = TENANT_ID
    req_olap.queries["q1"].tenant_id = TENANT_ID
    req_olap.queries["q1"].entity = "meter_reading"
    m_olap = req_olap.queries["q1"].metrics.add()
    m_olap.attribute = "reading_value"
    m_olap.aggregation = metri_pb2.AggregationFunction.SUM

    # OLTP ENGINE
    req_oltp = metri_pb2.QueryRequest()
    req_oltp.tenant_id = TENANT_ID
    req_oltp.queries["q1"].tenant_id = TENANT_ID
    req_oltp.queries["q1"].entity = "asset"

    # COD_001
    req_cod = metri_pb2.QueryRequest()
    req_cod.tenant_id = TENANT_ID
    req_cod.queries["q1"].tenant_id = TENANT_ID
    req_cod.queries["q1"].entity = "unicorn"

    return [
        TestCase(
            id="TC-F13-ENGINE-OLAP",
            phase=13,
            rpc="Query",
            description="Routing OLAP verification",
            requests=lambda: [req_olap],
            expected={"engine": "olap"},
            tolerance_type="metadata",
            tags=["routing", "olap"]
        ),
        TestCase(
            id="TC-F13-ENGINE-OLTP-ASSET",
            phase=13,
            rpc="Query",
            description="Routing OLTP verification",
            requests=lambda: [req_oltp],
            expected={"engine": "oltp"},
            tolerance_type="metadata",
            tags=["routing", "oltp"]
        ),
        TestCase(
            id="TC-F13-COD-001",
            phase=13,
            rpc="Query",
            description="COD_001 Error Verification",
            requests=lambda: [req_cod],
            expected={"error_code": "COD_001"},
            tolerance_type="error",
            tags=["error_catalog"]
        )
    ]

def setup(client):
    pass
