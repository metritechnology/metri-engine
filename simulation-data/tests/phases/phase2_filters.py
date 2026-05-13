import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def build_query(field, op, value, kind, entity="asset"):
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = entity
    
    node = q.filters.add()
    node.criteria.field = field
    node.criteria.op_ref = getattr(metri_pb2, op)
    
    if kind == "string_val":
        node.criteria.value.string_val = value
    elif kind == "number_val":
        node.criteria.value.number_val = value
    elif kind == "list_val":
        node.criteria.value.list_val.values.extend(value)
    elif kind == "range_values":
        v1 = node.criteria.value.range_values.values.add()
        v1.number_val = value[0]
        v2 = node.criteria.value.range_values.values.add()
        v2.number_val = value[1]
    elif kind == "none":
        pass # For IS_NULL / IS_NOT_NULL
        
    return req

def get_cases() -> list[TestCase]:
    return [
        TestCase(
            id="TC-F2-EQ",
            phase=2,
            rpc="Query",
            description="Operator EQ",
            requests=lambda: [build_query("status", "EQ", "ACTIVE", "string_val")],
            expected={"count": 334}, # Will check count or error
            tolerance_type="none",
            tags=["filters", "eq"]
        ),
        TestCase(
            id="TC-F2-NEQ",
            phase=2,
            rpc="Query",
            description="Operator NEQ",
            requests=lambda: [build_query("status", "NEQ", "INACTIVE", "string_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "neq"]
        ),
        TestCase(
            id="TC-F2-GT",
            phase=2,
            rpc="Query",
            description="Operator GT",
            requests=lambda: [build_query("area_value", "GT", 200.0, "number_val", entity="location")],
            expected={},
            tolerance_type="none",
            tags=["filters", "gt"]
        ),
        TestCase(
            id="TC-F2-GTE",
            phase=2,
            rpc="Query",
            description="Operator GTE",
            requests=lambda: [build_query("area_value", "GTE", 200.0, "number_val", entity="location")],
            expected={},
            tolerance_type="none",
            tags=["filters", "gte"]
        ),
        TestCase(
            id="TC-F2-LT",
            phase=2,
            rpc="Query",
            description="Operator LT",
            requests=lambda: [build_query("area_value", "LT", 200.0, "number_val", entity="location")],
            expected={},
            tolerance_type="none",
            tags=["filters", "lt"]
        ),
        TestCase(
            id="TC-F2-LTE",
            phase=2,
            rpc="Query",
            description="Operator LTE",
            requests=lambda: [build_query("area_value", "LTE", 200.0, "number_val", entity="location")],
            expected={},
            tolerance_type="none",
            tags=["filters", "lte"]
        ),
        TestCase(
            id="TC-F2-IN",
            phase=2,
            rpc="Query",
            description="Operator IN",
            requests=lambda: [build_query("status", "IN", ["ACTIVE", "INACTIVE"], "list_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "in"]
        ),
        TestCase(
            id="TC-F2-NOT_IN",
            phase=2,
            rpc="Query",
            description="Operator NOT_IN",
            requests=lambda: [build_query("status", "NOT_IN", ["IN_MAINTENANCE"], "list_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "not_in"]
        ),
        TestCase(
            id="TC-F2-BETWEEN",
            phase=2,
            rpc="Query",
            description="Operator BETWEEN",
            requests=lambda: [build_query("area_value", "BETWEEN", [100.0, 200.0], "range_values", entity="location")],
            expected={},
            tolerance_type="none",
            tags=["filters", "between"]
        ),
        TestCase(
            id="TC-F2-LIKE",
            phase=2,
            rpc="Query",
            description="Operator LIKE",
            requests=lambda: [build_query("name", "LIKE", "Asset%", "string_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "like"]
        ),
        TestCase(
            id="TC-F2-IS_NULL",
            phase=2,
            rpc="Query",
            description="Operator IS_NULL",
            requests=lambda: [build_query("omniclass_category", "IS_NULL", None, "none")],
            expected={},
            tolerance_type="none",
            tags=["filters", "is_null"]
        ),
        TestCase(
            id="TC-F2-IS_NOT_NULL",
            phase=2,
            rpc="Query",
            description="Operator IS_NOT_NULL",
            requests=lambda: [build_query("omniclass_category", "IS_NOT_NULL", None, "none")],
            expected={},
            tolerance_type="none",
            tags=["filters", "is_not_null"]
        ),
        TestCase(
            id="TC-F2-MATCHES",
            phase=2,
            rpc="Query",
            description="Operator MATCHES",
            requests=lambda: [build_query("serial_number", "MATCHES", "^SN-", "string_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "matches"]
        ),
        TestCase(
            id="TC-F2-CONTAINS",
            phase=2,
            rpc="Query",
            description="Operator CONTAINS",
            requests=lambda: [build_query("omniclass_category", "CONTAINS", "21-01", "string_val")],
            expected={},
            tolerance_type="none",
            tags=["filters", "contains"]
        )
    ]

def setup(client):
    pass
