import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def build_filter_node(field, op_str, value, kind):
    node = metri_pb2.FilterNode()
    node.criteria.field = field
    node.criteria.op_ref = getattr(metri_pb2, op_str)
    
    if kind == "string_val":
        node.criteria.value.string_val = value
    elif kind == "number_val":
        node.criteria.value.number_val = value
    elif kind == "list_val":
        node.criteria.value.list_val.values.extend(value)
    return node

def get_cases() -> list[TestCase]:
    # AND BASIC
    req_and = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q_and = req_and.queries["q1"]
    q_and.tenant_id = TENANT_ID
    q_and.entity = "location"
    group_and = q_and.filters.add().group
    group_and.conjunction = metri_pb2.FilterGroup.AND
    group_and.nodes.append(build_filter_node("type", "EQ", "SITE", "string_val"))
    group_and.nodes.append(build_filter_node("area_value", "GT", 200.0, "number_val"))

    # OR BASIC
    req_or = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q_or = req_or.queries["q1"]
    q_or.tenant_id = TENANT_ID
    q_or.entity = "asset"
    group_or = q_or.filters.add().group
    group_or.conjunction = metri_pb2.FilterGroup.OR
    group_or.nodes.append(build_filter_node("status", "EQ", "ACTIVE", "string_val"))
    group_or.nodes.append(build_filter_node("status", "EQ", "IN_MAINTENANCE", "string_val"))

    # NOT BASIC
    req_not = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q_not = req_not.queries["q1"]
    q_not.tenant_id = TENANT_ID
    q_not.entity = "asset"
    group_not = q_not.filters.add().group
    group_not.conjunction = metri_pb2.FilterGroup.NOT
    group_not.nodes.append(build_filter_node("status", "EQ", "INACTIVE", "string_val"))
    
    # CROSS FILTER
    req_cross = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    req_cross.cross_filter_context.source_chart_id = "pie_chart"
    req_cross.cross_filter_context.cross_filters.append(build_filter_node("status", "EQ", "ACTIVE", "string_val"))
    q_cross = req_cross.queries["q1"]
    q_cross.tenant_id = TENANT_ID
    q_cross.entity = "asset"

    return [
        TestCase(
            id="TC-F3-AND-BASIC",
            phase=3,
            rpc="Query",
            description="AND filter",
            requests=lambda: [req_and],
            expected={},
            tolerance_type="none",
            tags=["filters", "and"]
        ),
        TestCase(
            id="TC-F3-OR-BASIC",
            phase=3,
            rpc="Query",
            description="OR filter",
            requests=lambda: [req_or],
            expected={},
            tolerance_type="none",
            tags=["filters", "or"]
        ),
        TestCase(
            id="TC-F3-NOT-BASIC",
            phase=3,
            rpc="Query",
            description="NOT filter",
            requests=lambda: [req_not],
            expected={},
            tolerance_type="none",
            tags=["filters", "not"]
        ),
        TestCase(
            id="TC-F3-CROSS-FILTER",
            phase=3,
            rpc="Query",
            description="Cross Filter Context",
            requests=lambda: [req_cross],
            expected={},
            tolerance_type="none",
            tags=["filters", "cross_filter"]
        )
    ]

def setup(client):
    pass
