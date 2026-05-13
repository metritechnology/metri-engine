import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    # EXPLAIN PLAN
    req_explain = metri_pb2.QueryRequest()
    req_explain.tenant_id = TENANT_ID
    req_explain.explain_plan = True
    q_explain = req_explain.queries["q1"]
    q_explain.tenant_id = TENANT_ID
    q_explain.entity = "asset"

    # SEARCH
    req_search = metri_pb2.QueryRequest()
    req_search.tenant_id = TENANT_ID
    q_search = req_search.queries["q1"]
    q_search.tenant_id = TENANT_ID
    q_search.entity = "asset"
    q_search.search = "Asset 1"
    
    # HIERARCHY
    req_hierarchy = metri_pb2.QueryRequest()
    req_hierarchy.tenant_id = TENANT_ID
    q_hierarchy = req_hierarchy.queries["q1"]
    q_hierarchy.tenant_id = TENANT_ID
    q_hierarchy.entity = "location"
    q_hierarchy.hierarchy.parent_field = "parent_location_id"
    q_hierarchy.hierarchy.inject_has_children = True
    
    # SELECT TREE
    req_select = metri_pb2.QueryRequest()
    req_select.tenant_id = TENANT_ID
    q_select = req_select.queries["q1"]
    q_select.tenant_id = TENANT_ID
    q_select.entity = "asset"
    q_select.select_tree.update({"name": True, "status": True})

    return [
        TestCase(
            id="TC-F12-EXPLAIN-QUERY",
            phase=12,
            rpc="Query",
            description="Explain plan test",
            requests=lambda: [req_explain],
            expected={"has_ast": True},
            tolerance_type="metadata",
            tags=["explain_plan"]
        ),
        TestCase(
            id="TC-F12-SEARCH-ASSET",
            phase=12,
            rpc="Query",
            description="Search FTS test",
            requests=lambda: [req_search],
            expected={},
            tolerance_type="none",
            tags=["search"]
        ),
        TestCase(
            id="TC-F12-HIERARCHY-BASIC",
            phase=12,
            rpc="Query",
            description="Hierarchy test",
            requests=lambda: [req_hierarchy],
            expected={},
            tolerance_type="none",
            tags=["hierarchy"]
        ),
        TestCase(
            id="TC-F12-SELECT-TREE",
            phase=12,
            rpc="Query",
            description="Select tree projection",
            requests=lambda: [req_select],
            expected={},
            tolerance_type="none",
            tags=["select_tree"]
        )
    ]

def setup(client):
    pass
