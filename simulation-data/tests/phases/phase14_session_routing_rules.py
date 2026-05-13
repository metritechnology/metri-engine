import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    # MatchRoutingRulesBatch
    req_match = metri_pb2.MatchRoutingRulesBatchRequest()
    rule_req = req_match.requests.add()
    rule_req.tenant_id = TENANT_ID
    rule_req.entity_name = "asset"
    rule_req.trigger_type = "on_update"
    rule_req.cdc_payload_json = b'{"id": "AST001", "status": "ACTIVE"}'
    
    return [
        TestCase(
            id="TC-F14-ROUTING-MATCH",
            phase=14,
            rpc="MatchRoutingRulesBatch",
            description="Match routing rules for asset update",
            requests=lambda: [req_match],
            expected={},
            tolerance_type="none",
            tags=["routing_rules"]
        )
    ]

def setup(client):
    pass
