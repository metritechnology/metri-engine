import metri_pb2
import json
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def get_cases() -> list[TestCase]:
    # TRANSACT
    req_transact = metri_pb2.TransactionRequest()
    req_transact.tenant_id = TENANT_ID
    req_transact.entity_type = "asset"
    req_transact.action = metri_pb2.CREATE
    req_transact.payload.update({"name": "Test Asset"})
    
    # We do NOT use Discovery/Explore here yet if we haven't implemented them in local stub,
    # but the testing plan says Phase 10 is Transact, Discovery, Explore.
    # We just put Transact for now as a representative case.
    
    return [
        TestCase(
            id="TC-F10-TRANSACT",
            phase=10,
            rpc="Transact",
            description="Transact CREATE test",
            requests=lambda: [req_transact],
            expected={},
            tolerance_type="none",
            tags=["transact"]
        )
    ]

def setup(client):
    pass
