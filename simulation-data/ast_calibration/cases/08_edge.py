import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2
from ast_calibration.runner import TestCase

def build_case(i):
    req = metri_pb2.QueryRequest()
    req.tenant_id = f"test-tenant-{i}"
    req.explain_plan = True
    
    analytics_req = metri_pb2.AnalyticsRequest()
    analytics_req.entity = f"unknown_entity_{i}"
    
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"E{i:03d}",
        category="edge",
        description=f"Generated edge case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "expect_error": True,
            "has_tenant_isolation": False,
            "entity": None,
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
