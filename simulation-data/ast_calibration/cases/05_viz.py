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
    analytics_req.entity = "meter_reading"
    analytics_req.output_cast = metri_pb2.KPI
    
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"V{i:03d}",
        category="viz",
        description=f"Generated viz case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
