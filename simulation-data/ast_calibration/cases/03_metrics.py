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
    
    m1 = metri_pb2.MetricDefinition()
    m1.entity = "meter_reading"
    m1.attribute = f"reading_value_{i}"
    m1.aggregation = metri_pb2.SUM
    
    analytics_req.metrics.append(m1)
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"M{i:03d}",
        category="metrics",
        description=f"Generated metrics case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": True,
            "has_time_frame": False,
            "limit": 100
        }
    ))
