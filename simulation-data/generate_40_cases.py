import os

def rewrite_filters():
    content = """import os
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
    
    fn = metri_pb2.FilterNode()
    fn.criteria.field = "unit_of_measure"
    fn.criteria.op_ref = metri_pb2.EQ
    fn.criteria.value.string_val = f"VAL_{i}"
    
    analytics_req.filters.append(fn)
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"F{i:03d}",
        category="filters",
        description=f"Generated filter case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
"""
    with open("01_filters.py", "w") as f:
        f.write(content)

def rewrite_timeframes():
    content = """import os
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
    
    analytics_req.time_frame.type = metri_pb2.TimeFrameContext.LAST_N_DAYS
    analytics_req.time_frame.n_value = i
    analytics_req.time_frame.timezone = "America/Bogota"
    
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"T{i:03d}",
        category="timeframes",
        description=f"Generated timeframe case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": False,
            "has_time_frame": True,
            "limit": 100
        }
    ))
"""
    with open("02_timeframes.py", "w") as f:
        f.write(content)

def rewrite_metrics():
    content = """import os
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
"""
    with open("03_metrics.py", "w") as f:
        f.write(content)

def rewrite_multiseries():
    content = """import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
import metri_pb2
from ast_calibration.runner import TestCase

def build_case(i):
    req = metri_pb2.QueryRequest()
    req.tenant_id = f"test-tenant-{i}"
    req.explain_plan = True
    
    analytics_req = metri_pb2.AnalyticsRequest()
    analytics_req.entity = "asset"
    
    d1 = metri_pb2.DimensionDefinition()
    d1.entity = "asset"
    d1.attribute = f"status_{i}"
    
    analytics_req.dimensions.append(d1)
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"MS{i:03d}",
        category="multiseries",
        description=f"Generated multiseries case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "asset",
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
"""
    with open("04_multiseries.py", "w") as f:
        f.write(content)

def rewrite_viz():
    content = """import os
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
"""
    with open("05_viz.py", "w") as f:
        f.write(content)

def rewrite_comparisons():
    content = """import os
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
    
    comp = metri_pb2.AnalyticalComparison()
    comp.relative_granularity = "month"
    comp.relative_amount = -i
    
    analytics_req.comparisons.append(comp)
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"C{i:03d}",
        category="comparisons",
        description=f"Generated comparisons case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
"""
    with open("06_comparisons.py", "w") as f:
        f.write(content)

def rewrite_select():
    content = """import os
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
    
    analytics_req.select_tree.fields["id"].string_value = "true"
    analytics_req.select_tree.fields[f"reading_value_{i}"].string_value = "true"
    
    req.queries["q1"].CopyFrom(analytics_req)
    return req

CASES = []
for i in range(1, 41):
    CASES.append(TestCase(
        id=f"S{i:03d}",
        category="select",
        description=f"Generated select case {i}",
        build_request=lambda idx=i: build_case(idx),
        expectations={
            "has_tenant_isolation": True,
            "entity": "meter_reading",
            "has_metrics": False,
            "has_time_frame": False,
            "limit": 100
        }
    ))
"""
    with open("07_select.py", "w") as f:
        f.write(content)

def rewrite_edge():
    content = """import os
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
"""
    with open("08_edge.py", "w") as f:
        f.write(content)

if __name__ == "__main__":
    os.chdir("/Users/macuser/projects/metri/metri-engine/simulation-data/ast_calibration/cases")
    rewrite_filters()
    rewrite_timeframes()
    rewrite_metrics()
    rewrite_multiseries()
    rewrite_viz()
    rewrite_comparisons()
    rewrite_select()
    rewrite_edge()
    print("Rewrote all 8 categories with 40 cases each!")
