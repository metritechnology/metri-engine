import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def build_tf_query(tf_str, n_value=None):
    req = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = "meter_reading"
    m = q.metrics.add()
    m.attribute = "reading_value"
    m.aggregation = metri_pb2.AggregationFunction.SUM
    q.time_frame.type = getattr(metri_pb2.TimeFrameContext, tf_str)
    if n_value:
        q.time_frame.n_value = n_value
    q.time_frame.timezone = "UTC"
    return req

def get_cases() -> list[TestCase]:
    tfs = [
        "TODAY", "YESTERDAY", "TOMORROW", "LAST_N_MINUTES", "LAST_N_HOURS",
        "LAST_N_DAYS", "NEXT_N_DAYS", "THIS_WEEK", "LAST_WEEK", "NEXT_WEEK",
        "LAST_N_WEEKS", "NEXT_N_WEEKS", "WEEK_TO_DATE", "THIS_MONTH",
        "LAST_MONTH", "NEXT_MONTH", "LAST_N_MONTHS", "MONTH_TO_DATE",
        "THIS_YEAR", "LAST_YEAR", "ALL_TIME"
    ]
    
    cases = []
    for tf in tfs:
        n_val = 5 if "_N_" in tf else None
        cases.append(TestCase(
            id=f"TC-F5-{tf}",
            phase=5,
            rpc="Query",
            description=f"TimeFrame {tf}",
            requests=lambda t=tf, n=n_val: [build_tf_query(t, n)],
            expected={},
            tolerance_type="none",
            tags=["timeframes", tf.lower()]
        ))
        
    # CUSTOM RANGE
    req_custom = metri_pb2.QueryRequest(tenant_id=TENANT_ID)
    q = req_custom.queries["q1"]
    q.tenant_id = TENANT_ID
    q.entity = "meter_reading"
    m = q.metrics.add()
    m.attribute = "reading_value"
    m.aggregation = metri_pb2.AggregationFunction.SUM
    q.time_frame.type = metri_pb2.TimeFrameContext.CUSTOM_RANGE
    q.time_frame.start_ts = 1751328000
    q.time_frame.end_ts = 1751328000 + 86400
    cases.append(TestCase(
        id="TC-F5-CUSTOM_RANGE",
        phase=5,
        rpc="Query",
        description="TimeFrame CUSTOM_RANGE",
        requests=lambda: [req_custom],
        expected={},
        tolerance_type="none",
        tags=["timeframes", "custom_range"]
    ))
    
    return cases

def setup(client):
    pass
