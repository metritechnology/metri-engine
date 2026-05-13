import json
import os
import metri_pb2
from tests.core.test_case import TestCase

TENANT_ID = "golden-tenant"

def _load_json(filename):
    path = os.path.join(os.path.dirname(__file__), "..", "output", "golden_set", "data", filename)
    with open(path) as f:
        return json.load(f)

def build_ingest_requests(entity_type, filename):
    data = _load_json(filename)
    chunk_size = 5
    requests = []
    
    if len(data) == 0:
        return requests
        
    keys = list(data[0].keys())
    
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        req = metri_pb2.BulkRequest()
        req.tenant_id = TENANT_ID
        req.entity_type = entity_type
        req.action = metri_pb2.CREATE
        
        for key in keys:
            col = req.data.columns.add()
            col.key = key
            col.type = "string"
            
        for item in chunk:
            row = req.data.rows_json.iter.add()
            for key in keys:
                val = item.get(key)
                v = row.values.add()
                if isinstance(val, str):
                    v.string_value = val
                elif isinstance(val, (int, float)):
                    v.number_value = float(val)
                elif isinstance(val, bool):
                    v.bool_value = val
                elif val is None:
                    v.null_value = 0
                else:
                    v.string_value = str(val)
        requests.append(req)
    return requests

def get_cases() -> list[TestCase]:
    return [
        TestCase(
            id="TC-F1-INGEST-LOC",
            phase=1,
            rpc="BulkIngest",
            description="Ingest 1000 Locations",
            requests=lambda: build_ingest_requests("location", "locations.json"),
            expected={"count": 1000},
            tolerance_type="count",
            tags=["ingest", "location", "oltp"]
        ),
        TestCase(
            id="TC-F1-INGEST-AST",
            phase=1,
            rpc="BulkIngest",
            description="Ingest 1000 Assets",
            requests=lambda: build_ingest_requests("asset", "assets.json"),
            expected={"count": 1000},
            tolerance_type="count",
            tags=["ingest", "asset", "oltp"]
        ),
        TestCase(
            id="TC-F1-INGEST-MRD",
            phase=1,
            rpc="BulkIngest",
            description="Ingest 1000 Meter Readings",
            requests=lambda: build_ingest_requests("meter_reading", "meter_readings.json"),
            expected={"count": 1000},
            tolerance_type="count",
            tags=["ingest", "meter_reading", "olap"]
        )
    ]

def setup(client) -> None:
    pass
