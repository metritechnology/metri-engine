import os
import json
from google.protobuf.json_format import MessageToDict
from tests.phases.phase2_filters import build_query
from tests.core.channel import get_client

def main():
    client = get_client("local")
    # TC-F2-GT: field="area_value", op="GT", value=200, kind="number_val"
    req = build_query("area_value", "BETWEEN", [100.0, 200.0], "range_values", entity="location")
    resp = client.query(req)
    print("GRPC RESPONSE:")
    print(json.dumps(MessageToDict(resp), indent=2))

if __name__ == "__main__":
    main()
