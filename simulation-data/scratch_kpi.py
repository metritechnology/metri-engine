import os
import json
from google.protobuf.json_format import MessageToDict
from tests.phases.phase7_viz import build_viz_query
from tests.core.channel import get_client

def main():
    client = get_client("production")
    req = build_viz_query("kpi")
    
    resp = client.query(req)
    
    print(json.dumps(MessageToDict(resp), indent=2))

if __name__ == "__main__":
    main()
