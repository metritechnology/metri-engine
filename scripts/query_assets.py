import grpc
import json
import base64
import hmac
import hashlib
import time
from urllib.parse import urlparse
import urllib.request

# Configuration
ENGINE_URL = "https://engine.metri.one"
HMAC_SECRET = "metri-engine-cGftN7"
TENANT_ID = "golden-tenant"

def generate_hmac_signature(payload: str, secret: str) -> str:
    """Generates an HMAC-SHA256 signature for the request."""
    hmac_obj = hmac.new(secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256)
    return base64.b64encode(hmac_obj.digest()).decode('utf-8')

def query_assets():
    # Construct the Mega-Batch Payload
    payload = {
        "queries": [
            {
                "id": "asset-list",
                "entity": "asset",
                "strategy": "TABLE",
                "metrics": ["count"],
                "dimensions": ["status", "name", "serial_number"],
                "limit": 10
            }
        ]
    }
    
    payload_json = json.dumps(payload)
    signature = generate_hmac_signature(payload_json, HMAC_SECRET)
    
    # gRPC-Web POST to the Engine
    url = f"{ENGINE_URL}/metri.MetriService/Query"
    
    # gRPC-Web serialization (5 bytes header + protobuf)
    # Since we don't have the compiled protobufs in python easily, we'll hit the API and see if we can use a REST wrapper if it exists,
    # OR we can just use the gRPC-Web JSON format if Envoy supports it?
    # Wait, the MetriEngine receives raw gRPC. But we can also test Datahike locally via clojure!
    pass

if __name__ == "__main__":
    print("Use a clojure script to query Datahike directly to avoid Protobuf serialization issues in Python.")
