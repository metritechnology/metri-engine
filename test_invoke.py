import boto3
import json
import base64

payload = {
    "entity_type": "asset",
    "action": 1, # CREATE
    "tenant_id": "golden-tenant",
    "payload": {
        "fields": {
            "name": {"string_value": "Test Asset"},
            "criticality": {"string_value": "A"}
        }
    }
}
print(json.dumps(payload, indent=2))
