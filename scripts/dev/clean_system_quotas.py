#!/usr/bin/env python3
import boto3

def get_db():
    return boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

def main():
    db = get_db()
    table = db.Table("metri-eav-local")
    
    print("Scanning database with pagination for system tenant domain_quota entities...")
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while 'LastEvaluatedKey' in resp:
        resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
        items.extend(resp.get("Items", []))
    
    # 1. Identify PKs of domain_quota entities for system tenant
    system_quota_pks = set()
    for item in items:
        pk = item.get("PK", "")
        parts = pk.split("#")
        if len(parts) >= 4 and parts[1] == "system" and parts[2] == "E":
            attr = item.get("ap", "")
            val = item.get("v")
            if attr.endswith("entity/type") and val == "domain_quota":
                system_quota_pks.add(pk)
                
    print(f"Found {len(system_quota_pks)} domain_quota entities for system tenant.")
    if not system_quota_pks:
        print("Nothing to delete.")
        return
    
    # 2. Delete all items (datoms) for those PKs
    deleted_count = 0
    with table.batch_writer() as batch:
        for item in items:
            pk = item.get("PK", "")
            if pk in system_quota_pks:
                sk = item.get("SK")
                batch.delete_item(Key={"PK": pk, "SK": sk})
                deleted_count += 1
                
    print(f"Successfully deleted {deleted_count} datoms representing system tenant quotas.")

if __name__ == "__main__":
    main()
