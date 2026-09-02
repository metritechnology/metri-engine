#!/usr/bin/env python3
import sys
import boto3
import re

def main():
    print("==================================================")
    print("      METRI PRODUCTION DYNAMODB DIAGNOSTIC        ")
    print("==================================================")
    
    session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
    dynamo = session.client("dynamodb")
    table_name = "metri-dynamo"
    
    print(f"Scanning table: {table_name}...")
    
    tenants = {}
    total_scanned = 0
    
    try:
        paginator = dynamo.get_paginator('scan')
        # Scan to find PKs and count them by tenant and type
        for page in paginator.paginate(
            TableName=table_name,
            ProjectionExpression="PK, ap",
            Limit=1000 # Limit page size for quick overview
        ):
            items = page.get('Items', [])
            total_scanned += len(items)
            
            for item in items:
                pk = item.get("PK", {}).get("S", "")
                ap = item.get("ap", {}).get("S", "")
                
                # Check for tenant pattern in PK (e.g. T#tenant_id#...)
                m = re.match(r"^T#([^#]+)#(.*)", pk)
                if m:
                    tenant_id = m.group(1)
                    sub = m.group(2)
                    
                    if tenant_id not in tenants:
                        tenants[tenant_id] = {"total": 0, "entities": {}, "fts": 0}
                    
                    tenants[tenant_id]["total"] += 1
                    
                    if "#FTS#" in pk:
                        tenants[tenant_id]["fts"] += 1
                    else:
                        # Find the entity type from sub
                        # e.g. E#usr_master#... or similar? Let's check how entity ID is mapped.
                        # Wait, let's look at the ap attribute
                        # ap has form: T#tenant_id#A#entity_type/attribute or T#tenant_id#A#entity/type etc.
                        ap_match = re.match(r"^T#[^#]+#A#([^/]+)/", ap)
                        if ap_match:
                            ent_type = ap_match.group(1)
                            tenants[tenant_id]["entities"][ent_type] = tenants[tenant_id]["entities"].get(ent_type, 0) + 1
            
            # Stop if we scanned enough to get a representative diagnostic (e.g., 50000 items)
            if total_scanned >= 50000:
                print("Scanned 50,000 items, stopping scan.")
                break
                
    except Exception as e:
        print(f"Error scanning table {table_name}: {e}")
        sys.exit(1)
        
    print(f"\nScan completed. Total datoms scanned: {total_scanned}")
    print("==================================================")
    print("SUMMARY BY TENANT:")
    for t_id, data in sorted(tenants.items()):
        print(f"\nTenant: '{t_id}' (Total Datoms: {data['total']}, FTS: {data['fts']})")
        print("  Reconstructed Attribute Assertions by Entity Type:")
        if not data["entities"]:
            print("    (No entity attributes found)")
        for ent, count in sorted(data["entities"].items()):
            print(f"    - {ent:<20} : {count} datoms")
    print("==================================================")

if __name__ == "__main__":
    main()
