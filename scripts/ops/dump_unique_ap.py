#!/usr/bin/env python3
import sys
import boto3

def main():
    session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
    dynamo = session.client("dynamodb")
    table_name = "metri-dynamo"
    
    print(f"Scanning table {table_name} for unique 'ap' values...")
    
    unique_aps = set()
    total_scanned = 0
    
    try:
        paginator = dynamo.get_paginator('scan')
        for page in paginator.paginate(
            TableName=table_name,
            ProjectionExpression="ap",
            Limit=1000
        ):
            items = page.get('Items', [])
            total_scanned += len(items)
            for item in items:
                ap = item.get("ap", {}).get("S", "")
                if ap:
                    unique_aps.add(ap)
            
            if total_scanned >= 50000:
                break
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
        
    print(f"Scanned {total_scanned} items.")
    print("Unique 'ap' values found:")
    for ap in sorted(unique_aps):
        print(f"  {ap}")

if __name__ == "__main__":
    main()
