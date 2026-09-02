#!/usr/bin/env python3
import argparse
import boto3

def get_client(env, region):
    if env == "local":
        return boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:8000",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test"
        )
    else:
        session = boto3.Session(profile_name="metri-dev", region_name=region)
        return session.client("dynamodb")

def count_fts(client, table_name):
    print(f"Scanning table {table_name} for FTS records...")
    try:
        paginator = client.get_paginator('scan')
        count = 0
        for page in paginator.paginate(TableName=table_name):
            items = page.get('Items', [])
            for item in items:
                pk_val = item.get('PK', {}).get('S', '')
                if "#FTS#" in pk_val:
                    count += 1
        print(f"✓ Found {count} FTS records in table {table_name}.")
    except Exception as e:
        print(f"✗ Error scanning FTS: {e}")

def lookup_trigrams(client, table_name, tenant_id, trigrams):
    for trigram in trigrams:
        pk = f"T#{tenant_id}#FTS#{trigram}"
        print(f"\nQuerying FTS records for PK: {pk}")
        try:
            response = client.query(
                TableName=table_name,
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": {"S": pk}}
            )
            items = response.get('Items', [])
            print(f"Trigram '{trigram}' -> Items found: {len(items)}")
            for item in items:
                sk_val = item.get('SK', {})
                sk_bytes = sk_val.get('B')
                print(f"  SK bytes: {sk_bytes}")
                if sk_bytes and len(sk_bytes) > 2:
                    try:
                        eid = sk_bytes[2:].decode('utf-8')
                        print(f"  eid extracted: {eid}")
                    except Exception as e:
                        print(f"  error decoding: {e}")
        except Exception as e:
            print(f"  ✗ Query failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Metri FTS Index Utility Tool")
    parser.add_argument("--env", choices=["local", "prod"], default="local", help="Target environment")
    parser.add_argument("--table", help="DynamoDB Table Name (defaults: metri-eav-local / metri-dynamo)")
    parser.add_argument("--region", default="us-east-1", help="AWS region for production")
    parser.add_argument("--tenant", default="golden-tenant-benchmark", help="Tenant ID for lookup")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Count sub-command
    subparsers.add_parser("count", help="Count all FTS records in DynamoDB")
    
    # Lookup sub-command
    lookup_parser = subparsers.add_parser("lookup", help="Lookup trigrams in the FTS index")
    lookup_parser.add_argument("trigrams", nargs="+", help="Space-separated list of trigrams (e.g. 06e 6e5)")
    
    args = parser.parse_args()
    
    # Defaults for tables
    table_name = args.table
    if not table_name:
        table_name = "metri-eav-local" if args.env == "local" else "metri-dynamo"
        
    client = get_client(args.env, args.region)
    
    if args.command == "count":
        count_fts(client, table_name)
    elif args.command == "lookup":
        lookup_trigrams(client, table_name, args.tenant, args.trigrams)

if __name__ == "__main__":
    main()
