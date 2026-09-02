#!/usr/bin/env python3
import sys
import boto3
import argparse
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_table_keys(dynamo, table_name):
    try:
        desc = dynamo.describe_table(TableName=table_name)
        key_schema = desc['Table']['KeySchema']
        return [attr['AttributeName'] for attr in key_schema]
    except Exception as e:
        print(f"Error describing table {table_name}: {e}")
        sys.exit(1)

def delete_batch(dynamo, table_name, batch, lock=None, stats=None):
    unprocessed = {table_name: batch}
    retries = 0
    backoff = 0.1
    
    while unprocessed and unprocessed.get(table_name):
        try:
            response = dynamo.batch_write_item(RequestItems=unprocessed)
            unprocessed = response.get('UnprocessedItems', {})
            
            if unprocessed and unprocessed.get(table_name):
                time.sleep(backoff)
                retries += 1
                backoff = min(backoff * 2, 2.0)
                if retries > 12:
                    print(f"Warning: Reached max retries (12) for batch delete of {len(unprocessed[table_name])} items on {table_name}!")
                    break
        except Exception as e:
            print(f"Error executing batch write on {table_name}: {e}")
            break
            
    actually_deleted = len(batch) - (len(unprocessed.get(table_name, [])) if unprocessed else 0)
    
    if lock and stats:
        with lock:
            stats['deleted'] += actually_deleted
            current_time = time.time()
            if stats['deleted'] % 10000 == 0 or (current_time - stats['last_log'] > 5):
                elapsed = current_time - stats['start_time']
                rate = stats['deleted'] / elapsed if elapsed > 0 else 0
                print(f"  Progress: Deleted {stats['deleted']} items so far... (Avg rate: {rate:.2f} items/sec)")
                stats['last_log'] = current_time
    
    return actually_deleted

# --- COMPLETE PURGE VIA SEGMENT SCANS ---
def purge_segment(profile, region, table_name, key_names, total_segments, segment_id, delete_executor, lock, stats):
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamo = session.client("dynamodb")
    paginator = dynamo.get_paginator('scan')
    requests = []
    
    try:
        for page in paginator.paginate(
            TableName=table_name, 
            ProjectionExpression=", ".join(key_names),
            TotalSegments=total_segments,
            Segment=segment_id
        ):
            items = page.get('Items', [])
            if not items:
                continue
            
            for item in items:
                key_dict = {kn: item[kn] for kn in key_names if kn in item}
                requests.append({'DeleteRequest': {'Key': key_dict}})
                
                if len(requests) == 25:
                    delete_executor.submit(delete_batch, dynamo, table_name, requests, lock, stats)
                    requests = []
                    
        if requests:
            delete_executor.submit(delete_batch, dynamo, table_name, requests, lock, stats)
    except Exception as e:
        print(f"Error scanning segment {segment_id} of {table_name}: {e}")

def purge_table_complete(profile, region, table_name, segments, threads):
    print(f"\n--- Purging table in parallel: {table_name} ---")
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamo = session.client("dynamodb")
    
    key_names = get_table_keys(dynamo, table_name)
    stats = {'deleted': 0, 'start_time': time.time(), 'last_log': time.time()}
    lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=threads) as delete_executor:
        with ThreadPoolExecutor(max_workers=segments) as scan_executor:
            futures = [
                scan_executor.submit(purge_segment, profile, region, table_name, key_names, segments, i, delete_executor, lock, stats)
                for i in range(segments)
            ]
            for f in as_completed(futures):
                f.result()
                
    elapsed = time.time() - stats['start_time']
    print(f"Purge complete! Deleted a total of {stats['deleted']} items from {table_name} in {elapsed:.2f} seconds.")

# --- SURGICAL TENANT PURGE VIA GSI-AEVT ---
def query_attribute_keys(dynamo, table_name, tenant_id, attr_name):
    pk_ap = f"T#{tenant_id}#A#{attr_name}"
    keys = []
    try:
        paginator = dynamo.get_paginator('query')
        for page in paginator.paginate(
            TableName=table_name,
            IndexName="GSI-AEVT",
            KeyConditionExpression="#ap = :ap",
            ExpressionAttributeNames={"#ap": "ap"},
            ExpressionAttributeValues={":ap": {"S": pk_ap}},
            ProjectionExpression="PK, SK"
        ):
            items = page.get('Items', [])
            for item in items:
                keys.append((item['PK']['S'], item['SK']['B']))
        print(f"  Attribute '{attr_name}': found {len(keys)} items.")
    except Exception as e:
        print(f"  Error querying attribute '{attr_name}': {e}", file=sys.stderr)
    return keys

def purge_tenant_surgical(profile, region, table_name, tenant_id, threads):
    print(f"\n--- Surgical tenant purge: {tenant_id} in {table_name} ---")
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamo = session.client("dynamodb")
    
    attributes = [
        "entity/ulid", "entity/type", "tenant/id", 
        "meta/created_at", "meta/updated_at",
        "created_at", "createdAt", "updated_at", "updatedAt",
        "name", "status", "location_id", "criticality", "category", "model",
        "health_score", "current_meter_reading", "type"
    ]
    
    print("Retrieving primary keys from GSI-AEVT in parallel...")
    unique_keys = set()
    
    with ThreadPoolExecutor(max_workers=min(20, len(attributes))) as executor:
        futures = {executor.submit(query_attribute_keys, dynamo, table_name, tenant_id, attr): attr for attr in attributes}
        for fut in futures:
            attr_keys = fut.result()
            for pk, sk in attr_keys:
                unique_keys.add((pk, sk))

    total_found = len(unique_keys)
    print(f"\nTotal unique items found to delete: {total_found}")
    if total_found == 0:
        print("Nothing to delete.")
        return

    keys_to_delete = [{'DeleteRequest': {'Key': {'PK': {'S': pk}, 'SK': {'B': sk}}}} for pk, sk in unique_keys]
    batches = [keys_to_delete[i:i + 25] for i in range(0, len(keys_to_delete), 25)]
    
    print(f"Deleting {total_found} items in {len(batches)} batches in parallel ({threads} workers)...")
    deleted_count = 0
    
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(delete_batch, dynamo, table_name, b) for b in batches]
        for fut in futures:
            deleted_count += fut.result()
            if deleted_count % 10000 == 0 or deleted_count == total_found:
                print(f"  Deleted {deleted_count}/{total_found} items...")

    print(f"\nPurge complete! Deleted a total of {deleted_count} items from {table_name} for tenant '{tenant_id}'.")

def main():
    parser = argparse.ArgumentParser(description="Safely purge production DynamoDB tables (complete or surgical).")
    parser.add_argument("--profile", default="metri-dev", help="AWS CLI profile name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--tables", nargs="+", default=["metri-dynamo", "metri-engine-MetriSchemasTable-7SFEBA7CN8S"], help="Target tables")
    parser.add_argument("--confirm", help="Bypass prompt by supplying 'CONFIRM-PURGE-METRI-PROD'")
    parser.add_argument("--segments", type=int, default=64, help="Scan segments for complete purge")
    parser.add_argument("--threads", type=int, default=128, help="Delete worker threads")
    
    # Surgical options
    parser.add_argument("--tenant", help="Tenant ID for surgical purge (bypasses full table scan)")
    
    args = parser.parse_args()

    print("==================================================")
    print("      METRI PRODUCTION DYNAMODB PURGE UTILITY     ")
    print("==================================================")
    print(f"AWS Profile    : {args.profile}")
    print(f"Region         : {args.region}")
    print(f"Tables         : {', '.join(args.tables)}")
    print(f"Threads        : {args.threads}")
    if args.tenant:
        print(f"Purge Mode     : SURGICAL (Tenant: {args.tenant})")
    else:
        print(f"Purge Mode     : COMPLETE (Segments: {args.segments})")
    print("==================================================")

    # Validate AWS Identity
    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"AWS Account ID : {identity['Account']}")
        print(f"Caller ARN     : {identity['Arn']}")
    except Exception as e:
        print(f"\n[ERROR] Failed to establish AWS session: {e}")
        sys.exit(1)

    # Confirmation step
    required_phrase = "CONFIRM-PURGE-METRI-PROD"
    if args.confirm:
        user_input = args.confirm
    else:
        print("\n" + "!" * 50)
        print("WARNING: THIS IS A DESTRUCTIVE PRODUCTION WIPE.")
        print(f"MODE: {'SURGICAL' if args.tenant else 'COMPLETE TABLE WIPE'}")
        print("!" * 50)
        user_input = input(f"\nType '{required_phrase}' to proceed: ").strip()

    if user_input != required_phrase:
        print("\n[CANCELLED] Confirmation phrase did not match. Exiting.")
        sys.exit(0)

    # Run Purge
    for t in args.tables:
        if args.tenant:
            purge_tenant_surgical(args.profile, args.region, t, args.tenant, args.threads)
        else:
            purge_table_complete(args.profile, args.region, t, args.segments, args.threads)

    print("\n==================================================")
    print("      PURGE COMPLETED SUCCESSFULLY                ")
    print("==================================================")

if __name__ == "__main__":
    main()
