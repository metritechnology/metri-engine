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
            if stats['deleted'] % 5000 == 0 or (current_time - stats['last_log'] > 5):
                elapsed = current_time - stats['start_time']
                rate = stats['deleted'] / elapsed if elapsed > 0 else 0
                print(f"  Progress: Deleted {stats['deleted']} items... (Avg rate: {rate:.2f} items/sec)")
                stats['last_log'] = current_time
    
    return actually_deleted

def scan_and_purge_segment(profile, region, table_name, tenant_id, total_segments, segment_id, delete_executor, lock, stats):
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamo = session.client("dynamodb")
    paginator = dynamo.get_paginator('scan')
    requests = []
    
    prefix = f"T#{tenant_id}#"
    
    try:
        for page in paginator.paginate(
            TableName=table_name, 
            ProjectionExpression="PK, SK",
            TotalSegments=total_segments,
            Segment=segment_id
        ):
            items = page.get('Items', [])
            if not items:
                continue
            
            for item in items:
                pk_val = item.get('PK', {}).get('S', '')
                if pk_val.startswith(prefix):
                    # PK starts with the tenant prefix, we delete it
                    key_dict = {
                        'PK': item['PK'],
                        'SK': item['SK']
                    }
                    requests.append({'DeleteRequest': {'Key': key_dict}})
                    
                    if len(requests) == 25:
                        delete_executor.submit(delete_batch, dynamo, table_name, requests, lock, stats)
                        requests = []
                        
        if requests:
            delete_executor.submit(delete_batch, dynamo, table_name, requests, lock, stats)
    except Exception as e:
        print(f"Error scanning segment {segment_id} of {table_name}: {e}")

def purge_tenant_via_scan(profile, region, table_name, tenant_id, segments, threads):
    print(f"\n--- Purging tenant '{tenant_id}' in table '{table_name}' via parallel scan ---")
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamo = session.client("dynamodb")
    
    # Verify table keys
    key_names = get_table_keys(dynamo, table_name)
    if 'PK' not in key_names or 'SK' not in key_names:
        print(f"Error: Table {table_name} does not have PK and SK keys (got {key_names})")
        sys.exit(1)
        
    stats = {'deleted': 0, 'start_time': time.time(), 'last_log': time.time()}
    lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=threads) as delete_executor:
        with ThreadPoolExecutor(max_workers=segments) as scan_executor:
            futures = [
                scan_executor.submit(scan_and_purge_segment, profile, region, table_name, tenant_id, segments, i, delete_executor, lock, stats)
                for i in range(segments)
            ]
            for f in as_completed(futures):
                f.result()
                
    elapsed = time.time() - stats['start_time']
    print(f"Purge complete! Deleted a total of {stats['deleted']} items from {table_name} for tenant '{tenant_id}' in {elapsed:.2f} seconds.")

def main():
    parser = argparse.ArgumentParser(description="Purge DynamoDB data for a specific tenant via scan.")
    parser.add_argument("--profile", default="metri-dev", help="AWS CLI profile name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--table", default="metri-dynamo", help="Target table")
    parser.add_argument("--tenant", required=True, help="Tenant ID to purge")
    parser.add_argument("--confirm", help="Bypass prompt by supplying 'CONFIRM-PURGE'")
    parser.add_argument("--segments", type=int, default=16, help="Scan segments")
    parser.add_argument("--threads", type=int, default=32, help="Delete worker threads")
    
    args = parser.parse_args()
    
    print("==================================================")
    print("      METRI TENANT SCAN PURGE UTILITY             ")
    print("==================================================")
    print(f"AWS Profile    : {args.profile}")
    print(f"Region         : {args.region}")
    print(f"Table          : {args.table}")
    print(f"Tenant         : {args.tenant}")
    print(f"Segments       : {args.segments}")
    print(f"Threads        : {args.threads}")
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
        
    required_phrase = "CONFIRM-PURGE"
    if args.confirm:
        user_input = args.confirm
    else:
        user_input = input(f"\nType '{required_phrase}' to proceed: ").strip()
        
    if user_input != required_phrase:
        print("\n[CANCELLED] Confirmation phrase did not match. Exiting.")
        sys.exit(0)
        
    purge_tenant_via_scan(args.profile, args.region, args.table, args.tenant, args.segments, args.threads)

if __name__ == "__main__":
    main()
