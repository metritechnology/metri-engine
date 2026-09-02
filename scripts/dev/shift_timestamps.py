#!/usr/bin/env python3
import boto3
import time

def main():
    ddb = boto3.client(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    
    table_name = 'metri-eav-local'
    print(f"Scanning table {table_name} for meta/created_at items...")
    
    resp = ddb.scan(TableName=table_name)
    items = resp.get('Items', [])
    
    # We want to find the 'meta/created_at' attribute rows for assets
    created_at_items = [
        item for item in items 
        if item.get('vp', {}).get('S') == 'T#golden-tenant-benchmark#AV#meta/created_at'
    ]
    
    print(f"Found {len(created_at_items)} assets with meta/created_at.")
    
    # Shift 50 of them to 36 hours in the past
    now_ms = int(time.time() * 1000)
    past_ms = now_ms - (36 * 3600 * 1000) # 36 hours ago
    
    shift_count = 50
    shifted = 0
    
    for item in created_at_items[:shift_count]:
        pk = item['PK']
        sk = item['SK']
        
        # Update meta/created_at to the past timestamp
        ddb.update_item(
            TableName=table_name,
            Key={'PK': pk, 'SK': sk},
            UpdateExpression="SET v = :val",
            ExpressionAttributeValues={':val': {'N': str(past_ms)}}
        )
        
        # Also let's update meta/updated_at for this entity to be consistent
        # Find the meta/updated_at row for this PK
        updated_at_item = next(
            (x for x in items if x['PK'] == pk and x.get('vp', {}).get('S') == 'T#golden-tenant-benchmark#AV#meta/updated_at'),
            None
        )
        if updated_at_item:
            ddb.update_item(
                TableName=table_name,
                Key={'PK': pk, 'SK': updated_at_item['SK']},
                UpdateExpression="SET v = :val",
                ExpressionAttributeValues={':val': {'N': str(past_ms)}}
            )
            
        shifted += 1
        
    print(f"Successfully shifted timestamps for {shifted} assets to 36 hours in the past ({past_ms}).")

if __name__ == '__main__':
    main()
