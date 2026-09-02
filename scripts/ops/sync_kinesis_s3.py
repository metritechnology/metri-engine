#!/usr/bin/env python3
import sys
import os
import time
import uuid
import argparse
from datetime import datetime
import boto3

# --- AWS Production Config for Athena/Iceberg ---
PROFILE = "metri-dev"
REGION = "us-east-1"
DATABASE = "metri_olap"
WORKGROUP = "metri-analytics"
BUCKET_NAME = "metri-lake-982592308819-us-east-1"
S3_OUTPUT = f"s3://{BUCKET_NAME}/athena-results/"

ICEBERG_QUERIES = {
    "meter_reading_raw": f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.meter_reading_raw (
  id string,
  _tenant string,
  created_at bigint,
  asset_id string,
  metric_code string,
  reading_value double,
  raw_value string,
  unit_of_measure string,
  data_quality string,
  protocol string,
  source_address string,
  timestamp double,
  ingested_at double,
  metadata string
)
PARTITIONED BY (_tenant)
LOCATION 's3://{BUCKET_NAME}/iceberg-data/meter_reading_raw/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
""",
    "meter_reading_rollup": f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.meter_reading_rollup (
  tenant_id string,
  asset_id string,
  metric_code string,
  unit_of_measure string,
  reading_value_avg double,
  reading_value_min double,
  reading_value_max double,
  reading_count bigint,
  day string
)
PARTITIONED BY (tenant_id, day)
LOCATION 's3://{BUCKET_NAME}/iceberg-data/meter_reading_rollup/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
""",
    "audit_log": f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.audit_log (
  id string,
  _tenant string,
  created_at bigint,
  tenant_id string,
  user_id string,
  action_type string,
  resource_domain string,
  resource_id string,
  client_ip string,
  security_context string,
  execution_time_ms bigint,
  plugin_telemetry string
)
PARTITIONED BY (_tenant)
LOCATION 's3://{BUCKET_NAME}/iceberg-data/audit_log/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
""",
    "domain_fault": f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.domain_fault (
  id string,
  _tenant string,
  created_at bigint,
  trace_id string,
  tenant_id string,
  user_id string,
  error_code string,
  severity string,
  stage string,
  component string,
  entity_type string,
  retryable boolean,
  occurred_at double,
  context string
)
PARTITIONED BY (_tenant)
LOCATION 's3://{BUCKET_NAME}/iceberg-data/domain_fault/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
""",
    "inventory_ledger": f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.inventory_ledger (
  id string,
  _tenant string,
  created_at bigint,
  movement_id string,
  part_id string,
  location_id string,
  timestamp double,
  quantity_change double,
  running_balance double,
  running_financial_value double
)
PARTITIONED BY (_tenant)
LOCATION 's3://{BUCKET_NAME}/iceberg-data/inventory_ledger/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
"""
}

# --- LOCALSTACK KINESIS-TO-S3 SYNC ---
def run_local_kinesis_sync(endpoint):
    bucket_name = "metri-bi-data"
    stream_name = "metri-olap-stream-meter-reading"
    prefix = "olap/"
    
    print(f"Connecting to Kinesis and S3 local stack at {endpoint}...")
    kinesis = boto3.client(
        "kinesis",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

    try:
        desc = kinesis.describe_stream(StreamName=stream_name)
        shards = desc["StreamDescription"]["Shards"]
    except Exception as e:
        print(f"✗ Error describing stream {stream_name}: {e}")
        return

    print(f"Found {len(shards)} shards in stream {stream_name}.")
    all_records = []

    for shard in shards:
        shard_id = shard["ShardId"]
        print(f"Reading from shard {shard_id}...")
        try:
            iterator_resp = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON"
            )
            shard_iterator = iterator_resp["ShardIterator"]
        except Exception as e:
            print(f"  Error getting shard iterator for {shard_id}: {e}")
            continue

        while shard_iterator:
            try:
                records_resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
                records = records_resp.get("Records", [])
                if not records:
                    break
                print(f"  Retrieved {len(records)} records from shard {shard_id}.")
                for record in records:
                    all_records.append(record["Data"])
                shard_iterator = records_resp.get("NextShardIterator")
            except Exception as e:
                print(f"  Error getting records: {e}")
                break

    if not all_records:
        print("No records found in Kinesis stream.")
        return

    print(f"Total retrieved records: {len(all_records)}")
    concatenated_data = b"".join(all_records)

    now = datetime.utcnow()
    time_prefix = now.strftime("%Y/%m/%d/%H/")
    filename = f"{stream_name}-1-{now.strftime('%Y-%m-%d-%H-%M-%S')}-{uuid.uuid4()}"
    s3_key = f"{prefix}{time_prefix}{filename}"

    print(f"Uploading to S3 bucket '{bucket_name}' with key '{s3_key}'...")
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=concatenated_data)
        print("✅ Successfully uploaded records to S3!")
    except Exception as e:
        print(f"✗ Error uploading to S3: {e}")

# --- PRODUCTION ATHENA ICEBERG PROVISIONING ---
def create_production_iceberg_tables(profile, region):
    print(f"Initializing AWS Athena session using profile: {profile} in {region}...")
    try:
        session = boto3.Session(profile_name=profile, region_name=region)
        athena = session.client("athena")
    except Exception as e:
        print(f"✗ Failed to authenticate to AWS: {e}")
        return

    execution_ids = {}
    for table_name, query_str in ICEBERG_QUERIES.items():
        print(f"Starting creation of Iceberg table: {table_name}")
        try:
            response = athena.start_query_execution(
                QueryString=query_str,
                QueryExecutionContext={"Database": DATABASE},
                ResultConfiguration={"OutputLocation": S3_OUTPUT},
                WorkGroup=WORKGROUP
            )
            exec_id = response["QueryExecutionId"]
            execution_ids[table_name] = exec_id
            print(f"  Query execution ID: {exec_id}")
        except Exception as e:
            print(f"  ✗ Failed to launch Athena query for {table_name}: {e}")

    # Wait for queries to complete
    pending = list(execution_ids.keys())
    while pending:
        time.sleep(2)
        print(f"\nChecking status of pending tables: {pending}")
        for table_name in list(pending):
            exec_id = execution_ids[table_name]
            try:
                status_response = athena.get_query_execution(QueryExecutionId=exec_id)
                state = status_response["QueryExecution"]["Status"]["State"]
                
                if state in ["SUCCEEDED"]:
                    print(f"  ✅ Table '{table_name}' created successfully!")
                    pending.remove(table_name)
                elif state in ["FAILED", "CANCELLED"]:
                    reason = status_response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                    print(f"  ❌ Table '{table_name}' failed to create: {reason}")
                    pending.remove(table_name)
                else:
                    print(f"  ... Table '{table_name}' status: {state}")
            except Exception as e:
                print(f"  Error checking status for {table_name}: {e}")
                pending.remove(table_name)

    print("\nIceberg tables setup complete!")

def main():
    parser = argparse.ArgumentParser(description="OLAP Lake Integration and Sync Ops Utility")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Local sync command
    local_sync = subparsers.add_parser("sync-local", help="Simulate Localstack Kinesis stream consumption to S3")
    local_sync.add_argument("--endpoint", default="http://localhost:4566", help="Localstack endpoint URL")
    
    # Iceberg create tables command
    create_tables = subparsers.add_parser("create-iceberg", help="Initialize Iceberg Glue/Athena Tables in Production")
    create_tables.add_argument("--profile", default=PROFILE, help="AWS CLI profile name")
    create_tables.add_argument("--region", default=REGION, help="AWS Region")
    
    args = parser.parse_args()
    
    if args.command == "sync-local":
        run_local_kinesis_sync(args.endpoint)
    elif args.command == "create-iceberg":
        create_production_iceberg_tables(args.profile, args.region)

if __name__ == "__main__":
    main()
