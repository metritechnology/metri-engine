#!/usr/bin/env python3
import boto3

glue = boto3.client("glue", region_name="us-east-1")
t = glue.get_table(DatabaseName="metri_olap", Name="olap_events")["Table"]
print("StorageDescriptor columns:")
for col in t["StorageDescriptor"]["Columns"]:
    print(f"  {col['Name']:<30} {col['Type']}")
print()
print("Partition keys:")
for pk in t.get("PartitionKeys", []):
    print(f"  {pk['Name']:<30} {pk['Type']}")
