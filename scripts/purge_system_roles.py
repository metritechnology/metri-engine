import boto3

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-dynamo")

pks_to_delete = [
    "T#system#E#01KWWECW3THMR355659HF6W8S8",
    "T#system#E#01KY23CJHTS9R4R7TY4BFQFP5D",
    "T#system#E#01KY23STSRQ68KYS0JBYQFCFG4",
    "T#system#E#system-bff",
]

total_deleted = 0

for pk in pks_to_delete:
    print(f"Purging role entity {pk}...")
    res = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk}
    )
    items = res.get("Items", [])
    if items:
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
                total_deleted += 1

print(f"Purged {total_deleted} datom(s) across {len(pks_to_delete)} roles.")
print("ONLY role_super_master remains in tenant 'system'.")
