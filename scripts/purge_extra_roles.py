import boto3

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-dynamo")

roles_to_delete = [
    "T#system#E#01KYJJNZK6ZYXXB4CXNE3Q2JSD",
    "T#system#E#system-bff"
]

total_deleted = 0
for pk in roles_to_delete:
    print(f"Purging extra role entity {pk}...")
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

print(f"Deleted {total_deleted} items across extra roles.")
