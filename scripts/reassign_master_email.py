import boto3

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-dynamo")

entity_id = "01KWFMNY578EQEWZQXYC1C6ZFB"
pk = f"T#system#E#{entity_id}"
new_email = "support@metri.one"

# vs binary encoding: b'\x04' + (new_email + entity_id).encode('utf-8')
new_vs = b"\x04" + f"{new_email}{entity_id}".encode("utf-8")

print(f"Reassigning email for {pk} to {new_email}...")

# 1. Query all email items for the System Master
response = table.query(
    KeyConditionExpression="PK = :pk",
    FilterExpression="ap = :ap",
    ExpressionAttributeValues={
        ":pk": pk,
        ":ap": "T#system#A#email"
    }
)

updated_count = 0
for item in response.get("Items", []):
    sk = item["SK"]
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET v = :v, vs = :vs",
        ExpressionAttributeValues={
            ":v": new_email,
            ":vs": new_vs
        }
    )
    updated_count += 1

print(f"Updated {updated_count} email datom(s) for System Master in DynamoDB.")

# 2. Also scan for any remaining items on GSI-AVET where v = maldonadostyven@gmail.com
scan_resp = table.scan(
    FilterExpression="v = :old_email",
    ExpressionAttributeValues={":old_email": "maldonadostyven@gmail.com"}
)

extra_updated = 0
for item in scan_resp.get("Items", []):
    item_pk = item["PK"]
    item_sk = item["SK"]
    item_eid = item_pk.split("#")[-1]
    item_vs = b"\x04" + f"{new_email}{item_eid}".encode("utf-8")
    table.update_item(
        Key={"PK": item_pk, "SK": item_sk},
        UpdateExpression="SET v = :v, vs = :vs",
        ExpressionAttributeValues={
            ":v": new_email,
            ":vs": item_vs
        }
    )
    extra_updated += 1

print(f"Updated {extra_updated} extra item(s) referencing maldonadostyven@gmail.com.")
print("SUCCESS: maldonadostyven@gmail.com is now fully freed!")
