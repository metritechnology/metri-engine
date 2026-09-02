import boto3

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-dynamo")

tenant_id = "01KY3W500T7GYHGXGG0H0K5688"
entity_id = "01KYJ069SXBVFZSWH0E1QK06TE"
pk = f"T#{tenant_id}#E#{entity_id}"
target_email = "maldonadostyven@gmail.com"

# vs binary encoding: b'\x04' + (target_email + entity_id).encode('utf-8')
new_vs = b"\x04" + f"{target_email}{entity_id}".encode("utf-8")

print(f"Assigning {target_email} to user {pk}...")

response = table.query(
    KeyConditionExpression="PK = :pk",
    FilterExpression="ap = :ap",
    ExpressionAttributeValues={
        ":pk": pk,
        ":ap": f"T#{tenant_id}#A#email"
    }
)

updated = 0
for item in response.get("Items", []):
    sk = item["SK"]
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET v = :v, vs = :vs",
        ExpressionAttributeValues={
            ":v": target_email,
            ":vs": new_vs
        }
    )
    updated += 1

print(f"Updated {updated} email datom(s) for user {entity_id}.")
