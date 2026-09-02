import boto3

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-dynamo")

response = table.query(
    IndexName="GSI-AEVT",
    KeyConditionExpression="ap = :ap",
    ExpressionAttributeValues={":ap": "T#system#A#entity_type#role"}
)

role_entities = set()
for item in response.get("Items", []):
    pk = item["PK"]
    role_entities.add(pk)

print(f"Total role entities found in tenant 'system': {len(role_entities)}")
for pk in sorted(role_entities):
    # Fetch name or code datoms for this PK
    res = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk}
    )
    name = "N/A"
    code = "N/A"
    for d in res.get("Items", []):
        ap = d.get("ap", "")
        if ap.endswith("#name"):
            name = d.get("v")
        elif ap.endswith("#code") or ap.endswith("#role_code"):
            code = d.get("v")
    print(f"  PK: {pk} | name: {name} | code: {code}")
