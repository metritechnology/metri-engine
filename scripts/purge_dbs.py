import boto3
import json

session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
dynamodb = session.resource("dynamodb")
table = dynamodb.Table("metri-datahike-prod")

scan = table.scan()
with table.batch_writer() as batch:
    for each in scan['Items']:
        batch.delete_item(Key={'Key': each['Key']})

while 'LastEvaluatedKey' in scan:
    scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'Key': each['Key']})

print("Purge completed")
