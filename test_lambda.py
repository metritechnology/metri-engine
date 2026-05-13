import boto3
import json
import base64

client = boto3.client('lambda', region_name='us-east-1')

# We can craft a base64 encoded QueryRequest protobuf if we had the protobuf library.
# Since we don't have it easily without installing, maybe we can just use the Datahike REPL?
