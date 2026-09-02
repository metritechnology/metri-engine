#!/usr/bin/env python3
import sys
import boto3
import json

def main():
    print("==================================================")
    print("      METRI PRODUCTION USERS LIST                 ")
    print("==================================================")
    
    session = boto3.Session(profile_name="metri-dev", region_name="us-east-1")
    dynamo = session.client("dynamodb")
    table_name = "metri-dynamo"
    
    # We want to find all items that represent a user
    # Users have PK starting with: T#system#E#usr_ (or similar)
    # Let's scan for items with ap = T#system#A#username or similar
    
    users = {}
    
    try:
        paginator = dynamo.get_paginator('scan')
        # Scan for user attributes in table
        for page in paginator.paginate(
            TableName=table_name,
            FilterExpression="begins_with(PK, :usr_prefix)",
            ExpressionAttributeValues={":usr_prefix": {"S": "T#"}}
        ):
            items = page.get('Items', [])
            for item in items:
                pk = item.get("PK", {}).get("S", "")
                ap = item.get("ap", {}).get("S", "")
                v = item.get("v", {})
                
                # Check if it is an EAV datom for an entity
                # PK format: T#tenant_id#E#entity_id
                # ap format: T#tenant_id#A#user/username or similar
                if "#E#" in pk:
                    parts = pk.split("#")
                    tenant_id = parts[1]
                    entity_id = parts[3]
                    
                    if tenant_id not in users:
                        users[tenant_id] = {}
                    if entity_id not in users[tenant_id]:
                        users[tenant_id][entity_id] = {}
                        
                    # Extract attribute name
                    # ap is: T#tenant_id#A#attribute_name
                    ap_parts = ap.split("#")
                    if len(ap_parts) >= 4:
                        attr_name = ap_parts[3]
                        val_str = v.get("S") or v.get("N") or str(v)
                        users[tenant_id][entity_id][attr_name] = val_str
                        
    except Exception as e:
        print(f"Error scanning users: {e}")
        sys.exit(1)
        
    print("\nRECONSTRUCTED USERS BY TENANT:")
    for tenant_id, tenant_users in sorted(users.items()):
        # Filter only entities that look like users (have 'entity/type' == 'user' or have username/email)
        user_entities = {}
        for ent_id, attrs in tenant_users.items():
            ent_type = attrs.get("entity/type")
            # If it has username or email or is known user
            if ent_type == "user" or "username" in attrs or "email" in attrs:
                user_entities[ent_id] = attrs
                
        if user_entities:
            print(f"\nTenant: '{tenant_id}'")
            for ent_id, attrs in sorted(user_entities.items()):
                print(f"  User ID: {ent_id}")
                print(f"    Username   : {attrs.get('username', 'N/A')}")
                print(f"    Email      : {attrs.get('email', 'N/A')}")
                print(f"    First Name : {attrs.get('first_name', 'N/A')}")
                print(f"    Last Name  : {attrs.get('last_name', 'N/A')}")
                print(f"    Status     : {attrs.get('status', 'N/A')}")
                print(f"    Role IDs   : {attrs.get('role_ids', 'N/A')}")
                print(f"    User Type  : {attrs.get('user_type', 'N/A')}")
        else:
            print(f"\nTenant: '{tenant_id}' (No user entities found)")
            
    print("==================================================")

if __name__ == "__main__":
    main()
