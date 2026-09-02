#!/usr/bin/env python3
import sys
import argparse
import boto3

TABLES_CONFIG = {
    "metri-eav-local": {
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "B"},
            {"AttributeName": "ap", "AttributeType": "S"},
            {"AttributeName": "as", "AttributeType": "B"},
            {"AttributeName": "vp", "AttributeType": "S"},
            {"AttributeName": "vs", "AttributeType": "B"},
            {"AttributeName": "rp", "AttributeType": "S"},
            {"AttributeName": "rs", "AttributeType": "B"}
        ],
        # Las marcas de idempotencia de QuotaLedger::settle_once viven aquí y
        # se borran solas a las 24 h. Sin TTL habilitado se acumularían.
        "TimeToLiveAttribute": "ttl",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI-AEVT",
                "KeySchema": [
                    {"AttributeName": "ap", "KeyType": "HASH"},
                    {"AttributeName": "as", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "GSI-AVET",
                "KeySchema": [
                    {"AttributeName": "vp", "KeyType": "HASH"},
                    {"AttributeName": "vs", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "GSI-VAET",
                "KeySchema": [
                    {"AttributeName": "rp", "KeyType": "HASH"},
                    {"AttributeName": "rs", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
    },
    "metri-schemas-local": {
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"}
        ]
    },
    # Reservas de tokens de IA en vuelo. GSI-SWEEP es por donde cualquier
    # réplica encuentra lo vencido: gp reparte en particiones ("OPEN#<shard>")
    # y gs es expires_at. Las dos claves solo existen mientras la reserva está
    # abierta, así que el índice contiene lo que falta por cerrar y nada más.
    #
    # El TTL NO devuelve tokens: borra los items ya cerrados hasta 48 h después.
    # Quien devuelve es el barrido (src/quota/sweeper.rs).
    "metri-quota-local": {
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "gp", "AttributeType": "S"},
            {"AttributeName": "gs", "AttributeType": "N"}
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI-SWEEP",
                "KeySchema": [
                    {"AttributeName": "gp", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ],
        "TimeToLiveAttribute": "ttl"
    }
}

def get_client():
    return boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

def purge_table(ddb, table_name):
    print(f"Purging all items from {table_name}...")
    try:
        paginator = ddb.get_paginator('scan')
        keys_to_get = [k["AttributeName"] for k in TABLES_CONFIG[table_name]["KeySchema"]]
        
        deleted_count = 0
        for page in paginator.paginate(TableName=table_name):
            items = page.get('Items', [])
            if not items:
                continue
            
            requests = []
            for item in items:
                key_dict = {k: item[k] for k in keys_to_get}
                requests.append({
                    'DeleteRequest': {
                        'Key': key_dict
                    }
                })
                
                if len(requests) == 25:
                    ddb.batch_write_item(RequestItems={table_name: requests})
                    deleted_count += 25
                    requests = []
                    
            if requests:
                ddb.batch_write_item(RequestItems={table_name: requests})
                deleted_count += len(requests)
                
        print(f"  ✓ Purged {deleted_count} items from {table_name}.")
    except Exception as e:
        print(f"  ✗ Error purging {table_name}: {e}")

def recreate_table(ddb, table_name):
    print(f"Recreating table {table_name}...")
    try:
        ddb.delete_table(TableName=table_name)
        print(f"  Deleting existing table {table_name}...")
        waiter = ddb.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
    except ddb.exceptions.ResourceNotFoundException:
        pass
    except Exception as e:
        print(f"  ✗ Failed to delete {table_name}: {e}")
        return

    config = TABLES_CONFIG[table_name]
    create_params = {
        "TableName": table_name,
        "KeySchema": config["KeySchema"],
        "AttributeDefinitions": config["AttributeDefinitions"],
        "BillingMode": "PAY_PER_REQUEST"
    }
    if "GlobalSecondaryIndexes" in config:
        create_params["GlobalSecondaryIndexes"] = config["GlobalSecondaryIndexes"]
        
    try:
        ddb.create_table(**create_params)
        print(f"  ✓ Created table {table_name} successfully.")
    except Exception as e:
        print(f"  ✗ Failed to create table {table_name}: {e}")
        return

    enable_ttl(ddb, table_name, config.get("TimeToLiveAttribute"))


def enable_ttl(ddb, table_name, attribute):
    """El TTL es limpieza física, no lógica de negocio: nada depende de cuándo
    borre DynamoDB, solo de que acabe borrando."""
    if not attribute:
        return
    try:
        waiter = ddb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        ddb.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": attribute}
        )
        print(f"  ✓ TTL enabled on {table_name}.{attribute}")
    except Exception as e:
        print(f"  ✗ Failed to enable TTL on {table_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Purge or Recreate local DynamoDB tables.")
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete and recreate all tables (faster than scan + delete if tables contain many items)."
    )
    args = parser.parse_args()
    
    ddb = get_client()
    
    for table_name in TABLES_CONFIG.keys():
        if args.recreate:
            recreate_table(ddb, table_name)
        else:
            purge_table(ddb, table_name)

if __name__ == "__main__":
    main()
