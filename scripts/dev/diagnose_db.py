#!/usr/bin/env python3
import sys
import argparse
import boto3

def get_db():
    return boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

def scan_entity(table, pk):
    print(f"Scanning datoms for PK: {pk}")
    try:
        response = table.query(
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": pk}
        )
        items = response.get("Items", [])
        print(f"Found {len(items)} datoms:")
        print("-" * 55)
        for item in items:
            sk = item.get("SK")
            sk_bytes = sk.value if hasattr(sk, 'value') else sk
            
            # Decode SK bytes (2 bytes attr ID, 8 bytes TX ID, 1 byte OP type)
            if isinstance(sk_bytes, bytes) and len(sk_bytes) == 11:
                attr_id = int.from_bytes(sk_bytes[0:2], byteorder="big")
                tx_id = int.from_bytes(sk_bytes[2:10], byteorder="big")
                op = sk_bytes[10] != 0
                sk_str = f"AttrID: {attr_id} (0x{attr_id:04x}) | TxID: {tx_id} | OP: {'Assert' if op else 'Retract'}"
            else:
                sk_str = f"Raw SK: {sk_bytes}"

            v = item.get("v")
            print(f"  SK: {sk_str}")
            print(f"  Attribute: {item.get('ap', 'N/A')} -> Value: {v} ({type(v).__name__})")
            print("-" * 55)
    except Exception as e:
        print(f"Error scanning entity: {e}")

def dump_reconstructed_entities(table):
    print("Scanning database and reconstructing entities from EAV datoms...")
    try:
        # Scan entire EAV table with pagination
        items = []
        resp = table.scan()
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        
        entities = {}
        for item in items:
            pk = item.get("PK", "")
            if "#FTS#" in pk:
                continue
                
            parts = pk.split("#")
            if len(parts) >= 4 and parts[2] == "E":
                tenant = parts[1]
                ent_id = parts[3]
                attr = item.get("ap", "")
                v = item.get("v")
                
                if attr.startswith(f"T#{tenant}#A#"):
                     attr = attr[len(f"T#{tenant}#A#"):]
                
                key = (tenant, ent_id)
                if key not in entities:
                    entities[key] = {}
                entities[key][attr] = v
                
        print(f"Total logical entities reconstructed: {len(entities)}")
        print("=" * 80)
        for (tenant, ent_id), attrs in sorted(entities.items()):
            print(f"Tenant: {tenant} | ID: {ent_id}")
            for k, val in sorted(attrs.items()):
                print(f"  {k:<30}: {val}")
            print("-" * 80)
    except Exception as e:
        print(f"Error dumping entities: {e}")

def print_stats(table):
    print("Calculating database EAV statistics...")
    try:
        items = []
        resp = table.scan(ProjectionExpression="PK, ap")
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = table.scan(ProjectionExpression="PK, ap", ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        
        total_records = len(items)
        unique_entities = set()
        fts_records = 0
        attrs_counts = {}
        
        for item in items:
            pk = item.get("PK", "")
            if "#FTS#" in pk:
                fts_records += 1
            else:
                unique_entities.add(pk)
                
            attr = item.get("ap", "unknown")
            attrs_counts[attr] = attrs_counts.get(attr, 0) + 1
            
        print(f"Total Raw Records (Datoms)      : {total_records}")
        print(f"  - FTS Index Records           : {fts_records}")
        print(f"  - EAV Attribute Assertions   : {total_records - fts_records}")
        print(f"Total Unique Entities          : {len(unique_entities)}")
        print("\nTop 10 Attributes by Frequency:")
        for attr, count in sorted(attrs_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  - {attr:<40} : {count}")
    except Exception as e:
        print(f"Error compiling stats: {e}")

def main():
    parser = argparse.ArgumentParser(description="Metri EAV Database Diagnostics Tool")
    parser.add_argument("--scan-entity", metavar="PK", help="Scan and decode datoms for a specific entity PK")
    parser.add_argument("--dump", action="store_true", help="Scan and reconstruct all logical entities")
    parser.add_argument("--stats", action="store_true", help="Print table statistics (datoms counts, attributes)")
    
    args = parser.parse_args()
    
    # Default to printing stats if no argument is passed
    if not (args.scan_entity or args.dump or args.stats):
        args.stats = True
        
    db = get_db()
    table = db.Table("metri-eav-local")
    
    if args.scan_entity:
        scan_entity(table, args.scan_entity)
    if args.dump:
        dump_reconstructed_entities(table)
    if args.stats:
        print_stats(table)

if __name__ == "__main__":
    main()
