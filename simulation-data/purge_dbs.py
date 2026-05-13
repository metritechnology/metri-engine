#!/usr/bin/env python3
"""
purge_dbs.py — Metri Engine: Purgador de Bases de Datos

Modos:
  --oltp-only  Purga selectiva OLTP (inventory_movement, location, part)
               Preserva OLAP (S3/Glue) y schemas. Útil para re-testing OLTP.
  --full       Purga completa: OLTP + schemas + Glue + S3
  (sin args)   Por defecto: purga OLTP selectiva
"""
import boto3
import sys
import time
import argparse


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_session(region='us-east-1'):
    return boto3.Session(profile_name='metri-dev', region_name=region)


# ── Purga completa de tabla DynamoDB ──────────────────────────────────────────

def purge_dynamodb(table_name, region_name='us-east-1'):
    """Elimina TODOS los items de una tabla DynamoDB usando batch_writer."""
    ddb = _get_session(region_name).resource('dynamodb')
    table = ddb.Table(table_name)

    print(f"[{table_name}] Iniciando escaneo para purgación completa (Batch Writer)...")
    try:
        key_names = [k['AttributeName'] for k in table.key_schema]
        scan = table.scan()
        items = scan.get('Items', [])

        if not items:
            print(f"[{table_name}] La tabla ya está vacía.")
            return

        deleted = 0
        with table.batch_writer() as batch:
            while True:
                for each in items:
                    key_dict = {k: each[k] for k in key_names}
                    batch.delete_item(Key=key_dict)
                    deleted += 1
                    time.sleep(0.01) # Throttling to avoid ProvisionedThroughputExceededException
                    if deleted % 500 == 0:
                        print(f"[{table_name}] {deleted} registros procesados para eliminación en batch...")

                if 'LastEvaluatedKey' in scan:
                    scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
                    items = scan.get('Items', [])
                else:
                    break

        print(f"[{table_name}] ✅ Purga completa: {deleted} items eliminados.")
    except Exception as e:
        print(f"[{table_name}] ❌ Error al purgar: {e}")


# ── A4-fix: Purga selectiva por entity_type ────────────────────────────────────

def purge_datahike_by_entity(table_name, entity_types, region_name='us-east-1'):
    """
    A4-fix: Purga selectiva de Datahike por entity_type.
    Lee el PK de cada item y elimina solo los que pertenecen a entity_types dados.

    El PK de Datahike tiene formato:  "<tenant>#<entity_type>#<ulid>"
    Ejemplo: "golden-tenant#inventory_movement#01KXXX..."
    """
    ddb = _get_session(region_name).resource('dynamodb')
    table = ddb.Table(table_name)

    try:
        key_names = [k['AttributeName'] for k in table.key_schema]
    except Exception as e:
        print(f"[{table_name}] ⚠ No se pudo acceder (puede no existir): {e}")
        return 0

    print(f"[{table_name}] Buscando {entity_types} (Batch Writer)...")
    deleted = 0
    try:
        scan = table.scan()
        with table.batch_writer() as batch:
            while True:
                for item in scan.get('Items', []):
                    pk_val = str(item.get(key_names[0], ''))
                    # Coincidencia flexible: busca el entity_type dentro del PK
                    matches = any(etype in pk_val for etype in entity_types)
                    if matches:
                        key_dict = {k: item[k] for k in key_names}
                        batch.delete_item(Key=key_dict)
                        deleted += 1
                        time.sleep(0.01) # Throttling
                        if deleted % 500 == 0:
                            print(f"  ... {deleted} procesados para eliminación en batch")

                if 'LastEvaluatedKey' in scan:
                    scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
                else:
                    break

        print(f"[{table_name}] ✅ {deleted} items de {entity_types} eliminados.")
        return deleted
    except Exception as e:
        print(f"[{table_name}] ❌ Error: {e}")
        return 0


def purge_oltp_testing_data(region_name='us-east-1'):
    """
    A4-fix: Purga selectiva OLTP de testing.
    Elimina inventory_movement, location, part — preserva schemas y OLAP.
    """
    entity_types = [
        'asset',
        'inventory_movement', 'inventory-movement',
        'location',
        'part',
    ]
    for tbl in ['metri-datahike-prod-v2', 'metri-datahike-prod']:
        purge_datahike_by_entity(tbl, entity_types, region_name=region_name)


# ── OLAP ───────────────────────────────────────────────────────────────────────

def purge_s3_lake(region_name='us-east-1'):
    sts = _get_session(region_name).client('sts')
    try:
        account_id = sts.get_caller_identity()['Account']
        bucket_name = f"metri-lake-{account_id}-{region_name}"
    except Exception as e:
        print(f"[S3 OLAP] ❌ No se pudo obtener la identidad de AWS: {e}")
        return

    s3 = _get_session(region_name).resource('s3')
    bucket = s3.Bucket(bucket_name)

    print(f"[{bucket_name}] Identificando objetos en S3 (OLAP Lake)...")
    try:
        count = sum(1 for _ in bucket.objects.all())
        if count == 0:
            print(f"[{bucket_name}] El bucket ya está vacío.")
            return
        print(f"[{bucket_name}] Eliminando {count} objetos Parquet/JSON...")
        bucket.objects.all().delete()
        print(f"[{bucket_name}] ✅ Purga de S3 completada exitosamente.")
    except Exception as e:
        print(f"[{bucket_name}] ❌ Error al purgar S3: {e}")


def purge_glue_tables(database_name='metri_olap', region_name='us-east-1'):
    glue = _get_session(region_name).client('glue')
    try:
        tables = glue.get_tables(DatabaseName=database_name).get('TableList', [])
        if not tables:
            print(f"[{database_name}] No hay tablas en Glue para purgar.")
            return
        print(f"[{database_name}] Eliminando {len(tables)} tablas de Glue (Iceberg/Athena)...")
        for t in tables:
            glue.delete_table(DatabaseName=database_name, Name=t['Name'])
            print(f"  - Tabla eliminada: {t['Name']}")
        print(f"[{database_name}] ✅ Purga de tablas Glue completada.")
    except Exception as e:
        print(f"[{database_name}] ❌ Error al purgar tablas Glue: {e}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Metri Engine — Purgador de Bases de Datos")
    parser.add_argument('--oltp-only', action='store_true',
                        help='Purga selectiva OLTP (inventory_movement, location, part)')
    parser.add_argument('--full', action='store_true',
                        help='Purga completa del entorno (OLTP + OLAP + Glue + S3)')
    args = parser.parse_args()

    print("==================================================")
    print("🔥 METRI ENGINE - PURGADOR DE BASES DE DATOS 🔥")
    print("==================================================\n")

    if args.oltp_only:
        print("[MODO: OLTP selectivo — preserva OLAP y schemas]\n")
        purge_oltp_testing_data()
    elif args.full:
        print("[MODO: Purga completa]\n")
        purge_dynamodb('metri-datahike-prod-v2')
        purge_dynamodb('metri-datahike-prod')
        purge_dynamodb('metri-schemas')
        purge_glue_tables()
        purge_s3_lake()
    else:
        # Default: purga OLTP selectiva (más segura para re-testing)
        print("[MODO: OLTP selectivo (default)]\n")
        purge_oltp_testing_data()

    print("\n✅ Proceso finalizado. El entorno está listo para nuevas simulaciones.")
