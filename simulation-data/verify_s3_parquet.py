#!/usr/bin/env python3
import boto3
import time
import sys

def run_athena_query(query, database="metri_olap", region="us-east-1"):
    athena = boto3.client("athena", region_name=region)
    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    output_loc = f"s3://metri-lake-{account_id}-{region}/athena-results/"
    
    print(f"Ejecutando query: {query}")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_loc}
    )
    
    query_id = response["QueryExecutionId"]
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)
        
    if state == "SUCCEEDED":
        results = athena.get_query_results(QueryExecutionId=query_id)
        rows = results["ResultSet"]["Rows"]
        return rows
    else:
        reason = status["QueryExecution"]["Status"]["StateChangeReason"]
        print(f"Query falló: {reason}")
        return None

def verify():
    # 1. Contar meter_reading
    print("\n--- Verificando meter_reading ---")
    rows_mr = run_athena_query("SELECT count(*) as count FROM olap_events WHERE _entity = 'meter_reading'")
    if rows_mr and len(rows_mr) > 1:
        count = rows_mr[1]["Data"][0].get("VarCharValue", "0")
        print(f"✅ Total meter_reading encontrados en Parquet: {count}")
    
    # 2. Muestra de meter_reading para verificar columnas de dominio
    rows_mr_sample = run_athena_query("SELECT asset_id, reading_value, unit_of_measure FROM olap_events WHERE _entity = 'meter_reading' LIMIT 1")
    if rows_mr_sample and len(rows_mr_sample) > 1:
        data = [col.get("VarCharValue", "NULL") for col in rows_mr_sample[1]["Data"]]
        print(f"✅ Ejemplo de meter_reading (asset_id, reading_value, unit): {data}")
    
    # 3. Contar audit_log
    print("\n--- Verificando audit_log ---")
    rows_al = run_athena_query("SELECT count(*) as count FROM olap_events WHERE _entity = 'audit_log'")
    if rows_al and len(rows_al) > 1:
        count = rows_al[1]["Data"][0].get("VarCharValue", "0")
        print(f"✅ Total audit_log encontrados en Parquet: {count}")
        
    # 4. Muestra de audit_log para verificar columnas de dominio
    rows_al_sample = run_athena_query("SELECT user_id, action_type, resource_domain, execution_time_ms FROM olap_events WHERE _entity = 'audit_log' LIMIT 1")
    if rows_al_sample and len(rows_al_sample) > 1:
        data = [col.get("VarCharValue", "NULL") for col in rows_al_sample[1]["Data"]]
        print(f"✅ Ejemplo de audit_log (user_id, action_type, domain, time_ms): {data}")

if __name__ == '__main__':
    print("==================================================")
    print("🔥 METRI ENGINE - VALIDACIÓN ATHENA/PARQUET 🔥")
    print("==================================================")
    verify()
