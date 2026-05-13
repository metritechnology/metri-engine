#!/usr/bin/env python3
import boto3
import time
import sys

def run_athena_query(query, database="metri_olap", region="us-east-1"):
    athena = boto3.client("athena", region_name=region)
    sts = boto3.client("sts", region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    output_loc = f"s3://metri-lake-{account_id}-{region}/athena-results/"
    
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
        return results["ResultSet"]["Rows"]
    else:
        return None

def analyze():
    print("==================================================")
    print("🔍 ANALIZANDO PARQUET: audit_log")
    print("==================================================")
    
    # Obtener una muestra completa de audit_log
    query = """
    SELECT 
        id, user_id, action_type, resource_domain, resource_id, 
        client_ip, security_context, execution_time_ms, plugin_telemetry,
        _tenant, _entity, _timestamp, _partition_path
    FROM olap_events 
    WHERE _entity = 'audit_log' 
    LIMIT 5
    """
    
    rows = run_athena_query(query)
    if not rows or len(rows) <= 1:
        print("❌ No se encontraron registros de audit_log.")
        return

    headers = [col.get("VarCharValue", "NULL") for col in rows[0]["Data"]]
    
    for i, row in enumerate(rows[1:]):
        print(f"\n--- Registro {i+1} ---")
        data = [col.get("VarCharValue", "NULL") for col in row["Data"]]
        for h, v in zip(headers, data):
            status = "✅" if v != "NULL" else "⚠️ NULL"
            # Algunos campos pueden ser NULL por diseño (resource_id, security_context, plugin_telemetry)
            if h in ["resource_id", "security_context", "plugin_telemetry"] and v == "NULL":
                status = "ℹ️ (Empty)"
            print(f"{h:20} : {v[:50] + '...' if len(v) > 50 else v} {status}")

if __name__ == "__main__":
    analyze()
