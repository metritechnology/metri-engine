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
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        print(f"❌ Error en la query: {reason}")
        return None

def verify():
    print("==================================================")
    print("🔥 METRI ENGINE - VALIDACIÓN OLAP GENÉRICO 🔥")
    print("==================================================")
    
    print("\n[1] Reparando particiones...")
    run_athena_query("MSCK REPAIR TABLE olap_events")

    print("\n[2] Verificando conteos por entidad...")
    query_count = "SELECT _entity, count(*) as cnt FROM olap_events GROUP BY _entity"
    rows = run_athena_query(query_count)
    if rows:
        for row in rows[1:]:
            entity = row["Data"][0].get("VarCharValue", "NULL")
            count = row["Data"][1].get("VarCharValue", "0")
            print(f"✅ Entidad '{entity}': {count} registros")

    print("\n[3] Probando extracción abstracta de campos (JSON)...")
    # Ejemplo: extraer reading_value de meter_reading y action_type de audit_log
    query_extract = """
    SELECT 
        _entity,
        CASE 
            WHEN _entity = 'meter_reading' THEN json_extract_scalar(payload, '$.reading_value')
            WHEN _entity = 'audit_log' THEN json_extract_scalar(payload, '$.action_type')
        END as extracted_value
    FROM olap_events 
    LIMIT 10
    """
    rows = run_athena_query(query_extract)
    if rows:
        print(f"{'_entity':15} | {'extracted_value':20}")
        print("-" * 40)
        for row in rows[1:]:
            entity = row["Data"][0].get("VarCharValue", "NULL")
            val = row["Data"][1].get("VarCharValue", "NULL")
            print(f"{entity:15} | {val:20}")

if __name__ == "__main__":
    verify()
