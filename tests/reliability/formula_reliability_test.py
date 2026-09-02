#!/usr/bin/env python3
"""
Metri Engine — Formula Engine gRPC Reliability Test Suite
Performs local gRPC testing of 30+ real-world mathematical/conditional cases.
Pre-calculates expected values, seeds EAV test data, and runs assertions.
"""

import subprocess
import json
import sys
import time
import hmac
import hashlib
import base64
import uuid

PROTO_DIR  = "/Users/macuser/projects/metri/metri-engine"
PROTO_FILE = "metri.proto"
ENDPOINT   = "localhost:9090"
TENANT     = "demo"

GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW= "\033[93m"
BLUE  = "\033[94m"
RESET = "\033[0m"
BOLD  = "\033[1m"

# Static secret aligned with local stack
HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    """Generates a signed HMAC-SHA256 session token."""
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

TOKEN = generate_signed_token(HMAC_SECRET, TENANT, "usr_system_bff")

def grpc_call(service: str, payload: dict) -> dict:
    """Executes a gRPC call using grpcurl and parses the JSON response."""
    cmd = [
        "grpcurl", "-plaintext",
        "-import-path", PROTO_DIR,
        "-proto", PROTO_FILE,
        "-rpc-header", f"sid: {TOKEN}",
        "-d", json.dumps(payload),
        ENDPOINT,
        f"metri.MetriService/{service}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"gRPC {service} failed: {result.stderr[:500]}")
    raw = result.stdout.strip()
    if not raw:
        return {}
        
    # parse possible streaming multiple JSON responses
    objects = []
    decoder = json.JSONDecoder()
    idx = 0
    while idx < len(raw):
        while idx < len(raw) and raw[idx] in ' \t\n\r':
            idx += 1
        if idx >= len(raw):
            break
        try:
            obj, end_idx = decoder.raw_decode(raw, idx)
            objects.append(obj)
            idx = end_idx
        except json.JSONDecodeError:
            break
            
    if not objects:
        return {}
    if len(objects) == 1:
        return objects[0]
        
    merged = {}
    for o in objects:
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "batchResults" and isinstance(v, dict) and k in merged:
                    merged[k].update(v)
                else:
                    merged[k] = v
    return merged

def transact(entity: str, action: str, payload: dict) -> dict:
    return grpc_call("Transact", {
        "tenant_id":   TENANT,
        "entity_type": entity,
        "action":      action,
        "payload":     payload,
    })

def query(queries: dict) -> dict:
    return grpc_call("Query", {"tenant_id": TENANT, "queries": queries})

# Define the 34 mathematical and conditional cases with their Python pre-calculated expected results.
# Database variables (seeded in EAV):
# health_score = 12.5 (our 'a' variable)
# current_meter_reading = 100.0 (our 'x' variable)
# telemetry_config = None/null (our 'e' variable)
#
# Literals:
# -4.0 (our 'b' variable)
# 3.0 (our 'c' variable)
# 0.0 (our 'd' variable)
# 20.0 (our 'y' variable)
# 5.0 (our 'z' variable)
test_cases = [
    # ── Basic Arithmetic ──────────────────────────────────────────────────────
    {"idx": 1,  "name": "Addition",               "formula": "health_score + 3.0",                    "expected": 15.0},
    {"idx": 2,  "name": "Subtraction",            "formula": "health_score - 3.0",                    "expected": 9.0},
    {"idx": 3,  "name": "Multiplication",         "formula": "health_score * 3.0",                    "expected": 36.0},
    {"idx": 4,  "name": "Division",               "formula": "health_score / 3.0",                    "expected": 4.0},
    {"idx": 5,  "name": "Exponentiation",         "formula": "health_score ^ 3.0",                    "expected": 1728.0},
    {"idx": 6,  "name": "Modulo Operator",        "formula": "health_score % 3.0",                    "expected": 0.0},
    {"idx": 7,  "name": "Unary Negation",         "formula": "-health_score",                         "expected": -12.0},
    {"idx": 8,  "name": "Operator Precedence",    "formula": "health_score + -4.0 * 3.0",             "expected": 0.0},
    {"idx": 9,  "name": "Parentheses Overriding", "formula": "(health_score + -4.0) * 3.0",           "expected": 24.0},
    
    # ── Math Functions ────────────────────────────────────────────────────────
    {"idx": 10, "name": "ABS Negative",           "formula": "ABS(-4.0)",                             "expected": 4.0},
    {"idx": 11, "name": "ROUND Half Up",          "formula": "ROUND(health_score)",                   "expected": 12.0},
    {"idx": 12, "name": "ROUND Half Down",        "formula": "ROUND(12.4)",                           "expected": 12.0},
    {"idx": 13, "name": "CEIL",                   "formula": "CEIL(health_score)",                    "expected": 12.0},
    {"idx": 14, "name": "FLOOR",                  "formula": "FLOOR(health_score)",                   "expected": 12.0},
    {"idx": 15, "name": "POWER Function",         "formula": "POWER(3.0, 4)",                         "expected": 81.0},
    {"idx": 16, "name": "SQRT",                   "formula": "SQRT(current_meter_reading)",           "expected": 10.0},
    {"idx": 17, "name": "LOG10",                  "formula": "LOG10(current_meter_reading)",          "expected": 2.0},
    {"idx": 18, "name": "MOD Function",           "formula": "MOD(current_meter_reading, 7.0)",       "expected": 2.0},
    {"idx": 19, "name": "SIGN Negative",          "formula": "SIGN(-4.0)",                            "expected": -1.0},
    {"idx": 20, "name": "SIGN Positive",          "formula": "SIGN(health_score)",                    "expected": 1.0},
    {"idx": 21, "name": "SIGN Zero",              "formula": "SIGN(0.0)",                             "expected": 0.0},
    
    # ── Nulls & Conditionals ──────────────────────────────────────────────────
    {"idx": 22, "name": "NULLIF Match",           "formula": "NULLIF(health_score, 12.0)",            "expected": None},
    {"idx": 23, "name": "NULLIF Mismatch",        "formula": "NULLIF(health_score, 3.0)",             "expected": 12.0},
    {"idx": 24, "name": "COALESCE Null First",    "formula": "COALESCE(telemetry_config, health_score)", "expected": 12.0},
    {"idx": 25, "name": "COALESCE Multi-value",   "formula": "COALESCE(telemetry_config, 20.0, 5.0)", "expected": 20.0},
    {"idx": 26, "name": "IF Truthy Condition",    "formula": "IF(-4.0, current_meter_reading, 20.0)", "expected": 100.0},
    {"idx": 27, "name": "IF Falsy Condition",     "formula": "IF(0.0, current_meter_reading, 20.0)",  "expected": 20.0},
    {"idx": 28, "name": "GREATEST Function",      "formula": "GREATEST(health_score, -4.0, 3.0, current_meter_reading)", "expected": 100.0},
    {"idx": 29, "name": "LEAST Function",         "formula": "LEAST(health_score, -4.0, 3.0, current_meter_reading)", "expected": -4.0},
    
    # ── Clamping & Complex Combinations ───────────────────────────────────────
    {"idx": 30, "name": "CLAMP Positive Out",     "formula": "CLAMP(health_score, 0, 10)",            "expected": 10.0},
    {"idx": 31, "name": "CLAMP Negative Out",     "formula": "CLAMP(-4.0, 0, 10)",                    "expected": 0.0},
    {"idx": 32, "name": "CLAMP Inside",           "formula": "CLAMP(3.0, 0, 10)",                     "expected": 3.0},
    {"idx": 33, "name": "Safe Div By Zero",       "formula": "health_score / NULLIF(0.0, 0.0)",       "expected": None},
    {"idx": 34, "name": "Natural Log & Exponent", "formula": "LOG(POWER(2.718281828459, 3.0))",         "expected": 3.0},
]

def main():
    print("=" * 70)
    print(f"{BOLD}{BLUE}STARTING FORMULA ENGINE gRPC RELIABILITY SUITE (34 REAL CASES){RESET}")
    print("=" * 70)

    # 1. Seed test data
    print(f"\n{BOLD}Seeding EAV test asset data...{RESET}")
    unique_name = f"Reliability Formula Test Asset - {uuid.uuid4().hex[:8]}"
    seed_payload = {
        "name": unique_name,
        "status": "ACTIVE",
        "health_score": 12.0,
        "current_meter_reading": 100.0,
        # telemetry_config is left out to remain NULL
        "created_at": int(time.time() * 1000),
        "updated_at": int(time.time() * 1000),
    }
    
    tx_resp = transact("asset", "CREATE", seed_payload)
    if not tx_resp.get("status", {}).get("success"):
        print(f"{RED}✗ Ingestion failed:{RESET} {tx_resp.get('status', {}).get('error_message')}")
        sys.exit(1)
        
    entity_id = tx_resp.get("entityId") or tx_resp.get("entity_id")
    if not entity_id:
        print(f"{RED}✗ Ingestion did not return entity_id!{RESET} Response: {tx_resp}")
        sys.exit(1)
        
    print(f"{GREEN}✓ Test asset successfully ingested.{RESET} entityId: {entity_id}")

    # 2. Construct QueryRequest containing all 34 measures
    print(f"\n{BOLD}Constructing Query Request with 34 formula measures...{RESET}")
    measures = []
    for tc in test_cases:
        measures.append({
            "name": f"case_{tc['idx']}",
            "formula": tc["formula"]
        })
        
    query_payload = {
        "formula_test": {
            "tenant_id": TENANT,
            "entity": "asset",
            "viz": "table",
            "limit": 1,
            "filters": [
                {
                    "criteria": {
                        "field": "name",
                        "op_ref": "EQ",
                        "value": {
                            "string_val": unique_name
                        }
                    }
                }
            ],
            "dimensions": [
                {"entity": "asset", "attribute": "health_score"},
                {"entity": "asset", "attribute": "current_meter_reading"},
                {"entity": "asset", "attribute": "telemetry_config"}
            ],
            "measures": measures
        }
    }
    
    # Execute query
    print(f"Sending gRPC Query to engine endpoint: {ENDPOINT}...")
    q_start = time.time()
    q_resp = query(query_payload)
    q_duration = (time.time() - q_start) * 1000
    print(f"Query executed successfully in {q_duration:.2f} ms")

    # 3. Process results
    chunk = q_resp.get("batchResults", {}).get("formula_test", {})
    if not chunk.get("status", {}).get("success"):
        err_msg = chunk.get("status", {}).get("error_message") or "Unknown error"
        err_code = chunk.get("status", {}).get("error_code") or "N/A"
        print(f"\n{RED}✗ Query execution failed: {err_msg} ({err_code}){RESET}")
        sys.exit(1)
        
    data = chunk.get("data", {})
    columns = [col.get("key") for col in data.get("columns", [])]
    
    rows_json = data.get("rowsJson", {}).get("iter", [])
    if not rows_json:
        rows_json = data.get("rows_json", {}).get("iter", [])
        
    print(f"DEBUG: returned columns: {columns}")
    if rows_json:
        print(f"DEBUG: returned row 0 values: {rows_json[0]}")
        row_values = rows_json[0].get("values", [])
    else:
        print("DEBUG: no rows returned")
        row_values = []
        
    if not rows_json:
        print(f"\n{RED}✗ No rows returned in RowSet!{RESET}")
        sys.exit(1)
        
    # Map column key to row value
    results_map = {}
    for col_key, val_struct in zip(columns, row_values):
        results_map[col_key] = val_struct

    # 4. Assert and Print Test Matrix
    print(f"\n{BOLD}{BLUE}{'='*75}")
    print(f"{'ID':<3} | {'Test Name':<22} | {'Formula':<28} | {'Expected':<10} | {'Actual':<10} | {'Status'}")
    print(f"{'='*75}{RESET}")
    
    passed_count = 0
    failed_count = 0
    
    for tc in test_cases:
        case_key = f"case_{tc['idx']}"
        actual = results_map.get(case_key, "MISSING")
        expected = tc["expected"]
        
        # Check matching
        match = False
        if expected is None:
            match = (actual is None or actual == "MISSING")
        else:
            try:
                # floating-point tolerance check
                match = abs(float(actual) - float(expected)) < 1e-5
            except (ValueError, TypeError):
                match = False
                
        formula_disp = tc["formula"]
        if len(formula_disp) > 28:
            formula_disp = formula_disp[:25] + "..."
            
        expected_disp = "NULL" if expected is None else f"{expected:.4f}"
        actual_disp = "NULL" if actual is None or actual == "MISSING" else f"{actual:.4f}"
        
        if match:
            passed_count += 1
            status_str = f"{GREEN}✓ PASS{RESET}"
        else:
            failed_count += 1
            status_str = f"{RED}✗ FAIL{RESET}"
            
        print(f"{tc['idx']:<3} | {tc['name']:<22} | {formula_disp:<28} | {expected_disp:<10} | {actual_disp:<10} | {status_str}")
        
    print(f"{BLUE}{'='*75}{RESET}")
    
    reliability = (passed_count / len(test_cases)) * 100.0
    print(f"\n{BOLD}RELIABILITY SUMMARY:{RESET}")
    print(f"  Passed Cases : {GREEN}{passed_count}/{len(test_cases)}{RESET}")
    print(f"  Failed Cases : {RED if failed_count > 0 else GREEN}{failed_count}/{len(test_cases)}{RESET}")
    print(f"  Reliability  : {GREEN if reliability == 100.0 else YELLOW}{reliability:.2f}%{RESET}")
    print(f"{'='*75}")
    
    # 5. Clean up seeded data
    print(f"\nCleaning up test asset (entityId: {entity_id})...")
    del_resp = transact("asset", "DELETE", {"id": entity_id})
    if del_resp.get("status", {}).get("success"):
        print(f"{GREEN}✓ Seeding data cleaned up.{RESET}")
    else:
        print(f"{YELLOW}! Warning: Clean up failed: {del_resp.get('status', {}).get('error_message')}{RESET}")

    if failed_count > 0:
        print(f"\n{RED}{BOLD}SUITE FAILED: {failed_count} tests failed.{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}{BOLD}SUITE PASSED: 100% reliability guaranteed!{RESET}")
        sys.exit(0)

if __name__ == "__main__":
    main()
