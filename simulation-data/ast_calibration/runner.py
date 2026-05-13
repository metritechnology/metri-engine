import os
import sys
import json
import time
import argparse
import logging
import importlib
import struct
import hashlib
import requests
import traceback
from dataclasses import dataclass

from . import grader
# Need to add simulation-data to path to import metri_pb2
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"

@dataclass
class TestCase:
    id: str
    category: str
    description: str
    build_request: callable
    expectations: dict

def invoke_query(req: metri_pb2.QueryRequest) -> str:
    """Invokes the gRPC endpoint and returns the AST IR string for q1."""
    proto_bytes = req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query", 
        data=framed_data, 
        headers={
            'Content-Type': 'application/grpc-web+proto',
            'x-amz-content-sha256': payload_hash
        },
        stream=True
    )
    
    if resp.status_code != 200:
        raise Exception(f"HTTP Status {resp.status_code}: {resp.text}")

    response_bytes = resp.content
    if len(response_bytes) < 5:
        raise Exception("Response too short")
        
    offset = 0
    while offset < len(response_bytes):
        flag, length = struct.unpack('!BI', response_bytes[offset:offset+5])
        offset += 5
        
        if flag == 0x00: # Data frame
            chunk_bytes = response_bytes[offset:offset+length]
            proto_resp = metri_pb2.QueryResponse()
            proto_resp.ParseFromString(chunk_bytes)
            
            # Find the query key. In our test suite, it's always "q1" if not specified.
            if "q1" in proto_resp.batch_results:
                res = proto_resp.batch_results["q1"]
            elif len(proto_resp.batch_results) > 0:
                res = list(proto_resp.batch_results.values())[0]
            else:
                raise Exception("No batch_results found")
                
            if not res.status.success:
                raise Exception(f"Query failed: {res.status}")
                
            rows = res.data.rows_json.iter
            cols = [c.key for c in res.data.columns]
            
            if len(rows) > 0 and "ast_ir" in cols:
                ast_idx = cols.index("ast_ir")
                ast_val = rows[0].values[ast_idx]
                if ast_val and ast_val.HasField("string_value"):
                    return ast_val.string_value
                else:
                    raise Exception("ast_ir not a string")
            else:
                raise Exception("ast_ir column not found")
                
        elif flag == 0x80: # Trailing headers
            pass
        
        offset += length
        
    raise Exception("No data frame found")

def load_cases():
    cases = []
    cases_dir = os.path.join(os.path.dirname(__file__), "cases")
    for filename in sorted(os.listdir(cases_dir)):
        if filename.endswith(".py") and not filename.startswith("__"):
            mod_name = f"ast_calibration.cases.{filename[:-3]}"
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "CASES"):
                cases.extend(mod.CASES)
    return cases

def run_suite(target_category=None, target_id=None, only_fails=False):
    cases = load_cases()
    results = []
    
    print("┌────────┬──────────────┬──────┬───────┬──────────┐")
    print("│ Cat    │ Description  │ Score│ Status│ Duration │")
    print("├────────┼──────────────┼──────┼───────┼──────────┤")
    
    total_score = 0
    num_run = 0
    
    for case in cases:
        if target_category and case.category != target_category:
            continue
        if target_id and case.id != target_id:
            continue
            
        start_time = time.time()
        status = "✅"
        score = 0
        error = None
        ast_ir = None
        
        try:
            req = case.build_request()
            ast_ir = invoke_query(req)
            if case.expectations.get("expect_error"):
                score = 0
                status = "❌"
                error = "Expected error but got AST"
            else:
                score, details = grader.grade_ast(ast_ir, case.expectations)
                
                if score >= 95:
                    status = "✅"
                elif score >= 80:
                    status = "⚠️"
                else:
                    status = "❌"
        except Exception as e:
            if case.expectations.get("expect_error"):
                # If HTTP Status 500 contains message, or anything
                score = 100
                status = "✅"
                error = None
            else:
                status = "❌"
                error = str(e)
            
        duration_ms = int((time.time() - start_time) * 1000)
        
        if only_fails and status == "✅":
            continue
            
        desc = case.description[:12] + ".." if len(case.description) > 12 else case.description.ljust(14)
        
        print(f"│ {case.id.ljust(6)} │ {desc} │ {str(score).rjust(4)} │  {status}   │ {str(duration_ms).rjust(5)}ms │")
        
        if status == "❌":
            print(f"  └─ Error: {error}")
            if ast_ir:
                print(f"  └─ AST IR: {ast_ir}")
            if 'details' in locals():
                print(f"  └─ Details: {details}")
        
        total_score += score
        num_run += 1
        
        results.append({
            "id": case.id,
            "category": case.category,
            "score": score,
            "status": status,
            "duration_ms": duration_ms,
            "error": error,
            "ast_ir": ast_ir
        })
        
    print("└────────┴──────────────┴──────┴───────┴──────────┘")
    
    if num_run > 0:
        avg = total_score / num_run
        print(f"TOTAL: {num_run} cases run. Average score: {avg:.2f}")
        
    # Save results
    report_dir = os.path.join(os.path.dirname(__file__), "report")
    os.makedirs(report_dir, exist_ok=True)
    with open(os.path.join(report_dir, "results.json"), "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", help="Run only specific category")
    parser.add_argument("--id", help="Run specific case ID")
    parser.add_argument("--only-fails", action="store_true", help="Show only failures")
    args = parser.parse_args()
    
    run_suite(args.category, args.id, args.only_fails)
