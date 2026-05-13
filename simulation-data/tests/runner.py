import argparse
import os
import json
import time
import importlib
from datetime import datetime

from tests.core.channel import get_client
from tests.core.contract_evaluator import ContractEvaluator
from tests.core.findings import FindingsManager

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["local", "production"])
    parser.add_argument("--phase", type=int, nargs="+")
    parser.add_argument("--only-fails", action="store_true")
    args = parser.parse_args()
    
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = os.path.join(os.path.dirname(__file__), "output", "runs", run_timestamp)
    os.makedirs(os.path.join(run_dir, "phases"), exist_ok=True)
    
    # Symlink to latest
    latest_link = os.path.join(os.path.dirname(__file__), "output", "latest")
    if os.path.lexists(latest_link):
        os.remove(latest_link)
    os.symlink(os.path.join("runs", run_timestamp), latest_link)
    
    client = get_client(args.env)
    
    expected_path = os.path.join(os.path.dirname(__file__), "output", "golden_set", "expected_values.json")
    if not os.path.exists(expected_path):
        print("❌ Faltan expected_values.json. Ejecuta phase0 primero.")
        return
        
    evaluator = ContractEvaluator(expected_path)
    findings = FindingsManager()
    
    phases_to_run = args.phase if args.phase else list(range(1, 15))
    
    audit_report = {
        "timestamp": run_timestamp,
        "environment": args.env,
        "total_passed": 0,
        "total_failed": 0,
        "phases": {}
    }
    
    print(f"🚀 Iniciando Auditoría Metri gRPC en entorno: {args.env}")
    
    for phase_num in phases_to_run:
        phase_name = f"phase{phase_num}"
        # Find module
        mod_name = None
        phases_dir = os.path.join(os.path.dirname(__file__), "phases")
        for f in os.listdir(phases_dir):
            if f.startswith(f"phase{phase_num}_") and f.endswith(".py"):
                mod_name = f"tests.phases.{f[:-3]}"
                break
                
        if not mod_name:
            print(f"⚠️ No se encontró módulo para fase {phase_num}")
            continue
            
        print(f"\n▶️  Ejecutando Fase {phase_num}...")
        mod = importlib.import_module(mod_name)
        
        if hasattr(mod, "setup"):
            mod.setup(client)
            
        cases = mod.get_cases()
        phase_results = []
        phase_passed = 0
        phase_failed = 0
        
        for case in cases:
            print(f"  {case.id}: {case.description}", end="... ")
            try:
                start_t = time.time()
                reqs = case.requests()
                total_processed = 0
                eval_result = None
                
                for req in reqs:
                    if case.rpc == "Query":
                        resp = client.query(req)
                    elif case.rpc == "BulkIngest":
                        resp = client.bulk_ingest(req)
                    elif case.rpc == "Transact":
                        resp = client.transact(req)
                    elif case.rpc == "MatchRoutingRulesBatch":
                        resp = client.match_routing_rules_batch(req)
                    else:
                        raise ValueError(f"RPC {case.rpc} no soportado")
                        
                    if hasattr(resp, "total_processed"):
                        total_processed += resp.total_processed
                    elif hasattr(resp, "ingested_count"):
                        total_processed += resp.ingested_count
                    
                    # Store last response for evaluation if not BulkIngest where we sum
                    last_resp = resp
                    
                # Create a pseudo-response for evaluator if it's bulk
                if case.rpc == "BulkIngest":
                    class PseudoResp:
                        pass
                    pseudo = PseudoResp()
                    pseudo.total_processed = total_processed
                    last_resp = pseudo
                    
                duration = time.time() - start_t
                eval_result = evaluator.evaluate(case, req, last_resp)
                
                all_errors = (
                    eval_result.layer1_errors
                    + eval_result.layer2_errors
                    + eval_result.layer3_errors
                )
                
                if eval_result.passed:
                    print(f"✅ ({duration:.2f}s)")
                    phase_passed += 1
                else:
                    print(f"❌ ({duration:.2f}s)")
                    phase_failed += 1
                    for err in eval_result.layer1_errors:
                        print(f"      🔴 [L1-PROTO]  {err}")
                        findings.open(case.id, "Proto", "CRITICAL", err, case.expected, "FIX_REQUEST")
                    for err in eval_result.layer2_errors:
                        print(f"      🟠 [L2-AST-IR] {err}")
                        findings.open(case.id, "AST-IR", "HIGH", err, case.expected, "FIX_ENGINE")
                    for err in eval_result.layer3_errors:
                        print(f"      🟡 [L3-VALUE]  {err}")
                        findings.open(case.id, "Value", "MEDIUM", err, case.expected, "VER_LOG")
                        
                phase_results.append({
                    "id": case.id,
                    "passed": eval_result.passed,
                    "duration_ms": int(duration * 1000),
                    "layer1_errors": eval_result.layer1_errors,
                    "layer2_errors": eval_result.layer2_errors,
                    "layer3_errors": eval_result.layer3_errors,
                    "raw_response": eval_result.raw_response_summary,
                })
            except Exception as e:
                print(f"❌ Error interno: {e}")
                phase_failed += 1
                findings.open(case.id, "System", "CRITICAL", str(e), None, None)
                phase_results.append({
                    "id": case.id,
                    "passed": False,
                    "errors": [str(e)]
                })
                
        audit_report["phases"][phase_num] = {
            "passed": phase_passed,
            "failed": phase_failed
        }
        audit_report["total_passed"] += phase_passed
        audit_report["total_failed"] += phase_failed
        
        with open(os.path.join(run_dir, "phases", f"phase{phase_num}.json"), "w") as f:
            json.dump(phase_results, f, indent=2)
            
    findings.save(os.path.join(run_dir, "findings.json"))
    with open(os.path.join(run_dir, "audit_report.json"), "w") as f:
        json.dump(audit_report, f, indent=2)
        
    print(f"\n🏁 Auditoría Finalizada.")
    print(f"✅ Pasados: {audit_report['total_passed']}")
    print(f"❌ Fallidos: {audit_report['total_failed']}")
    print(f"📄 Reporte en: {run_dir}/audit_report.json")

if __name__ == "__main__":
    main()
