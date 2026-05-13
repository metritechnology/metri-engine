import json
import os
import glob

RUN_DIR = "/Users/macuser/projects/metri/metri-engine/simulation-data/tests/output/runs/2026-05-01_15-32-15"

def generate():
    with open(os.path.join(RUN_DIR, "audit_report.json"), "r") as f:
        report = json.load(f)
        
    phases_data = {}
    for i in range(1, 15):
        try:
            with open(os.path.join(RUN_DIR, "phases", f"phase{i}.json"), "r") as f:
                phases_data[f"phase_{i}"] = json.load(f)
        except:
            pass
            
    report["phase_details"] = phases_data
    
    with open("final_inspection_report.json", "w") as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    generate()
