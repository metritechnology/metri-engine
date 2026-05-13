import json
import os
from datetime import datetime

class FindingsManager:
    def __init__(self):
        self.findings = []
        self.counter = 1
        
    def open(self, test_id, layer, severity, description, expected, actual):
        bug_id = f"BUG-{self.counter:03d}"
        self.counter += 1
        finding = {
            "bug_id": bug_id,
            "test_id": test_id,
            "layer": layer,
            "severity": severity,
            "description": description,
            "expected": str(expected),
            "actual": str(actual),
            "status": "OPEN",
            "opened_at": datetime.utcnow().isoformat()
        }
        self.findings.append(finding)
        return bug_id
        
    def save(self, output_path):
        with open(output_path, "w") as f:
            json.dump(self.findings, f, indent=2)
