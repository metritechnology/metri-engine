import json
import os
import statistics

def load_data():
    base_dir = os.path.join(os.path.dirname(__file__), "..", "output", "golden_set", "data")
    with open(os.path.join(base_dir, "locations.json")) as f:
        locations = json.load(f)
    with open(os.path.join(base_dir, "assets.json")) as f:
        assets = json.load(f)
    with open(os.path.join(base_dir, "meter_readings.json")) as f:
        readings = json.load(f)
    return locations, assets, readings

def precalculate():
    locations, assets, readings = load_data()
    
    # Precalculate logic
    expected = {}
    
    # TC-F1-BULK-INGEST
    expected["TC-F1-INGEST-LOC"] = {"count": len(locations)}
    expected["TC-F1-INGEST-AST"] = {"count": len(assets)}
    expected["TC-F1-INGEST-MRD"] = {"count": len(readings)}
    
    # TC-F2 Filters (Counts)
    active_assets = [a for a in assets if a["status"] == "ACTIVE"]
    expected["TC-F2-EQ-ACTIVE"] = {"count": len(active_assets)}
    
    in_status_assets = [a for a in assets if a["status"] in ["ACTIVE", "INACTIVE"]]
    expected["TC-F2-IN-STATUS"] = {"count": len(in_status_assets)}
    
    # Phase 4 Aggregations
    reading_values = [r["reading_value"] for r in readings]
    expected["TC-F4-SUM-READING"] = {"sum": sum(reading_values)}
    expected["TC-F4-AVG-READING"] = {"avg": statistics.mean(reading_values)}
    if len(reading_values) > 1:
        expected["TC-F4-STD-DEV"] = {"std_dev": statistics.stdev(reading_values)}
    
    # Phase 5 TimeFrames
    expected["TC-F5-ALL-TIME"] = {"count": len(readings)}
    
    # This dictionary will be expanded as phases are developed.
    # For now, it seeds the file.
    
    out_file = os.path.join(os.path.dirname(__file__), "..", "output", "golden_set", "expected_values.json")
    with open(out_file, "w") as f:
        json.dump(expected, f, indent=2)
        
    print(f"✅ expected_values.json generado con {len(expected)} casos de test precálculados.")
    print(f"📁 Guardado en: {os.path.abspath(out_file)}")

if __name__ == "__main__":
    precalculate()
