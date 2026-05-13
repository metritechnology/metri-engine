import json
import os
import time
from itertools import cycle

SEED = 42
TENANT_ID = "golden-tenant"
BASE_TS = int(time.time()) - (20 * 86400)  # ~20 days ago

def get_deterministic_ulid(prefix, index):
    hex_str = f"{prefix.upper()}{index:05d}".rjust(10, '0')
    return f"01KQBPBDXVXVBKSA{hex_str}"

def generate_locations(count=1000):
    locations = []
    types = ["SITE", "BUILDING", "FLOOR", "ROOM", "ZONE"]
    for i in range(1, count + 1):
        loc = {
            "id": get_deterministic_ulid("loc", i),
            "tenant_id": TENANT_ID,
            "name": f"Location {i}",
            "tag": f"L-000{i:03d}",
            "type": types[(i - 1) % len(types)],
            "area_value": 100.0 + (i * 10.5),
            "area_unit": "m2"
        }
        if i > 5 and i % 5 == 0:
            loc["parent_location_id"] = get_deterministic_ulid("loc", 1)
        locations.append(loc)
    return locations

def generate_assets(count=1000):
    assets = []
    statuses = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
    for i in range(1, count + 1):
        loc_idx = (i % 1000)
        if loc_idx == 0: loc_idx = 1000
        
        asset = {
            "id": get_deterministic_ulid("ast", i),
            "tenant_id": TENANT_ID,
            "name": f"Asset {i}",
            "serial_number": f"SN-{10000+i}",
            "tag": f"A-000{i:04d}",
            "status": statuses[(i - 1) % len(statuses)],
            "location_id": get_deterministic_ulid("loc", loc_idx),
            "purchase_cost": 1000.0 + (i * 15.75),
            "commission_ts": BASE_TS + (i * 86400),
            "omniclass_category": f"21-0{i%9}"
        }
        assets.append(asset)
    return assets

def generate_meter_readings(count=1000):
    readings = []
    units = ["kWh", "m3", "L", "°C"]
    qualities = ["GOOD", "SUSPECT", "BAD"]
    
    for i in range(1, count + 1):
        ast_idx = (i % 1000)
        if ast_idx == 0: ast_idx = 1000
        
        reading = {
            "id": get_deterministic_ulid("mrd", i),
            "tenant_id": TENANT_ID,
            "asset_id": get_deterministic_ulid("ast", ast_idx),
            "reading_value": 50.0 + (i % 200) * 0.75,
            "unit": units[(i - 1) % len(units)],
            "read_ts": BASE_TS + (i * 3600),
            "quality_flag": qualities[(i - 1) % len(qualities)],
            "source_system": "simulation"
        }
        readings.append(reading)
    return readings

def main():
    print("Generando Golden Set...")
    locations = generate_locations(1000)
    assets = generate_assets(1000)
    readings = generate_meter_readings(1000)
    
    out_dir = os.path.join(os.path.dirname(__file__), "..", "output", "golden_set", "data")
    os.makedirs(out_dir, exist_ok=True)
    
    with open(os.path.join(out_dir, "locations.json"), "w") as f:
        json.dump(locations, f, indent=2)
    with open(os.path.join(out_dir, "assets.json"), "w") as f:
        json.dump(assets, f, indent=2)
    with open(os.path.join(out_dir, "meter_readings.json"), "w") as f:
        json.dump(readings, f, indent=2)
        
    print(f"✅ Generados: {len(locations)} locations, {len(assets)} assets, {len(readings)} meter_readings")
    print(f"📁 Guardados en: {os.path.abspath(out_dir)}")

if __name__ == "__main__":
    main()
