#!/usr/bin/env python3
import sys
import os
import argparse
import random
import uuid
import csv as csv_mod
from pathlib import Path

# Add script directory to path to enable local imports
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from seed_base import SeederBase, pb, DEFAULT_HMAC_SECRET, GRPC_AVAILABLE, generate_ulid
except ImportError:
    print("✗ Error: No se pudo importar seed_base.py. Asegúrate de ejecutar el script en su directorio.")
    sys.exit(1)

class AssetSeeder(SeederBase):
    def seed_real_hierarchy(self):
        print(f"\n--- Seeding 1000 locations and 1000 assets with real relationships and inheritances ---")
        loc_created = 0
        asset_created = 0
        
        # We need exactly 1000 locations and 1000 assets: 10 + 90 + 450 + 450 = 1000
        for s_idx in range(10):
            # 1. SITE location
            site_name = f"Site {s_idx + 1:02d}"
            site_id = self.transact("location", {"name": site_name, "type": "SITE"})
            if not site_id:
                continue
            loc_created += 1
            
            # 1. SYSTEM asset (top-level asset linked to SITE)
            sys_name = f"Sistema Electrico {site_name}"
            sys_serial = f"SRN-SYS-{s_idx+1:02d}-{uuid.uuid4().hex[:6].upper()}"
            sys_id = self.transact("asset", {
                "name": sys_name,
                "type": "SYSTEM",
                "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                "location_id": site_id,
                "parent_asset_id": None,
                "serial_number": sys_serial,
                "criticality": "A",
                "health_score": round(random.uniform(80.0, 100.0), 2)
            })
            if sys_id:
                asset_created += 1
                
            if loc_created % 100 == 0:
                print(f"  ... created {loc_created}/1000 locations, {asset_created}/1000 assets")
                
            for b_idx in range(9):
                # 2. BUILDING location
                bld_name = f"Edificio Site {s_idx + 1:02d} - {b_idx + 1:02d}"
                bld_id = self.transact("location", {
                    "name": bld_name, 
                    "type": "BUILDING", 
                    "parent_location_id": site_id
                })
                if not bld_id:
                    continue
                loc_created += 1
                
                # 2. SUBSYSTEM asset (linked to BUILDING and parent is SYSTEM)
                sub_name = f"Subestacion Edificio {s_idx + 1:02d} - {b_idx + 1:02d}"
                sub_serial = f"SRN-SUB-{s_idx+1:02d}-{b_idx+1:02d}-{uuid.uuid4().hex[:4].upper()}"
                sub_id = self.transact("asset", {
                    "name": sub_name,
                    "type": "SUBSYSTEM",
                    "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                    "location_id": bld_id,
                    "parent_asset_id": sys_id if sys_id else None,
                    "serial_number": sub_serial,
                    "criticality": "B",
                    "health_score": round(random.uniform(70.0, 95.0), 2)
                })
                if sub_id:
                    asset_created += 1
                    
                if loc_created % 100 == 0:
                    print(f"  ... created {loc_created}/1000 locations, {asset_created}/1000 assets")
                    
                for f_idx in range(5):
                    # 3. FLOOR location
                    flr_name = f"Piso Bldg {s_idx+1:02d}-{b_idx+1:02d} - Floor {f_idx + 1}"
                    flr_id = self.transact("location", {
                        "name": flr_name, 
                        "type": "FLOOR", 
                        "parent_location_id": bld_id
                    })
                    if not flr_id:
                        continue
                    loc_created += 1
                    
                    # 3. EQUIPMENT asset (linked to FLOOR and parent is SUBSYSTEM)
                    eq_name = f"Tablero Distribucion Piso {f_idx + 1} Bldg {s_idx+1:02d}-{b_idx+1:02d}"
                    eq_serial = f"SRN-EQ-{s_idx+1:02d}-{b_idx+1:02d}-{f_idx+1:02d}-{uuid.uuid4().hex[:4].upper()}"
                    eq_id = self.transact("asset", {
                        "name": eq_name,
                        "type": "EQUIPMENT",
                        "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                        "location_id": flr_id,
                        "parent_asset_id": sub_id if sub_id else None,
                        "serial_number": eq_serial,
                        "criticality": "B",
                        "health_score": round(random.uniform(65.0, 90.0), 2)
                    })
                    if eq_id:
                        asset_created += 1
                        
                    if loc_created % 100 == 0:
                        print(f"  ... created {loc_created}/1000 locations, {asset_created}/1000 assets")
                        
                    for r_idx in range(1):
                        # 4. ROOM location
                        rm_name = f"Sala Room {s_idx+1:02d}-{b_idx+1:02d}-{f_idx+1:02d}"
                        rm_id = self.transact("location", {
                            "name": rm_name, 
                            "type": "ROOM", 
                            "parent_location_id": flr_id
                        })
                        if not rm_id:
                            continue
                        loc_created += 1
                        
                        # 4. COMPONENT asset (linked to ROOM and parent is EQUIPMENT)
                        comp_name = f"Medidor Energia Sala {s_idx+1:02d}-{b_idx+1:02d}-{f_idx+1:02d}"
                        comp_serial = f"SRN-COMP-{s_idx+1:02d}-{b_idx+1:02d}-{f_idx+1:02d}-{uuid.uuid4().hex[:4].upper()}"
                        comp_id = self.transact("asset", {
                            "name": comp_name,
                            "type": "COMPONENT",
                            "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                            "location_id": rm_id,
                            "parent_asset_id": eq_id if eq_id else None,
                            "serial_number": comp_serial,
                            "criticality": "C",
                            "health_score": round(random.uniform(60.0, 98.0), 2)
                        })
                        if comp_id:
                            asset_created += 1
                            
                        if loc_created % 100 == 0:
                            print(f"  ... created {loc_created}/1000 locations, {asset_created}/1000 assets")

        print(f"Done seeding: created {loc_created} locations and {asset_created} assets.")

    def seed_bulk_real_hierarchy(self):
        print(f"\n--- Seeding 10000 locations and 10000 assets via BulkIngest with real relationships and inheritances ---")
        
        # 1. Generate Locations
        sites = []
        for i in range(10):
            sites.append({
                "id": generate_ulid(),
                "name": f"Bulk Site {i+1:02d}",
                "type": "SITE",
                "parent_location_id": None
            })

        buildings = []
        for i in range(100):
            parent_site = sites[i // 10]
            buildings.append({
                "id": generate_ulid(),
                "name": f"Bulk Edificio {i+1:03d}",
                "type": "BUILDING",
                "parent_location_id": parent_site["id"]
            })

        floors = []
        for i in range(1000):
            parent_bld = buildings[i // 10]
            floors.append({
                "id": generate_ulid(),
                "name": f"Bulk Piso {i+1:04d}",
                "type": "FLOOR",
                "parent_location_id": parent_bld["id"]
            })

        rooms = []
        room_count = 0
        for f_idx in range(1000):
            num_rooms = 9 if f_idx < 890 else 8
            for r in range(num_rooms):
                rooms.append({
                    "id": generate_ulid(),
                    "name": f"Bulk Sala {room_count+1:04d}",
                    "type": "ROOM",
                    "parent_location_id": floors[f_idx]["id"]
                })
                room_count += 1

        # 2. Ingest Locations level by level
        print("Ingesting locations...")
        loc_ingested = 0
        
        # Sites
        loc_ingested += self.bulk_ingest("location", sites)
        print(f"  ... ingested {loc_ingested}/10000 locations")
        
        # Buildings
        loc_ingested += self.bulk_ingest("location", buildings)
        print(f"  ... ingested {loc_ingested}/10000 locations")
        
        # Floors
        loc_ingested += self.bulk_ingest("location", floors)
        print(f"  ... ingested {loc_ingested}/10000 locations")
        
        # Rooms (in batches of 1000)
        for i in range(0, len(rooms), 1000):
            batch = rooms[i:i+1000]
            loc_ingested += self.bulk_ingest("location", batch)
            print(f"  ... ingested {loc_ingested}/10000 locations")

        # 3. Generate Assets
        systems = []
        for i in range(10):
            systems.append({
                "id": generate_ulid(),
                "name": f"Bulk Sistema {i+1:02d}",
                "type": "SYSTEM",
                "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                "location_id": sites[i]["id"],
                "parent_asset_id": None,
                "serial_number": f"SRN-SYS-{generate_ulid()[:8]}",
                "criticality": "A",
                "health_score": round(random.uniform(80.0, 100.0), 2)
            })

        subsystems = []
        for i in range(100):
            parent_sys = systems[i // 10]
            subsystems.append({
                "id": generate_ulid(),
                "name": f"Bulk Subestacion {i+1:03d}",
                "type": "SUBSYSTEM",
                "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                "location_id": buildings[i]["id"],
                "parent_asset_id": parent_sys["id"],
                "serial_number": f"SRN-SUB-{generate_ulid()[:8]}",
                "criticality": "B",
                "health_score": round(random.uniform(70.0, 95.0), 2)
            })

        equipments = []
        for i in range(1000):
            parent_sub = subsystems[i // 10]
            equipments.append({
                "id": generate_ulid(),
                "name": f"Bulk Tablero {i+1:04d}",
                "type": "EQUIPMENT",
                "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                "location_id": floors[i]["id"],
                "parent_asset_id": parent_sub["id"],
                "serial_number": f"SRN-EQ-{generate_ulid()[:8]}",
                "criticality": "B",
                "health_score": round(random.uniform(65.0, 90.0), 2)
            })

        components = []
        comp_count = 0
        room_idx = 0
        for f_idx in range(1000):
            num_rooms = 9 if f_idx < 890 else 8
            for r in range(num_rooms):
                room_id = rooms[room_idx]["id"]
                parent_eq_id = equipments[f_idx]["id"]
                components.append({
                    "id": generate_ulid(),
                    "name": f"Bulk Medidor {comp_count+1:04d}",
                    "type": "COMPONENT",
                    "status": random.choice(["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]),
                    "location_id": room_id,
                    "parent_asset_id": parent_eq_id,
                    "serial_number": f"SRN-COMP-{generate_ulid()[:8]}",
                    "criticality": "C",
                    "health_score": round(random.uniform(60.0, 98.0), 2)
                })
                comp_count += 1
                room_idx += 1

        # 4. Ingest Assets level by level
        print("Ingesting assets...")
        asset_ingested = 0
        
        # Systems
        asset_ingested += self.bulk_ingest("asset", systems)
        print(f"  ... ingested {asset_ingested}/10000 assets")
        
        # Subsystems
        asset_ingested += self.bulk_ingest("asset", subsystems)
        print(f"  ... ingested {asset_ingested}/10000 assets")
        
        # Equipments
        asset_ingested += self.bulk_ingest("asset", equipments)
        print(f"  ... ingested {asset_ingested}/10000 assets")
        
        # Components (in batches of 1000)
        for i in range(0, len(components), 1000):
            batch = components[i:i+1000]
            asset_ingested += self.bulk_ingest("asset", batch)
            print(f"  ... ingested {asset_ingested}/10000 assets")

        print(f"Done bulk seeding: ingested {loc_ingested} locations and {asset_ingested} assets.")

    def seed_benchmark_assets(self, count: int):
        print(f"\n--- Seeding {count} benchmark assets for tenant: {self.tenant_id} ---")
        
        # 1. Determine location IDs to assign to assets
        loc_ids = self.context.get("leaf_location_ids") or self.context.get("location_ids")
        
        # 2. If no location IDs in memory context, try querying them from local engine database
        if not loc_ids and not self.dry_run:
            print("No locations found in memory context. Querying existing locations from engine...")
            try:
                stub = self.get_stub()
                req = pb.QueryRequest(
                    tenant_id=self.tenant_id,
                    queries={
                        "get_locs": pb.AnalyticsRequest(
                            tenant_id=self.tenant_id,
                            entity="location",
                            viz="table",
                            limit=1000
                        )
                    }
                )
                fetched_ids = []
                for chunk in stub.Query(req, timeout=10, metadata=self.metadata):
                    for key, res in chunk.batch_results.items():
                        if res.data and res.data.rows_json and res.data.rows_json.iter:
                            for r in res.data.rows_json.iter:
                                for col, val in zip(res.data.columns, r.values):
                                    if col.key == "id":
                                        fetched_ids.append(val.string_value)
                if fetched_ids:
                    print(f"  ✓ Found {len(fetched_ids)} locations in engine database.")
                    loc_ids = fetched_ids
            except Exception as e:
                print(f"  ⚠ Failed to query existing locations: {e}")
                
        # 3. Fallback to synthetic IDs if none exist/found
        if not loc_ids:
            print("  ⚠ No locations found. Falling back to synthetic location IDs.")
            loc_ids = [f"LOC-{i:03d}" for i in range(1, 101)]

        statuses = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]
        types = ["PUMP", "MOTOR", "COMPRESSOR", "VALVE", "SENSOR", "TURBINE"]
        
        ok = 0
        for i in range(count):
            asset_name = f"Asset Benchmark {i+1} - Motor {uuid.uuid4().hex[:4].upper()}"
            loc_id = loc_ids[i % len(loc_ids)]
            payload = {
                "name": asset_name,
                "status": random.choice(statuses),
                "type": random.choice(types),
                "health_score": round(random.uniform(30.0, 100.0), 2),
                "location_id": loc_id,
                "tags": f"{random.choice(types)},{random.choice(statuses)}"
            }
            eid = self.transact("asset", payload)
            if eid:
                ok += 1
                if ok % 100 == 0 or ok == count:
                    print(f"  ... ingested {ok}/{count} benchmark assets")
        print(f"Done benchmark assets seeding: {ok} created.")

    def generate_assets_csv(self, output_dir: str) -> str:
        print(f"\n--- Generating CSV file with 10000 assets ---")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate local locations hierarchy to map references
        sites = []
        for i in range(10):
            sites.append({"name": f"Site {i+1:02d}", "type": "SITE"})

        buildings = []
        for i in range(100):
            buildings.append({"name": f"Edificio {i+1:03d}", "type": "BUILDING"})

        floors = []
        for i in range(1000):
            floors.append({"name": f"Piso {i+1:04d}", "type": "FLOOR"})

        rooms = []
        room_count = 0
        for f_idx in range(1000):
            num_rooms = 9 if f_idx < 890 else 8
            for r in range(num_rooms):
                rooms.append({"name": f"Sala {room_count+1:05d}", "type": "ROOM"})
                room_count += 1

        # Generate Assets
        assets = []
        manufacturers = ["Siemens", "Caterpillar", "Schneider Electric", "Atlas Copco", "ABB"]
        statuses = ["ACTIVE", "INACTIVE", "IN_MAINTENANCE"]

        systems = []
        for i in range(10):
            sys_asset = {
                "name": f"Sistema Electrico Site {i+1:02d}",
                "type": "SYSTEM",
                "status": random.choice(statuses),
                "location_id": sites[i]["name"],
                "parent_asset_id": "",
                "serial_number": f"SRN-SYS-{uuid.uuid4().hex[:8].upper()}",
                "criticality": "A",
                "manufacturer": random.choice(manufacturers),
                "health_score": round(random.uniform(80.0, 100.0), 2)
            }
            systems.append(sys_asset)
            assets.append(sys_asset)

        subsystems = []
        for i in range(100):
            parent_sys = systems[i // 10]
            sub = {
                "name": f"Subestacion {i+1:03d}",
                "type": "SUBSYSTEM",
                "status": random.choice(statuses),
                "location_id": buildings[i]["name"],
                "parent_asset_id": parent_sys["name"],
                "serial_number": f"SRN-SUB-{uuid.uuid4().hex[:8].upper()}",
                "criticality": "B",
                "manufacturer": random.choice(manufacturers),
                "health_score": round(random.uniform(70.0, 95.0), 2)
            }
            subsystems.append(sub)
            assets.append(sub)

        equipments = []
        for i in range(1000):
            parent_sub = subsystems[i // 10]
            eq = {
                "name": f"Tablero Distribucion {i+1:04d}",
                "type": "EQUIPMENT",
                "status": random.choice(statuses),
                "location_id": floors[i]["name"],
                "parent_asset_id": parent_sub["name"],
                "serial_number": f"SRN-EQ-{uuid.uuid4().hex[:8].upper()}",
                "criticality": "B",
                "manufacturer": random.choice(manufacturers),
                "health_score": round(random.uniform(65.0, 90.0), 2)
            }
            equipments.append(eq)
            assets.append(eq)

        components = []
        comp_count = 0
        room_idx = 0
        for f_idx in range(1000):
            num_rooms = 9 if f_idx < 890 else 8
            for r in range(num_rooms):
                comp = {
                    "name": f"Medidor Energia {comp_count+1:05d}",
                    "type": "COMPONENT",
                    "status": random.choice(statuses),
                    "location_id": rooms[room_idx]["name"],
                    "parent_asset_id": equipments[f_idx]["name"],
                    "serial_number": f"SRN-COMP-{uuid.uuid4().hex[:8].upper()}",
                    "criticality": "C",
                    "manufacturer": random.choice(manufacturers),
                    "health_score": round(random.uniform(60.0, 98.0), 2)
                }
                components.append(comp)
                assets.append(comp)
                comp_count += 1
                room_idx += 1

        asset_file = output_path / "assets_10k.csv"
        asset_headers = ["name", "type", "status", "location_id", "parent_asset_id",
                         "serial_number", "criticality", "manufacturer", "health_score"]
        with open(asset_file, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=asset_headers)
            writer.writeheader()
            writer.writerows(assets)
        
        print(f"  ✓ Generated {asset_file} ({len(assets)} assets)")
        return str(asset_file.resolve())

def main():
    parser = argparse.ArgumentParser(description="Metri Asset Domain Seeder")
    parser.add_argument("--host", default="localhost", help="gRPC host")
    parser.add_argument("--port", type=int, default=9090, help="gRPC port")
    parser.add_argument("--tenant", default="demo", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--execute", action="store_true", help="Send actual requests instead of dry-run")
    parser.add_argument("--force-non-local", action="store_true", help="Bypass local host safety check")
    parser.add_argument("--clean", action="store_true", help="Clean assets table before seeding")
    
    parser.add_argument("--real-hierarchy", action="store_true", help="Seed 1000 hierarchical locations and 1000 hierarchical assets with cross-relations")
    parser.add_argument("--bulk-real-hierarchy", action="store_true", help="Seed 10000 hierarchical locations and 10000 hierarchical assets using BulkIngest")
    parser.add_argument("--benchmark-assets", nargs="?", const=1000, type=int, default=0, metavar="COUNT", help="Seed N benchmark assets")
    parser.add_argument("--generate-csv", nargs="?", const=".", type=str, default=None, metavar="DIR", help="Generate assets_10k.csv in DIR")

    args = parser.parse_args()

    # ── Security Check: Environment ──
    env = os.environ.get("ENVIRONMENT", "").lower()
    if env in ("production", "prod", "staging"):
        print(f"✗ Security Error: Seeder script cannot be run in a production or staging environment (ENVIRONMENT={env}).")
        sys.exit(1)

    # ── Security Check: Local Host ──
    is_local_host = args.host in ("localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal")
    if not is_local_host and not args.force_non_local:
        print(f"✗ Security Error: Seeder script cannot be run against remote hosts unless --force-non-local is specified.")
        sys.exit(1)

    # If generating CSV, execute locally
    if args.generate_csv is not None:
        seeder = AssetSeeder(tenant_id=args.tenant, host=args.host, port=args.port, hmac_secret=args.secret, dry_run=True)
        seeder.generate_assets_csv(args.generate_csv)
        sys.exit(0)

    # Default to real-hierarchy if no flags provided and not generating CSV
    if not (args.real_hierarchy or args.bulk_real_hierarchy or args.benchmark_assets > 0 or args.clean):
        args.real_hierarchy = True

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    dry_run = not args.execute
    seeder = AssetSeeder(
        tenant_id=args.tenant,
        host=args.host,
        port=args.port,
        hmac_secret=args.secret,
        dry_run=dry_run
    )

    # Clean phase
    if args.clean or args.real_hierarchy or args.bulk_real_hierarchy or args.benchmark_assets > 0:
        print("\n--- [Fase 1] Limpiando tabla existente de asset ---")
        seeder.clear_entity("asset")

    if args.real_hierarchy:
        seeder.seed_real_hierarchy()
    if args.bulk_real_hierarchy:
        seeder.seed_bulk_real_hierarchy()
    if args.benchmark_assets > 0:
        seeder.seed_benchmark_assets(args.benchmark_assets)

if __name__ == "__main__":
    main()
