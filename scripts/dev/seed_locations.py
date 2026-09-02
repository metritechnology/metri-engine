#!/usr/bin/env python3
import sys
import os
import argparse
import csv as csv_mod
from pathlib import Path

# Add script directory to path to enable local imports
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from seed_base import SeederBase, pb, DEFAULT_HMAC_SECRET, GRPC_AVAILABLE
except ImportError:
    print("✗ Error: No se pudo importar seed_base.py. Asegúrate de ejecutar el script en su directorio.")
    sys.exit(1)

class LocationSeeder(SeederBase):
    def generate_locations_csv_data(self, count: int = 1000) -> list[dict]:
        locations = []
        if count == 1000:
            # 10 Sites
            sites = []
            for i in range(10):
                site = {"name": f"Site {i+1:02d}", "type": "SITE", "parent_location_id": ""}
                sites.append(site)
                locations.append(site)

            # 90 Buildings (9 per site)
            buildings = []
            for i in range(90):
                parent_site = sites[i // 9]
                bld = {
                    "name": f"Edificio Site {i // 9 + 1:02d} - {i % 9 + 1:02d}",
                    "type": "BUILDING",
                    "parent_location_id": parent_site["name"]
                }
                buildings.append(bld)
                locations.append(bld)

            # 450 Floors (5 per building)
            floors = []
            for i in range(450):
                parent_bld = buildings[i // 5]
                flr = {
                    "name": f"Piso Bldg {i // 5 + 1:02d} - Floor {i % 5 + 1}",
                    "type": "FLOOR",
                    "parent_location_id": parent_bld["name"]
                }
                floors.append(flr)
                locations.append(flr)

            # 450 Rooms (1 per floor)
            for i in range(450):
                rm = {
                    "name": f"Sala Room {i // 5 + 1:02d}-{i % 5 + 1:02d}",
                    "type": "ROOM",
                    "parent_location_id": floors[i]["name"]
                }
                locations.append(rm)
        else:
            # 10k hierarchy
            # 10 Sites
            sites = []
            for i in range(10):
                site = {"name": f"Site {i+1:02d}", "type": "SITE", "parent_location_id": ""}
                sites.append(site)
                locations.append(site)

            # 100 Buildings
            buildings = []
            for i in range(100):
                parent_site = sites[i // 10]
                bld = {
                    "name": f"Edificio {i+1:03d}",
                    "type": "BUILDING",
                    "parent_location_id": parent_site["name"]
                }
                buildings.append(bld)
                locations.append(bld)

            # 1000 Floors
            floors = []
            for i in range(1000):
                parent_bld = buildings[i // 10]
                flr = {
                    "name": f"Piso {i+1:04d}",
                    "type": "FLOOR",
                    "parent_location_id": parent_bld["name"]
                }
                floors.append(flr)
                locations.append(flr)

            # 8890 Rooms (Total = 10000)
            room_count = 0
            for f_idx in range(1000):
                num_rooms = 9 if f_idx < 890 else 8
                for r in range(num_rooms):
                    rm = {
                        "name": f"Sala {room_count+1:05d}",
                        "type": "ROOM",
                        "parent_location_id": floors[f_idx]["name"]
                    }
                    locations.append(rm)
                    room_count += 1
        return locations

    def seed_locations_hierarchy(self):
        print(f"\n--- Seeding 1000 locations hierarchically (via CSV Import simulation) ---")
        # 1. Generar datos de 1000 localizaciones
        locations = self.generate_locations_csv_data(1000)
        
        # 2. Guardar en un CSV temporal
        temp_dir = Path("/tmp/metri-csv")
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_csv = temp_dir / "locations_1k_temp.csv"
        
        loc_headers = ["name", "type", "parent_location_id"]
        with open(temp_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=loc_headers)
            writer.writeheader()
            writer.writerows(locations)
            
        # 3. Importar usando la estrategia de resolución de herencias por CSV
        self.import_locations_csv(str(temp_csv))
        
        # 4. Limpiar archivo temporal
        try:
            temp_csv.unlink()
        except Exception:
            pass

    def import_locations_csv(self, csv_file_path: str):
        print(f"\n--- Importing locations from CSV: {csv_file_path} ---")
        if not Path(csv_file_path).exists():
            print(f"✗ Error: El archivo CSV no existe en la ruta: {csv_file_path}")
            return
        
        # 1. Leer todas las ubicaciones del CSV
        rows = []
        with open(csv_file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            for r in reader:
                rows.append(dict(r))
        
        print(f"  ✓ {len(rows)} ubicaciones leídas del CSV.")
        
        # 2. Agrupar las ubicaciones por tipo para procesarlas nivel por nivel:
        # SITE -> BUILDING -> FLOOR -> ROOM -> cualquier otro
        # Esto asegura que los padres se creen antes de que los hijos intenten referenciarlos.
        level_order = {
            "SITE": 0,
            "BUILDING": 1,
            "FLOOR": 2,
            "ROOM": 3,
            "ZONE": 4
        }
        
        # Agrupar rows por nivel jerárquico
        levels = {}
        for r in rows:
            l_type = r.get("type", "").upper()
            lvl = level_order.get(l_type, 99)
            levels.setdefault(lvl, []).append(r)
            
        name_to_id = {}
        import concurrent.futures
        total_ingested = 0
        
        for lvl in sorted(levels.keys()):
            batch_rows = levels[lvl]
            if not batch_rows:
                continue
                
            lvl_type = batch_rows[0].get("type", "UNKNOWN")
            print(f"\n  Ingestando {len(batch_rows)} ubicaciones de nivel {lvl} ({lvl_type})...")
            
            # Preparar los payloads resolviendo las relaciones parent_location_id
            payloads = []
            for row in batch_rows:
                payload = {k: v for k, v in row.items() if v != ""}
                parent_ref = payload.get("parent_location_id")
                if parent_ref:
                    # Intentar resolver usando nuestro diccionario de nombres a IDs
                    if parent_ref in name_to_id:
                        payload["parent_location_id"] = name_to_id[parent_ref]
                    else:
                        print(f"    [WARNING] No se pudo resolver el padre '{parent_ref}' para '{payload.get('name')}'. Se enviará el valor original.")
                payloads.append(payload)
            
            # Ejecutar transacciones en paralelo
            max_workers = min(10, len(payloads)) if payloads else 1
            
            def transact_row(p):
                orig_name = p.get("name")
                eid = self.transact("location", p)
                return orig_name, eid
                
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_payload = {executor.submit(transact_row, p): p for p in payloads}
                
                lvl_created = 0
                for future in concurrent.futures.as_completed(future_to_payload):
                    orig_name, eid = future.result()
                    if eid:
                        name_to_id[orig_name] = eid
                        lvl_created += 1
                        
            total_ingested += lvl_created
            print(f"    ✓ Nivel {lvl} completado: {lvl_created}/{len(payloads)} ubicaciones creadas.")
            
        print(f"\nImportación completada exitosamente. Total ingresado: {total_ingested} ubicaciones.")
        self.context["location_ids"] = list(name_to_id.values())
        leafs = [eid for name, eid in name_to_id.items() if any(name.startswith(prefix) for prefix in ("Sala", "Room"))]
        self.context["leaf_location_ids"] = leafs if leafs else list(name_to_id.values())

    def generate_locations_csv(self, output_dir: str) -> str:
        print(f"\n--- Generating CSV file with 10000 locations ---")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        locations = self.generate_locations_csv_data(10000)

        loc_file = output_path / "locations_10k.csv"
        loc_headers = ["name", "type", "parent_location_id"]
        with open(loc_file, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=loc_headers)
            writer.writeheader()
            writer.writerows(locations)
        
        print(f"  ✓ Generated {loc_file} ({len(locations)} locations)")
        return str(loc_file.resolve())

def main():
    parser = argparse.ArgumentParser(description="Metri Location Domain Seeder")
    parser.add_argument("--host", default="localhost", help="gRPC host")
    parser.add_argument("--port", type=int, default=9090, help="gRPC port")
    parser.add_argument("--tenant", default="demo", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--execute", action="store_true", help="Send actual requests instead of dry-run")
    parser.add_argument("--force-non-local", action="store_true", help="Bypass local host safety check")
    
    parser.add_argument("--hierarchy", action="store_true", help="Seed 1000 hierarchical locations")
    parser.add_argument("--generate-csv", nargs="?", const=".", type=str, default=None, metavar="DIR", help="Generate locations_10k.csv in DIR")
    parser.add_argument("--import-csv", type=str, default=None, metavar="FILE", help="Import locations from CSV file, resolving parent name references to database IDs")

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
        seeder = LocationSeeder(tenant_id=args.tenant, host=args.host, port=args.port, hmac_secret=args.secret, dry_run=True)
        seeder.generate_locations_csv(args.generate_csv)
        sys.exit(0)

    # Default to seeding hierarchy if no flags provided and not generating/importing CSV
    if not args.hierarchy and not args.import_csv:
        args.hierarchy = True

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available.")
        sys.exit(1)

    dry_run = not args.execute
    seeder = LocationSeeder(
        tenant_id=args.tenant,
        host=args.host,
        port=args.port,
        hmac_secret=args.secret,
        dry_run=dry_run
    )

    if args.import_csv:
        seeder.import_locations_csv(args.import_csv)
    elif args.hierarchy:
        seeder.seed_locations_hierarchy()

if __name__ == "__main__":
    main()
