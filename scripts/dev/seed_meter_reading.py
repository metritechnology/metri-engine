#!/usr/bin/env python3
import sys
import os
import argparse
import json
import random
import time
from pathlib import Path

# Add script directory to path to enable local imports
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR))

try:
    from seed_base import SeederBase, pb, DEFAULT_HMAC_SECRET, GRPC_AVAILABLE
except ImportError:
    print("✗ Error: No se pudo importar seed_base.py. Asegúrate de ejecutar el script en su directorio.")
    sys.exit(1)

class MeterReadingSeeder(SeederBase):
    def get_existing_assets_and_locations(self) -> tuple[list[dict], list[dict]]:
        """Queries existing assets and locations from local database via gRPC."""
        print("  → Consultando activos y ubicaciones existentes...")
        assets = self._query_entity("asset")
        locations = self._query_entity("location")
        print(f"    ✓ Encontrados {len(assets)} activos y {len(locations)} ubicaciones.")
        return assets, locations

    def _query_entity(self, entity: str) -> list[dict]:
        if self.dry_run:
            return [{"id": f"mock-{entity}-{i}", "tag": f"MOCK-{entity.upper()}-{i}"} for i in range(5)]
            
        stub = self.get_stub()
        analytics_req = pb.AnalyticsRequest(
            tenant_id=self.tenant_id,
            entity=entity,
            limit=5000,
        )
        query_req = pb.QueryRequest(
            tenant_id=self.tenant_id,
            queries={"list": analytics_req}
        )
        
        entities = []
        try:
            responses = stub.Query(query_req, timeout=30, metadata=self.metadata)
            columns = []
            rows = []
            for resp in responses:
                if resp.status and not resp.status.success:
                    print(f"    [ERROR] Query {entity} falló: {resp.status.error_message}")
                    return []
                
                batch_res = resp.batch_results.get("list") if resp.batch_results else None
                row_set = batch_res.data if batch_res else resp.data
                
                if row_set:
                    if row_set.columns:
                        columns = [c.key for c in row_set.columns]
                    if row_set.rows_json and row_set.rows_json.iter:
                        rows.extend(list(row_set.rows_json.iter))
            
            for r in rows:
                ent = {}
                for idx, col in enumerate(columns):
                    if idx < len(r.values):
                        val = r.values[idx]
                        field_name = col.split("/")[-1] # Normalizar nombre (ej: asset/id -> id)
                        if val.HasField("string_value"):
                            ent[field_name] = val.string_value
                        elif val.HasField("number_value"):
                            ent[field_name] = val.number_value
                        elif val.HasField("bool_value"):
                            ent[field_name] = val.bool_value
                if ent:
                    entities.append(ent)
        except Exception as e:
            print(f"    [WARNING] Falló consulta de {entity}: {e}")
        
        return entities

    def generate_readings(self, assets: list[dict], locations: list[dict], total_records: int = 10000) -> list[dict]:
        """Generates realistic telemetry time-series readings."""
        print(f"  → Generando {total_records} lecturas realistas...")
        
        # Si no hay activos, creamos mocks
        if not assets:
            print("    [WARNING] No hay activos en la DB. Usando activos mock generados.")
            assets = [{"id": f"ast_mock_{i:02d}", "tag": f"AST-MOCK-{i:02d}"} for i in range(10)]
            
        # Elegir un subconjunto de hasta 30 activos para tener series continuas y largas
        selected_assets = assets[:30]
        
        # Mapear ubicaciones para fácil acceso
        loc_map = {l["id"]: l for l in locations if "id" in l}
        
        # Configurar perfiles de sensores por activo
        asset_configs = []
        protocols = ["MQTT", "MODBUS_TCP", "OPC_UA"]
        
        for idx, asset in enumerate(selected_assets):
            ast_id = asset.get("id")
            ast_tag = asset.get("tag", f"AST-{idx:02d}")
            loc_id = asset.get("location_id")
            
            # Si el activo no tiene ubicación, asignar una aleatoria si hay disponibles
            if not loc_id and locations:
                loc_id = random.choice(locations).get("id")
                
            protocol = random.choice(protocols)
            
            # Asignar sensores
            sensor_pool = [
                {"code": "ACTIVE_POWER", "unit": "KWT", "base_val": 150.0},
                {"code": "TEMPERATURE", "unit": "CEL", "base_val": 65.0},
                {"code": "VIBRATION", "unit": "MMS", "base_val": 2.5},
                {"code": "PRESSURE", "unit": "BAR", "base_val": 6.2}
            ]
            
            # Cada activo tendrá entre 2 y 3 sensores
            sensors = random.sample(sensor_pool, random.randint(2, 3))
            
            asset_configs.append({
                "asset_id": ast_id,
                "asset_tag": ast_tag,
                "location_id": loc_id,
                "protocol": protocol,
                "sensors": sensors
            })
            
        # Calcular cantidad total de series temporales independientes
        total_series = sum(len(config["sensors"]) for config in asset_configs)
        records_per_series = total_records // total_series
        extra_records = total_records % total_series
        
        readings = []
        now_ms = int(time.time() * 1000)
        interval_ms = 15 * 60 * 1000 # Intervalo de 15 minutos entre mediciones
        
        # Calidades de datos y sus pesos de probabilidad
        qualities = ["GOOD", "UNCERTAIN", "COMM_FAILURE"]
        quality_weights = [0.985, 0.010, 0.005]
        
        series_index = 0
        for config in asset_configs:
            ast_id = config["asset_id"]
            ast_tag = config["asset_tag"]
            loc_id = config["location_id"]
            protocol = config["protocol"]
            
            for sensor in config["sensors"]:
                code = sensor["code"]
                unit = sensor["unit"]
                val = sensor["base_val"]
                
                # Definir cuántos puntos generar para esta serie
                num_points = records_per_series
                if series_index < extra_records:
                    num_points += 1
                series_index += 1
                
                for step in range(num_points):
                    # Generate a random timestamp in the last 28 days (in ms) to fit the default 30-day UI range
                    twenty_eight_days_ms = 28 * 24 * 60 * 60 * 1000
                    ts = now_ms - random.randint(0, twenty_eight_days_ms)
                    
                    # Simulación de calidad de datos
                    quality = random.choices(qualities, weights=quality_weights)[0]
                    
                    reading_val = None
                    raw_val = ""
                    
                    if quality == "COMM_FAILURE":
                        # Falla de comunicación: sin valor numérico
                        reading_val = None
                        raw_val = random.choice(["ERR_TIMEOUT", "ERR_NO_RESPONSE", "ERR_BUSY"])
                    else:
                        # Generación física según la métrica
                        # Obtener hora del día del timestamp para perfiles diarios
                        struct_time = time.gmtime(ts / 1000)
                        hour = struct_time.tm_hour
                        is_weekend = struct_time.tm_wday >= 5
                        
                        if code == "ACTIVE_POWER":
                            # Ciclo de potencia: más consumo durante el día
                            if is_weekend:
                                hourly_factor = 0.15 # Fondo de fin de semana
                            elif 8 <= hour <= 18:
                                hourly_factor = random.uniform(0.8, 1.1)
                            else:
                                hourly_factor = random.uniform(0.2, 0.4)
                            
                            val = val * 0.95 + (sensor["base_val"] * hourly_factor) * 0.05
                            val += random.normalvariate(0, 5.0)
                            val = max(5.0, min(500.0, val))
                            reading_val = round(val, 2)
                            raw_val = f"{reading_val:.2f}"
                            
                        elif code == "TEMPERATURE":
                            # Temperatura correlacionada con potencia (si existe la serie)
                            # Simular inercia térmica
                            hour_factor = math_temp_hour_factor(hour)
                            val = val * 0.98 + (40.0 + 30.0 * hour_factor) * 0.02
                            val += random.normalvariate(0, 0.5)
                            val = max(20.0, min(95.0, val))
                            reading_val = round(val, 1)
                            raw_val = f"{reading_val:.1f}"
                            
                        elif code == "VIBRATION":
                            # Caminata aleatoria con picos ocasionales de vibración
                            val = val * 0.97 + sensor["base_val"] * 0.03 + random.normalvariate(0, 0.15)
                            # 0.5% de probabilidad de vibración transitoria alta (anomalía)
                            if random.random() < 0.005:
                                val += random.uniform(3.0, 6.0)
                            val = max(0.2, min(12.0, val))
                            reading_val = round(val, 3)
                            raw_val = f"{reading_val:.3f}"
                            
                        elif code == "PRESSURE":
                            # Rango cerrado con caídas y subidas repentinas
                            val = val * 0.9 + sensor["base_val"] * 0.1 + random.normalvariate(0, 0.1)
                            if random.random() < 0.05:
                                val -= random.uniform(0.5, 1.5) # Caída por consumo
                            val = max(3.5, min(8.5, val))
                            reading_val = round(val, 2)
                            raw_val = f"{reading_val:.2f}"
                    
                    # Dirección origen física
                    source = ""
                    if protocol == "MQTT":
                        source = f"telemetry/industrial/{protocol.lower()}/assets/{ast_tag}/{code.lower()}"
                    elif protocol == "MODBUS_TCP":
                        reg = 40000 + random.choice([1, 3, 5, 7, 10])
                        source = f"192.168.1.{10+selected_assets.index(asset)}:502 [register: {reg}]"
                    elif protocol == "OPC_UA":
                        source = f"ns=2;s=Device.{ast_tag}.{code}"
                        
                    # Simulación de latencia de red en ingested_at
                    latency = random.randint(50, 1200)
                    ingested_at = ts + latency
                    
                    # Generar lectura completa
                    reading = {
                        "asset_id": ast_id,
                        "metric_code": code,
                        "unit_of_measure": unit,
                        "data_quality": quality,
                        "protocol": protocol,
                        "timestamp": ts,
                        "ingested_at": ingested_at
                    }
                    if loc_id:
                        reading["location_id"] = loc_id
                    if reading_val is not None:
                        reading["reading_value"] = reading_val
                    if raw_val:
                        reading["raw_value"] = raw_val
                    if source:
                        reading["source_address"] = source
                        
                    # Metadata semántica dinámica simple
                    reading["metadata"] = {
                        "sensor_model": f"SNS-{code[:3]}-V1",
                        "firmware_version": "v2.4.11"
                    }
                    
                    readings.append(reading)
                    
        # Ordenar por timestamp para consistencia temporal
        readings.sort(key=lambda x: x["timestamp"])
        return readings

    def seed_data(self, count: int = 10000, execute: bool = False, generate_json_path: str = None):
        """Execute the seed process."""
        assets, locations = self.get_existing_assets_and_locations()
        
        # 1. Generar lecturas
        readings = self.generate_readings(assets, locations, count)
        
        # 2. Guardar a JSON si se solicitó
        if generate_json_path:
            print(f"\n  → Guardando {len(readings)} registros en {generate_json_path}...")
            with open(generate_json_path, "w", encoding="utf-8") as f:
                json.dump(readings, f, indent=2)
            print(f"    ✓ Archivo {generate_json_path} generado exitosamente.")
            
        # 3. Ingestar en la base de datos si se especificó el modo ejecutable
        if execute:
            print(f"\n  → Ingestando {len(readings)} registros en batches de 500...")
            # bulk_ingest
            batch_size = 500
            total_ingested = 0
            for i in range(0, len(readings), batch_size):
                batch = readings[i:i + batch_size]
                ingested = self.bulk_ingest("meter_reading", batch)
                total_ingested += ingested
                if ingested > 0:
                    print(f"    Batch {i//batch_size + 1}: Ingestadas {total_ingested}/{len(readings)} filas...")
            print(f"\n  ✓ Sembrado completado: Ingestados {total_ingested} registros de meter_reading.")

def math_temp_hour_factor(hour: int) -> float:
    # Simular temperatura ambiente / ciclo de temperatura: máxima a las 3 PM (15h), mínima a las 5 AM (5h)
    import math
    return 0.5 * (1.0 + math.sin(math.pi * (hour - 9) / 12.0))

def main():
    parser = argparse.ArgumentParser(description="Metri Meter Reading Domain Seeder")
    parser.add_argument("--host", default="localhost", help="gRPC host")
    parser.add_argument("--port", type=int, default=9090, help="gRPC port")
    parser.add_argument("--tenant", default="demo", help="Tenant ID")
    parser.add_argument("--secret", default=DEFAULT_HMAC_SECRET, help="HMAC sign secret")
    parser.add_argument("--execute", action="store_true", help="Send actual requests instead of dry-run")
    parser.add_argument("--force-non-local", action="store_true", help="Bypass local host safety check")
    
    parser.add_argument("--count", type=int, default=10000, help="Number of records to generate")
    parser.add_argument("--generate-json", type=str, default=None, metavar="FILE", help="Save generated data to FILE")
    parser.add_argument("--clear", action="store_true", help="Clear existing meter readings before seeding")

    args = parser.parse_args()

    # ── Security Check: Environment ──
    env = os.environ.get("ENVIRONMENT", "").lower()
    if env in ("production", "prod", "staging"):
        print(f"✗ Security Error: Seeder script cannot be run in a production or staging environment.")
        sys.exit(1)

    # ── Security Check: Local Host ──
    is_local_host = args.host in ("localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal")
    if not is_local_host and not args.force_non_local:
        print(f"✗ Security Error: Seeder script cannot be run against remote hosts unless --force-non-local is specified.")
        sys.exit(1)

    dry_run = not args.execute
    seeder = MeterReadingSeeder(
        tenant_id=args.tenant,
        host=args.host,
        port=args.port,
        hmac_secret=args.secret,
        dry_run=dry_run
    )

    if args.clear:
        seeder.clear_entity("meter_reading")

    seeder.seed_data(
        count=args.count,
        execute=args.execute,
        generate_json_path=args.generate_json
    )

if __name__ == "__main__":
    main()
