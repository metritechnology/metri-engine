import os
import json
import pandas as pd

def load_data(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

def build_golden_set():
    base_dir = os.path.dirname(__file__)
    dataset_path = os.path.join(base_dir, "dataset.json")
    
    if not os.path.exists(dataset_path):
        print(f"Error: {dataset_path} no encontrado. Ejecuta seeder.py primero.")
        return
        
    data = load_data(dataset_path)
    
    # Cargar en DataFrames
    df_loc = pd.DataFrame(data["location"])
    df_ast = pd.DataFrame(data["asset"])
    df_wo = pd.DataFrame(data["work_order"])
    
    expectations = {}
    
    print("Calculando Oráculo Pandas (Golden Set)...")
    
    # ---------------------------------------------------------
    # Categoría B: Matemáticas y Agrupaciones (Ejemplos representativos)
    # ---------------------------------------------------------
    
    # Q001: Count de assets por status
    cnt_status = df_ast.groupby("status").size().to_dict()
    expectations["Q001_count_assets_by_status"] = cnt_status
    
    # Q002: Sumatoria de total_cost de Work Orders por priority
    sum_cost = df_wo.groupby("priority")["total_cost"].sum().to_dict()
    expectations["Q002_sum_cost_by_priority"] = sum_cost
    
    # Q003: Promedio de completion_percentage de Work Orders en status = 'IN_PROGRESS'
    avg_completion = df_wo[df_wo["status"] == "IN_PROGRESS"]["completion_percentage"].mean()
    expectations["Q003_avg_completion_in_progress"] = float(avg_completion)
    
    # ---------------------------------------------------------
    # Categoría C: Navegación de Grafo (Join / Ref-Filters)
    # ---------------------------------------------------------
    
    # Q004: Sumatoria de costo de Work Orders pero agrupado por el 'type' de la Location
    # Requiere JOIN WO -> Asset (location_id) -> Location (type)
    df_wo_loc = df_wo.merge(df_loc, left_on="location_id", right_on="id", suffixes=('_wo', '_loc'))
    sum_cost_loc = df_wo_loc.groupby("type")["total_cost"].sum().to_dict()
    expectations["Q004_sum_cost_by_location_type"] = sum_cost_loc
    
    # Q005: Count de Work Orders críticas en assets con estatus 'IN_MAINTENANCE'
    df_wo_ast = df_wo.merge(df_ast, left_on="asset_id", right_on="id", suffixes=('_wo', '_ast'))
    critical_in_maint = len(df_wo_ast[(df_wo_ast["priority"] == "CRITICAL") & (df_wo_ast["status_ast"] == "IN_MAINTENANCE")])
    expectations["Q005_count_critical_in_maintenance"] = critical_in_maint
    
    # ---------------------------------------------------------
    # Categoría A: Operaciones de Tiempo
    # ---------------------------------------------------------
    
    # Q006: Max total_cost de WO creadas en la primera mitad del año (antes del 2026-07-01)
    mid_year = pd.Timestamp("2026-07-01").timestamp()
    max_cost_h1 = df_wo[df_wo["due_date"] < mid_year]["total_cost"].max()
    expectations["Q006_max_cost_first_half"] = float(max_cost_h1)

    # ---------------------------------------------------------
    # Extrapolación de casos a 1000
    # ---------------------------------------------------------
    import random
    random.seed(42)
    
    status_list = df_wo["status"].unique().tolist()
    priority_list = df_wo["priority"].unique().tolist()
    
    print("Generando 1000 casos aleatorios...")
    for i in range(1000):
        # Escoger un atributo y agregación al azar
        agg = random.choice(["SUM", "AVG", "MAX", "MIN"])
        field = random.choice(["total_cost", "completion_percentage"])
        status = random.choice(status_list)
        
        # Calcular con pandas
        subset = df_wo[df_wo["status"] == status][field]
        if len(subset) == 0:
            val = None
        else:
            if agg == "SUM": val = float(subset.sum())
            elif agg == "AVG": val = float(subset.mean())
            elif agg == "MAX": val = float(subset.max())
            elif agg == "MIN": val = float(subset.min())
            
        case_id = f"Q_AUTO_{i:04d}"
        expectations[case_id] = {
            "entity": "work_order",
            "agg": agg,
            "field": field,
            "filter_status": status,
            "expected_value": val
        }
    
    out_path = os.path.join(base_dir, "expectations.json")
    with open(out_path, 'w') as f:
        json.dump(expectations, f, indent=2)
        
    print(f"✅ Golden Set compilado exitosamente. {len(expectations)} Casos calculados listos.")
    print(f"📁 Guardado en: {out_path}")

if __name__ == "__main__":
    build_golden_set()
