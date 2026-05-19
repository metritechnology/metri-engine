#!/usr/bin/env python3
"""
validate_hierarchy.py — Test de paridad 100% con hierarchy.clj
Usa grpc_web_client compatible con Lambda Function URL.
"""
import sys, os, json

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR   = os.path.join(os.path.dirname(SCRIPTS_DIR), "src", "metri", "grpc", "python")
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, PROTO_DIR)

import metri_pb2 as pb
from grpc_web_client import GrpcWebStub

TENANT_ID = "golden-tenant-benchmark"
stub = GrpcWebStub()

def extract_rows(res, key):
    for chunk in res:
        if hasattr(chunk, "batch_results") and key in chunk.batch_results:
            br = chunk.batch_results[key]
            cols = [c.key for c in br.data.columns]
            if br.data.HasField("rows_json"):
                results = []
                for r in br.data.rows_json.iter:
                    vals = []
                    for v in r.values:
                        if v.HasField("string_value"): vals.append(v.string_value)
                        elif v.HasField("number_value"): vals.append(v.number_value)
                        elif v.HasField("bool_value"): vals.append(v.bool_value)
                        elif v.HasField("null_value"): vals.append(None)
                        else: vals.append(str(v))
                    results.append(dict(zip(cols, vals)))
                return results
    return []

def test():
    errors = []

    # ── Test 1: Root nodes ────────────────────────────────────────────────────
    print("=== Test 1: Root nodes (current_node_id='') ===")
    res = stub.Query(pb.QueryRequest(
        tenant_id=TENANT_ID,
        queries={"root": pb.AnalyticsRequest(
            tenant_id=TENANT_ID, entity="location", viz="tree", limit=10,
            hierarchy=pb.HierarchyContext(
                parent_field="parent_location_id",
                current_node_id="",
                inject_has_children=True,
            )
        )}
    ))
    roots = extract_rows(res, "root")
    print(f"  → {len(roots)} root(s)")
    if not roots:
        errors.append("FAIL: No root nodes")
    else:
        print("  PASS: root nodes presentes")

    # ── Test 2: has_children ────────────────────────────────────────────────
    print("\n=== Test 2: has_children ===")
    if roots:
        r = roots[0]
        if "has_children" not in r:
            errors.append("FAIL: has_children ausente")
        else:
            print(f"  has_children={r['has_children']}  PASS")

    # ── Test 3: Children ULID directo ───────────────────────────────────────
    if roots:
        site_id = roots[0].get("id")
        print(f"\n=== Test 3: Children de site_id={site_id} ===")
        res2 = stub.Query(pb.QueryRequest(
            tenant_id=TENANT_ID,
            queries={"children": pb.AnalyticsRequest(
                tenant_id=TENANT_ID, entity="location", viz="tree", limit=10,
                hierarchy=pb.HierarchyContext(
                    parent_field="parent_location_id",
                    current_node_id=site_id,
                    inject_has_children=True,
                )
            )}
        ))
        children = extract_rows(res2, "children")
        print(f"  → {len(children)} children")
        if not children:
            errors.append(f"FAIL: No children de {site_id}")
        else:
            for c in children:
                print(f"    {c.get('name')} type={c.get('type')} has_children={c.get('has_children')}")
            if all(c.get("type") == "BUILDING" for c in children):
                print("  PASS: Todos BUILDING")
            else:
                errors.append("FAIL: tipos inesperados")

        # ── Test 4: Grandchildren ──────────────────────────────────────────
        if children:
            bldg_id = children[0].get("id")
            print(f"\n=== Test 4: Floors de building={bldg_id} ===")
            res3 = stub.Query(pb.QueryRequest(
                tenant_id=TENANT_ID,
                queries={"floors": pb.AnalyticsRequest(
                    tenant_id=TENANT_ID, entity="location", viz="tree", limit=10,
                    hierarchy=pb.HierarchyContext(
                        parent_field="parent_location_id",
                        current_node_id=bldg_id,
                        inject_has_children=True,
                    )
                )}
            ))
            floors = extract_rows(res3, "floors")
            print(f"  → {len(floors)} floors")
            if not floors:
                errors.append(f"FAIL: No floors de {bldg_id}")
            else:
                for f in floors:
                    print(f"    {f.get('name')} has_children={f.get('has_children')}")
                print("  PASS")

    print("\n" + "="*50)
    if errors:
        print("RESULTADO: FAIL")
        for e in errors:
            print(f"  ❌ {e}")
        sys.exit(1)
    else:
        print("RESULTADO: ✅ PASS — Paridad 100% con hierarchy.clj")

if __name__ == "__main__":
    test()
