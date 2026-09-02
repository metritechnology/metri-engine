#!/usr/bin/env python3
import time
from grpc_web_client import GrpcWebStub, pb

def create_search_request(tenant_id: str, entity: str, term: str) -> pb.QueryRequest:
    return pb.QueryRequest(
        tenant_id=tenant_id,
        queries={
            "res": pb.AnalyticsRequest(
                tenant_id=tenant_id,
                entity=entity,
                viz="table",
                limit=10,
                search=term
            )
        }
    )

def print_results(results):
    found = False
    for chunk in results:
        for key, res in chunk.batch_results.items():
            if res.data and res.data.rows_json and res.data.rows_json.iter:
                rows = res.data.rows_json.iter
                if not found:
                    print(f"  → Found {len(rows)} results")
                    found = True
                for r in rows:
                    row_dict = {}
                    for col, val in zip(res.data.columns, r.values):
                        row_dict[col.key] = val.string_value or str(val.number_value)
                    print(f"    - {row_dict.get('id', '???')} | {row_dict.get('name', 'N/A')}")
    if not found:
        print("  → Found 0 results")

def main():
    stub = GrpcWebStub("127.0.0.1:9090")
    tenant_id = "golden-tenant-benchmark"

    print("\n=== Searching for: '06E5' (Fuzzy Asset) ===")
    print_results(stub.Query(create_search_request(tenant_id, "asset", "06E5")))

if __name__ == "__main__":
    main()
