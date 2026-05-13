import struct
import logging
import requests
import hashlib
import json
import time
import uuid
import argparse
import concurrent.futures
from dataclasses import dataclass
from typing import List, Optional

import metri_pb2

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

FUNCTION_URL = "https://engine.metri.one/"
TENANT_ID    = "golden-tenant-benchmark"
TOKEN        = "datalog-golden-tenant"

def invoke_grpc_web(endpoint: str, proto_req, token=TOKEN, session=None):
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack('!BI', 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    headers = {
        'Content-Type':         'application/grpc-web+proto',
        'X-Grpc-Web':           '1',
        'User-Agent':           'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'X-Metri-Origin-Token': token,
        'x-amz-content-sha256': payload_hash,
    }

    t0 = time.monotonic()
    try:
        req_func = session.post if session else requests.post
        resp = req_func(
            f"{FUNCTION_URL}{endpoint}",
            data=framed_data,
            headers=headers,
            timeout=30,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return False, latency_ms, f"HTTP {resp.status_code}: {resp.text[:200]}"
            
        return True, latency_ms, None

    except requests.exceptions.RequestException as e:
        latency_ms = (time.monotonic() - t0) * 1000
        return False, latency_ms, str(e)


def build_query_request():
    req = metri_pb2.QueryRequest()
    req.tenant_id = TENANT_ID
    
    # 1. TABLE
    q_table = metri_pb2.AnalyticsRequest()
    q_table.tenant_id = TENANT_ID
    q_table.entity = "asset"
    q_table.output_cast = metri_pb2.TABLE
    q_table.viz = "table"
    q_table.limit = 50
    q_table.select_tree.update({
        "status": True,
        "name": True,
        "location_id": True
    })
    req.queries["asset_table"].CopyFrom(q_table)

    # 2. PIE
    q_pie = metri_pb2.AnalyticsRequest()
    q_pie.tenant_id = TENANT_ID
    q_pie.entity = "asset"
    q_pie.output_cast = metri_pb2.PIE
    q_pie.viz = "pie"
    
    d1 = q_pie.dimensions.add()
    d1.attribute = "status"
    d1.label_template = "Estado: {{status}}"
    
    m1 = q_pie.metrics.add()
    m1.aggregation = metri_pb2.COUNT
    
    req.queries["asset_pie"].CopyFrom(q_pie)

    # 3. KPI
    q_kpi = metri_pb2.AnalyticsRequest()
    q_kpi.tenant_id = TENANT_ID
    q_kpi.entity = "asset"
    q_kpi.output_cast = metri_pb2.KPI
    q_kpi.viz = "kpi"
    
    m_kpi = q_kpi.metrics.add()
    m_kpi.aggregation = metri_pb2.COUNT
    
    req.queries["asset_kpi"].CopyFrom(q_kpi)

    return req

def run_benchmark(iterations, concurrency):
    req = build_query_request()
    
    print("\n" + "=" * 60)
    print(f"  🚀  Benchmark Metri Engine — Consultas (Table, Pie, KPI)")
    print("=" * 60)
    print(f"  Endpoint:     {FUNCTION_URL}")
    print(f"  Entity:       asset")
    print(f"  Iterations:   {iterations}")
    print(f"  Concurrency:  {concurrency}")
    print("=" * 60 + "\n")

    session = requests.Session()
    session.headers.update({"Connection": "keep-alive"})

    latencies = []
    errors = 0

    # WARMUP
    print("Ejecutando warmup (3 requests)...")
    for _ in range(3):
        invoke_grpc_web("metri.MetriService/Query", req, session=session)
    print("Warmup completado.\n")

    print(f"Lanzando {iterations} consultas...")
    
    t_start = time.monotonic()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(invoke_grpc_web, "metri.MetriService/Query", req, TOKEN, session)
            for _ in range(iterations)
        ]
        
        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            success, latency, err = future.result()
            done_count += 1
            latencies.append(latency)
            if not success:
                errors += 1
                
            if done_count % 10 == 0 or done_count == iterations:
                print(f"  Progreso: {done_count}/{iterations} completados...")

    t_end = time.monotonic()
    elapsed = t_end - t_start

    # STATS
    if not latencies:
        print("Error: No se recopilaron latencias.")
        return

    latencies.sort()
    avg_ms = sum(latencies) / len(latencies)
    min_ms = latencies[0]
    max_ms = latencies[-1]
    p50_ms = latencies[int(len(latencies) * 0.50)]
    p95_ms = latencies[int(len(latencies) * 0.95)]
    p99_ms = latencies[int(len(latencies) * 0.99)]
    
    rps = iterations / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print("  📊  RESULTADOS DEL BENCHMARK")
    print("=" * 60)
    print(f"  Total Requests:  {iterations}")
    print(f"  Errores:         {errors}")
    print(f"  RPS (aprox):     {rps:.2f} req/s")
    print(f"  Tiempo total:    {elapsed:.2f} s")
    print("-" * 60)
    print(f"  Min:             {min_ms:.1f} ms")
    print(f"  P50 (Mediana):   {p50_ms:.1f} ms")
    print(f"  Avg:             {avg_ms:.1f} ms")
    print(f"  P95:             {p95_ms:.1f} ms")
    print(f"  P99:             {p99_ms:.1f} ms")
    print(f"  Max:             {max_ms:.1f} ms")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--iterations", type=int, default=50)
    parser.add_argument("-c", "--concurrency", type=int, default=5)
    args = parser.parse_args()
    
    run_benchmark(args.iterations, args.concurrency)
