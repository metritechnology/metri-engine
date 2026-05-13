# Metri Engine Latency Optimization Results

## Root Cause Discovery
The baseline execution P50 was 972ms. The initial assumption that AWS Lambda Cold Starts (TCP restoration overhead from SnapStart) was the sole cause was incorrect. While Cold Starts affect the P99 (12.8s), the steady-state P50 was hampered by a code-level bottleneck.

We discovered that `metri.aegis.datalog.compiler` was generating Datalog queries that extracted the full entity map `(pull ?e [*])` for **every single matched record** (10,000+ entities) before performing aggregations in memory. This O(N) serialization cost within the JVM accounted for ~250-300ms of latency per sub-query.

## Solution Implemented
We implemented an AST-aware `pull` projection compiler logic (`infer-required-fields`). Instead of fetching all attributes, analytical queries (KPI, PIE, TIMESERIES) now only pull the explicit attributes requested in the `metrics`, `dimensions`, `order-by`, and `ts-field`. 
This drastically reduced the datom extraction from ~50 fields per entity to just 4-6 fields.

## Benchmark Results (Mega-Batch)

| Metric | Before Optimization | After Optimization | Improvement |
|--------|---------------------|--------------------|-------------|
| **P50** | 972.5 ms | 715.2 ms | **26.4%** |
| **KPI Query** | ~300 ms | ~185 ms | **~38%** |
| **PIE Query** | ~300 ms | ~200 ms | **~33%** |

*Note: The remaining latency is largely consumed by the `TABLE` query, which still has to sort 10,000 entities in-memory. If a UI provides a strict `select` constraint for the table, the Mega-Batch P50 drops significantly further.*

## Bug Fix
During the benchmark, we also detected and patched an NPE inside `metri.janus.normalizer/build-breakdown-meta` that occurred when a PIE chart query returned 0 rows.
