"""
metri_schema_engine — Motor unificado de análisis y transpilación de metros proto.

Unifica los algoritmos de proto-graph-analyzer y proto2edn en un único pipeline:

  Pipeline completo (metro.proto → análisis → contrato EDN):
  ┌─────────────────────────────────────────────────────────────────┐
  │  metri.proto                                                    │
  │       │                                                         │
  │  DescriptorWalker → DescriptorTree (IR rico, tipado)           │
  │       │                                                         │
  │  ┌────┴──────────────────────────────────────────────┐         │
  │  │  GraphEngine (ProtoGraph NetworkX)                 │         │
  │  │   ├── GraphMetrics    (densidad, pagerank, etc.)  │         │
  │  │   ├── GraphTraversal  (BFS, DFS, ciclos, topo)    │         │
  │  │   ├── CoherenceScorer (10 reglas, score 0-100)    │         │
  │  │   └── DomainPartitioner (Louvain, 8 dominios)     │         │
  │  └───────────────────────────────────────────────────┘         │
  │       │                                                         │
  │  ┌────┴──────────────────────────────────────────────┐         │
  │  │  ContractPipeline                                  │         │
  │  │   ├── TypeAlgebra     (15 reglas proto3 → Malli)  │         │
  │  │   ├── ContractSynth.  (7 reglas semánticas)       │         │
  │  │   └── EDNEmitter      (serialización idiomática)  │         │
  │  └───────────────────────────────────────────────────┘         │
  │       │                                                         │
  │  janus-aegis-contract-v4.edn + audit report                    │
  └─────────────────────────────────────────────────────────────────┘

Módulos:
  descriptor_walker   → IR unificado (DescriptorTree)
  naming              → conversiones PascalCase/snake → kebab
  graph_engine        → ProtoGraph NetworkX desde DescriptorTree
  graph_metrics       → métricas teóricas del grafo
  graph_traversal     → BFS, DFS, ciclos, topológico
  coherence           → score de coherencia estructural (10 reglas)
  domain_partitioner  → partición Louvain en 8 dominios
  type_algebra        → 15 reglas proto3 → MalliSpec
  contract_synthesizer→ 7 reglas semánticas Janus-Aegis
  edn_emitter         → serialización EDN idiomática
  dedup_checker       → auditoría y corrección de duplicaciones
"""

from .descriptor_walker import build_descriptor_tree, DescriptorTree
from .contract_synthesizer import synthesize, JanusAegisContract
from .edn_emitter import emit_edn
from .naming import to_kebab, to_spec_key
from .cedar_policy_engine import (
    derive_policy_set,
    emit_policy_edn_section,
    load_model,
    CedarPolicySet,
    CedarEntityType,
    CedarAction,
    CedarScopePolicy,
)


def build_proto_graph(*args, **kwargs):
    """Lazy import — requiere networkx. Usado por los modos analyse/domains."""
    from .graph_engine import build_proto_graph as _build
    return _build(*args, **kwargs)


__all__ = [
    "build_descriptor_tree", "DescriptorTree",
    "build_proto_graph",
    "synthesize", "JanusAegisContract",
    "emit_edn",
    "to_kebab", "to_spec_key",
    # Cedar Policy Engine
    "derive_policy_set",
    "emit_policy_edn_section",
    "load_model",
    "CedarPolicySet",
    "CedarEntityType",
    "CedarAction",
    "CedarScopePolicy",
]
