#!/usr/bin/env python3
"""Re-export del cliente gRPC-Web compartido (`scripts.common.grpc_client`)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.grpc_client import BASE_URL, ENGINE_HOST, HMAC_SECRET, GrpcWebStub, pb  # noqa: E402, F401
