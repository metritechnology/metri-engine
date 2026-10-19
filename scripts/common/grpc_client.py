#!/usr/bin/env python3
"""Cliente gRPC-Web compartido para scripts y suites de confiabilidad (DRY SSOT)."""
from __future__ import annotations

import os
import struct
import sys
from pathlib import Path
from typing import Iterator

import requests

ROOT = Path(__file__).resolve().parents[2]
PROTO_DIR = ROOT / "scripts" / "proto"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PROTO_DIR) not in sys.path:
    sys.path.insert(0, str(PROTO_DIR))

import metri_pb2 as pb  # noqa: E402
from scripts.common.auth import mint_hmac_token, resolve_hmac_secret  # noqa: E402

ENGINE_HOST = os.environ.get("ENGINE_HOST", "engine.metri.one")
HMAC_SECRET = resolve_hmac_secret()
BASE_URL = f"https://{ENGINE_HOST}"


class GrpcWebStub:
    """Cliente ligero gRPC-Web sobre HTTP/1.1 + HTTPS (compatible con AWS Lambda Function URL)."""

    def __init__(self, host: str | None = None, secret: str | None = None) -> None:
        self.host = host or ENGINE_HOST
        self.secret = resolve_hmac_secret(secret)
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"

    def _make_token(self, tenant_id: str = "golden-tenant-benchmark") -> str:
        return mint_hmac_token(tenant_id=tenant_id, secret=self.secret, bearer_prefix=True)

    @staticmethod
    def _grpc_frame(data: bytes) -> bytes:
        return b"\x00" + struct.pack(">I", len(data)) + data

    @staticmethod
    def _parse_frames(body: bytes) -> Iterator[tuple[bool, bytes]]:
        idx = 0
        while idx + 5 <= len(body):
            flag = body[idx]
            length = struct.unpack(">I", body[idx + 1 : idx + 5])[0]
            idx += 5
            payload = body[idx : idx + length]
            idx += length
            yield (bool(flag & 0x80), payload)

    def _post(self, path: str, proto_msg: object) -> bytes:
        data = proto_msg.SerializeToString()  # type: ignore[attr-defined]
        framed = self._grpc_frame(data)
        tenant_id = getattr(proto_msg, "tenant_id", "") or "golden-tenant-benchmark"
        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "Authorization": self._make_token(tenant_id),
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
        }
        resp = requests.post(f"{self.base}{path}", data=framed, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.content

    def Query(self, req: pb.QueryRequest) -> list[pb.QueryResponse]:
        body = self._post("/metri.MetriService/Query", req)
        results: list[pb.QueryResponse] = []
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.QueryResponse()
                msg.ParseFromString(payload)
                results.append(msg)
        return results

    def Transact(self, req: pb.TransactionRequest) -> pb.TransactionResponse:
        body = self._post("/metri.MetriService/Transact", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.TransactionResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.TransactionResponse()
