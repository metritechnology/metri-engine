#!/usr/bin/env python3
"""
grpc_web_client.py — Cliente gRPC-Web para el Metri Engine.
Usa HTTP + protobuf binary frames (Content-Type: application/grpc-web+proto).
Compatible con Lambda Function URL (gRPC-Web sobre HTTP/1.1 + HTTPS).
"""
import os, time, hmac, hashlib, struct, requests, json

PROTO_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "metri", "grpc", "python"
)
import sys
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

ENGINE_HOST = os.environ.get("ENGINE_HOST", "engine.metri.one")
HMAC_SECRET = os.environ.get("HMAC_SECRET", "local-dev-secret-do-not-use-in-prod")
BASE_URL    = f"https://{ENGINE_HOST}"

class GrpcWebStub:
    def __init__(self, host=None):
        self.host = host or ENGINE_HOST
        self.base = f"https://{self.host}"

    def _make_token(self):
        ts  = str(int(time.time()))
        sig = hmac.new(HMAC_SECRET.encode(), f"metri:{ts}".encode(), hashlib.sha256).hexdigest()
        return f"Bearer {sig}.{ts}"

    def _grpc_frame(self, data: bytes) -> bytes:
        """Wrap protobuf bytes in a gRPC DATA frame: 0x00 + 4-byte big-endian length."""
        return b'\x00' + struct.pack('>I', len(data)) + data

    def _parse_frames(self, body: bytes):
        """Parse gRPC-Web frames from response body. Yields (is_trailer, payload_bytes)."""
        idx = 0
        while idx < len(body):
            if idx + 5 > len(body):
                break
            flag   = body[idx]
            length = struct.unpack('>I', body[idx+1:idx+5])[0]
            idx   += 5
            payload = body[idx:idx+length]
            idx    += length
            yield (flag & 0x80 != 0, payload)

    def _post(self, path: str, proto_msg) -> bytes:
        data    = proto_msg.SerializeToString()
        framed  = self._grpc_frame(data)
        headers = {
            "Content-Type":  "application/grpc-web+proto",
            "Accept":        "application/grpc-web+proto",
            "Authorization": self._make_token(),
            "x-tenant-id":   "golden-tenant-benchmark",
            "x-grpc-web":    "1",
        }
        resp = requests.post(f"{self.base}{path}", data=framed, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.content

    def Query(self, req: pb.QueryRequest):
        """Streaming Query → list of QueryResponse."""
        body = self._post("/metri.MetriService/Query", req)
        results = []
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
