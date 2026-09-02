#!/usr/bin/env python3
"""
grpc_web_client.py — Cliente gRPC-Web para el Metri Engine.
Usa HTTP + protobuf binary frames (Content-Type: application/grpc-web+proto).
Compatible con Lambda Function URL (gRPC-Web sobre HTTP/1.1 + HTTPS).
"""
import os, time, hmac, hashlib, struct, requests, json, uuid, base64

curr = os.path.abspath(__file__)
ENGINE_ROOT = None
while curr != os.path.dirname(curr):
    curr = os.path.dirname(curr)
    if os.path.exists(os.path.join(curr, "Cargo.toml")) and os.path.exists(os.path.join(curr, "scripts", "proto")):
        ENGINE_ROOT = curr
        break
if not ENGINE_ROOT:
    ENGINE_ROOT = "/Users/macuser/projects/metri/metri-engine"
PROTO_DIR = os.path.join(ENGINE_ROOT, "scripts", "proto")
import sys
sys.path.insert(0, PROTO_DIR)
import metri_pb2 as pb

ENGINE_HOST = os.environ.get("ENGINE_HOST", "engine.metri.one")
HMAC_SECRET = os.environ.get("HMAC_SECRET", "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2")
BASE_URL    = f"https://{ENGINE_HOST}"

class GrpcWebStub:
    def __init__(self, host=None):
        self.host = host or ENGINE_HOST
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"

    def _make_token(self, tenant_id="golden-tenant-benchmark"):
        now = int(time.time())
        claims = {
            "tid": tenant_id,
            "uid": "usr_system_bff",
            "iat": now,
            "exp": now + 3600,
            "jti": str(uuid.uuid4())
        }
        payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
        payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
        
        mac = hmac.new(HMAC_SECRET.encode('utf-8'), payload_bytes, hashlib.sha256)
        sig_bytes = mac.digest()
        sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
        
        return f"Bearer mk_{payload_b64}.{sig_b64}"

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
        
        tenant_id = "golden-tenant-benchmark"
        if hasattr(proto_msg, "tenant_id") and proto_msg.tenant_id:
            tenant_id = proto_msg.tenant_id
            
        headers = {
            "Content-Type":  "application/grpc-web+proto",
            "Accept":        "application/grpc-web+proto",
            "Authorization": self._make_token(tenant_id),
            "x-tenant-id":   tenant_id,
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
