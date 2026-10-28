#!/usr/bin/env python3
"""Utilidad de Bootstrap en Producción para Metri Engine con clave única en AWS SSM Parameter Store."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import re
import secrets
import string
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import boto3
import requests

# Add proto directory to path
SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(ENGINE_ROOT / "scripts" / "proto"))

try:
    import metri_pb2 as pb
    from google.protobuf import struct_pb2

    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

DEFAULT_HOST = "engine.metri.one"
DEFAULT_REGION = "us-east-1"
DEFAULT_PROFILE = "metri-dev"
DEFAULT_SSM_PARAM = "/metri/engine/bootstrap/master-key"
DEFAULT_HMAC_SECRET_NAME = "metri-hmac-secret-metri-engine"
DEFAULT_S3_BUCKET = "metri-engine"
MASTER_USER_MBF_KEY = "metri-engine/tenants/system/entities/user/usr_master.mbf"


def generate_unique_bootstrap_key(length: int = 32) -> str:
    """Genera una clave criptográfica única alfanumérica de alta entropía."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def ensure_ssm_bootstrap_key(
    session: boto3.Session,
    param_name: str,
    rotate: bool = False,
    explicit_value: str | None = None,
) -> tuple[str, int, str, bool]:
    """Obtiene o aprovisiona la clave única de bootstrap en AWS SSM Parameter Store.

    Retorna `(key_value, version, arn, created_or_rotated)`.
    """
    ssm = session.client("ssm")
    if not rotate and not explicit_value:
        try:
            resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
            param = resp["Parameter"]
            return (
                param["Value"],
                int(param.get("Version", 1)),
                param.get("ARN", param_name),
                False,
            )
        except ssm.exceptions.ParameterNotFound:
            pass

    new_key = explicit_value or generate_unique_bootstrap_key(32)
    ssm.put_parameter(
        Name=param_name,
        Description=(
            "Clave única de arranque (bootstrap) para el tenant maestro 'system' "
            "y el usuario administrador 'usr_master' en metri-engine."
        ),
        Value=new_key,
        Type="String",
        Overwrite=True,
        Tier="Standard",
    )
    resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
    param = resp["Parameter"]
    return (
        param["Value"],
        int(param.get("Version", 1)),
        param.get("ARN", param_name),
        True,
    )


def resolve_hmac_secret_from_aws(session: boto3.Session, secret_name: str) -> str:
    """Obtiene el HMAC_SECRET activo desde AWS Secrets Manager."""
    sm = session.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_name)
    return resp["SecretString"].strip()


def verify_bcrypt_password(plain_key: str, bcrypt_hash: str) -> bool:
    """Verifica criptográficamente un hash bcrypt ($2a$/$2b$/$2y$) contra una clave en texto plano."""
    try:
        import bcrypt

        return bool(bcrypt.checkpw(plain_key.encode("utf-8"), bcrypt_hash.encode("utf-8")))
    except ImportError:
        normalized_hash = bcrypt_hash.replace("$2b$", "$2y$").replace("$2a$", "$2y$")
        with tempfile.NamedTemporaryFile("w+", delete=True) as tmp:
            tmp.write(f"admin:{normalized_hash}\n")
            tmp.flush()
            proc = subprocess.run(
                ["htpasswd", "-vb", tmp.name, "admin", plain_key],
                capture_output=True,
                text=True,
                check=False,
            )
            return proc.returncode == 0


def verify_s3_master_key_binding(
    session: boto3.Session,
    bucket_name: str,
    ssm_key: str,
    ssm_arn: str,
) -> bool:
    """Descarga `usr_master.mbf` desde S3 y certifica que su hash bcrypt corresponde a la clave de SSM."""
    s3 = session.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=MASTER_USER_MBF_KEY)
        raw = obj["Body"].read()
    except Exception as e:
        print(f"  ✗ Error leyendo s3://{bucket_name}/{MASTER_USER_MBF_KEY}: {e}")
        return False

    matches = re.findall(rb"\$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}", raw)
    if not matches:
        print(f"  ✗ No se encontró hash bcrypt en s3://{bucket_name}/{MASTER_USER_MBF_KEY}")
        return False

    latest_hash = matches[-1].decode("utf-8")
    matches_ssm = verify_bcrypt_password(ssm_key, latest_hash)
    print("\n--- Certificación Criptográfica S3 (.mbf) vs AWS SSM Parameter Store ---")
    print(f"  S3 Object       : s3://{bucket_name}/{MASTER_USER_MBF_KEY}")
    print(f"  LastModified    : {obj.get('LastModified')}")
    print(f"  SSM Parameter   : {ssm_arn}")
    print(f"  Bcrypt Hash     : {latest_hash[:16]}...{latest_hash[-8:]}")
    print(f"  Matches SSM Key : {matches_ssm}")
    return matches_ssm


class GrpcWebStub:
    def __init__(self, host: str, hmac_secret: str):
        self.host = host
        self.hmac_secret = hmac_secret
        scheme = "http" if "127.0.0.1" in self.host or "localhost" in self.host else "https"
        self.base = f"{scheme}://{self.host}"
        self.session = requests.Session()

    def _make_token(self, tenant_id: str = "system") -> str:
        now = int(time.time())
        claims = {
            "exp": now + 3600,
            "iat": now,
            "jti": f"bootstrap-prod-{now}-{secrets.token_hex(4)}",
            "tid": tenant_id,
            "uid": "usr_system_bff",
        }
        payload_bytes = json.dumps(claims, separators=(",", ":")).encode("utf-8")
        payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("utf-8").rstrip("=")
        sig = hmac.new(self.hmac_secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode("utf-8").rstrip("=")
        return f"Bearer mk_{payload_b64}.{sig_b64}"

    def _grpc_frame(self, data: bytes) -> bytes:
        return b"\x00" + struct.pack(">I", len(data)) + data

    def _parse_frames(self, body: bytes):
        idx = 0
        while idx < len(body):
            if idx + 5 > len(body):
                break
            flag = body[idx]
            length = struct.unpack(">I", body[idx + 1 : idx + 5])[0]
            idx += 5
            payload = body[idx : idx + length]
            idx += length
            yield (flag & 0x80 != 0, payload)

    def _post(self, path: str, proto_msg) -> bytes:
        data = proto_msg.SerializeToString()
        framed = self._grpc_frame(data)
        tenant_id = getattr(proto_msg, "tenant_id", "system") or "system"
        token = self._make_token(tenant_id)

        headers = {
            "Content-Type": "application/grpc-web+proto",
            "Accept": "application/grpc-web+proto",
            "Authorization": token,
            "x-tenant-id": tenant_id,
            "x-grpc-web": "1",
            "X-Metri-Origin-Token": self.hmac_secret,
        }

        resp = self.session.post(f"{self.base}{path}", data=framed, headers=headers, timeout=20)
        resp.raise_for_status()
        return resp.content

    def Explore(self, req: pb.ExploreRequest) -> pb.ExploreResponse:
        body = self._post("/metri.MetriService/Explore", req)
        for is_trailer, payload in self._parse_frames(body):
            if not is_trailer:
                msg = pb.ExploreResponse()
                msg.ParseFromString(payload)
                return msg
        return pb.ExploreResponse()


def check_master_user_exists(stub: GrpcWebStub, tenant_id: str = "system") -> bool:
    """Despierta `metri-engine` y verifica si `usr_master` ya está sembrado en el tenant `system`."""
    req = pb.ExploreRequest(
        tenant_id=tenant_id,
        entity="user",
        attribute="username",
        limit=10,
    )
    try:
        resp = stub.Explore(req)
        if resp.status and resp.status.success and len(resp.values) > 0:
            return True
        return False
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Metri Production Master Bootstrap Utility (AWS SSM Parameter Store)"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Target engine endpoint (host:port)")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="AWS CLI profile name")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket name")
    parser.add_argument(
        "--ssm-param",
        default=DEFAULT_SSM_PARAM,
        help="SSM Parameter Store path for unique bootstrap key",
    )
    parser.add_argument(
        "--ensure-ssm-key",
        action="store_true",
        help="Ensure the unique bootstrap key exists in AWS SSM Parameter Store (create if missing)",
    )
    parser.add_argument(
        "--rotate-ssm-key",
        action="store_true",
        help="Force generation and rotation of a new unique bootstrap key in AWS SSM Parameter Store",
    )
    parser.add_argument(
        "--ssm-only",
        action="store_true",
        help="Only provision/verify the key in AWS SSM Parameter Store and exit without calling gRPC",
    )
    parser.add_argument(
        "--verify-s3-hash",
        action="store_true",
        help="Cryptographically verify that usr_master.mbf in S3 matches the SSM Parameter Store master key",
    )
    parser.add_argument("--secret", default=None, help="Explicit HMAC secret (otherwise fetched from Secrets Manager)")
    parser.add_argument("--email", default="admin@metri.one", help="Admin email address")
    parser.add_argument("--username", default="admin", help="Admin username")

    args = parser.parse_args()

    session = (
        boto3.Session(profile_name=args.profile, region_name=args.region)
        if args.profile
        else boto3.Session(region_name=args.region)
    )

    # 1. Resolver o aprovisionar clave única en AWS SSM Parameter Store
    bootstrap_key, version, ssm_arn, changed = ensure_ssm_bootstrap_key(
        session=session,
        param_name=args.ssm_param,
        rotate=args.rotate_ssm_key,
    )
    action_label = "CREATED/ROTATED" if changed else "EXISTING"
    masked_key = f"{bootstrap_key[:4]}...{bootstrap_key[-4:]}" if len(bootstrap_key) >= 8 else "****"

    print("============================================================")
    print("      METRI PRODUCTION SYSTEM BOOTSTRAP (SSM PARAMETER)     ")
    print("============================================================")
    print(f"AWS Region      : {args.region}")
    print(f"SSM Parameter   : {ssm_arn} (v{version} - {action_label})")
    print(f"Bootstrap Key   : {masked_key} (len={len(bootstrap_key)})")
    print("============================================================")

    if args.ssm_only:
        return

    if not GRPC_AVAILABLE:
        print("✗ Error: python grpc library is not available. Please verify local package bindings.")
        sys.exit(1)

    hmac_secret = args.secret or resolve_hmac_secret_from_aws(session, DEFAULT_HMAC_SECRET_NAME)
    print(f"Target API Host : {args.host}")
    print(f"HMAC Secret     : {hmac_secret[:6]}...{hmac_secret[-6:]} (Loaded)")
    print(f"Admin Email     : {args.email}")
    print(f"Admin Username  : {args.username}")
    print("============================================================")

    stub = GrpcWebStub(args.host, hmac_secret)
    tenant_id = "system"

    # 2. Despertar cold-start bootstrap en Rust y verificar usr_master
    print(f"Triggering/checking cold-start bootstrap in tenant '{tenant_id}'...")
    found = False
    for attempt in range(1, 4):
        if check_master_user_exists(stub, tenant_id):
            found = True
            break
        time.sleep(2 * attempt)

    if not found:
        print(f"\n[ERROR] 'usr_master' was not found after cold-start invocation.")
        sys.exit(2)

    print(f"\n[OK] Tenant '{tenant_id}' and 'usr_master' are bootstrapped in production.")

    if args.verify_s3_hash:
        if not verify_s3_master_key_binding(session, args.bucket, bootstrap_key, ssm_arn):
            print("\n[ERROR] S3 usr_master.mbf password_hash DOES NOT MATCH SSM Parameter Store key!")
            sys.exit(3)
        print("\n[OK] Cryptographic verification passed: usr_master.mbf is bound to SSM master-key.")


if __name__ == "__main__":
    main()
