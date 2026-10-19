#!/usr/bin/env python3
"""Fuente Única de Verdad (DRY) para firma y tokens de sesión HMAC-SHA256 (`mk_...`)."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid

DEFAULT_DEV_HMAC_SECRET = "secret-key-development-metri-256-bits!!!"


def resolve_hmac_secret(explicit_secret: str | None = None) -> str:
    """Resuelve el secreto HMAC desde el argumento explícito o la variable `HMAC_SECRET`."""
    return explicit_secret or os.environ.get("HMAC_SECRET", DEFAULT_DEV_HMAC_SECRET)


def mint_hmac_token(
    tenant_id: str = "system",
    user_id: str = "usr_system_bff",
    ttl_seconds: int = 3600,
    secret: str | None = None,
    bearer_prefix: bool = True,
) -> str:
    """Genera un token canónico `mk_<payload_b64>.<sig_b64>` compatible con `HmacTokenVerifier`."""
    effective_secret = resolve_hmac_secret(secret)
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4()),
    }
    payload_bytes = json.dumps(claims, separators=(",", ":")).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("utf-8").rstrip("=")
    sig_bytes = hmac.new(effective_secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode("utf-8").rstrip("=")
    token = f"mk_{payload_b64}.{sig_b64}"
    return f"Bearer {token}" if bearer_prefix else token
