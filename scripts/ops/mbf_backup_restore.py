#!/usr/bin/env python3
"""CLI operativo de copias de seguridad, verificación binaria y restauración para archivos .mbf en Amazon S3."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import struct
import sys
import zlib
from pathlib import Path
from typing import Any

MBF_MAGIC_DATA = b"METRIBIN"
MBF_MAGIC_IDX = b"MBFINDEX"
MBF_FOOTER_MAGIC = b"MBFFOOTR"


def inspect_mbf_bytes(data: bytes) -> dict[str, Any]:
    """Valida la estructura física de 64 bytes, cabeceras y sumas CRC32 de un contenedor .mbf."""
    size = len(data)
    if size < 128 or (size % 64) != 0:
        raise ValueError(f"Tamaño inválido o no alineado a 64 bytes: {size} bytes")

    magic = data[0:8]
    if magic not in (MBF_MAGIC_DATA, MBF_MAGIC_IDX):
        raise ValueError(f"Magic header inválido: {magic!r} (se esperaba METRIBIN o MBFINDEX)")

    footer_magic = data[-8:]
    if footer_magic != MBF_FOOTER_MAGIC:
        raise ValueError(f"Footer magic inválido: {footer_magic!r} (se esperaba MBFFOOTR)")

    version, flags, row_count = struct.unpack("<HHI", data[8:16])
    min_tx_id, max_tx_id, min_ts_ms, max_ts_ms = struct.unpack("<QQqq", data[32:64])
    dir_offset, dir_len, hdr_crc, dir_crc, schema_hi, fiscal_lock_ms = struct.unpack(
        "<QIIIIq", data[-64:-32]
    )

    computed_hdr_crc = zlib.crc32(data[0:64]) & 0xFFFFFFFF
    if computed_hdr_crc != hdr_crc:
        raise ValueError(
            f"Discrepancia de Header CRC32: esperado 0x{hdr_crc:08X}, calculado 0x{computed_hdr_crc:08X}"
        )

    if dir_offset < 64 or (dir_offset + dir_len) > (size - 64):
        raise ValueError(
            f"Offsets de directorio fuera de rango: offset={dir_offset}, len={dir_len}, size={size}"
        )

    computed_dir_crc = zlib.crc32(data[dir_offset : dir_offset + dir_len]) & 0xFFFFFFFF
    if computed_dir_crc != dir_crc:
        raise ValueError(
            f"Discrepancia de Directory CRC32: esperado 0x{dir_crc:08X}, calculado 0x{computed_dir_crc:08X}"
        )

    return {
        "valid": True,
        "magic": magic.decode("ascii"),
        "version": version,
        "cell_id": (flags >> 10) & 0x3F,
        "is_hot_head": bool(flags & 0x01),
        "is_crypto_sealed": bool(flags & 0x02),
        "row_count": row_count,
        "min_tx_id": min_tx_id,
        "epoch_id": max_tx_id,
        "min_timestamp_ms": min_ts_ms,
        "max_timestamp_ms": max_ts_ms,
        "directory_offset": dir_offset,
        "directory_len": dir_len,
        "header_crc32": f"0x{hdr_crc:08X}",
        "directory_crc32": f"0x{dir_crc:08X}",
        "schema_fingerprint_hi": f"0x{schema_hi:08X}",
        "fiscal_lock_epoch_ms": fiscal_lock_ms,
        "byte_size": size,
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _s3_client(region: str, profile: str | None) -> Any:
    import boto3

    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("s3")


def cmd_verify(args: argparse.Namespace) -> int:
    """Verifica un archivo .mbf local o alojado en Amazon S3."""
    if args.file:
        raw = Path(args.file).read_bytes()
        source = str(args.file)
    elif args.bucket and args.key:
        s3 = _s3_client(args.region, args.profile)
        kwargs: dict[str, Any] = {"Bucket": args.bucket, "Key": args.key}
        if args.version_id:
            kwargs["VersionId"] = args.version_id
        resp = s3.get_object(**kwargs)
        raw = resp["Body"].read()
        source = f"s3://{args.bucket}/{args.key}" + (
            f"?versionId={args.version_id}" if args.version_id else ""
        )
    else:
        print("Error: especifica --file <ruta> o (--bucket <bkt> --key <key>)", file=sys.stderr)
        return 1

    report = inspect_mbf_bytes(raw)
    report["source"] = source
    print(json.dumps(report, indent=2))
    return 0


def cmd_checkpoint(args: argparse.Namespace) -> int:
    """Crea un checkpoint inmutable Server-Side en s3://<bucket>/backups/ sin transferencia de red."""
    s3 = _s3_client(args.region, args.profile)
    source_key = args.source_key or f"{args.prefix}/vortex/{args.table}.mbf"
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    head = s3.head_object(Bucket=args.bucket, Key=source_key)
    etag = head.get("ETag", "").strip('"')
    version_id = head.get("VersionId", "latest")

    if args.tenant_id:
        dest_key = f"backups/tenants/{args.tenant_id}/{ts}__etag_{etag[:12]}.mbf"
    else:
        dest_key = f"backups/checkpoints/{today}/{args.table}__{ts}__etag_{etag[:12]}.mbf"

    result = {
        "action": "server_side_checkpoint",
        "bucket": args.bucket,
        "source_key": source_key,
        "source_version_id": version_id,
        "source_etag": etag,
        "dest_key": dest_key,
        "size_bytes": head.get("ContentLength", 0),
        "dry_run": not args.apply,
    }
    if args.apply:
        copy_source: dict[str, str] = {"Bucket": args.bucket, "Key": source_key}
        if version_id and version_id != "latest":
            copy_source["VersionId"] = version_id
        s3.copy_object(
            Bucket=args.bucket,
            CopySource=copy_source,
            Key=dest_key,
            ContentType="application/x-metri-mbf",
            MetadataDirective="REPLACE",
            Metadata={
                "source-key": source_key,
                "source-etag": etag,
                "source-version-id": str(version_id),
                "checkpoint-utc": ts,
            },
        )
        result["status"] = "CREATED"
    else:
        result["status"] = "DRY_RUN_PREVIEW (agrega --apply para ejecutar en S3)"

    print(json.dumps(result, indent=2))
    return 0


def cmd_list_versions(args: argparse.Namespace) -> int:
    """Lista las versiones S3 de una partición/celda y los checkpoints disponibles."""
    s3 = _s3_client(args.region, args.profile)
    target_prefix = args.key_prefix or f"{args.prefix}/vortex/"
    resp = s3.list_object_versions(Bucket=args.bucket, Prefix=target_prefix, MaxKeys=args.limit)
    versions = [
        {
            "key": v["Key"],
            "version_id": v["VersionId"],
            "is_latest": v["IsLatest"],
            "size_bytes": v["Size"],
            "etag": v.get("ETag", "").strip('"'),
            "storage_class": v.get("StorageClass", "STANDARD"),
            "last_modified": v["LastModified"].isoformat(),
        }
        for v in resp.get("Versions", [])
    ]
    print(json.dumps({"bucket": args.bucket, "prefix": target_prefix, "versions": versions}, indent=2))
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    """Restaura una versión previa de S3 o un checkpoint verificando antes su integridad .mbf."""
    s3 = _s3_client(args.region, args.profile)
    source_key = args.checkpoint_key or args.target_key
    get_kwargs: dict[str, Any] = {"Bucket": args.bucket, "Key": source_key}
    if args.version_id:
        get_kwargs["VersionId"] = args.version_id

    obj = s3.get_object(**get_kwargs)
    raw_bytes = obj["Body"].read()
    integrity = inspect_mbf_bytes(raw_bytes)

    plan = {
        "action": "restore_mbf_object",
        "bucket": args.bucket,
        "source_key": source_key,
        "source_version_id": args.version_id,
        "target_key": args.target_key,
        "integrity_verified": integrity,
        "dry_run": not args.apply,
    }
    if args.apply:
        copy_source: dict[str, str] = {"Bucket": args.bucket, "Key": source_key}
        if args.version_id:
            copy_source["VersionId"] = args.version_id
        s3.copy_object(
            Bucket=args.bucket,
            CopySource=copy_source,
            Key=args.target_key,
            ContentType="application/x-metri-mbf",
        )
        plan["status"] = "RESTORED"
    else:
        plan["status"] = "DRY_RUN_VERIFIED (agrega --apply para confirmar la restauración)"

    print(json.dumps(plan, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gestor de copias de seguridad, verificación y restauración para Metri Vortex (.mbf)."
    )
    parser.add_argument("--bucket", default="metri-engine", help="Bucket S3 de Metri Engine")
    parser.add_argument("--prefix", default="metri-engine", help="Prefijo raíz en S3")
    parser.add_argument("--region", default="us-east-1", help="Región AWS")
    parser.add_argument("--profile", default=None, help="Perfil AWS CLI opcional")

    sub = parser.add_subparsers(dest="command", required=True)

    p_ver = sub.add_parser("verify", help="Verifica integridad física y CRC32 de un archivo .mbf")
    p_ver.add_argument("--file", help="Ruta a un archivo .mbf local")
    p_ver.add_argument("--key", help="Clave S3 del objeto .mbf")
    p_ver.add_argument("--version-id", help="VersionId opcional en S3")

    p_chk = sub.add_parser("checkpoint", help="Crea un checkpoint inmutable en backups/ (Server-Side Copy)")
    p_chk.add_argument("--table", default="metri-mbf-local", help="Nombre de la tabla/partición")
    p_chk.add_argument("--source-key", help="Clave S3 de origen explícita")
    p_chk.add_argument("--tenant-id", help="ID de tenant opcional para organizar en backups/tenants/")
    p_chk.add_argument("--apply", action="store_true", help="Ejecuta la copia real en S3 (sin esto es dry-run)")

    p_lst = sub.add_parser("list-versions", help="Lista versiones históricas en S3 Versioning")
    p_lst.add_argument("--key-prefix", help="Prefijo a consultar (ej. backups/ o metri-engine/vortex/)")
    p_lst.add_argument("--limit", type=int, default=25, help="Máximo de versiones a listar")

    p_rst = sub.add_parser("restore", help="Verifica y restaura una versión S3 o checkpoint")
    p_rst.add_argument("--target-key", required=True, help="Clave S3 destino a restaurar")
    p_rst.add_argument("--version-id", help="VersionId histórico a promover")
    p_rst.add_argument("--checkpoint-key", help="Clave en backups/checkpoints/ desde la cual restaurar")
    p_rst.add_argument("--apply", action="store_true", help="Aplica la restauración en S3")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    handlers = {
        "verify": cmd_verify,
        "checkpoint": cmd_checkpoint,
        "list-versions": cmd_list_versions,
        "restore": cmd_restore,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
