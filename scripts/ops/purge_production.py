#!/usr/bin/env python3
"""Utilidad de purga total del almacén binario S3 Metri Vortex (.mbf) en s3://metri-engine y reinicio en frío de AWS Lambda."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import boto3

REQUIRED_CONFIRM_PHRASE = "CONFIRM-PURGE-METRI-PROD"


def purge_s3_bucket(
    session: boto3.Session,
    bucket_name: str,
    prefix: str = "",
) -> dict[str, int]:
    """Elimina todos los objetos, versiones y marcadores de borrado en un bucket S3."""
    s3 = session.client("s3")
    start_time = time.time()
    deleted_objects = 0
    deleted_versions = 0
    aborted_uploads = 0

    print(f"\n--- Purgando bucket S3: s3://{bucket_name}/{prefix} ---")

    # 1. Eliminar objetos actuales (list_objects_v2 + delete_objects en lotes de 1000)
    paginator = s3.get_paginator("list_objects_v2")
    page_kwargs: dict[str, Any] = {"Bucket": bucket_name}
    if prefix:
        page_kwargs["Prefix"] = prefix

    for page in paginator.paginate(**page_kwargs):
        contents = page.get("Contents", [])
        if not contents:
            continue
        objects_batch = [{"Key": obj["Key"]} for obj in contents]
        for i in range(0, len(objects_batch), 1000):
            chunk = objects_batch[i : i + 1000]
            resp = s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": chunk, "Quiet": True},
            )
            errors = resp.get("Errors", [])
            deleted_objects += len(chunk) - len(errors)
            if errors:
                print(f"  [WARN] Errores al borrar lote en {bucket_name}: {errors[:3]}")
        print(f"  Progreso: {deleted_objects} objetos eliminados...")

    # 2. Eliminar versiones históricas y DeleteMarkers si el bucket tuvo versionado activo
    ver_paginator = s3.get_paginator("list_object_versions")
    for page in ver_paginator.paginate(**page_kwargs):
        version_items = []
        for v in page.get("Versions", []):
            version_items.append({"Key": v["Key"], "VersionId": v["VersionId"]})
        for dm in page.get("DeleteMarkers", []):
            version_items.append({"Key": dm["Key"], "VersionId": dm["VersionId"]})

        if not version_items:
            continue

        for i in range(0, len(version_items), 1000):
            chunk = version_items[i : i + 1000]
            resp = s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": chunk, "Quiet": True},
            )
            errors = resp.get("Errors", [])
            deleted_versions += len(chunk) - len(errors)

    # 3. Abortar cargas multiparte incompletas si existen
    try:
        mp_resp = s3.list_multipart_uploads(Bucket=bucket_name)
        for upload in mp_resp.get("Uploads", []):
            if prefix and not upload["Key"].startswith(prefix):
                continue
            s3.abort_multipart_upload(
                Bucket=bucket_name,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
            aborted_uploads += 1
    except Exception:
        pass

    # 4. Verificar que el bucket haya quedado limpio
    verify_resp = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=5, **({"Prefix": prefix} if prefix else {}))
    remaining = verify_resp.get("KeyCount", 0)
    elapsed = time.time() - start_time

    print(
        f"Purga completada en {elapsed:.2f}s | Objetos borrados: {deleted_objects} | "
        f"Versiones/Markers: {deleted_versions} | Multipart abortados: {aborted_uploads} | "
        f"Objetos restantes: {remaining}"
    )
    return {
        "deleted_objects": deleted_objects,
        "deleted_versions": deleted_versions,
        "aborted_uploads": aborted_uploads,
        "remaining": remaining,
    }


def restart_lambda_cold_start(
    session: boto3.Session,
    stack_name: str = "metri-engine",
    ssm_param: str = "/metri/engine/bootstrap/master-key",
) -> str:
    """Fuerza el reinicio de todas las microVMs de MetriEngineFunction y publica nueva versión en el alias `live`."""
    cfn = session.client("cloudformation")
    lmb = session.client("lambda")
    ssm = session.client("ssm")

    res = cfn.describe_stack_resource(
        StackName=stack_name,
        LogicalResourceId="MetriEngineFunction",
    )
    fn_name = res["StackResourceDetail"]["PhysicalResourceId"]
    ssm_key = ssm.get_parameter(Name=ssm_param, WithDecryption=True)["Parameter"]["Value"]

    print(f"\n--- Reiniciando microVMs de AWS Lambda: {fn_name} ---")
    cfg = lmb.get_function_configuration(FunctionName=fn_name)
    env_vars = dict(cfg.get("Environment", {}).get("Variables", {}))

    # Garantizar conexión exclusiva a METRI_BOOTSTRAP_MASTER_KEY desde SSM Parameter Store
    env_vars.pop("METRI_BOOTSTRAP_MASTER_PASSWORD", None)
    env_vars["METRI_BOOTSTRAP_MASTER_KEY"] = ssm_key
    env_vars["FORCE_DEPLOY"] = f"reset-{int(time.time())}"

    lmb.update_function_configuration(
        FunctionName=fn_name,
        Environment={"Variables": env_vars},
    )
    waiter = lmb.get_waiter("function_updated")
    waiter.wait(FunctionName=fn_name)

    ver_resp = lmb.publish_version(
        FunctionName=fn_name,
        Description=f"Cold-start reset linked to {ssm_param}",
    )
    new_version = ver_resp["Version"]
    waiter.wait(FunctionName=fn_name, Qualifier=new_version)

    try:
        lmb.update_alias(
            FunctionName=fn_name,
            Name="live",
            FunctionVersion=new_version,
        )
        print(f"  ✓ Alias 'live' actualizado a la versión limpia v{new_version} (0 microVMs calientes previas).")
    except lmb.exceptions.ResourceNotFoundException:
        print(f"  ✓ Versión v{new_version} publicada en $LATEST.")

    return new_version


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Purga de producción del almacén binario S3 Metri Vortex (.mbf) en s3://metri-engine."
    )
    parser.add_argument("--profile", default="metri-dev", help="AWS CLI profile name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--bucket", default="metri-engine", help="Target S3 bucket name")
    parser.add_argument("--prefix", default="", help="Optional S3 key prefix to purge (default: entire bucket)")
    parser.add_argument("--stack-name", default="metri-engine", help="CloudFormation stack name")
    parser.add_argument(
        "--restart-lambda",
        action="store_true",
        help="Force-restart MetriEngineFunction microVMs (alias live) and re-sweep S3 so no warm cache survives",
    )
    parser.add_argument(
        "--confirm",
        help=f"Bypass prompt by supplying '{REQUIRED_CONFIRM_PHRASE}'",
    )

    args = parser.parse_args()

    print("============================================================")
    print("      METRI PRODUCTION S3 (.MBF) PURGE UTILITY              ")
    print("============================================================")
    print(f"AWS Profile    : {args.profile}")
    print(f"Region         : {args.region}")
    print(f"S3 Bucket      : s3://{args.bucket}/{args.prefix}")
    print(f"Restart Lambda : {args.restart_lambda}")
    print("============================================================")

    try:
        session = (
            boto3.Session(profile_name=args.profile, region_name=args.region)
            if args.profile
            else boto3.Session(region_name=args.region)
        )
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"AWS Account ID : {identity['Account']}")
        print(f"Caller ARN     : {identity['Arn']}")
    except Exception as e:
        print(f"\n[ERROR] Failed to establish AWS session: {e}")
        sys.exit(1)

    if args.confirm:
        user_input = args.confirm
    else:
        print("\n" + "!" * 60)
        print(f"WARNING: THIS IS A DESTRUCTIVE WIPE OF s3://{args.bucket}/{args.prefix}")
        print("!" * 60)
        user_input = input(f"\nType '{REQUIRED_CONFIRM_PHRASE}' to proceed: ").strip()

    if user_input != REQUIRED_CONFIRM_PHRASE:
        print("\n[CANCELLED] Confirmation phrase did not match. Exiting.")
        sys.exit(0)

    stats = purge_s3_bucket(session, args.bucket, args.prefix)
    if args.restart_lambda:
        restart_lambda_cold_start(session, stack_name=args.stack_name)
        print("\n--- Segunda barrida post-reinicio para limpiar cualquier flush residual ---")
        stats = purge_s3_bucket(session, args.bucket, args.prefix)

    if stats["remaining"] != 0:
        print(f"\n[ERROR] Bucket still has {stats['remaining']} objects remaining!")
        sys.exit(2)

    print("\n============================================================")
    print("      S3 PURGE COMPLETED SUCCESSFULLY (0 OBJECTS LEFT)      ")
    print("============================================================")


if __name__ == "__main__":
    main()
