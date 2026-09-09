#!/usr/bin/env python3
"""Bootstrap del despliegue CI/CD: GitHub Actions → AWS vía OIDC.

Crea, de forma idempotente, la confianza que usa
.github/workflows/deploy-production.yml:

  1. Proveedor OIDC de GitHub (token.actions.githubusercontent.com) — solo si
     la cuenta aún no tiene uno.
  2. Rol `metri-engine-github-deploy` asumible SOLO desde
     repo:metritechnology/metri-engine, en la rama main o tras aprobar el
     environment "production" de GitHub.
  3. Política inline de mínimo privilegio, alineada 1:1 con template.yaml:
     CloudFormation ejecuta los cambios con las credenciales del desplegador,
     así que el rol necesita operar cada tipo de recurso del stack — con ARN
     acotado donde el servicio lo permite.

Sin --apply es dry-run: imprime los documentos IAM y no toca AWS.

Uso:
  python3 scripts/ops/bootstrap_github_oidc.py                       # dry-run
  python3 scripts/ops/bootstrap_github_oidc.py --apply               # aplica
  python3 scripts/ops/bootstrap_github_oidc.py --apply --profile metri-dev

Tras aplicar, crear el environment "production" en GitHub con required
reviewers (ver docs/guides/despliegue.md §Puesta en marcha, una sola vez).
"""

from __future__ import annotations

import argparse
import json
import sys

ACCOUNT_ID = "982592308819"
REGION = "us-east-1"
REPO = "metritechnology/metri-engine"
ROLE_NAME = "metri-engine-github-deploy"
POLICY_NAME = "metri-engine-deploy"
HOSTED_ZONE_ID = "Z0492322W8B6QV4SG4W4"
LAMBDA_ADAPTER_ACCOUNT = "753240598075"  # cuenta AWS del layer público Web Adapter
GITHUB_OIDC_PROVIDER = "token.actions.githubusercontent.com"
# Thumbprints publicados por GitHub para su proveedor OIDC. AWS ya no los
# valida para GitHub (rotan los certs bajo dominio de GitHub), pero la API de
# IAM exige al menos uno al crear el proveedor.
GITHUB_THUMBPRINTS = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
]


def trust_policy() -> dict:
    """El token OIDC del workflow solo vale para este repo, y solo desde main
    o desde un job del environment "production" (rollback de cualquier ref)."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Federated": (
                        f"arn:aws:iam::{ACCOUNT_ID}:oidc-provider/{GITHUB_OIDC_PROVIDER}"
                    )
                },
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        f"{GITHUB_OIDC_PROVIDER}:aud": "sts.amazonaws.com",
                        f"{GITHUB_OIDC_PROVIDER}:sub": [
                            f"repo:{REPO}:ref:refs/heads/main",
                            f"repo:{REPO}:environment:production",
                        ],
                    }
                },
            }
        ],
    }


def deploy_policy() -> dict:
    """Mínimo privilegio para `sam deploy` de este stack.

    CloudFormation hace las llamadas a cada servicio con las credenciales del
    desplegador (sin service role), así que cada statement cubre un tipo de
    recurso de template.yaml, con ARN acotado donde el servicio lo permite.
    Excluido a propósito: cloudformation:DeleteStack (borrar el stack es una
    operación manual) y kms:DisableKey (la key policy ya lo exige con MFA).
    """
    stack = f"arn:aws:cloudformation:{REGION}:{ACCOUNT_ID}:stack/metri-engine/*"
    change_set = (
        f"arn:aws:cloudformation:{REGION}:{ACCOUNT_ID}:changeSet/metri-engine-*/*"
    )
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/metri-engine*"
    kms_keys = [f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/*",
                f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:alias/metri-engine*"]
    ddb_tables = f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/metri*"
    sqs_queues = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:metri-*"
    lambda_fn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:metri-engine-*"
    cf_dist = f"arn:aws:cloudfront::{ACCOUNT_ID}:distribution/*"
    # CloudFront usa scope CLOUDFRONT: la región ARN es us-east-1 pero el
    # segmento de scope es "global".
    waf_arns = [
        f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:global/webacl/*/*",
        f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:global/ipset/*/*",
        f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:global/regexpatternset/*/*",
    ]
    zone = f"arn:aws:route53:::hostedzone/{HOSTED_ZONE_ID}"
    secret = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:metri-hmac-secret-*"
    log_group = f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/lambda/metri-*:*"
    glue_catalog = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog"
    glue_db = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/metri_olap"
    glue_tables = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/metri_olap/*"

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CloudFormationStack",
                "Effect": "Allow",
                "Action": [
                    "cloudformation:CreateStack",
                    "cloudformation:UpdateStack",
                    "cloudformation:CreateChangeSet",
                    "cloudformation:DeleteChangeSet",
                    "cloudformation:DescribeChangeSet",
                    "cloudformation:ExecuteChangeSet",
                    "cloudformation:DescribeStacks",
                    "cloudformation:DescribeStackEvents",
                    "cloudformation:DescribeStackResource",
                    "cloudformation:DescribeStackResources",
                    "cloudformation:ListStackResources",
                    "cloudformation:GetTemplate",
                    "cloudformation:TagResource",
                    "cloudformation:UntagResource",
                    "cloudformation:UpdateTerminationProtection",
                    "cloudformation:ContinueUpdateRollback",
                ],
                "Resource": [stack, change_set],
            },
            {
                "Sid": "CloudFormationUnscopedReads",
                "Effect": "Allow",
                "Action": [
                    "cloudformation:ValidateTemplate",
                    "cloudformation:GetTemplateSummary",
                ],
                "Resource": "*",
            },
            {
                # Bucket del paquete SAM (resolve_s3 lo descubre/crea).
                "Sid": "SamArtifactBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
                "Resource": [
                    "arn:aws:s3:::aws-sam-cli-managed-default-samclisourcebucket-*",
                    "arn:aws:s3:::aws-sam-cli-managed-default-samclisourcebucket-*/*",
                ],
            },
            {
                "Sid": "SamBucketDiscovery",
                "Effect": "Allow",
                "Action": "s3:ListAllMyBuckets",
                "Resource": "*",
            },
            {
                # MetriDataLakeBucket (metri-lake-ACCOUNT-REGION).
                "Sid": "StackS3Bucket",
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::metri-lake-{ACCOUNT_ID}-{REGION}",
                    f"arn:aws:s3:::metri-lake-{ACCOUNT_ID}-{REGION}/*",
                ],
            },
            {
                # Roles IAM que genera SAM (MetriEngineFunctionRole y los de
                # recursos con Policies) + PassRole hacia el propio Lambda.
                "Sid": "IamStackRoles",
                "Effect": "Allow",
                "Action": [
                    "iam:CreateRole",
                    "iam:DeleteRole",
                    "iam:GetRole",
                    "iam:UpdateRole",
                    "iam:UpdateAssumeRolePolicy",
                    "iam:PutRolePolicy",
                    "iam:GetRolePolicy",
                    "iam:DeleteRolePolicy",
                    "iam:ListRolePolicies",
                    "iam:AttachRolePolicy",
                    "iam:DetachRolePolicy",
                    "iam:ListAttachedRolePolicies",
                    "iam:TagRole",
                    "iam:UntagRole",
                    "iam:PassRole",
                ],
                "Resource": role_arn,
            },
            {
                # CreateKey no es acotable por ARN (la key no existe aún).
                "Sid": "KmsCreateKey",
                "Effect": "Allow",
                "Action": "kms:CreateKey",
                "Resource": "*",
            },
            {
                # MetriKmsMasterKey + alias/metri-engine-cmk. Las operaciones
                # destructivas (DisableKey, ScheduleKeyDeletion) además exigen
                # MFA por la key policy del template — defensa en profundidad.
                "Sid": "KmsStackKey",
                "Effect": "Allow",
                "Action": [
                    "kms:DescribeKey",
                    "kms:GetKeyPolicy",
                    "kms:PutKeyPolicy",
                    "kms:GetKeyRotationStatus",
                    "kms:EnableKeyRotation",
                    "kms:UpdateKeyDescription",
                    "kms:CreateAlias",
                    "kms:UpdateAlias",
                    "kms:DeleteAlias",
                    "kms:TagResource",
                    "kms:UntagResource",
                    "kms:ListResourceTags",
                    "kms:ScheduleKeyDeletion",
                    "kms:CancelKeyDeletion",
                ],
                "Resource": kms_keys,
            },
            {
                # Tablas EAV (param EavTableName=metri-dynamo) y esquemas.
                "Sid": "DynamoDbTables",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:CreateTable",
                    "dynamodb:DeleteTable",
                    "dynamodb:DescribeTable",
                    "dynamodb:UpdateTable",
                    "dynamodb:DescribeTimeToLive",
                    "dynamodb:UpdateTimeToLive",
                    "dynamodb:DescribeContinuousBackups",
                    "dynamodb:UpdateContinuousBackups",
                    "dynamodb:TagResource",
                    "dynamodb:UntagResource",
                    "dynamodb:ListTagsOfResource",
                ],
                "Resource": ddb_tables,
            },
            {
                # Outbox + DLQ FIFO.
                "Sid": "SqsQueues",
                "Effect": "Allow",
                "Action": [
                    "sqs:CreateQueue",
                    "sqs:DeleteQueue",
                    "sqs:GetQueueAttributes",
                    "sqs:SetQueueAttributes",
                    "sqs:TagQueue",
                    "sqs:UntagQueue",
                ],
                "Resource": sqs_queues,
            },
            {
                # GetQueueUrl/ListQueues no aceptan ARN como recurso.
                "Sid": "SqsUnscopedReads",
                "Effect": "Allow",
                "Action": ["sqs:GetQueueUrl", "sqs:ListQueues"],
                "Resource": "*",
            },
            {
                "Sid": "LambdaFunction",
                "Effect": "Allow",
                "Action": [
                    "lambda:CreateFunction",
                    "lambda:UpdateFunctionCode",
                    "lambda:UpdateFunctionConfiguration",
                    "lambda:CreateFunctionUrlConfig",
                    "lambda:UpdateFunctionUrlConfig",
                    "lambda:DeleteFunctionUrlConfig",
                    "lambda:GetFunction",
                    "lambda:GetFunctionConfiguration",
                    "lambda:GetFunctionUrlConfig",
                    "lambda:ListVersionsByFunction",
                    "lambda:PublishVersion",
                    "lambda:CreateAlias",
                    "lambda:UpdateAlias",
                    "lambda:DeleteAlias",
                    "lambda:GetAlias",
                    "lambda:AddPermission",
                    "lambda:RemovePermission",
                    "lambda:GetPolicy",
                    "lambda:TagResource",
                    "lambda:UntagResource",
                    "lambda:ListTags",
                ],
                "Resource": lambda_fn,
            },
            {
                # Lambda Web Adapter layer (público, cuenta AWS).
                "Sid": "LambdaPublicLayer",
                "Effect": "Allow",
                "Action": "lambda:GetLayerVersion",
                "Resource": f"arn:aws:lambda:{REGION}:{LAMBDA_ADAPTER_ACCOUNT}:layer:*",
            },
            {
                "Sid": "CloudFrontDistribution",
                "Effect": "Allow",
                "Action": [
                    "cloudfront:CreateDistribution",
                    "cloudfront:CreateDistributionWithTags",
                    "cloudfront:UpdateDistribution",
                    "cloudfront:DeleteDistribution",
                    "cloudfront:GetDistribution",
                    "cloudfront:GetDistributionConfig",
                    "cloudfront:TagResource",
                    "cloudfront:UntagResource",
                    "cloudfront:ListTagsForResource",
                ],
                "Resource": cf_dist,
            },
            {
                "Sid": "CloudFrontUnscopedReads",
                "Effect": "Allow",
                "Action": "cloudfront:ListDistributions",
                "Resource": "*",
            },
            {
                # MetriWebACL + ipsets/regexpatternsets (scope CLOUDFRONT).
                "Sid": "Wafv2WebAcl",
                "Effect": "Allow",
                "Action": [
                    "wafv2:CreateWebACL",
                    "wafv2:UpdateWebACL",
                    "wafv2:DeleteWebACL",
                    "wafv2:GetWebACL",
                    "wafv2:ListWebACLs",
                    "wafv2:ListResourcesForWebACL",
                    "wafv2:CreateIPSet",
                    "wafv2:UpdateIPSet",
                    "wafv2:DeleteIPSet",
                    "wafv2:GetIPSet",
                    "wafv2:ListIPSets",
                    "wafv2:CreateRegexPatternSet",
                    "wafv2:UpdateRegexPatternSet",
                    "wafv2:DeleteRegexPatternSet",
                    "wafv2:GetRegexPatternSet",
                    "wafv2:ListRegexPatternSets",
                    "wafv2:TagResource",
                    "wafv2:UntagResource",
                    "wafv2:ListTagsForResource",
                ],
                "Resource": waf_arns,
            },
            {
                # Registro engine.metri.one en la zona del dominio.
                "Sid": "Route53Zone",
                "Effect": "Allow",
                "Action": [
                    "route53:ChangeResourceRecordSets",
                    "route53:ListResourceRecordSets",
                    "route53:GetHostedZone",
                ],
                "Resource": zone,
            },
            {
                "Sid": "Route53UnscopedReads",
                "Effect": "Allow",
                "Action": ["route53:GetChange", "route53:ListHostedZones"],
                "Resource": "*",
            },
            {
                # MetriHMACSecret (GenerateSecretString lo crea/rota CFN).
                "Sid": "SecretsManager",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:CreateSecret",
                    "secretsmanager:UpdateSecret",
                    "secretsmanager:DeleteSecret",
                    "secretsmanager:DescribeSecret",
                    "secretsmanager:PutSecretValue",
                    "secretsmanager:RestoreSecret",
                    "secretsmanager:RotateSecret",
                    "secretsmanager:TagResource",
                    "secretsmanager:UntagResource",
                ],
                "Resource": secret,
            },
            {
                "Sid": "SecretsRandomPassword",
                "Effect": "Allow",
                "Action": "secretsmanager:GetRandomPassword",
                "Resource": "*",
            },
            {
                # MetriEngineLogGroup (/aws/lambda/metri-engine-metri-engine).
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:DeleteLogGroup",
                    "logs:DescribeLogStreams",
                    "logs:PutRetentionPolicy",
                    "logs:DeleteRetentionPolicy",
                    "logs:TagResource",
                    "logs:UntagResource",
                    "logs:ListTagsForResource",
                ],
                "Resource": log_group,
            },
            {
                "Sid": "CloudWatchLogsUnscopedReads",
                "Effect": "Allow",
                "Action": "logs:DescribeLogGroups",
                "Resource": "*",
            },
            {
                # Glue Data Catalog: database metri_olap y sus tablas Iceberg.
                "Sid": "GlueCatalog",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:TagResource",
                    "glue:UntagResource",
                ],
                "Resource": [glue_catalog, glue_db, glue_tables],
            },
            {
                "Sid": "StsIdentity",
                "Effect": "Allow",
                "Action": "sts:GetCallerIdentity",
                "Resource": "*",
            },
        ],
    }


def _provider_exists(iam) -> str | None:
    for p in iam.list_open_id_connect_providers()["OpenIDConnectProviderList"]:
        if p["Arn"].endswith(GITHUB_OIDC_PROVIDER):
            return p["Arn"]
    return None


def apply_changes(session) -> None:
    iam = session.client("iam")

    provider_arn = _provider_exists(iam)
    if provider_arn is None:
        print(f"→ Creando proveedor OIDC {GITHUB_OIDC_PROVIDER}…")
        provider_arn = iam.create_open_id_connect_provider(
            Url=f"https://{GITHUB_OIDC_PROVIDER}",
            ClientIDList=["sts.amazonaws.com"],
            ThumbprintList=GITHUB_THUMBPRINTS,
        )["OpenIDConnectProviderArn"]
    else:
        print(f"✓ Proveedor OIDC ya existe: {provider_arn}")

    trust = json.dumps(trust_policy(), indent=2)
    policy = json.dumps(deploy_policy(), indent=2)
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"

    try:
        role = iam.get_role(RoleName=ROLE_NAME)["Role"]
        print(f"→ Rol {ROLE_NAME} existe: {role['Arn']} — actualizando confianza…")
        iam.update_assume_role_policy(RoleName=ROLE_NAME, PolicyDocument=trust)
    except iam.exceptions.NoSuchEntityException:
        print(f"→ Creando rol {ROLE_NAME}…")
        iam.create_role(
            RoleName=ROLE_NAME,
            Description="Deploy de metri-engine desde GitHub Actions (OIDC)",
            AssumeRolePolicyDocument=trust,
            MaxSessionDuration=3600,
        )

    print(f"→ Publicando política inline {POLICY_NAME}…")
    iam.put_role_policy(
        RoleName=ROLE_NAME, PolicyName=POLICY_NAME, PolicyDocument=policy
    )
    print(f"✅ Listo. Rol asumible por el workflow: {role_arn}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true",
                        help="aplicar cambios (default: dry-run)")
    parser.add_argument("--profile", default=None,
                        help="perfil AWS (default: variables de entorno)")
    args = parser.parse_args()

    if not args.apply:
        print(f"DRY-RUN (usa --apply para crear/actualizar en {ACCOUNT_ID})\n")
        print("── Trust policy ──")
        print(json.dumps(trust_policy(), indent=2))
        print("\n── Deploy policy (inline) ──")
        print(json.dumps(deploy_policy(), indent=2))
        return 0

    import boto3

    session = (
        boto3.session.Session(profile_name=args.profile, region_name=REGION)
        if args.profile
        else boto3.session.Session(region_name=REGION)
    )
    apply_changes(session)
    return 0


if __name__ == "__main__":
    sys.exit(main())
