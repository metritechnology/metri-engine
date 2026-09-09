# Guide — Deployment

> Verified against `.github/workflows/deploy-production.yml`, `samconfig.toml` and `Makefile` · September 9, 2026

Production deployments run on **GitHub Actions**. The manual AWS CLI deploy
(`make deploy`) remains as the emergency path.

```bash
git push origin main   # build + tests + human gate + sam deploy (automatic)
```

## Flow (`deploy-production.yml`)

```
push to main (code/infra paths)
   │
   ▼
[build]  cargo test --lib + sam build (cargo lambda --release --arm64)
         immutable artifact (.aws-sam/build) uploaded to the run
   │
   ▼
[deploy] GitHub environment "production" → human approval
         OIDC credentials (aws-actions/configure-aws-credentials)
         sam deploy --config-env production
   │
   ▼
[verify] stack status + function "live" alias
```

Strategy decisions:

- **OIDC, not access keys**: the workflow assumes `metri-engine-github-deploy`
  via `sts:AssumeRoleWithWebIdentity`. There are no AWS secrets in GitHub to
  rotate/leak; the trust policy scopes the role to this repo, branch `main`, or
  the `production` environment.
- **Human gate in GitHub, not in the CLI**: `confirm_changeset=false` in CI;
  approval lives in the *required reviewers* of the `production` environment
  (recorded in Deployments — who, when, which commit).
- **Build and deploy are separate jobs**: exactly what was tested is what gets
  deployed (artifact), and waiting for approval doesn't hold a runner
  compiling.
- **Serialized concurrency**: `deploy-production` queues; there are never two
  `sam deploy` running in parallel.
- **Rollback = redeploy the previous commit**: *Run workflow* (manual) with the
  ref of the last known-good commit. If an update fails, CloudFormation rolls
  back automatically and the stack lands in `UPDATE_ROLLBACK_COMPLETE` serving
  traffic with the previous version.

## One-time setup

1. **AWS trust** (idempotent; without `--apply` it prints the IAM documents):

   ```bash
   python3 scripts/ops/bootstrap_github_oidc.py --apply --profile metri-dev
   # → creates the GitHub OIDC provider + metri-engine-github-deploy role
   #   with a least-privilege policy aligned to template.yaml
   ```

2. **`production` environment in GitHub**: Settings → Environments → New
   environment `production` → *Required reviewers* (who approves deploys).
   Without reviewers the deploy runs with no human gate.

Triggers: push to `main` touching `src/`, `config/`, `proto/`, `Cargo.*`,
`template.yaml`, `samconfig.toml`, `Makefile` or the workflow itself; and
`workflow_dispatch` (manual, any ref — the rollback mechanism).

## Emergency manual deploy

```bash
make deploy   # cargo build --release + sam build + sam deploy (metri-dev profile, us-east-1)
```

Uses the `default` config-env in `samconfig.toml` (`metri-dev` profile,
`confirm_changeset=true`). The CI workflow uses `--config-env production`
(no local profile, no changeset confirmation — the gate is the GitHub
environment). Both deploy the same stack parameters.

## What the stack creates (`template.yaml`)

| Resource | Purpose |
|---|---|
| ARM64 Lambda `provided.al2023` (`bootstrap`) | The whole engine |
| Function URL + CloudFront + WAF + Route53 | gRPC transport (grpc-web) |
| KMS + Secrets Manager | HMAC key and secret |
| S3 bucket + Glue Database | Parquet data lake / OLAP catalog |
| 2× DynamoDB | EAV engine + Códice schemas |
| 2× SQS (outbox + DLQ) | Domain events |

## Before deploying to production

- Strong `HMAC_SECRET` in Secrets Manager — startup **aborts** on a weak secret or unknown `ENVIRONMENT` (fail-closed).
- Explicit `ENVIRONMENT=production`.
- Deleting the whole stack (`cloudformation:DeleteStack`) is **excluded** from the CI role on purpose: it is a manual operation.
