# `deploy_infra` (Pulumi)

Pulumi program that provisions the S3-compatible bucket used as the landing zone for the platform's webserver logs.

## What it creates
- One bucket named `"<STORAGE_PREFIX>-webserver-logs"` (default: `b2b-ec-webserver-logs`)
- Pulumi outputs:
  - `bucket_endpoint` (from `STORAGE_*_ENDPOINT_URL`)
  - `active_bucket` (the created bucket name)

## Modes
- `STORAGE_LOCATION=minio` or `local`: uses a custom AWS provider pointing at your MinIO endpoint
- `STORAGE_LOCATION=s3`: uses the default AWS provider (real S3)

## Prerequisites
- Pulumi CLI installed and logged in
- Workspace deps installed (`uv sync --all-packages`)
- Repo-root `.env` configured:
  - `STORAGE_LOCATION`
  - `STORAGE_PREFIX` (optional)
  - MinIO: `STORAGE_MINIO_ENDPOINT_URL`, `STORAGE_MINIO_ROOT_USER`, `STORAGE_MINIO_ROOT_PASSWORD`
  - AWS: standard AWS credentials for the CLI/SDK (plus `STORAGE_REGION` if you want a non-default region)

## Run

```bash
# from repo root
uv sync --all-packages

cd deploy_infra

pulumi stack select dev || pulumi stack init dev
pulumi up
```

## Notes
- Bucket naming and endpoints are derived from `b2b_ec_utils.settings` and your `.env`.
- This module currently provisions only the webserver logs bucket (see `deploy_infra/__main__.py`).
