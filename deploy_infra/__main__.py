import pulumi
import pulumi_aws as aws
from b2b_ec_utils.settings import settings  # <--- Import your actual config

# 1. Determine if we are in Local/MinIO mode based on your Utils
is_local = settings.storage.location in ("local", "minio")

# 2. Configure the Provider
if is_local:
    custom_provider = aws.Provider(
        "minio-provider",
        access_key=settings.storage.access_key.get_secret_value(),
        secret_key=settings.storage.secret_key.get_secret_value(),
        skip_credentials_validation=True,
        skip_metadata_api_check=True,
        skip_region_validation=True,
        s3_use_path_style=True,
        endpoints=[
            aws.ProviderEndpointArgs(
                s3=settings.storage.endpoint_url,
            )
        ],
        region=settings.storage.region or "us-east-1",
    )
else:
    # Production AWS settings
    custom_provider = aws.Provider("aws-prod-provider", region=settings.storage.region)

# 3. Create the Bucket using the name defined in your settings
# This ensures Pulumi and your App always agree on the bucket name
webserlog_landing_bucket = aws.s3.BucketV2(
    "landing-zone-bucket",
    bucket=settings.storage.webserlog_bucket,
    opts=pulumi.ResourceOptions(provider=custom_provider),
)

# Export for visibility in the Pulumi CLI
pulumi.export("bucket_endpoint", settings.storage.endpoint_url)
pulumi.export("active_bucket", webserlog_landing_bucket.bucket)
