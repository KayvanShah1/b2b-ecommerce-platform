from pathlib import Path

import fsspec

from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.settings import settings

logger = get_logger("StorageUtils")


class Storage:
    def __init__(self, cfg=settings):
        self.cfg = cfg
        self.location = cfg.storage.location.lower()

        # 1. Initialize the Filesystem Engine
        if self.location == "local":
            self.fs = fsspec.filesystem("file")
            self.protocol = ""
            # Base is the absolute path to your 'var' directory
            self.base_root = cfg.var_dir.as_posix()

        elif self.location in ("s3", "minio"):
            self.fs = fsspec.filesystem(
                "s3",
                key=cfg.storage.access_key.get_secret_value(),
                secret=cfg.storage.secret_key.get_secret_value(),
                client_kwargs={
                    "endpoint_url": cfg.storage.endpoint_url,
                    "region_name": cfg.storage.region,
                },
            )
            self.protocol = "s3://"
            self.base_root = ""

        else:
            # GCS Logic
            self.fs = fsspec.filesystem(
                "gcs", project=cfg.storage.bucket, token=cfg.storage.secret_key.get_secret_value()
            )
            self.protocol = "gcs://"
            self.base_root = ""

    def get_path(self, bucket_name: str, *parts: str) -> str:
        """
        Builds a full URI based on the current storage location.
        Example (S3): s3://my-bucket/folder/file.log
        Example (Local): C:/project/var/my-bucket/folder/file.log
        """
        if self.location == "local":
            return "/".join([self.base_root, bucket_name, *parts])

        return "/".join([f"{self.protocol}{bucket_name}", *parts])

    def open(self, path: str, mode: str = "rb", **kwargs):
        """Opens a file handle. Automatically creates parent dirs for local storage."""
        if self.location == "local" and ("w" in mode or "a" in mode):
            Path(path).parent.mkdir(parents=True, exist_ok=True)

        return self.fs.open(path, mode, **kwargs)

    def glob(self, pattern: str):
        """Returns a list of paths matching the pattern."""
        return self.fs.glob(pattern)

    def exists(self, path: str) -> bool:
        """Checks if a path exists."""
        return self.fs.exists(path)

    def list_files(self, path: str):
        """List files in a directory (non-recursive)."""
        return self.fs.ls(path, detail=False)

    def create_bucket(self, bucket_name: str):
        """Creates a bucket if it doesn't exist (idempotent). Only applicable for S3/GCS."""
        if self.location == "local":
            # For local storage, we treat buckets as top-level directories under base_root
            bucket_path = self.get_path(bucket_name)
            Path(bucket_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured local bucket (directory) exists: {bucket_path}")
        else:
            # For S3/GCS, we use the filesystem's mkdir which is idempotent
            bucket_uri = f"{self.protocol}{bucket_name}"
            if not self.fs.exists(bucket_uri):
                self.fs.mkdir(bucket_uri)
                logger.info(f"Created bucket: {bucket_uri}")
            else:
                logger.info(f"Bucket already exists: {bucket_uri}")


# Export a singleton instance
storage = Storage()

if __name__ == "__main__":
    # Quick test to verify storage initialization
    logger.info(f"Storage initialized with location: {storage.location}")
    # Create buckets in MiniO/S3 if they don't exist (idempotent)
    buckets = [
        settings.storage.webserver_logs_bucket,
        settings.storage.marketing_leads_bucket,
        settings.storage.raw_data_bucket,
    ]
    for b in buckets:
        storage.create_bucket(b)
