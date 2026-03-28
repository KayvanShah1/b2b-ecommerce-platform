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


# Export a singleton instance
storage = Storage()
