import fsspec

from b2b_ec_utils.settings import settings


class Storage:
    def __init__(self, cfg=settings):
        if cfg.storage.location == "local":
            self.fs = fsspec.filesystem("file")
            self.base = cfg.var_dir.as_posix()

        elif cfg.storage.location in ("s3", "minio"):
            self.fs = fsspec.filesystem(
                "s3",
                key=cfg.storage.access_key,
                secret=cfg.storage.secret_key,
                client_kwargs={
                    "endpoint_url": cfg.storage.endpoint_url,
                    "region_name": cfg.storage.region,
                },
            )
            self.base = f"s3://{cfg.storage.bucket}/{cfg.storage.prefix}".rstrip("/")

        else:
            self.fs = fsspec.filesystem("gcs", project=cfg.storage.bucket, token=str(cfg.storage.secret_key))
            self.base = f"gcs://{cfg.storage.bucket}/{cfg.storage.prefix}".rstrip("/")

    def path(self, *parts):
        return "/".join([self.base, *parts])

    def open(self, path, mode="rb", **kw):
        return self.fs.open(path, mode, **kw)

    def exists(self, path):
        return self.fs.exists(path)

    def makedirs(self, path):
        self.fs.makedirs(path, exist_ok=True)

    def glob(self, pattern):
        return self.fs.glob(pattern)


storage = Storage()
