from enum import Enum
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresConfig(BaseSettings):
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: str = Field(default="postgres", description="PostgreSQL user")
    password: str = Field(default="postgres", description="PostgreSQL password")
    database: str = Field(default="b2b_source", description="PostgreSQL database")

    model_config = SettingsConfigDict(env_prefix="POSTGRES_")


class StorageLocation(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    MINIO = "minio"
    GCS = "gcs"


class StorageConfig(BaseSettings):
    location: StorageLocation = StorageLocation.LOCAL
    bucket: str = "b2b-ecommerce"
    prefix: str = "bec"

    # Unified endpoint logic for MinIO/S3
    endpoint_url: str | None = Field(
        default=None, validation_alias=AliasChoices("S3_ENDPOINT_URL", "MINIO_ENDPOINT_URL")
    )
    access_key: str = Field(default="minioadmin", validation_alias=AliasChoices("AWS_ACCESS_KEY_ID", "MINIO_ROOT_USER"))
    secret_key: str = Field(
        default="minio@123", validation_alias=AliasChoices("AWS_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD")
    )
    region: str = "us-east-1"

    model_config = SettingsConfigDict(env_prefix="STORAGE_", extra="ignore")


class Settings(BaseSettings):
    # Determine project root dynamically based on the location of this settings file
    project_root: Path = Path(__file__).resolve().parents[4]
    project_name: str = "b2b-ecommerce-platform"

    # File paths for data storage
    var_dir: Path = Path.joinpath(project_root, "var")

    # Local warehouse directory
    local_warehouse_dir: Path = Path.joinpath(var_dir, "warehouse")

    # Log directory setup
    log_dir: Path = Path.joinpath(var_dir, "logs")

    # Blob Storage: MinIO configuration
    storage: StorageConfig = StorageConfig()

    # PostgreSQL configuration
    postgres: PostgresConfig = PostgresConfig()

    # This tells Pydantic to look for an .env file automatically
    model_config = SettingsConfigDict(env_file=f"{project_root}/.env", env_file_encoding="utf-8", extra="ignore")

    def model_post_init(self, __context):
        """Bootstrap the local environment automatically."""
        for directory in [self.var_dir, self.local_warehouse_dir, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    # Override model_dump to make paths relative to base_dir for logging
    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        for k, v in dump.items():
            if isinstance(v, Path) and v.is_absolute():
                try:
                    dump[k] = v.relative_to(self.project_root)
                except ValueError:
                    pass
        return dump


settings = Settings()

if __name__ == "__main__":
    from rich.pretty import pretty_repr

    from b2b_ec_utils.logger import get_logger

    logger = get_logger("ApplicationSettings")
    s = pretty_repr(settings.model_dump())
    logger.info(f"Settings loaded: \n{s}")  # Debugging line to check settings
