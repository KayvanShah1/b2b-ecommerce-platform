from enum import Enum
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresConfig(BaseSettings):
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: SecretStr = Field(default="postgres", description="PostgreSQL user")
    password: SecretStr = Field(default="postgres", description="PostgreSQL password")
    database: str = Field(default="b2b_source_db", description="PostgreSQL database")

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore", env_file_encoding="utf-8")


class StorageLocation(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    MINIO = "minio"
    GCS = "gcs"


class StorageConfig(BaseSettings):
    location: StorageLocation = StorageLocation.LOCAL
    prefix: str = "b2b-ec"

    # Unified endpoint logic for MinIO/S3
    endpoint_url: str | None = Field(
        default=None, validation_alias=AliasChoices("STORAGE_S3_ENDPOINT_URL", "STORAGE_MINIO_ENDPOINT_URL")
    )
    access_key: SecretStr = Field(
        default="minioadmin", validation_alias=AliasChoices("STORAGE_AWS_ACCESS_KEY_ID", "STORAGE_MINIO_ROOT_USER")
    )
    secret_key: SecretStr = Field(
        default="minio@123",
        validation_alias=AliasChoices("STORAGE_AWS_SECRET_ACCESS_KEY", "STORAGE_MINIO_ROOT_PASSWORD"),
    )
    region: str = "us-east-1"

    # Bucket names
    webserver_logs_bucket: str = f"{prefix}-webserver-logs"
    marketing_leads_bucket: str = f"{prefix}-marketing-leads"

    model_config = SettingsConfigDict(env_prefix="STORAGE_", extra="ignore", env_file_encoding="utf-8")


class MotherDuckConfig(BaseSettings):
    local_database: str = Field(default="b2b_ec_warehouse.duckdb", description="MotherDuck/DuckDB database name")
    database: str = Field(default="b2b_ecommerce", description="MotherDuck database name")
    token: SecretStr | None = Field(default=None, description="MotherDuck Token")

    model_config = SettingsConfigDict(env_prefix="MOTHERDUCK_", extra="ignore", env_file_encoding="utf-8")


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
    storage: StorageConfig = Field(default_factory=StorageConfig)

    # PostgreSQL configuration (Source OLTP database)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)

    # MotherDuck/DuckDB configuration (Analytics warehouse)
    motherduck: MotherDuckConfig = Field(default_factory=MotherDuckConfig)

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
