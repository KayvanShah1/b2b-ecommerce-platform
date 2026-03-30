from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_project_root(marker: str = ".env") -> Path:
    """
    Search upwards from the current file's directory to find the project root.
    This prevents 'Path.parents[n]' from breaking if the file is moved.
    """
    current = Path(__file__).resolve().parent
    for parent in [current] + list(current.parents):
        if (parent / marker).exists():
            return parent
    return current.parent


class BaseProjectSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=find_project_root() / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    def model_dump(self, **kwargs):
        """Custom dump to make absolute paths relative for clean logging."""
        dump = super().model_dump(**kwargs)
        for k, v in dump.items():
            if isinstance(v, Path) and v.is_absolute():
                try:
                    dump[k] = str(v.relative_to(self.project_root))
                except ValueError:
                    dump[k] = str(v)
        return dump


class PostgresConfig(BaseProjectSettings):
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: SecretStr = Field(default="postgres", description="PostgreSQL user")
    password: SecretStr = Field(default="postgres", description="PostgreSQL password")
    database: str = Field(default="b2b_source_db", description="PostgreSQL database")

    # Inherit the env_file from BaseProjectSettings but add a prefix
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")


class StorageLocation(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    MINIO = "minio"
    GCS = "gcs"


class StorageConfig(BaseProjectSettings):
    location: StorageLocation = StorageLocation.LOCAL
    prefix: str = "b2b-ec"

    endpoint_url: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("STORAGE_S3_ENDPOINT_URL", "STORAGE_MINIO_ENDPOINT_URL")
    )
    access_key: SecretStr = Field(
        default="minioadmin", validation_alias=AliasChoices("STORAGE_S3_ACCESS_KEY_ID", "STORAGE_MINIO_ROOT_USER")
    )
    secret_key: SecretStr = Field(
        default="minio@123",
        validation_alias=AliasChoices("STORAGE_S3_SECRET_ACCESS_KEY", "STORAGE_MINIO_ROOT_PASSWORD"),
    )
    region: str = Field(default="us-east-1")

    # Buckets (for S3/MinIO/GCS) - these will be prefixed with the 'prefix' value
    webserver_logs_bucket: str = Field(default="webserver-logs")
    marketing_leads_bucket: str = Field(default="marketing-leads")
    # Raw data bucket is for storing ingested data in its original form read from source systems, before any transformations or processing.
    # It is separate from metadata to allow for different lifecycle policies and access controls.
    raw_data_bucket: str = Field(default="raw-data")
    # Metadata bucket is for storing things like watermarks, schema snapshots, and other auxiliary data for ingestion processes.
    # It is separate from raw data to allow for different lifecycle policies and access controls.
    metadata_bucket: str = Field(default="metadata")

    model_config = SettingsConfigDict(env_prefix="STORAGE_")

    def model_post_init(self, __context):
        self.webserver_logs_bucket = f"{self.prefix}-webserver-logs"
        self.marketing_leads_bucket = f"{self.prefix}-marketing-leads"
        self.raw_data_bucket = f"{self.prefix}-raw-data"
        self.metadata_bucket = f"{self.prefix}-metadata"


class MotherDuckConfig(BaseProjectSettings):
    local_database: str = Field(default="b2b_ec_warehouse.duckdb")
    database: str = Field(default="b2b_ecommerce")
    token: Optional[SecretStr] = Field(default="<API_TOKEN>", description="MotherDuck API token")

    model_config = SettingsConfigDict(env_prefix="MOTHERDUCK_")


class DBTConfig(BaseProjectSettings):
    target: str = Field(default="dev", description="DBT target profile to use")
    project_dir: Path = Field(default="b2b_ec_warehouse")
    profiles_dir: Path = Field(default=".dbt")

    model_config = SettingsConfigDict(env_prefix="DBT_")


class Settings(BaseProjectSettings):
    """
    Main Settings class. It coordinates the sub-configs and manages
    the physical directory structure.
    """

    project_root: Path = find_project_root()
    project_name: str = "b2b-ecommerce-platform"

    # Paths derived from root
    var_dir: Path = Field(default_factory=lambda: find_project_root() / "var")
    local_warehouse_dir: Path = Field(default_factory=lambda: find_project_root() / "var" / "warehouse")
    log_dir: Path = Field(default_factory=lambda: find_project_root() / "var" / "logs")

    # Components
    storage: StorageConfig = Field(default_factory=StorageConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    motherduck: MotherDuckConfig = Field(default_factory=MotherDuckConfig)
    dbt: DBTConfig = Field(default_factory=DBTConfig)

    def model_post_init(self, __context):
        """Ensure directories exist on startup."""
        for directory in [self.var_dir, self.local_warehouse_dir, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)


# Instantiate the singleton
settings = Settings()

if __name__ == "__main__":
    from rich.pretty import pretty_repr

    from b2b_ec_utils.logger import get_logger

    logger = get_logger("ApplicationSettings")
    logger.debug(f"Project Root Detected: {settings.project_root}")
    logger.debug("--- Loaded Settings ---")
    logger.debug(pretty_repr(settings.model_dump()))
