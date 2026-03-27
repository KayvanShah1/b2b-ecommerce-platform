from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


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
    minio_root_user: str
    minio_root_password: str

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

    logger = get_logger("GWSSettings")
    s = pretty_repr(settings.model_dump())
    logger.info(f"Settings loaded: \n{s}")  # Debugging line to check settings
