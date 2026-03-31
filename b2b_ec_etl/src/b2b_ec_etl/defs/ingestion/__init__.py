from dagster import Definitions

from .asset import ingestion_assets
from .jobs import ingestion_job
from .resources import ingestion_resources

defs = Definitions(
    assets=ingestion_assets,
    jobs=[ingestion_job],
    resources=ingestion_resources,
)
