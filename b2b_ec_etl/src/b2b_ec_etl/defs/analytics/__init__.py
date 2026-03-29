from dagster import Definitions

from .asset import analytics_assets
from .jobs import analytics_jobs
from .resources import analytics_resources

defs = Definitions(
    assets=analytics_assets,
    jobs=analytics_jobs,
    resources=analytics_resources,
)
