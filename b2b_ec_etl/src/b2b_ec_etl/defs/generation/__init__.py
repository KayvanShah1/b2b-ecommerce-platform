from dagster import Definitions

from .asset import generation_assets
from .jobs import generation_simulation_job
from .resources import generation_resources

defs = Definitions(
    assets=generation_assets,
    jobs=[generation_simulation_job],
    resources=generation_resources,
)
