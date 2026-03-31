from dagster import Definitions

from .asset import generation_assets
from .jobs import data_generation_simulation_job
from .resources import generation_resources

defs = Definitions(
    assets=generation_assets,
    jobs=[data_generation_simulation_job],
    resources=generation_resources,
)
