from dagster import ConfigurableResource
from pydantic import Field

from b2b_ec_sources.marketing_leads import MarketingLeadsGenerator
from b2b_ec_sources.postgres_gen import run_source_generation
from b2b_ec_sources.webserver_logs import WebLogGenerator


class DataGenerationResource(ConfigurableResource):
    marketing_leads_count: int | None = Field(default=None, ge=1)
    webserver_logs_count: int | None = Field(default=None, ge=1)

    def run_source_generation(self) -> None:
        run_source_generation()

    def run_marketing_leads_generation(self) -> None:
        MarketingLeadsGenerator().generate(count=self.marketing_leads_count)

    def run_webserver_logs_generation(self) -> None:
        WebLogGenerator().generate(log_count=self.webserver_logs_count)


generation_resources = {"data_generation_resource": DataGenerationResource()}
