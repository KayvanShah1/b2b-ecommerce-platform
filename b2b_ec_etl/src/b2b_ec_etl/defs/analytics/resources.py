from b2b_ec_utils import settings
from dagster_dbt import DbtCliResource, DbtProject

dbt_project = DbtProject(project_dir=settings.dbt.project_dir, profiles_dir=settings.dbt.profiles_dir)
dbt_project.prepare_if_dev()
dbt_resource = DbtCliResource(project_dir=dbt_project)

analytics_resources = {"dbt": dbt_resource}
