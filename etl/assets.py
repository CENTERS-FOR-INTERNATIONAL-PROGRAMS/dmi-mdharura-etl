from dagster import AssetExecutionContext, Config
from dagster_dbt import DbtCliResource, dbt_assets
from project import dbt_project

class DbtConfig(Config):
    full_refresh: bool


@dbt_assets(manifest=dbt_project.manifest_path)
def mdharura_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    dbt_build_args = ["build"]
    if config.full_refresh:
        dbt_build_args += ["--full-refresh"]

    yield from dbt.cli(dbt_build_args, context=context).stream()