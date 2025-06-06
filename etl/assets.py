from pathlib import Path
from dagster import AssetExecutionContext, Config, file_relative_path
from dagster_dbt import DbtCliResource, dbt_assets
from project import dbt_project

class DbtConfig(Config):
    full_refresh: bool

DBT_PROJECT_DIR = file_relative_path(__file__, "./mdharura_dbt")

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
dbt_parse_invocation = dbt_resource.cli(["--quiet", "parse"], target_path=Path("target")).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


@dbt_assets(manifest=dbt_manifest_path)
def mdharura_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_build_args = ["build"]
    # if config.full_refresh:
    #     dbt_build_args += ["--full-refresh"]

    yield from dbt.cli(dbt_build_args, context=context).stream()