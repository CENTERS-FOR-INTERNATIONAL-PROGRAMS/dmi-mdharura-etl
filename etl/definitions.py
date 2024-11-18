from dagster import AssetSelection, DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules
from dagster_dbt import DbtCliResource

from mongo_postgres.resources import MdharuraDltResource
from mongo_postgres.assets import mdharura_assets
from assets import mdharura_dbt_assets
from project import dbt_project

sync_job = define_asset_job(
    "sync_job", AssetSelection.groups("mdharura"),
)

sync_schedule = ScheduleDefinition(
    job=sync_job,
    cron_schedule="*/20 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)


defs = Definitions(
    # assets=[*dlt_assets, mdharura_dbt_assets],
    assets=[*mdharura_assets, mdharura_dbt_assets],
    
    resources={
        "pipeline": MdharuraDltResource(
            pipeline_name = "mdharura",
            dataset_name = "central_raw",
            destination = "postgres"
        ),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
    # jobs=[sync_job],
    # schedules=[sync_schedule],
)
