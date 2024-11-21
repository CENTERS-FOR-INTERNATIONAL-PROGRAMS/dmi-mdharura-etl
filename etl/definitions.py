from dagster import AssetSelection, DefaultScheduleStatus, Definitions, RunConfig, ScheduleDefinition, define_asset_job, load_assets_from_modules
from dagster_dbt import DbtCliResource, build_dbt_asset_selection, build_schedule_from_dbt_selection

from mdharura_dlt.resources import MdharuraDltResource
from mdharura_dlt.assets import mdharura_assets
from assets import DbtConfig, mdharura_dbt_assets
from project import dbt_project

sync_job = define_asset_job(
    "mongo_postgres_sync_job", AssetSelection.groups("mdharura"),
)

sync_schedule = ScheduleDefinition(
    job=sync_job,
    cron_schedule="*/20 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

dbt_schedule_job = define_asset_job(
    name="all_dbt_assets",
    selection=build_dbt_asset_selection(
        [mdharura_dbt_assets],
    ),
    config=RunConfig(
        ops={"mdharura_dbt_assets": DbtConfig(full_refresh=True, seed=True)}
    ),
)

dbt_assets_schedule = build_schedule_from_dbt_selection(
    [mdharura_dbt_assets],
    job_name="dbt_assets_schedule",
    cron_schedule="*/30 * * * *",
    # dbt_select="tag:daily",
    # If your definition of `@dbt_assets` has Dagster Configuration, you can specify it here.
    # config=RunConfig(ops={"my_dbt_assets": MyDbtConfig(full_refresh=True)}),
)


defs = Definitions(
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
    jobs=[sync_job, dbt_schedule_job],
    # schedules=[sync_schedule],
    schedules=[sync_schedule, dbt_assets_schedule],
)
