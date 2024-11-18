from dagster import AssetOut, OpExecutionContext, get_dagster_logger, multi_asset
from . import mongodb
from .resources import MdharuraDltResource
import os

URL = os.getenv('SOURCES__MONGODB__CONNECTION__URL')

DATABASE_COLLECTIONS = {
    "mdharura": [
        "units",
        "roles",
        "tasks"  
    ],
}


def dlt_asset_factory(collection_list):
    multi_assets = []

    for db, collection_name in collection_list.items():
        @multi_asset(
            name=db,
            group_name=db,
            outs={
                stream: AssetOut(key_prefix=[f'central_raw_{db}'])
                for stream in collection_name}

        )
        def collections_asset(context: OpExecutionContext, pipeline: MdharuraDltResource):

            # Getting Data From MongoDB    
            data = mongodb(URL, db, parallel=True).with_resources(*collection_name)

            logger = get_dagster_logger()
            results = pipeline.load_collection(data, db)
            logger.info(results)

            return tuple([None for _ in context.selected_output_names])

        multi_assets.append(collections_asset)

    return multi_assets


mdharura_assets = dlt_asset_factory(DATABASE_COLLECTIONS)