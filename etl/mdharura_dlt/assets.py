from dagster import AssetOut, OpExecutionContext, get_dagster_logger, multi_asset
import dlt
from . import mongodb
from .resources import MdharuraDltResource
import os

from dotenv import load_dotenv
load_dotenv()

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

    for db, collection_names in collection_list.items():
        @multi_asset(
            name=db,
            group_name=db,
            outs={
                stream: AssetOut(key_prefix=[f'central_raw_{db}'])
                for stream in collection_names}

        )
        def collections_asset(context: OpExecutionContext, pipeline: MdharuraDltResource):
            # Getting Data From MongoDB    
            data = mongodb(URL, db, parallel=False).with_resources(*collection_names)
            
            # enable incremental loading for each collection
            # for collection_name in collection_names:
            #     data.resources[collection_name].apply_hints(incremental=dlt.sources.incremental("updatedAt", initial_value='2021-01-01T00:00:00Z'))
                
            
            logger = get_dagster_logger()
            results = pipeline.load_collection(data, db)
            logger.info(results)

            return tuple([None for _ in context.selected_output_names])

        multi_assets.append(collections_asset)

    return multi_assets


mdharura_assets = dlt_asset_factory(DATABASE_COLLECTIONS)