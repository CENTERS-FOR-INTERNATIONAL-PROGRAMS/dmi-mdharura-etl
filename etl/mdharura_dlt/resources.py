from dagster import ConfigurableResource 
import dlt

class MdharuraDltResource(ConfigurableResource):
    pipeline_name: str
    dataset_name: str
    destination: str

    def load_collection(self, resource_data, database):
        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
          pipeline_name=f"{database}_{self.pipeline_name}", 
          destination=self.destination, 
          dataset_name=f"{self.dataset_name}_{database}",
          progress=dlt.progress.tqdm(colour="yellow"),
          export_schema_path="schema/"
        )

        # load the pipeline
        pipeline.load(self.destination, pipeline.dataset_name)

        # load the data into the destination
        # use incremental loading. See: https://dlthub.com/docs/general-usage/incremental-loading
        # write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        # columns={"updated_at": {"dedup_sort": "desc"}}
        # write_disposition={"disposition": "replace", "strategy": "staging-optimized"},

        load_info = pipeline.run(
          resource_data, 
          write_disposition={"disposition": "merge"},
          primary_key="_id",
          columns={"updated_at": {"dedup_sort": "desc"}}
        )

        return load_info