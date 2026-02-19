from dagster import AssetSelection, define_asset_job

etl_job = define_asset_job(
    name="etl_job",
    selection=AssetSelection.all(),
    description="Full ETL pipeline: ingest raw data from APIs, then run dbt transformations",
)
