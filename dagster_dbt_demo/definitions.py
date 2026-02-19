import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_dbt_demo.assets.dbt_assets import dbt_project_assets
from dagster_dbt_demo.assets.ingestion import (
    raw_comments,
    raw_countries,
    raw_posts,
    raw_users,
)
from dagster_dbt_demo.jobs.etl_job import etl_job
from dagster_dbt_demo.resources.database import WarehouseResource
from dagster_dbt_demo.resources.dbt import dbt_project
from dagster_dbt_demo.schedules.daily_schedule import daily_etl_schedule

defs = Definitions(
    assets=[raw_users, raw_posts, raw_comments, raw_countries, dbt_project_assets],
    resources={
        "warehouse": WarehouseResource(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "dagster"),
            password=os.getenv("POSTGRES_PASSWORD", "dagster_password"),
            database=os.getenv("WAREHOUSE_DB", "warehouse"),
        ),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.project_dir,
        ),
    },
    jobs=[etl_job],
    schedules=[daily_etl_schedule],
)
