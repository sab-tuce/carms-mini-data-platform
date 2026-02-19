from dagster import Definitions
from dagster_app.dagster_app.assets.etl import etl_job

defs = Definitions(
    jobs=[etl_job],
)
