from dagster import Definitions, load_assets_from_modules

from dagster_app.dagster_app.assets import etl as etl_job_module
from dagster_app.dagster_app.assets import etl_assets as etl_assets_module

# Load all @asset definitions from the assets module
assets = load_assets_from_modules([etl_assets_module])

defs = Definitions(
    assets=assets,
    jobs=[
        etl_job_module.etl_job,         # old (job/op) - keep for now
        etl_assets_module.etl_assets_job # new (assets-based) job
    ],
)
