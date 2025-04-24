from dagster import Definitions, load_assets_from_modules, EnvVar
from minimal_lakehouse.ressources import OpenF1Api, MinioStorage
from dagster_duckdb import DuckDBResource
from minimal_lakehouse import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources= {
        "minio": MinioStorage(
            minio_endpoint = EnvVar('MINIO_API_HOST'),
            minio_username = EnvVar('MINIO_ACCESS_KEY'),
            minio_password = EnvVar('MINIO_SECRET_KEY')
        ),
        "openf1_api": OpenF1Api(endpoint='sessions'),
        "duckdb": DuckDBResource(database="data/mydb.duckdb")
    }
)
