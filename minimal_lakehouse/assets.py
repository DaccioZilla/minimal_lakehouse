#%%
import dagster as dg
import io
from minimal_lakehouse.ressources import OpenF1Api, MinioStorage
import json
from dagster_duckdb import DuckDBResource

daily_partition = dg.DailyPartitionsDefinition(start_date= '2025-01-01')
monthly_partition = dg.MonthlyPartitionsDefinition(start_date= '2025-01-01', end_offset= 1)

def openf1_interface(context: dg.AssetExecutionContext, api: OpenF1Api, minio: MinioStorage, endpoint:str, request_params:dict[str, str] = {}) -> dg.MaterializeResult:
          
    context.log.info(f"Requesting endpoint: {api.base_url}/{endpoint}")
    res = api.request(endpoint, **request_params)
    if res.status_code != 200:
        raise Exception(f'Request for {endpoint} on date {context.partition_key} failed with status {res.status_code}')
    
    data = res.json()

    if not data:
        return dg.MaterializeResult(metadata= {'result': dg.MetadataValue.text(f'No data for date {context.partition_key}')})
    
    data_bytes = json.dumps(data).encode('utf-8')

    data_stream = io.BytesIO(data_bytes)

    file_name = f'{endpoint}/{context.partition_key[:4]}/{context.partition_key[:-3]}/{context.partition_key}.json'

    minio_client = minio.get_client()
    minio_client.put_object(
        minio.dest_bucket, 
        file_name, 
        data=data_stream, 
        length=len(data_bytes), 
        content_type='application/json'
        )
    
    return dg.MaterializeResult(metadata= {'result': dg.MetadataValue.text(f"{file_name} salvo no storage")})


@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw",
        automation_condition= dg.AutomationCondition.on_cron('@daily')
)
def openf1_sessions(context: dg.AssetExecutionContext, openf1_api: OpenF1Api, minio: MinioStorage):
    endpoint = 'sessions'
    request_params = {'date_start': context.partition_key}
    return openf1_interface(context, openf1_api, minio, endpoint, request_params)
    
   
@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw",
        automation_condition= dg.AutomationCondition.on_cron('@daily')
)
def openf1_positions(context: dg.AssetExecutionContext, openf1_api: OpenF1Api, minio: MinioStorage):
    endpoint = 'positions'
    request_params = {'date': context.partition_key}
    return openf1_interface(context, openf1_api, minio, endpoint, request_params)

@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw",
        automation_condition= dg.AutomationCondition.on_cron('@daily')
)
def openf1_weather(context: dg.AssetExecutionContext, openf1_api: OpenF1Api, minio: MinioStorage):
    endpoint = 'weather'
    request_params = {'date': context.partition_key}
    return openf1_interface(context, openf1_api, minio, endpoint, request_params)

@dg.asset(
    partitions_def = monthly_partition,
    compute_kind = 'python',
    group_name="openf1_bronze",
    deps = [dg.AssetDep('openf1_sessions', partition_mapping= dg.TimeWindowPartitionMapping(allow_nonexistent_upstream_partitions=True))],
    automation_condition= dg.AutomationCondition.on_cron('@daily')
)
def bronze_sessions(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as cnn:

        cnn.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        category VARCHAR,
                        date TIMESTAMP,
                        driver_number INT,
                        flag VARCHAR,
                        lap_number INT,
                        meeting_key INT,
                        message VARCHAR,
                        scope : VARCHAR,
                        sector INT,
                        session_key INT
                    )
                    """)

        cnn.execute(f"""
                    SET s3_region='us-east-1';
                    SET s3_url_style='path';
                    SET s3_endpoint='{dg.EnvVar('MINIO_API_HOST').get_value()}';
                    SET s3_access_key_id='{dg.EnvVar('MINIO_ACCESS_KEY').get_value()}' ;
                    SET s3_secret_access_key='{dg.EnvVar('MINIO_SECRET_KEY').get_value()}';
                    SET s3_use_ssl='false';
                    """)
        
        cnn.execute('CREATE OR REPLACE TEMP VIEW sessions_stg AS SELECT * FROM sessions WHERE 1=2')

        try:
            cnn.execute(
                f"""
                INSERT INTO sessions_stg
                SELECT * FROM 's3://lakehouse/sessions/{context.partition_key[:4]}/{context.partition_key[:-3]}/*.json'
                """
                )
        except Exception as e:
            return dg.MaterializeResult(metadata={'error': dg.MetadataValue.text(e)})

        

        cnn.execute(
            """
            WITH new_data AS (
                SELECT t1.* 
                FROM sessions_stg AS t1
                LEFT JOIN sessions AS t2 ON t1.session_key = t2.session_key
                WHERE t2.session_key IS NULL
            )
            INSERT INTO sessions
            SELECT * FROM new_data
            """
        )

        preview_query = "select * from sessions limit 10"
        preview_df = cnn.execute(preview_query).fetchdf()

        row_count = cnn.execute("select count(*) from sessions").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )