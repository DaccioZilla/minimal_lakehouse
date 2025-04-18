#%%
import dagster as dg
from dotenv import load_dotenv
import io
from minimal_lakehouse.ressources import OpenF1Api, MinioStorage
import json
load_dotenv('.env')

daily_partition = dg.DailyPartitionsDefinition(start_date= '2023-01-01')

def openf1_interface(context: dg.AssetExecutionContext, api: OpenF1Api, minio: MinioStorage):
        res = api.request(date_start = context.partition_key)
        if res.status_code != 200:
            return f'Request for {api.endpoint} on date {context.partition_key} failed with status {res.status_code}'
        
        data = res.json()

        if not data:
            return f'No data for date {context.partition_key}'
        
        data_bytes = json.dumps(data).encode('utf-8')

        data_stream = io.BytesIO(data_bytes)

        file_name = f'{api.endpoint}/{context.partition_key[:4]}/{context.partition_key[:-3]}/{context.partition_key}.json'

        minio_client = minio.get_client()
        minio_client.put_object(
            minio.dest_bucket, 
            file_name, 
            data=data_stream, 
            length=len(data_bytes), 
            content_type='application/json'
            )
        
        return f"{file_name} salvo no storage"


@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw"
)
def openf1_sessions(context: dg.AssetExecutionContext, sessions_api: OpenF1Api, minio: MinioStorage):
    return openf1_interface(context, sessions_api, minio)
    
@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw"
)
def openf1_cars(context: dg.AssetExecutionContext, cars_api: OpenF1Api, minio: MinioStorage):
    return openf1_interface(context, cars_api, minio)
    

@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw"
)
def openf1_drivers(context: dg.AssetExecutionContext, drivers_api: OpenF1Api, minio: MinioStorage):
    return openf1_interface(context, drivers_api, minio)
    
@dg.asset(
        partitions_def = daily_partition,
        compute_kind = 'python',
        group_name="openf1_raw"
)
def openf1_positions(context: dg.AssetExecutionContext, positions_api: OpenF1Api, minio: MinioStorage):
    return openf1_interface(context, positions_api, minio)
