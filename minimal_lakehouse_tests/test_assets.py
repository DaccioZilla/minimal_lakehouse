import dagster as dg
from minimal_lakehouse.assets import openf1_sessions
from minimal_lakehouse.ressources import OpenF1Api, MinioStorage


class DummyMinio:
    class DummyMinioClient:
        def put_object(self, *args, **kwargs):
            print ('Saved on Storage')
    
    def __init__(self):
        self.dest_bucket = ''
    
    def get_client(self):
        return DummyMinio.DummyMinioClient()

class DummyResponse:
    
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data

class DummyFailApi:

    def request(self, **kwargs):
        return DummyFailApi(404, [])

class DummyNoDataApi:

    def request(self):
        pass

def test_openf1_session_success() -> None:
    class ApiSuccess:
        def request(self,**kwargs):
            return DummyResponse(200, True)
    context = dg.build_asset_context(partition_key = '2025-04-17')
    api = ApiSuccess()
    minio = DummyMinio()
    assert openf1_sessions(context, sessions_api = api, minio = minio) == f"f'sessions/{context.partition_key[:4]}/{context.partition_key[:-3]}/{context.partition_key}.json salvo no storage"

def test_openf1_session_invalid_status() -> None:
    class ApiStatusFail:
        def request(self,**kwargs):
            return DummyResponse(404, True)
    context = dg.build_asset_context(partition_key = '2025-04-17')
    api = ApiStatusFail()
    minio = DummyMinio()
    assert openf1_sessions(context, sessions_api = api, minio = minio) == f'Request for session on date {context.partition_key} failed with status 404'

def test_openf1_session_no_data_call() -> None:
    class ApiNoData:
        def request(self,**kwargs):
            return DummyResponse(200, False)
    context = dg.build_asset_context(partition_key = '2025-04-17')
    api = ApiNoData()
    minio = DummyMinio()
    assert openf1_sessions(context, sessions_api = api, minio = minio) == f'No data for date {context.partition_key}'

def test_openf1_session_real_call() -> None:

    context = dg.build_asset_context(partition_key = '2023-07-29')
    api = OpenF1Api(endpoint = 'sessions')
    minio = MinioStorage()
    assert openf1_sessions(context, sessions_api = api, minio = minio) == f"file_name = f'sessions/{context.partition_key[:4]}/{context.partition_key[:-3]}/{context.partition_key}.json salvo no storage"