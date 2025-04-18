#%%
from minimal_lakehouse.ressources import OpenF1Api, MinioStorage
import io

def test_api_init():
    api = OpenF1Api(endpoint='some')
    assert isinstance(api, OpenF1Api)

def test_api_request():
    api = OpenF1Api(endpoint='sessions')
    res = api.request(date_start = '2023-07-29')
    print(res.json())
    assert res.status_code == 200

def test_minio_put_object():
    minio = MinioStorage()
    cliente = minio.get_client()
    obj_name = 'test.json'
    data = "{'test': 'test'}".encode('utf-8')
    cliente.put_object(minio.dest_bucket, 'test.json', io.BytesIO(data), len(data), 'application/json' )
    objs = [obj.object_name for obj in cliente.list_objects(minio.dest_bucket)]
    cliente.remove_object(minio.dest_bucket, obj_name)
    assert obj_name in objs
# %%

