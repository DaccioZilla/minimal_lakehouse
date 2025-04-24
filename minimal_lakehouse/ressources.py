import dagster as dg
import requests
from minio import Minio

class OpenF1Api(dg.ConfigurableResource):
    base_url:str = 'https://api.openf1.org/v1'

    def request(self, endpoint, **kwargs):
        url = f'{self.base_url}/{endpoint}'
        res = requests.get(url, kwargs) 
        return res

class MinioStorage(dg.ConfigurableResource):
    dest_bucket:str = "lakehouse"
    minio_endpoint:str
    minio_username:str
    minio_password:str

    def get_client(self):
        return Minio(
                    self.minio_endpoint,
                    secure = False,
                    access_key= self.minio_username,
                    secret_key= self.minio_password
                    )
    