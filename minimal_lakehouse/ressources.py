import dagster as dg
import requests
from minio import Minio

class OpenF1Api(dg.ConfigurableResource):
    endpoint:str 

    def request(self, **kwargs):
        url = f'https://api.openf1.org/v1/{self.endpoint}'
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
    