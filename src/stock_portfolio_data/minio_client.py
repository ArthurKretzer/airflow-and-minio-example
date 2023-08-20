import os

from minio import Minio

minio_client = Minio(
    os.environ["minio_endpoint"],
    access_key=os.environ["minio_api_access_key"],
    secret_key=os.environ["minio_api_access_secret_key"],
    secure=False,  # Set to False if you are using HTTP instead of HTTPS
)
