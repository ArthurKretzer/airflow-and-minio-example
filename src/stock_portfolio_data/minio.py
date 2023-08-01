from minio import Minio

minio_client = Minio(
    "stock-portfolio-data-minio-1:9000",
    access_key="XBJhoPKpkIqIPYmrgPGh",
    secret_key="UJQb2VWWystUFv6WffVtCxWvakYW0HTlrF9tz0yq",
    secure=False,  # Set to False if you are using HTTP instead of HTTPS
)
