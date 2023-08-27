import os
import time
from generate_mocked_ops import generate_mocked_ops

print("Starting MinIO and Airflow...")
os.system("cp .env.example .env")
os.system("docker compose -f docker-compose.yaml up -d")
os.system("cd src")
os.system("python3 setup.py clean --all")
os.system("python3 setup.py bdist_wheel")
os.system(
    "pip install --upgrade --force-reinstall --no-cache-dir dist/stock_portfolio_data-0.0.0-py3-none-any.whl"
)

# Polling interval (in seconds) for checking container status
polling_interval = 5
max_attempts = 12  # Adjust the number of attempts based on your needs
running = False

for attempt in range(max_attempts):
    # Check the status of the container using docker ps command
    ps_output = os.popen(
        "docker ps --filter 'name=stock-portfolio-data-minio-1' --format '{{.Status}}'"
    ).read()

    if "Up" in ps_output:
        print("Container is up and running.")
        running = True
        break
    else:
        print("Container is not ready yet. Waiting...")
        time.sleep(polling_interval)
else:
    print("Container didn't start within the expected time.")

if running:
    input(
        "Update airflow_config/variables.json with the MinIO credentials and press Enter to continue..."
    )

    print("Creating mocked data...")

    generate_mocked_ops()

    print("Uploading file to MinIO...")
    from minio import Minio
    from minio.error import S3Error
    import json

    variables = json.load(open("airflow_config/variables.json"))
    # Set your MinIO server information
    minio_endpoint = "localhost:9000"
    minio_access_key = variables["minio_api_access_key"]
    minio_secret_key = variables["minio_api_access_secret_key"]
    bucket_name = "raw"
    file_path = "dados_operacoes.csv"  # Replace with the actual path to your local file
    object_name = "operacoes.csv"  # The name you want to give the object in MinIO

    # Initialize the MinIO client
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,  # Set to True if using HTTPS
    )

    try:
        # Check if the bucket exists, and create it if not
        if not minio_client.bucket_exists(bucket_name):
            print("Creating buckets...")
            minio_client.make_bucket("raw")
            minio_client.make_bucket("interim")
            minio_client.make_bucket("processed")
            minio_client.make_bucket("external")

        # Upload the file
        minio_client.fput_object(bucket_name, object_name, file_path)

        print(f"File '{object_name}' uploaded successfully to '{bucket_name}' bucket.")

    except S3Error as e:
        print("Error:", e)
