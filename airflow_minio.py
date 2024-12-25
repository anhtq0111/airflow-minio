from minio import Minio
from airflow.decorators import dag, task
import pendulum 
from minio.error import S3Error
import json
source_file = "/opt/airflow/airflow_minio.txt"

# The destination bucket and filename on the MinIO server
bucket_name = "airflow-bucket"
destination_file = "test.txt"

def get_minio_client():
    return Minio(
        endpoint="192.168.1.18:32023",
        access_key="airflow-minio",
        secret_key="airflow-minio",
        secure=False
    )
def generate_csv_file(source_file):
    data = 'hehe haha hihi'
    with open(source_file, 'w') as f:
        f.write(data)

@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False)
def demo_airflow_minio():
    @task
    def create_file(source_file):
        generate_csv_file(source_file)
        print("Created file")
    create_f = create_file(source_file)
    @task
    def create_bucket():
        client = get_minio_client()
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print("Created bucket", bucket_name)
        else:
            print("Bucket", bucket_name, "already exists")
    create = create_bucket()
    @task
    def upload_file(bucket_name, source_file, destination_file):
        client = get_minio_client()
        client.fput_object(
            bucket_name, destination_file, source_file,
        )
        print(
            source_file, "successfully uploaded as object",
            destination_file, "to bucket", bucket_name,
        )
    upload = upload_file(bucket_name, source_file, destination_file)
    create_f >> create >> upload

airflow_minio = demo_airflow_minio()

