from minio import Minio
from airflow.decorators import dag, task
import pendulum 
from minio.error import S3Error
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# The destination bucket and filename on the MinIO server

@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False)
def upload_manifest_minio():

    run_compile = KubernetesPodOperator(
        task_id='run_etl',
        name='run_etl',
        namespace='anhtq-airflow',
        image='huonganh2202/dbt-trino:sample_trino_iceberg_f',
        cmds=["/bin/bash", "-c"],
        arguments=[
        "dbt compile --profiles-dir /dbt --project-dir /dbt"
        ],
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=7200,
        # log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        volumes=[
        {
            "name": "dbt-storage", "persistentVolumeClaim": {"claimName": "dbt-storage-pvc"}
        }
        ],
        volume_mounts=[
            {"name": "dbt-storage", "mountPath": "/dbt/target"}
        ],
    )

    run_upload = KubernetesPodOperator(
        task_id='run_upload',
        name='run_upload',
        namespace='anhtq-airflow',
        image='minio/mc',
        cmd=["sh", "-c"],
        arguments=[
        "mc alias set myminio http://192.168.1.17:32023 minioadmin minioadmin && "
        "mc cp /dbt/target/manifest.json myminio/dbt-artifacts/dbt/manifest.json"
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        volumes=[
            {
                "name": "dbt-storage", "persistentVolumeClaim": {"claimName": "dbt-storage-pvc"}
            }
        ],
        volume_mounts=[
            {"name": "dbt-storage", "mountPath": "/dbt/target"}
        ]
    )

    run_compile >> run_upload

upload_manifest_minio()

