from minio import Minio
from airflow.decorators import dag, task
import pendulum 
from minio.error import S3Error
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# The destination bucket and filename on the MinIO server
volume = k8s.V1Volume(
    name="dbt-storage",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="dbt-storage-pvc"),
)

volume_mount = k8s.V1VolumeMount(
    name="dbt-storage", mount_path="/dbt/target", sub_path=None
)


@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False)
def upload_manifest_minio():

    @task()
    def start():
        print("Start")

    run_compile = KubernetesPodOperator(
        task_id='run_etl',
        name='run_etl',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dwh:v2',
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
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    run_upload = KubernetesPodOperator(
        task_id='run_upload',
        name='run_upload',
        namespace='anhtq-airflow',
        image='minio/mc',
        cmds=["sh", "-c"],
        arguments=[
        "mc alias set minio http://192.168.1.17:32023 rafflesit rafflesit && "
        "mc cp --recursive /dbt/target minio/om-dbt-demo/dbt-manifest"
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    @task()
    def end():
        print("End")

    
    start() >> run_compile >> run_upload >> end()
    # start() >> run_compile >> end()

upload_manifest_minio()

