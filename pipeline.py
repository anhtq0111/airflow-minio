from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task



PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

METADATA_PATH = "models/operational_metadata"

STAGING_PATH_EXACT = "models/staging/exact101"

RAW_VAULT_PATH = "models/raw_vault"


@dag(dag_id="etlpipeline__exact101",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    description='A DAG writed by Quoc Anh to run dbt models for exact101',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=['etl_pipeline'],
    catchup=False
)
def etlpipeline__exact101():

    bash_command_operation = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} -s path:{METADATA_PATH}/*"""
    
    bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR}  -s path:{STAGING_PATH_EXACT}/*"""

    bash_command_raw_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} -s path:{RAW_VAULT_PATH}/*"""
    @task()
    def start():
        print("Start")

    run_operation = KubernetesPodOperator(
        task_id='operation',
        name='operation',
        namespace='anhtq-airflow-helm',
        image='huonganh2202/airflow-helm:latest',
        cmds=["/bin/bash", "-c", bash_command_operation],
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=7200,
        # log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        # enable pushing to XCom
        do_xcom_push=True,
    )

    run_staging = KubernetesPodOperator(
        task_id='staging',
        name='staging',
        namespace='anhtq-airflow-helm',
        image='huonganh2202/airflow-helm:latest',
        cmds=["/bin/bash", "-c", bash_command_staging],
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=7200,
        # log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        # enable pushing to XCom
        do_xcom_push=True,
    )

    run_rawvault = KubernetesPodOperator(
        task_id='raw_vault',
        name='raw_vault',
        namespace='anhtq-airflow-helm',
        image='huonganh2202/airflow-helm:latest',
        cmds=["/bin/bash", "-c", bash_command_raw_vault],
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=7200,
        # log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        # enable pushing to XCom
        do_xcom_push=True,
    )

    @task()
    def end():
        print("End")

    start() >> run_operation >>  run_staging >> run_rawvault >> end()

etlpipeline__exact101()
