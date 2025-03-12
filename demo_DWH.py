from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import dag, task

ETL_DATE = '20240122'

PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

STAGING_PATH_INCREMENTAL = "models/staging/incremental"

STAGING_PATH_SOURCE_IMAGE = "models/staging/source_image"

RAW_VAULT_PATH = "models/raw_vault"


@dag(dag_id="etlpipeline__demo_dwh",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    description='A DAG writed by Quoc Anh to run dbt models for demo dwh',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['etl_pipeline_demo'],
    catchup=False
)
def etlpipeline__demo_dwh():

    bash_command_incremental = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{STAGING_PATH_INCREMENTAL}/*"""
    
    bash_command_source_image = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{STAGING_PATH_SOURCE_IMAGE}/*"""

    bash_command_raw_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{RAW_VAULT_PATH}/*"""
    @task()
    def start():
        print("Start")

    run_incremental = KubernetesPodOperator(
        task_id='incrementalsource_image',
        name='incrementalsource_image',
        namespace='anhtq-airflow',
        image='quocanh2202/airflow-dbt:latest',
        cmds=["/bin/bash", "-c", bash_command_incremental],
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

    run_source_image = KubernetesPodOperator(
        task_id='source_image',
        name='source_image',
        namespace='anhtq-airflow',
        image='quocanh2202/airflow-dbt:latest',
        cmds=["/bin/bash", "-c", bash_command_source_image],
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
        namespace='anhtq-airflow',
        image='quocanh2202/airflow-dbt:latest',
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

    start() >> run_incremental >>  run_source_image >> run_rawvault >> end()

etlpipeline__demo_dwh()
