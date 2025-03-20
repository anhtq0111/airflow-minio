from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import dag, task

ETL_DATE = '20250121'

PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

STAGING_PATH_INCREMENTAL = "models/staging/Incremental"
STAGING_PATH_SOURCE_IMAGE = "models/staging/Source_image"
RAW_VAULT_PATH = "models/raw_vault"
DATA_MART_PATH = "models/data_mart"


@dag(dag_id="demo_DLH",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    description='A DAG writed by Quoc Anh to run dbt models for demo dlh',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=['demo_dlh'],
    catchup=False
)
def demo_dlh():

    bash_command_staging_inc = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{STAGING_PATH_INCREMENTAL}/*"""
    
    bash_command_staging_si = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}'  -s path:{STAGING_PATH_SOURCE_IMAGE}/*"""

    bash_command_raw_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{RAW_VAULT_PATH}/*"""

    bash_command_data_mart = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{DATA_MART_PATH}/*"""
    @task()
    def start():
        print("Start")

    run_inc = KubernetesPodOperator(
        task_id='staging_inc',
        name='staging_inc',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dlh:v1',
        cmds=["/bin/bash", "-c", bash_command_staging_inc],
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

    run_si = KubernetesPodOperator(
        task_id='staging_si',
        name='staging_si',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dlh:v1',
        cmds=["/bin/bash", "-c", bash_command_staging_si],
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
        image='huonganh2202/demo-dlh:v1',
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

    run_data_mart = KubernetesPodOperator(
        task_id='data_mart',
        name='data_mart',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dlh:v1',
        cmds=["/bin/bash", "-c", bash_command_data_mart],
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

    start() >> run_inc >>  run_si >> run_rawvault >> run_data_mart >> end()

demo_dlh()
