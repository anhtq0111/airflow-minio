from datetime import datetime, timedelta
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

ETL_DATE = '20240125'

PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

STAGING_PATH_INCREMENTAL = "models/staging/incremental"
RAW_VAULT_PATH = "models/raw_vault"
DATA_MART_PATH = "models/data_mart"

AIRBYTE_CONN_ID = 'airbyte'
FRKRG_TO_STAGING = '65bf03dd-2ffa-41c7-97ce-40212cfbf2a4'
FRSRG_TO_STAGING = '8c715ef7-aafc-438e-b404-69d6995bc82e'
PRPROJECT_TO_STAGING = 'c72f8f9c-e75d-44ce-b50a-5365a2b66e4a'

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

    bash_command_raw_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{RAW_VAULT_PATH}/*"""

    bash_command_data_mart = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{ "etl_date": "{ETL_DATE}"}}' -s path:{DATA_MART_PATH}/*"""
    @task()
    def start():
        print("Start")

    with TaskGroup("airbyte_syncs") as airbyte_syncs:
        # Trigger Airbyte connections in parallel
        frkrg_to_staging = AirbyteTriggerSyncOperator(
            task_id='frkrg_to_staging',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=FRKRG_TO_STAGING,
            asynchronous=True
        )

        frsrg_to_staging = AirbyteTriggerSyncOperator(
            task_id='frsrg_to_staging',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=FRSRG_TO_STAGING,
            asynchronous=True
        )

        prproject_to_staging = AirbyteTriggerSyncOperator(
            task_id='prproject_to_staging',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=PRPROJECT_TO_STAGING,
            asynchronous=True
        )
        # Wait for Airbyte jobs to finish
        frkrg_to_staging_sensor = AirbyteJobSensor(
            task_id='wait_frkrg_to_stagingl',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=frkrg_to_staging.output
        )

        frsrg_to_staging_sensor = AirbyteJobSensor(
            task_id='wait_frsrg_to_staging',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=frsrg_to_staging.output
        )

        prproject_to_staging_sensor = AirbyteJobSensor(
            task_id='wait_prproject_to_staging',
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=prproject_to_staging.output
        )
        # Define dependencies
        frkrg_to_staging >> frkrg_to_staging_sensor
        frsrg_to_staging >> frsrg_to_staging_sensor
        prproject_to_staging >> prproject_to_staging_sensor

    run_incremental = KubernetesPodOperator(
        task_id='incrementalsource_image',
        name='incrementalsource_image',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dwh:v2',
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

    run_rawvault = KubernetesPodOperator(
        task_id='raw_vault',
        name='raw_vault',
        namespace='anhtq-airflow',
        image='huonganh2202/demo-dwh:v2',
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
        image='huonganh2202/demo-dwh:v2',
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

    start() >> airbyte_syncs >> run_incremental >> run_rawvault >> run_data_mart >> end()

etlpipeline__demo_dwh()
