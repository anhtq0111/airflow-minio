from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task



PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

STAGING_PATH_EXACT = "models/staging/*"

RAW_VAULT_PATH = "models/raw_vault/*"


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

    # Define bash command for dbt run staging for all sources
    
    bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR}  -s path:{STAGING_PATH_EXACT}"""

    bash_command_raw_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} -s path:{RAW_VAULT_PATH}"""
    @task()
    def start():
        print("Start")
        

    run_staging = KubernetesPodOperator(
        task_id='staging',
        name='staging',
        namespace='anhtq-airflow',
        image='quocanh2202/airflow-dbt:latest',
        cmds=["/bin/bash", "-c", bash_command_staging],
        in_cluster=True,
        startup_timeout_seconds=7200,
        is_delete_operator_pod=True,
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
        in_cluster=True,
        startup_timeout_seconds=7200,
        is_delete_operator_pod=True,
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

    start() >> run_staging >> run_rawvault >> end()

etlpipeline__exact101()