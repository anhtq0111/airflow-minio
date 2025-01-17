from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.decorators import dag, task



PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

model_path = "models/dbt_trino_model"



@dag(dag_id="full_from_ingess",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    description='A DAG writed by Quoc Anh to run etl from ingess',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['etl_pipeline'],
    catchup=False
)
def full_from_ingess():

    bash_run_etl = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} -s path:{model_path}/*"""

    @task()
    def start():
        print("Start")

    run_ingess = AirbyteTriggerSyncOperator(
        task_id='run_ingess',
        airbyte_conn_id='airbyte',
        connection_id='7c4509f3-95e6-4ca5-adfd-b7867f60bc5b',
        asynchronous=False,
        # timeout=3600,
        # wait_seconds=3
    )

    run_etl = KubernetesPodOperator(
        task_id='run_etl',
        name='run_etl',
        namespace='anhtq-airflow-helm',
        image='huonganh2202/dbt-trino:sample_trino_iceberg_f',
        cmds=["/bin/bash", "-c", bash_run_etl],
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

    start() >> run_ingess >> run_etl  >> end()

full_from_ingess()
