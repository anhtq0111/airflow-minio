from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import dag, task



PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

TEST_PATH = "models/test_rada"




@dag(dag_id="etlpipeline__exact101",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    description='A DAG writed by Quoc Anh to run dbt models for test RADA',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=['etl_pipeline'],
    catchup=False
)
def test_rada():

    bash_test = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} -s path:{TEST_PATH}/*"""

    @task()
    def start():
        print("Start")

    run_test = KubernetesPodOperator(
        task_id='operation',
        name='operation',
        namespace='anhtq-airflow-helm',
        image='huonganh2202/airflow-helm:dbt-trino-test',
        cmds=["/bin/bash", "-c", bash_test],
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

    start() >> run_test  >> end()

test_rada()
