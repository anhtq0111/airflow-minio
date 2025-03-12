from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
# from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=datetime(2025, 1, 1)
) 
def trigger_airbyte_job_example():
    @task()
    def start():
        print("Start")

    # oracle_to_mysql = AirbyteTriggerSyncOperator(
    #     task_id='oracle_to_mysql',
    #     airbyte_conn_id='airbyte',
    #     connection_id='628a1b08-bf3b-4a02-87b7-76e94e1d5fb9',
    #     asynchronous=True,
    #     # timeout=3600,
    #     # wait_seconds=3
    # )

    # sample_to_s3 = AirbyteTriggerSyncOperator(
    #     task_id='sample_to_s3',
    #     airbyte_conn_id='airbyte',
    #     connection_id='25faff6b-536b-4328-bf68-189c5c492e12',
    #     asynchronous=True,
    #     # timeout=3600,
    #     # wait_seconds=3
    # )

    sample_to_postgres = AirbyteTriggerSyncOperator(
        task_id='sample_to_postgres',
        airbyte_conn_id='airbyte',
        connection_id='67867b10-12f9-4a95-a135-95029a8fce43',
        asynchronous=True,
        # timeout=3600,
        # wait_seconds=3
    )
    @task()
    def end():
        print("End")

    start() >> sample_to_postgres  >> end()
    # start() >> oracle_to_mysql >> sample_to_s3 >> sample_to_postgres >> end()


trigger_airbyte_job_example()