from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=datetime(2025, 1, 1)
) 
def trigger_airbyte_job_example():
    @task()
    def start():
        print("Start")

    airbyte_sync_job = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte',
        connection_id='628a1b08-bf3b-4a02-87b7-76e94e1d5fb9',
        asynchronous=True,
        timeout=3600,
        wait_seconds=3
    )

    airbyte_sensor = AirbyteJobSensor(
        task_id='asensor_example',
        airbyte_conn_id='airbyte',
        airbyte_job_id=airbyte_sync_job.output
    )

    @task()
    def end():
        print("End")

    start() >> airbyte_sync_job >> airbyte_sensor >> end()

trigger_airbyte_job_example()