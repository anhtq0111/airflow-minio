from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
) 
def trigger_airbyte_job_example():
    @task()
    def start():
        print("Start")

    sync_job = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte_conn_example',
        connection_id='cba5d8de-32e6-4235-b167-1cd275dd7069',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    @task()
    def end():
        print("End")

    start() >> sync_job >> end()

trigger_airbyte_job_example()