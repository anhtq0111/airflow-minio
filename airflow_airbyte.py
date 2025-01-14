from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte_conn_example',
        connection_id='cba5d8de-32e6-4235-b167-1cd275dd7069',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )