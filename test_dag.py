from __future__ import annotations

import pendulum 
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weekday import WeekDay


@dag(schedule=None, start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False)
def test_dag():
    @task.bash
    def run_me(sleep_seconds: int, task_instance_key_str: str) -> str:
        return f"echo {task_instance_key_str} && sleep {sleep_seconds}"

    run_me_loop = [run_me.override(task_id=f"runme_{i}")(sleep_seconds=i) for i in range(3)]
    run_me_loop
test_dag()