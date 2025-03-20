from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def update_etl_date():
    etl_date = Variable.get("etl_date")
    
    # Chuyển đổi sang kiểu datetime và tăng thêm 1 ngày
    new_etl_date = (datetime.strptime(etl_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

    # Cập nhật lại biến etl_date trong Airflow
    Variable.set("etl_date", new_etl_date)

    return new_etl_date

with DAG(
    dag_id="test_variable",
    schedule_interval=None,
    start_date=datetime(2025, 1, 20),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="get_and_update_etl_date",
        python_callable=update_etl_date
    )
