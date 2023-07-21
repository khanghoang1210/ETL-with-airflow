from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_cron_expression_v4',
    default_args=default_args,
    start_date=datetime(2023, 7, 15),
    schedule_interval='0 3 * * Tue-Fri',
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo dag with cron expression!'
    )

    task1