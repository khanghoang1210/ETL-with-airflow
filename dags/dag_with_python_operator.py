from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG


def greeting(name, age):
    print(f"Hello world, my name is {name}, i am {age} years old!")

default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(2023, 7, 30)
}

with DAG(
    default_args = default_args,
    dag_id = 'dag_using_python_operator_v2',
    description = 'This is first Dag using python operator',
    start_date=datetime(2023, 7, 20),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greeting,
        op_kwargs={'name': 'Khang', 'age': 20}
    )

    task1
