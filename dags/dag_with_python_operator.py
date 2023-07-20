from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG


def greeting(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello world, my name is {first_name} {last_name}, i am {age} years old!")  


def get_name(ti):
    ti.xcom_push(key='first_name', value='Hoang')
    ti.xcom_push(key='last_name', value='Khang')

def get_age(ti):
    ti.xcom_push(key='age', value=20)

default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(2023, 7, 30)
}

with DAG(
    default_args = default_args,
    dag_id = 'dag_using_python_operator_v6',
    description = 'This is first Dag using python operator',
    start_date=datetime(2023, 7, 20),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greeting,
        #op_kwargs={'age': 20}
    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )

    #task1
    [task2,task3] >> task1
