from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    dag_id ="first_dag_v4",
    default_args = default_args,
    description = "This is first dag",
    start_date = datetime(2023, 7, 20, 2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo Hello world, this is my first task!!"
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command= "echo This is task 2, will be executed after task 1!!"
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo I am task 3, will be running after task 1, at the same time as task 2!!"
    )
    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    
    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]

