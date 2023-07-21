from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)

}

@dag(
        dag_id='dag_with_taskflow_api_v2',
        start_date=datetime(2023, 7, 21),
        default_args=default_args,
        schedule_interval='@daily'  
)

def hello_world_etl():
    

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Khang',
            'last_name': 'Hoang'
        }
    @task()
    def get_age():
        return 20
    @task()
    def greet(first_name,last_name, age):
        print(f"Hello world, my name is {first_name} {last_name}, i am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age)

greeting_dag = hello_world_etl()