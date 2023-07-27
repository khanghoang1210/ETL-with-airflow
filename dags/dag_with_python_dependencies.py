from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_scikitlearn():
    import sklearn
    print(f"scikit learn with version {sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"matlpotlib with version {matplotlib.__version__}")

default_args = {
    'owner': 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='dag_with_python_dependencies_v2',
    start_date=datetime(2023, 7, 25),
    schedule_interval='@daily'
) as dag:
    get_scikitlearn = PythonOperator(
        task_id = 'get_scikitlearn',
        python_callable=get_scikitlearn
    )

    get_matplotlib = PythonOperator(
        task_id = 'get_matplotlib',
        python_callable=get_matplotlib
    )
    get_scikitlearn >> get_matplotlib