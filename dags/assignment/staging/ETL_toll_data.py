from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# Define DAG arguments
args = {
    'owner': 'Khang Hoang',
    'start_date': datetime.today(),
    'email': 'nguyenhoangkhang12102003@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=args,
    description='Apache Airflow Final Assignment'
)

# Task 1.3 - Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xvzf /d/STUDY/Airflow-Docker/dags/assignment/tolldata.tgz -C  /d/STUDY/Airflow-Docker/dags/assignment',
    dag=dag
)

# Task 1.4 - Create a task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d, -f1-4 /d/STUDY/Airflow-Docker/dags/assignment/vehicle-data.csv > /d/STUDY/Airflow-Docker/dags/assignment/staging/csv_data.csv',
    dag=dag
)

# Task 1.5 - Create a task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f2,4,5 /d/STUDY/Airflow-Docker/dags/assignment/tollplaza-data.tsv > /d/STUDY/Airflow-Docker/dags/assignment/staging/tsv_data.csv',
    dag=dag
)

# Task 1.6 - Create a task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c3-4,7 /d/STUDY/Airflow-Docker/dags/assignment/payment-data.txt > /d/STUDY/Airflow-Docker/dags/assignment/staging/fixed_width_data.csv',
    dag=dag
)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /d/STUDY/Airflow-Docker/dags/assignment/staging/csv_data.csv /d/STUDY/Airflow-Docker/dags/assignment/staging/tsv_data.csv /d/STUDY/Airflow-Docker/dags/assignment/staging/fixed_width_data.csv | cut -d "," -f1-4,7-9,5-6 > /d/STUDY/Airflow-Docker/dags/assignment/staging/extracted_data.csv',
    dag=dag
)

# Task 1.8 - Transform and load the data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[:lower:]" "[:upper:]" < /d/STUDY/Airflow-Docker/dags/assignment/staging/extracted_data.csv > /d/STUDY/Airflow-Docker/dags/assignment/staging/transformed_data.csv',
    dag=dag
)

# Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
