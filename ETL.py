from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Juanillo',
    'start_date': days_ago(0),
    'email': 'juanillo@hello.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('ETL_toll_data',
description='Apache Airflow Final Assignment',
default_args=default_args,
schedule_interval=timedelta(days=1), 
)

# Define tasks
# task 1

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar xvfz /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
	dag=dag
)

#task 2

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d "," -f -4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
	dag=dag
)

#task 3

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f 5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr "\t" "," > /home/project/airflow/dags/finalassignment/tsv_data.csv',
	dag=dag
)

# task 4

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut  -c 59- /home/project/airflow/dags/finalassignment/payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
	dag=dag
)

# task 5

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d "," /home/project/airflow/dags/finalassignment/csv_data.csv \
    /home/project/airflow/dags/finalassignment/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/extracted_data.csv',
	dag=dag
)

# task 6

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr [:lower:][:upper:] < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/transformed_data.csv',
	dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data