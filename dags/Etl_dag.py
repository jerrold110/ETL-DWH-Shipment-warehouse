import os
os.chdir("/home/me/airflow/dags")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
#from airflow.contrib.sensors.file_sensor import FileSensor

# import ETL scripts 
from scripts.s3_extract import extract
from scripts.spark_transform_load import transform_load

default_arguments = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "email_on_success": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        #'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    	}
    	
from datetime import datetime
job_timestamp = datetime.now().replace(microsecond=0)
job_timestamp = datetime(2023, 11, 30, 0, 0,0)
print(f'extracted_data/{job_timestamp}/Customers.csv')
#print('extracted_data/2023-11-30 00:00:00/Customers.csv')
	
with DAG(
    dag_id="Etl_test_1",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_arguments,
    description="ETL process",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"]
	) as dag:

	t1 = PythonOperator(
		task_id='extract',
		python_callable=extract,
		sla=timedelta(hours=1),
		op_kwargs={'job_timestamp':job_timestamp}
		)
         
	fs1 = FileSensor(
		task_id="check_file_1",
		fs_conn_id='my_path_1',
		filepath=f'extracted_data/{job_timestamp}/Customers.csv',
		poke_interval=1,
		timeout=5,
		mede='poke'
		)
		
	fs2 = FileSensor(
		task_id="check_file_1",
		fs_conn_id='my_path_1',
		filepath=f'extracted_data/{job_timestamp}/Shipments.csv',
		poke_interval=1,
		timeout=5,
		mede='poke'
		)
		
	t2 = PythonOperator(
		task_id='transform_load',
		python_callable=transform_load,
		sla=timedelta(hours=1),
		op_kwargs={'job_timestamp':job_timestamp}
		)

	t1 >> fs1 >> f2 >> t2
