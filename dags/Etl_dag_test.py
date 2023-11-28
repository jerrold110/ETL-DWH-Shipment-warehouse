import os
os.chdir("/home/me/airflow/dags")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# importETL scripts 
from scripts.s3_extract import extract
from scripts.spark_transform import transform
#from scripts.spark_test import transform

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
    	task_id='Extract',
        python_callable=extract,
        sla=timedelta(hours=1)
        )
        
    t2 = PythonOperator(
    	task_id='Transform',
        python_callable=transform,
        sla=timedelta(hours=1)
        )

    t1 >> t2
