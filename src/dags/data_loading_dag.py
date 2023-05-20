from py.config import *
from py.utils import *
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the tasks
def load_staging_task():
    """
    Task to load data into the staging layer.

    This task instantiates the LoadStaging class with the connection information
    for the PostgreSQL source database and Vertica target database, and then
    calls the load_data method to perform the data loading process.
    """
    load_staging = LoadStaging(postgres_conn_info, vertica_conn_info)
    load_staging.load_data()


def load_cdm_task():
    """
    Task to load data into the common data marts layer.

    This task instantiates the LoadCDM class with the connection information
    for the Vertica database, and then calls the load_data method to perform
    the data loading process.
    """
    load_cdm = LoadCDM(vertica_conn_info)
    load_cdm.load_data()


# Define the DAG
dag = DAG(
    'data_loading_dag',
    description='DAG for data loading processes',
    schedule_interval=None,
    start_date=datetime(2023, 5, 20),
    catchup=False
)


load_staging = PythonOperator(
    task_id='load_staging',
    python_callable=load_staging_task,
    dag=dag
)


load_cdm = PythonOperator(
    task_id='load_cdm',
    python_callable=load_cdm_task,
    dag=dag
)

# Define the dependencies
load_staging >> load_cdm
