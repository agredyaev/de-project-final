from py.config import postgres_conn_info, vertica_conn_info
from py.utils import LoadStaging, LoadCDM
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def staging_load_currencies():
    """
    Task to load currency data into the staging layer.

    This task instantiates the LoadStaging class with the connection information
    for the PostgreSQL source database and Vertica target database, and then
    calls the load_data method to perform the data loading process for currencies.
    """
    load_staging = LoadStaging(postgres_conn_info, vertica_conn_info)
    load_staging.load_data(
        path_to_pg_file='./sql/etl_staging_currencies_extract.sql',
        path_to_vertica_file='./sql/etl_staging_currencies_load.sql',
        builder_method='build_currencies'
    )


def staging_load_transactions():
    """
    Task to load transaction data into the staging layer.

    This task instantiates the LoadStaging class with the connection information
    for the PostgreSQL source database and Vertica target database, and then
    calls the load_data method to perform the data loading process for transactions.
    """
    load_staging = LoadStaging(postgres_conn_info, vertica_conn_info)
    load_staging.load_data(
        path_to_pg_file='./sql/etl_staging_transactions_extract.sql',
        path_to_vertica_file='./sql/etl_staging_transactions_load.sql',
        builder_method='build_transactions'
    )



def cdm_load_global_metrics():
    """
    Task to load data into the common data marts layer.

    This task instantiates the LoadCDM class with the connection information
    for the Vertica database, and then calls the load_data method to perform
    the data loading process.
    """
    load_cdm = LoadCDM(vertica_conn_info)
    load_cdm.load_data()


dag = DAG(
    'data_loading_dag',
    description='DAG for data loading processes',
    schedule_interval=None,
    start_date=datetime(2023, 5, 20),
    catchup=False
)


stg_load_transactions = PythonOperator(
    task_id='staging_load_transactions',
    python_callable=staging_load_transactions,
    dag=dag
)

stg_load_currencies = PythonOperator(
    task_id='staging_load_currencies',
    python_callable=staging_load_currencies,
    dag=dag
)


cdm_load_glbl_metrics = PythonOperator(
    task_id='load_cdm',
    python_callable=cdm_load_global_metrics,
    dag=dag
)

stg_load_currencies >> stg_load_transactions, stg_load_currencies >> cdm_load_glbl_metrics
