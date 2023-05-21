"""
This DAG is responsible for loading data into the staging layer. It consists of tasks for loading transaction data and currency data separately.

Tasks:
    - staging_load_transactions: Load transaction data into the staging layer.
    - staging_load_currencies: Load currency data into the staging layer.

Each task retrieves the respective SQL template, renders it with the provided variable (in this case, the execution date), and executes the rendered query on the source PostgreSQL database. The resulting data is then inserted into the target Vertica table using the COPY command.

The target Vertica tables and the corresponding SQL templates are defined in the common module, and the PostgresHook and VerticaHook objects are used to establish connections to the source and target databases, respectively.

Make sure to configure the appropriate connection details and file paths in the common module before running this DAG.
"""

import sys
sys.path.append('/root/de-project-final/src')

from py.common import TAMPLATES_PATH, STG_TEMPLATE, STG_SCHEMA
from py.utils import get_rendered_sql_template, insert_dataframe_to_vertica
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from pendulum import datetime


@dag(
    dag_id='staging_load_data',
    description=__doc__,
    schedule="@daily",
    start_date=datetime(2022, 10, 1),
    catchup=True
)
def staging_load_dag():
    @task
    def staging_load_currencies(variable):

        pg_hook = PostgresHook('postgres_connection')
        vertica_hook = VerticaHook('vertica_connection')

        table_name = f'{STG_SCHEMA}.currencies'
        template_name = f'{STG_TEMPLATE}_currencies.sql'
        
        rendered_query = get_rendered_sql_template(
            path_to_templates_dir=TAMPLATES_PATH,
            template_name=template_name,
            variable=variable
        )

        dataframe = pg_hook.get_pandas_df(sql=rendered_query)
        insert_dataframe_to_vertica(
            vertica_hook=vertica_hook,
            dataframe=dataframe,
            table_name=table_name
        )

    @task
    def staging_load_transactions(variable):

        pg_hook = PostgresHook('postgres_connection')
        vertica_hook = VerticaHook('vertica_connection')

        table_name = f'{STG_SCHEMA}.transactions'
        template_name = f'{STG_TEMPLATE}_transactions.sql'
        
        rendered_query = get_rendered_sql_template(
            path_to_templates_dir=TAMPLATES_PATH,
            template_name=template_name,
            variable=variable
        )

        dataframe = pg_hook.get_pandas_df(sql=rendered_query)
        insert_dataframe_to_vertica(
            vertica_hook=vertica_hook,
            dataframe=dataframe,
            table_name=table_name
        )

    [staging_load_currencies('{{ ds }}'), staging_load_transactions('{{ ds }}')]



staging_load_dag()

