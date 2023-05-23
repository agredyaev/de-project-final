"""
This DAG is responsible for loading data into the common data mart (CDM) layer. It consists of task for loading global metrics data.

Tasks:
    - cdm_load_global_metrics: Load global metrics data into the CDM layer.

Task retrieves the respective SQL template, renders it with the provided variable (in this case, the execution date), and executes the rendered query on the source PostgreSQL database. The resulting data is then inserted into the target Vertica table using the COPY command.

The target Vertica tables and the corresponding SQL templates are defined in the common module, and the PostgresHook and VerticaHook objects are used to establish connections to the source and target databases, respectively.

Make sure to configure the appropriate connection details and file paths in the common module before running this DAG.
"""

import sys
sys.path.append('/root/de-project-final/src')

from pendulum import datetime
from py.utils import DataProcessor
from airflow.decorators import dag, task
from py.common import TAMPLATES_PATH, CDM_TEMPLATE, CDM_SCHEMA
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id='cdm_load_data',
    description=__doc__,
    schedule="@daily",
    start_date=datetime(2022, 10, 2),
    end_date=datetime(2022, 11, 2),
    catchup=True
)
def cdm_load_dag():
    @task
    def cdm_load_global_metrics(variable: str):

        template_path = TAMPLATES_PATH
        table_name = f'{CDM_SCHEMA}.global_metrics'
        template_name = f'{CDM_TEMPLATE}_global_metrics.sql'

        vertica_hook = VerticaHook('vertica_connection')

        processor = DataProcessor(
            variable=variable,
            template_path=template_path,
            table_name=table_name,
            template_name=template_name,
            ingest_hook=vertica_hook,
            egest_hook=vertica_hook
        )

        processor.run()
        
    cdm_load_global_metrics('{{ ds }}')


cdm_load_dag()

