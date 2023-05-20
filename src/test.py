from py.config import postgres_conn_info, vertica_conn_info, DIR
import psycopg2
import os
from py.utils import BuildStaging
from vertica_python import connect

os.chdir(DIR)

def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as f:
        sql_query = f.read()
    return sql_query

with psycopg2.connect(**postgres_conn_info) as postgres_conn:
    with connect(**vertica_conn_info) as vertica_conn:
        postgres_cursor = postgres_conn.cursor()
        vertica_cursor = vertica_conn.cursor()

        postgres_query = read_sql_file('./sql/etl_staging_transactions_extract.sql')
        postgres_cursor.execute(postgres_query)
        
        for row in postgres_cursor.fetchall():
            print(row)
            staging_data = BuildStaging(row).build_transactions()
            print(staging_data)
            vertica_query = read_sql_file('./sql/etl_staging_transactions_load.sql')
            vertica_cursor.execute(vertica_query, staging_data)




