import os
from typing import Union
from jinja2 import Template
from pandas import DataFrame
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataProcessor:
    def __init__(self, variable: str,
                 template_path: str,
                 template_name: str,
                 table_name: str,
                 ingest_hook: Union[PostgresHook, VerticaHook],
                 egest_hook: VerticaHook):
        """
        DataProcessor class for processing SQL templates and loading data into a target Vertica table.
        
        Args:
            variable (str): The variable to be rendered in the SQL template.
            template_path (str): The file path to the directory containing the SQL template file.
            template_name (str): The name of the SQL template file.
            table_name (str): The name of the target table where the data will be loaded.
            ingest_hook (Union[PostgresHook, VerticaHook]): The hook object for connecting to the source database.
            egest_hook (VerticaHook): The hook object for connecting to the target Vertica database.

        Methods:
            _get_rendered_sql_template(self) -> str:
                Renders a SQL template file with a given template variable.

            _insert_dataframe_to_vertica(self, dataframe: DataFrame) -> None:
                Inserts a Pandas DataFrame into a Vertica table using the COPY command.
                Establishes a connection to the Vertica database using the VerticaHook object. 
                It efficiently inserts the DataFrame into the specified table using the COPY command. 
                The DataFrame is converted to a CSV string with index and header set to False using the to_csv method. 
                The COPY command is executed on the Vertica connection's cursor, and the changes are committed to the database.

            run(self):
                Executes the data processing and loading steps.
        """
        self.variable = variable
        self.template_path = template_path
        self.template_name = template_name
        self.table_name = table_name
        self.ingest_hook = ingest_hook
        self.egest_hook = egest_hook

    def _get_rendered_sql_template(self) -> str:
        """
        Renders a SQL template file with a given template variable.

        Returns:
            str: The rendered SQL query.

        Raises:
            FileNotFoundError: If the template file does not exist.
        """
        full_template_path = os.path.join(self.template_path, self.template_name)

        with open(full_template_path, 'r') as file:
            sql_template_content = file.read()
            template = Template(sql_template_content)
            return template.render(variable=self.variable)

    def _insert_dataframe_to_vertica(self, dataframe: DataFrame) -> None:
        """
        Inserts a Pandas DataFrame into a target table using the COPY command.

        Args:
            dataframe (pd.DataFrame): The Pandas DataFrame to be inserted into target table.
        """
        with self.egest_hook.get_conn() as conn:
            cur = conn.cursor()
            cur.copy(
                "COPY {} FROM STDIN DELIMITER ',' ENFORCELENGTH DIRECT".format(
                    self.table_name),
                dataframe.to_csv(index=False, header=False),
            )
            conn.commit()

    def run(self):
        """
        Executes the data processing and loading steps.
        """
        rendered_query = self._get_rendered_sql_template()
        dataframe = self.ingest_hook.get_pandas_df(sql=rendered_query)
        self._insert_dataframe_to_vertica(dataframe)