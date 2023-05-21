
from airflow.providers.vertica.hooks.vertica import VerticaHook
from pandas import DataFrame
from jinja2 import Template
import os


def get_rendered_sql_template(path_to_templates_dir: str, template_name:str, variable: str) -> str:
    """
    Renders a SQL template file with a given template variable.

    Args:
        path_to_templates_dir (str): The directory path to the SQL template files.
        template_name (str): The name of the SQL template file.
        variable (str): The template variable to be rendered in the SQL template.

    Returns:
        str: The rendered SQL query.

    Raises:
        FileNotFoundError: If the template file does not exist.

    Example Usage:
        rendered_query = render_sql_template('/path/to/template.sql', 'example_variable')
    """

    full_template_path = os.path.join(path_to_templates_dir, template_name)

    with open(full_template_path, 'r') as file:
        sql_template_content = file.read()
        template = Template(sql_template_content)
        return template.render(variable=variable)


def insert_dataframe_to_vertica(vertica_hook: VerticaHook , dataframe: DataFrame, table_name: str) -> None:
    """
    Inserts a Pandas DataFrame into a Vertica table using the COPY command.

    Args:
        vertica_hook (VerticaHook): The VerticaHook object for connecting to the Vertica database.
        dataframe (pd.DataFrame): The Pandas DataFrame to be inserted into the Vertica table.
        table_name (str): The name of the target Vertica table.

    Raises:
        None

    Returns:
        None

    Example Usage:
        insert_dataframe_to_vertica(vertica_hook, dataframe, 'my_table')

    This function uses the VerticaHook object to establish a connection with the Vertica database.
    It then uses the COPY command to efficiently insert the DataFrame into the specified table.
    The DataFrame is first converted to a CSV string using the to_csv method with index and header set to False.
    The COPY command is executed on the Vertica connection's cursor, and the changes are committed to the database.

    Note: Make sure to provide the appropriate VerticaHook object and table name for a successful insertion.
    """
    with vertica_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.copy(
            "COPY {} FROM STDIN DELIMITER ',' ENFORCELENGTH DIRECT".format(table_name),
            dataframe.to_csv(index=False, header=False),
        )
        conn.commit()