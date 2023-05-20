from abc import ABC, abstractmethod
from vertica_python import connect
import psycopg2
from typing import Optional, Dict, List, Tuple
from py.model import Currency, Transaction, GlobalMetrics


class BuildStaging:
    """
    Class to build data models from a single row fetched from PostgreSQL.

    Attributes:
        row (Tuple): The row fetched from PostgreSQL.
    """

    def __init__(self, row: Tuple):
        """
        Initialize the BuildStaging object.

        Args:
            row (Tuple): The row fetched from PostgreSQL.
        """
        self.row = row

    def build_currencies(self) -> Dict:
        """
        Build a dictionary representing a Currency object from the fetched row.

        Returns:
            Dict: A dictionary representing a Currency object.
        """
        currency_data = {
            "date_update": self.row[0],
            "currency_code": self.row[1],
            "currency_code_with": self.row[2],
            "currency_code_div": self.row[3]
        }
        return Currency(**currency_data).dict()

    def build_transactions(self) -> Dict:
        """
        Build a dictionary representing a Transaction object from the fetched row.

        Returns:
            Dict: A dictionary representing a Transaction object.
        """
        transaction_data = {
            "operation_id": self.row[0],
            "account_number_from": self.row[1],
            "account_number_to": self.row[2],
            "currency_code": self.row[3],
            "country": self.row[4],
            "status": self.row[5],
            "transaction_type": self.row[6],
            "amount": self.row[7],
            "transaction_dt": self.row[8]
        }
        return Transaction(**transaction_data).dict()



class AbstractDataLoader(ABC):
    """
    Abstract base class for data loaders.

    Attributes:
        postgres_conn_info (dict): Connection information for the PostgreSQL database.
        vertica_conn_info (dict): Connection information for the Vertica database.
    """

    def __init__(self,
                 postgres_conn_info: Optional[Dict[str, str]] = None,
                 vertica_conn_info: Optional[Dict[str, str]] = None
                 ):
        """
        Initialize the AbstractDataLoader object.

        Args:
            postgres_conn_info (dict, optional): Connection information for the PostgreSQL database.
            vertica_conn_info (dict, optional): Connection information for the Vertica database.
        """
        self.postgres_conn_info = postgres_conn_info
        self.vertica_conn_info = vertica_conn_info

    def read_sql_file(self, file_path: str) -> str:
        """
        Read an SQL file and return its contents.

        Args:
            file_path (str): Path to the SQL file.

        Returns:
            str: Contents of the SQL file.

        Raises:
            FileNotFoundError: If the SQL file does not exist.
        """
        with open(file_path, 'r') as f:
            sql_query = f.read()
        return sql_query

    @abstractmethod
    def load_data(self):
        """
        Abstract method to load data into the target layer.

        This method should be implemented by the derived classes.
        """
        pass


class LoadStaging(AbstractDataLoader):
    """
    Class for loading data into the staging layer.

    Inherits from AbstractDataLoader.

    Attributes:
        postgres_conn_info (Dict[str, str]): PostgreSQL connection information.
        vertica_conn_info (Dict[str, str]): Vertica connection information.
    """

    def __init__(self, postgres_conn_info: Dict[str, str], vertica_conn_info: Dict[str, str]):
        """
        Initialize the LoadStaging object.

        Args:
            postgres_conn_info (Dict[str, str]): PostgreSQL connection information.
            vertica_conn_info (Dict[str, str]): Vertica connection information.
        """
        super().__init__(vertica_conn_info=vertica_conn_info, postgres_conn_info=postgres_conn_info)

    def load_data(self, path_to_pg_file: str, path_to_vertica_file: str, builder_method: str) -> None:
        """
        Load data into the staging layer.
        
        Args:
            path_to_pg_file (str): Path to the PostgreSQL SQL file.
            path_to_vertica_file (str): Path to the Vertica SQL file.
            builder_method (str): The method name to use for building staging data.

        Returns:
            None
        """
        with psycopg2.connect(**self.postgres_conn_info) as postgres_conn:
            with connect(**self.vertica_conn_info) as vertica_conn:
                postgres_cursor = postgres_conn.cursor()
                vertica_cursor = vertica_conn.cursor()

                postgres_query = self.read_sql_file(path_to_pg_file)
                postgres_cursor.execute(postgres_query)

                for row in postgres_cursor.fetchall():
                    staging_data = getattr(BuildStaging, builder_method)(row)

                    vertica_query = self.read_sql_file(path_to_vertica_file)
                    vertica_cursor.execute(vertica_query, staging_data)



class LoadCDM(AbstractDataLoader):
    """
    Class for loading data into the common data marts layer.

    Inherits from AbstractDataLoader.
    """

    def __init__(self, postgres_conn_info: Dict[str, str], vertica_conn_info: Dict[str, str]):
        super().__init__(conn_info=vertica_conn_info)
        self.postgres_conn_info = postgres_conn_info

    def load_data(self):
        """
        Load data into the common data marts layer.

        Implement the logic to load data into the common data marts layer here.
        """
        # Connect to the Vertica database
        vertica_conn = connect(**self.vertica_conn_info)
        vertica_cursor = vertica_conn.cursor()

        # Load data into the common data marts layer
        # Implement your logic here

        # Commit and close the connection
        vertica_conn.commit()
        vertica_conn.close()
