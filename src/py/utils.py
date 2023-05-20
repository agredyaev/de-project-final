from abc import ABC, abstractmethod
from vertica_python import connect
import psycopg2
from typing import Optional, Dict, List, Tuple
from py.model import Currency, Transaction, GlobalMetrics


class BuildStaging:
    """
    Class to build data models from rows fetched from PostgreSQL.

    Attributes:
        rows (List[Tuple]): The rows fetched from PostgreSQL.
    """

    def __init__(self, rows: List[Tuple]):
        """
        Initialize the BuildStaging object.

        Args:
            rows (List[Tuple]): The rows fetched from PostgreSQL.
        """
        self.rows = rows

    def build_currencies(self) -> List[Currency]:
        """
        Build a list of Currency objects from the fetched rows.

        Returns:
            List[Currency]: A list of Currency objects.
        """
        currencies = []
        for row in self.rows:
            currency_data = {
                "date_update": row[0],
                "currency_code": row[1],
                "currency_code_with": row[2],
                "currency_code_div": row[3]
            }
            currency = Currency(**currency_data)
            currencies.append(currency)
        return currencies

    def build_transactions(self) -> List[Transaction]:
        """
        Build a list of Transaction objects from the fetched rows.

        Returns:
            List[Transaction]: A list of Transaction objects.
        """
        transactions = []
        for row in self.rows:
            transaction_data = {
                "operation_id": row[0],
                "account_number_from": row[1],
                "account_number_to": row[2],
                "currency_code": row[3],
                "country": row[4],
                "status": row[5],
                "transaction_type": row[6],
                "amount": row[7],
                "transaction_dt": row[8]
            }
            transaction = Transaction(**transaction_data)
            transactions.append(transaction)
        return transactions


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
    """

    def __init__(self, postgres_conn_info: Dict[str, str], vertica_conn_info: Dict[str, str]):
        super().__init__(vertica_conn_info=vertica_conn_info,
                         postgres_conn_info=postgres_conn_info)

    def load_data(self, path_to_sql_file: str, path_to_vertica_file: str, builder_method: str) -> None:
        """
        Load data into the staging layer.

        Implement the logic to load data into the staging layer here.
        """
        with psycopg2.connect(**self.postgres_conn_info) as postgres_conn:
            postgres_cursor = postgres_conn.cursor()

            with connect(**self.vertica_conn_info) as vertica_conn:
                vertica_cursor = vertica_conn.cursor()

                postgres_query = self.read_sql_file(path_to_sql_file)
                postgres_cursor.execute(postgres_query)
                postres_rows = postgres_cursor.fetchall()
                staging_model = getattr(
                    BuildStaging, builder_method)(postres_rows)

                vertica_query = self.read_sql_file(
                    path_to_vertica_file).format(**staging_model)
                vertica_cursor.executemany(vertica_query)

                vertica_conn.commit()
            postgres_conn.commit()


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
