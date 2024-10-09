import logging
import os

import duckdb
import pandas as pd

from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv
from .storage import LocalStorage, StorageInterface

# Configure the logger with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

#Load environment variables
load_dotenv()


class DataProcessing:
    """
    A class used to process data using DuckDB and Delta Lake.

    Attributes:
        connector (duckdb.DuckDBPyConnection): A connection to an in-memory DuckDB database.
        cursor (duckdb.DuckDBPyConnection.cursor): A cursor for executing SQL queries on the DuckDB connection.
    """

    def __init__(self, storage: StorageInterface):
        """
        Initializes the DataProcessing class by creating an in-memory DuckDB connection and cursor.
        """
        self.connector = duckdb.connect(':memory:')
        self.cursor = self.connector.cursor()
        self.storage = storage
        logger.info('DuckDB in-memory database connected.')

    ## Reader

    def read_raw_data(
        self, file_extension: str, bucket_name: str, file_path: str
    ) -> pd.DataFrame:
        """
        Reads raw data from a file into a DuckDB table.

        Parameters:
            file_extension (str): The file extension of the raw data file. Supported extensions are 'csv', 'parquet', and 'json'.
            bucket_name (str): The name of the bucket where the file is stored.
            file_path (str): The path to the raw data file.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the file.

        Raises:
            ValueError: If the file extension is not supported.
        """
        logger.info(
            f'Reading raw data from {bucket_name}/{file_path} with extension {file_extension}.'
        )
        response = self.storage.get_object_data(bucket_name, file_path)

        if file_extension == 'csv':
            df = duckdb.read_csv(response).df()
        elif file_extension == 'parquet':
            df = duckdb.read_parquet(response).df()
        elif file_extension == 'json':
            df = duckdb.read_json(response).df()
        else:
            logger.error('Invalid file extension provided.')
            raise ValueError(
                'Invalid file extension. Please provide a valid file extension. Supported file extensions are: csv, parquet, json'
            )

        logger.info(f'Data read successfully from {bucket_name}/{file_path}.')
        return df

    def read_delta_table(self, table_path: str) -> pd.DataFrame:
        """
        Reads a Delta table into a Pandas DataFrame.

        Parameters:
            table_path (str): The path to the Delta table.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the Delta table.
        """
        logger.info(f'Reading Delta table from {table_path}.')
        df = DeltaTable(table_path).to_pandas()
        logger.info(f'Delta table read successfully from {table_path}.')
        return df

    ## Writer

    def save_delta_table(
        self,
        table_path: str,
        write_mode: str = 'overwrite',
        schema_mode: str = 'overwrite',
        data: pd.DataFrame = pd.DataFrame(),
    ):
        """
        Saves a Pandas DataFrame to a Delta table.

        Parameters:
            table_path (str): The path to the Delta table.
            write_mode (str, optional): The write mode for the Delta table. Options are: 'overwrite' or 'append'. Default is 'overwrite'.
            schema_mode (str, optional): The schema mode for the Delta table. Options are: 'merge' or 'overwrite'. Default is 'overwrite'.
            data (pd.DataFrame): The DataFrame to be saved to the Delta table.
        """
        logger.info(
            f'Saving DataFrame to Delta table at {table_path} with write mode {write_mode} and schema mode {schema_mode}.'
        )
        write_deltalake(
            table_path, data, mode=write_mode, schema_mode=schema_mode
        )
        logger.info(
            f'DataFrame saved to Delta table at {table_path} successfully.'
        )

    def register_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Registers a Pandas DataFrame as a table in DuckDB.

        Parameters:
            df (pd.DataFrame): The DataFrame to be registered.
            table_name (str): The name of the table in DuckDB.
        """
        self.connector.register('temp_df', df)
        self.connector.execute(
            f'CREATE TABLE {table_name} AS SELECT * FROM temp_df'
        )
        self.connector.unregister('temp_df')
        logger.info(f"DataFrame registered as table '{table_name}' in DuckDB.")

    ## Analytical

    def run_sql_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query on the DuckDB connection.

        Parameters:
            query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: A DataFrame containing the results of the SQL query.
        """
        logger.info(f'Executing SQL query: {query}')
        df = self.cursor.execute(query).fetchdf()
        logger.info('SQL query executed successfully.')
        return df
