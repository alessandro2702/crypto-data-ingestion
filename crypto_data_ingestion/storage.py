import logging
import os
from typing import Protocol

import yaml
from minio import Minio

# Configure the logger with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Determine the absolute path to the config file
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'config.yaml')

# Load the configuration file
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)


class StorageInterface(Protocol):
    """
    Interface for data storage operations.
    """

    # Bucket Operations

    def create_bucket(self, bucket_name: str) -> None:
        pass

    def edit_bucket(self, bucket_name: str) -> None:
        pass

    def delete_bucket(self, bucket_name: str) -> None:
        pass

    # Data Operations

    def save_raw_data(
        self,
        bucket_name: str,
        object_name: str,
        data: object,
        length: int,
        content_type: str,
    ) -> None:
        pass


class LocalStorage:
    """
    Implementation of the storage interface for local storage.
    """

    def __init__(self):
        self.minio_client = Minio(
            endpoint=config['minio']['endpoint'],
            access_key=config['minio']['access_key'],
            secret_key=config['minio']['secret_key'],
            secure=False,
        )
        logger.info('Minio client initialized.')

    def create_bucket(self, bucket_name: str) -> None:
        """
        Creates a bucket on MinIO Storage.

        Parameters:
            bucket_name (str): The desired bucket name.

        Returns:
            None
        """
        found = self.minio_client.bucket_exists(bucket_name)
        if not found:
            self.minio_client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

    def save_raw_data(
        self,
        bucket_name: str,
        object_name: str,
        data: object,
        length: int,
        content_type: str,
    ) -> None:
        """
        Saves raw data to a file in MinIO, creating the bucket if it does not exist.

        Parameters:
            bucket_name (str): The name of the bucket where the data will be saved.
            object_name (str): The name of the object to be saved within the bucket (full path).
            data (object): The data to be saved, converted to a format compatible with byte storage.
            length (int): The size of the data in bytes.
            content_type (str): The content type of the file (e.g., 'application/json').

        Returns:
            None
        """
        found = self.minio_client.bucket_exists(bucket_name)
        if not found:
            logger.error(f"Bucket '{bucket_name}' does not exist.")
            raise Exception('Bucket does not exist.')
        else:
            self.minio_client.put_object(
                bucket_name, object_name, data, length, content_type
            )
            logger.info(
                f"Object '{object_name}' saved to bucket '{bucket_name}'."
            )
