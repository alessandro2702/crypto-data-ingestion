import logging
import os
from io import BytesIO
from typing import Protocol

from dotenv import load_dotenv
from minio import Minio

# Configure the logger with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

#Load environment variables
load_dotenv()

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

    def get_object_data(self, bucket_name: str, object_name: str) -> object:
        pass


class LocalStorage:
    """
    Implementation of the storage interface for local storage.
    """

    def __init__(self):
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
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
        Saves a raw data to specified bucket in MinIO Storage.

        Parameters:
            bucket_name (str): The name of the bucket where the data will be saved.
            object_name (str): The name of the object to be saved within the bucket (full path).
            data (object): The data to be saved, converted to a format compatible with BinaryIO.
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

    def get_object_data(self, bucket_name: str, object_name: str) -> BytesIO:
        """
        Retrieves an object from a specified bucket in MinIO Storage.

        Parameters:
            bucket_name (str): The name of the bucket where the object is stored.
            object_name (str): The name of the object to be retrieved within the bucket.

        Returns:
            BytesIO: The object data as a file-like object.
        """
        found = self.minio_client.bucket_exists(bucket_name)
        if not found:
            logger.error(f"Bucket '{bucket_name}' does not exist.")
            raise Exception('Bucket does not exist.')
        else:
            logger.info('Retrieving object from MinIO Storage.')
            data = self.minio_client.get_object(bucket_name, object_name)
            try:
                # Read the data and convert it to a file-like object
                file_data = data.read()
                file_converted = BytesIO(file_data)
                return file_converted
            finally:
                data.close()
                data.release_conn()
