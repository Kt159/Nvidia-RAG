from minio import Minio
from config import MinioDb

class MinIOClient:
    """
    A client interface for interacting with MinIO storage

    This class provides methods to initialize a connection to MinIO and 
    download files from a specified bucket.
    """

    def __init__(self):
        self.minio_client = self._initialize_client()

    def _initialize_client(self):
        """
        Initialize and return a MinIO client instance.

        This method retrieves connection details such as the endpoint, access key,
        secret key, and security settings from the MinioDb configuration. It then
        uses these credentials to create and return a MinIO client instance.

        Returns:
            Minio: An instance of the MinIO client used to interact with the server.
        """
        minio_client = Minio(
            endpoint=MinioDb.endpoint,
            access_key=MinioDb.access_key,
            secret_key=MinioDb.secret_key,
        )
        return minio_client

    def download_file(self, bucket_name, file_name, file_path):
        """
        Download a file from a specified MinIO bucket.

        This method attempts to download a file from the specified bucket and 
        saves it to the provided file path on the local system. If the download 
        fails, an exception is raised.

        Args:
            bucket_name (str): The name of the bucket in MinIO.
            file_name (str): The name of the file to download from the bucket.
            file_path (str): The path where the file will be saved locally.

        Returns:
            bool: True if the file download is successful.

        Raises:
            RuntimeError: If the file download fails, the error message is raised.
        """
        try:
            self.minio_client.fget_object(bucket_name, file_name, file_path)
            return True
        except Exception as e:
            raise RuntimeError(f"Failed to download file from MinIO: {str(e)}")
