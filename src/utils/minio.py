from minio import Minio
from src.utils.logger import logger

class MinioClient:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        """Initialize Minio Client."""
        try:
            self.client = Minio(endpoint, access_key, secret_key, secure=secure)
            logger.info("Minio client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Minio client: {e}")
            raise

    def upload_file(self, bucket_name, object_name, file_path, content_type=None):
        """Upload file to Minio bucket."""
        try:
            result = self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path,
                content_type=content_type
            )
            logger.info(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )

        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise