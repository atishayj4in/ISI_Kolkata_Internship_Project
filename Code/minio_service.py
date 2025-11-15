from minio import Minio
from minio.error import S3Error
from config import settings
from io import BytesIO

class MinioService:
    def __init__(self):
        try:
            self.client = Minio(
                settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE
            )
            self.bucket_name = settings.MINIO_BUCKET_NAME
            self._ensure_bucket_exists()
        except Exception as err:
            raise ConnectionError(f"Could not initialize MinIO client: {err}. Is MinIO running?")

    def _ensure_bucket_exists(self):
        """Checks if the bucket exists and creates it if it doesn't."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as err:
            print(f"FATAL: MinIO service connection error: {err}")
            raise ConnectionError("Could not connect to MinIO. Is it running?")

    def upload_file(self, filename: str, data: bytes, content_type: str):
        data_stream = BytesIO(data)
        self.client.put_object(
            self.bucket_name,
            filename,
            data_stream,
            len(data),
            content_type=content_type
        )
        return True

    def fetch_file(self, filename: str) -> BytesIO:
        """Fetches file data from MinIO and returns as BytesIO."""
        response = self.client.get_object(self.bucket_name, filename)
        data = BytesIO(response.read())
        response.close()
        response.release_conn()
        return data

try:
    minio_service = MinioService()
except ConnectionError as e:
    minio_service = None 