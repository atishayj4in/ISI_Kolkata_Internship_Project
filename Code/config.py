from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_USER: str = "atishay jain"
    DB_PASSWORD: str = "temp123"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "file_service_db"
    
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "data-files"
    MINIO_SECURE: bool = False 

settings = Settings()