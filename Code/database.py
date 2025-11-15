from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings
from typing import Generator
from sqlalchemy.pool import NullPool

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@"
    f"{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    poolclass=NullPool  
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class FileMetadata(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, unique=True, index=True) 
    format = Column(String, index=True)

def get_db() -> Generator:
    """Provides a database session for use in FastAPI dependencies."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Creates the necessary tables (if they don't exist) in the connected database."""
    print("Attempting to create tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully (or already exist).")