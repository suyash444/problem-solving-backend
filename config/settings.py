"""
Configuration settings for Problem Solving Backend
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Application settings"""

    # App Info
    APP_NAME: str = "Problem Solving Tracker API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True

    # Database Configuration (from .env)
    DB_SERVER: str
    DB_NAME: str
    DB_USERNAME: str
    DB_PASSWORD: str
    DB_DRIVER: str = "ODBC Driver 18 for SQL Server"
    DB_TRUST_CERTIFICATE: str = "yes"

    # External API Configuration
    ORDERS_API_BASE_URL: str
    BEARER_TOKEN: str

    # File Paths (CONTAINER paths)
    DUMPTRACK_PATH: str
    MONITOR_PATH: str

    # Scheduler Settings
    IMPORT_SCHEDULE_HOUR: int = 5
    IMPORT_SCHEDULE_MINUTE: int = 0

    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 9000
    CORS_ORIGINS: List[str] = ["*"]

    @property
    def database_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.DB_DRIVER}}};"
            f"SERVER={self.DB_SERVER};"
            f"DATABASE={self.DB_NAME};"
            f"UID={self.DB_USERNAME};"
            f"PWD={self.DB_PASSWORD};"
            f"Encrypt=no;"
            f"TrustServerCertificate={self.DB_TRUST_CERTIFICATE};"
        )

    @property
    def sqlalchemy_database_url(self) -> str:
        return f"mssql+pyodbc:///?odbc_connect={self.database_connection_string}"

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
