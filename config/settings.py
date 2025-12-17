"""
Configuration settings for Problem Solving Backend
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings"""
    
    # App Info
    APP_NAME: str = "Problem Solving Tracker API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    
    # Database Configuration
    DB_SERVER: str = "192.168.36.111,1405"
    DB_NAME: str = "ProblemSolvingTrackerDB"
    DB_USERNAME: str = "sa"
    DB_PASSWORD: str = "Sqlvaprio1"
    DB_DRIVER: str = "ODBC Driver 18 for SQL Server"

    DB_TRUST_CERTIFICATE: str = "yes"
    
    # External API Configuration
    ORDERS_API_BASE_URL: str = "https://veepee.idsistemi.it/PowerStoreAPI/api/v1"
    BEARER_TOKEN: str = "pNM86pxMWjYk2dZD7zCgemA3lH7bVSKwWCBXPvfgeybCLxlf20qGdwOteYqQ2zZxQEc2pKkR77xMjaRlTTYlWBxCrPbW0CqIvINeHSsqNllROck_h3wJDUHmSKYqvPYE56OcpoZ78iWTqjt-_0UjubV-pIf9vlLz_4Dflqc1-YWKJYGcCZ3XAMF5lG9Ox3qdK1FoS9XdgfeHvK83hQe4cC1j3tQDygMIVFC2pBRF_o2IrwhpGRmFuLfLk926ltqw-arKLTDhoFb_eCtoN8lDkrFI9u9iQzstpzvt5W8kJUkHAFC8aEDWmH6XbtcSa3qi"
    
    # File Paths
    DUMPTRACK_PATH: str = "H:/tek/MSBD/DumpTrack"
    MONITOR_PATH: str = "H:/tek/MSBD/Monitor"
    
    # Scheduler Settings
    IMPORT_SCHEDULE_HOUR: int = 5
    IMPORT_SCHEDULE_MINUTE: int = 0
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 9000
    CORS_ORIGINS: list = ["*"]
    
    @property
    def database_connection_string(self) -> str:
        """Generate SQL Server connection string"""
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
        """Generate SQLAlchemy connection URL"""
        return f"mssql+pyodbc:///?odbc_connect={self.database_connection_string}"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings()