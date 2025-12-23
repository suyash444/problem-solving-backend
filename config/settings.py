"""
Configuration settings for Problem Solving Backend
"""
from pydantic_settings import BaseSettings
from typing import List, Dict, Optional


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

    # Company-specific Bearer Tokens (from .env)
    BEARER_TOKEN_BENETTON101: str
    BEARER_TOKEN_SISLEY88: str
    BEARER_TOKEN_FASHIONTEAM108: str

    # File Paths (CONTAINER paths)
    DUMPTRACK_PATH: str
    MONITOR_PATH: str

    # Company Selection (default company)
    DEFAULT_COMPANY: str = "benetton101"

    # Company File Prefixes (DumpTrack + Monitor)
    # You can extend this dict without changing any importer logic later.
    COMPANIES: Dict[str, Dict[str, str]] = {
        "benetton101": {
            "dumptrack_prefix": "DumpTrackBenetton_",
            "monitor_prefix": "MonitorBenetton",
            "token_env_key": "BEARER_TOKEN_BENETTON101",
        },
        "sisley88": {
            "dumptrack_prefix": "DumpTrackSisley_",
            "monitor_prefix": "MonitorSisley",
            "token_env_key": "BEARER_TOKEN_SISLEY88",
        },
        "fashionteam108": {
            "dumptrack_prefix": "DumpTrackFashion team 108_",
            "monitor_prefix": "MonitorFashion team 108",
            "token_env_key": "BEARER_TOKEN_FASHIONTEAM108",
        },
    }

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

    def get_company_config(self, company: Optional[str] = None) -> Dict[str, str]:
        company_key = (company or self.DEFAULT_COMPANY).strip().lower()
        if company_key not in self.COMPANIES:
            raise ValueError(
                f"Unknown company '{company}'. Allowed: {list(self.COMPANIES.keys())}"
            )
        return self.COMPANIES[company_key]

    def get_bearer_token(self, company: Optional[str] = None) -> str:
        cfg = self.get_company_config(company)
        token_env_key = cfg.get("token_env_key")
        if not token_env_key:
            raise ValueError(f"Missing token_env_key for company: {company}")
        token = getattr(self, token_env_key, None)
        if not token:
            raise ValueError(f"Missing token value in settings for: {token_env_key}")
        return token

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
