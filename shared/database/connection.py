"""
Database connection and session management
"""
import pyodbc
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from typing import Generator
from loguru import logger
from config.settings import settings

# SQLAlchemy Base
Base = declarative_base()

# Default company 
DEFAULT_COMPANY = settings.DEFAULT_COMPANY.strip().lower()


# Create SQLAlchemy engine
engine = create_engine(
    settings.sqlalchemy_database_url,
    echo=settings.DEBUG,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ---------------------------------------------------------------------------
# Multi-company safety net:
# If a new ORM object has a "company" column and it was not set,
# automatically set it to DEFAULT_COMPANY so existing flows do not break.
# ---------------------------------------------------------------------------
@event.listens_for(Session, "before_flush")
def _set_default_company_before_flush(session: Session, flush_context, instances):
    def norm(val: str) -> str:
        return val.strip().lower()

    # new objects: ensure company is set
    for obj in session.new:
        if hasattr(obj, "company"):
            val = getattr(obj, "company", None)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                setattr(obj, "company", DEFAULT_COMPANY)
            elif isinstance(val, str):
                setattr(obj, "company", norm(val))

    # optional: normalize company on updated objects too
    for obj in session.dirty:
        if hasattr(obj, "company"):
            val = getattr(obj, "company", None)
            if isinstance(val, str) and val.strip():
                setattr(obj, "company", norm(val))


def get_pyodbc_connection() -> pyodbc.Connection:
    """
    Get a raw pyodbc connection for bulk operations
    """
    try:
        conn = pyodbc.connect(settings.database_connection_string)
        logger.info("PyODBC connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to create PyODBC connection: {e}")
        raise


def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI to get database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """
    Context manager for database session
    Usage:
        with get_db_context() as db:
            # do something with db
            db.commit()  # User must commit explicitly
    """
    db = SessionLocal()
    try:
        yield db
        # DON'T auto-commit here - let the caller decide when to commit
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()


def test_connection() -> bool:
    """
    Test database connection
    """
    try:
        with get_pyodbc_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result and result[0] == 1:
                logger.info("Database connection test successful")
                return True
        return False
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def init_db():
    """
    Initialize database (create tables if needed)
    Note: Tables are already created via SQL script
    """
    try:
        # Just test the connection
        if test_connection():
            logger.info("Database initialized successfully")
        else:
            logger.error("Database initialization failed")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
