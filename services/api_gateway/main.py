"""
Main FastAPI Application
API Gateway for Problem Solving Tracker
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from loguru import logger

from config.settings import settings
from shared.database import init_db, test_connection
from services.ingestion_service.scheduler import ImportScheduler

# Import routers
from .routes import imports, missions, checks

# Initialize scheduler
scheduler = ImportScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan events - startup and shutdown
    """
    # Startup
    logger.info("Starting Problem Solving Tracker API...")
    
    # Test database connection
    if test_connection():
        logger.info("Database connection successful")
        init_db()
    else:
        logger.error("Database connection failed!")
    
    # Start scheduler for automatic imports
    scheduler.start()
    logger.info("Import scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Problem Solving Tracker API...")
    scheduler.stop()
    logger.info("Import scheduler stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API for tracking and resolving missing items in warehouse problem-solving baskets",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(imports.router, prefix="/api")
app.include_router(missions.router, prefix="/api")
app.include_router(checks.router, prefix="/api")


@app.get("/")
async def root():
    """Root endpoint - API info"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_status = test_connection()
    
    return {
        "status": "healthy" if db_status else "unhealthy",
        "database": "connected" if db_status else "disconnected",
        "scheduler": "running" if scheduler.scheduler.running else "stopped"
    }


@app.post("/scheduler/trigger")
async def trigger_scheduler():
    """
    Manually trigger the daily import job (for testing)
    Normally runs automatically at 5:00 AM
    """
    try:
        scheduler.run_now()
        return {
            "success": True,
            "message": "Daily import job triggered successfully"
        }
    except Exception as e:
        logger.error(f"Error triggering scheduler: {e}")
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting server on {settings.API_HOST}:{settings.API_PORT}")
    
    uvicorn.run(
        "services.api_gateway.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )