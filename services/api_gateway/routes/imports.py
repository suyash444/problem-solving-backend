"""
Import API Routes
Endpoints for importing data from DumpTrack, Monitor, and APIs
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import date
from typing import Optional

from services.ingestion_service.dumptrack_importer import DumptrackImporter
from services.ingestion_service.monitor_importer import MonitorImporter
from services.ingestion_service.api_client import PowerStoreAPIClient

router = APIRouter(prefix="/imports", tags=["Imports"])

# Initialize importers
dumptrack_importer = DumptrackImporter()
monitor_importer = MonitorImporter()
api_client = PowerStoreAPIClient()


@router.post("/dumptrack/auto")
async def import_dumptrack_auto():
    """
    Automatic DumpTrack import - finds and imports latest file
    Runs daily at 5:30 AM via scheduler
    Automatically rebuilds UDC inventory after import
    """
    result = dumptrack_importer.import_latest()
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['message'])
    
    return result


@router.post("/dumptrack/manual")
async def import_dumptrack_manual(
    start_date: str = Query(..., description="Start date (e.g., yyyy-mm-dd)"),
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)")
):
    """
    Manual DumpTrack import with date range
    User must specify start and end date
    Automatically rebuilds UDC inventory after import
    
    Example: 2025-11-21 to 2025-12-11
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        
        if start > end:
            raise HTTPException(
                status_code=400, 
                detail="start_date must be before or equal to end_date"
            )
        
        # Import with date range
        result = dumptrack_importer.import_date_range(start, end)
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['message'])
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.post("/prelievo/manual")
async def import_prelievo_manual(
    start_date: str = Query(..., description="Start date (e.g., yyyy-mm-dd)"),
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)")
):
    """
    Manual PrelievoPowerSort API import
    User must specify start and end date range
    
    Example: 2025-11-21 to 2025-12-11
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        
        if start > end:
            raise HTTPException(
                status_code=400, 
                detail="start_date must be before or equal to end_date"
            )
        
        # Import from API
        result = api_client.call_prelievo_powersort(
            start_date=start,
            end_date=end
        )
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['message'])
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.post("/monitor/auto")
async def import_monitor_auto():
    """
    Automatic Monitor import - imports yesterday's file
    Runs daily at 5:30 AM via scheduler
    """
    result = monitor_importer.import_yesterday()
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['message'])
    
    return result


@router.post("/monitor/manual")
async def import_monitor_manual(
    start_date: str = Query(..., description="Start date (e.g., yyyy-mm-dd)"),
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)")
):
    """
    Manual Monitor import with date range
    User must specify start and end date
    
    Example: 2025-11-21 to 2025-12-11
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        
        if start > end:
            raise HTTPException(
                status_code=400, 
                detail="start_date must be before or equal to end_date"
            )
        
        # Import with date range
        result = monitor_importer.import_date_range(start, end)
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['message'])
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.get("/status")
async def get_import_status():
    """
    Get import status and statistics
    Shows last import times and record counts for all sources
    """
    try:
        from shared.database import get_db_context
        from shared.database.models import ImportLog
        from sqlalchemy import func
        
        with get_db_context() as db:
            # Get latest imports for each source
            dumptrack_latest = db.query(ImportLog).filter(
                ImportLog.source_type == 'DUMPTRACK',
                ImportLog.status == 'SUCCESS'
            ).order_by(ImportLog.import_completed_at.desc()).first()
            
            monitor_latest = db.query(ImportLog).filter(
                ImportLog.source_type == 'MONITOR',
                ImportLog.status == 'SUCCESS'
            ).order_by(ImportLog.import_completed_at.desc()).first()
            
            prelievo_latest = db.query(ImportLog).filter(
                ImportLog.source_type == 'PRELIEVO',
                ImportLog.status == 'SUCCESS'
            ).order_by(ImportLog.import_completed_at.desc()).first()
            
            # Get total counts
            total_imports = db.query(func.count(ImportLog.id)).scalar()
            successful_imports = db.query(func.count(ImportLog.id)).filter(
                ImportLog.status == 'SUCCESS'
            ).scalar()
            failed_imports = db.query(func.count(ImportLog.id)).filter(
                ImportLog.status == 'FAILED'
            ).scalar()
            
            return {
                "success": True,
                "total_imports": total_imports,
                "successful_imports": successful_imports,
                "failed_imports": failed_imports,
                "latest_imports": {
                    "dumptrack": {
                        "last_import": str(dumptrack_latest.import_completed_at) if dumptrack_latest else None,
                        "records_imported": dumptrack_latest.records_imported if dumptrack_latest else 0,
                        "file_date": str(dumptrack_latest.file_date) if dumptrack_latest and dumptrack_latest.file_date else None
                    },
                    "monitor": {
                        "last_import": str(monitor_latest.import_completed_at) if monitor_latest else None,
                        "records_imported": monitor_latest.records_imported if monitor_latest else 0,
                        "file_date": str(monitor_latest.file_date) if monitor_latest and monitor_latest.file_date else None
                    },
                    "prelievo": {
                        "last_import": str(prelievo_latest.import_completed_at) if prelievo_latest else None,
                        "records_imported": prelievo_latest.records_imported if prelievo_latest else 0,
                        "file_date": str(prelievo_latest.file_date) if prelievo_latest and prelievo_latest.file_date else None
                    }
                }
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")