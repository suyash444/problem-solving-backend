"""
Import API Routes
Endpoints for importing data from DumpTrack, Monitor, and APIs
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import date
from typing import Optional

from config.settings import settings
from services.ingestion_service.dumptrack_importer import DumptrackImporter
from services.ingestion_service.monitor_importer import MonitorImporter
from services.ingestion_service.api_client import PowerStoreAPIClient
from services.ingestion_service.rebuild_udc_inventory import rebuild_udc_inventory

router = APIRouter(prefix="/imports", tags=["Imports"])

# Initialize importers
dumptrack_importer = DumptrackImporter()
monitor_importer = MonitorImporter()
api_client = PowerStoreAPIClient()


@router.post("/dumptrack/auto")
async def import_dumptrack_auto(
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)")
):
    """
    Automatic DumpTrack import - finds and imports latest file
    Runs daily at 5:00 AM via scheduler
    """
    result = dumptrack_importer.import_latest(company=company)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.post("/dumptrack/manual")
async def import_dumptrack_manual(
    start_date: str = Query(..., description="Start date (e.g., yyyy-mm-dd)"),
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)"),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Manual DumpTrack import with date range
    User must specify start and end date

    Example: 2025-11-21 to 2025-12-11
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

        result = dumptrack_importer.import_date_range(start, end, company=company)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["message"])

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
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)"),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Manual PrelievoPowerSort API import
    User must specify start and end date range

    Example: 2025-11-21 to 2025-12-11
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

        result = api_client.call_prelievo_powersort(
            start_date=start,
            end_date=end,
            company=company,
        )

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["message"])

        # IMPORTANT: rebuild UDC inventory after picking events import
        rebuild_result = rebuild_udc_inventory(company=company)

        result["udc_inventory_rebuilt"] = rebuild_result.get("success", False)
        result["udc_inventory_records"] = rebuild_result.get("records_created", 0)
        if not rebuild_result.get("success"):
            result["udc_inventory_error"] = rebuild_result.get("error")

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.post("/monitor/auto")
async def import_monitor_auto(
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)")
):
    """
    Automatic Monitor import - imports yesterday's file
    Runs daily at 5:00 AM via scheduler
    """
    result = monitor_importer.import_yesterday(company=company)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.post("/monitor/manual")
async def import_monitor_manual(
    start_date: str = Query(..., description="Start date (e.g., yyyy-mm-dd)"),
    end_date: str = Query(..., description="End date (e.g., yyyy-mm-dd)"),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Manual Monitor import with date range
    User must specify start and end date

    Example: 2025-11-21 to 2025-12-11
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")

        result = monitor_importer.import_date_range(start, end, company=company)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["message"])

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@router.get("/status")
async def get_import_status(
    company: Optional[str] = Query(None, description="Optional company key to filter status")
):
    """
    Get import status and statistics.
    If company is provided, returns per-company status. Otherwise returns global.
    """
    try:
        from shared.database import get_db_context
        from shared.database.models import ImportLog
        from sqlalchemy import func

        with get_db_context() as db:
            q = db.query(ImportLog)
            if company:
                q = q.filter(ImportLog.company == company)

            total_imports = q.with_entities(func.count(ImportLog.id)).scalar()

            successful_imports = q.filter(ImportLog.status == "SUCCESS").with_entities(func.count(ImportLog.id)).scalar()
            failed_imports = q.filter(ImportLog.status == "FAILED").with_entities(func.count(ImportLog.id)).scalar()

            def latest_for(source_type: str):
                qq = db.query(ImportLog).filter(
                    ImportLog.source_type == source_type,
                    ImportLog.status == "SUCCESS",
                )
                if company:
                    qq = qq.filter(ImportLog.company == company)
                return qq.order_by(ImportLog.import_completed_at.desc()).first()

            dumptrack_latest = latest_for("DUMPTRACK")
            monitor_latest = latest_for("MONITOR")
            prelievo_latest = latest_for("PRELIEVO")

            return {
                "success": True,
                "company": company,
                "total_imports": total_imports,
                "successful_imports": successful_imports,
                "failed_imports": failed_imports,
                "latest_imports": {
                    "dumptrack": {
                        "last_import": str(dumptrack_latest.import_completed_at) if dumptrack_latest else None,
                        "records_imported": dumptrack_latest.records_imported if dumptrack_latest else 0,
                        "file_date": str(dumptrack_latest.file_date) if dumptrack_latest and dumptrack_latest.file_date else None,
                    },
                    "monitor": {
                        "last_import": str(monitor_latest.import_completed_at) if monitor_latest else None,
                        "records_imported": monitor_latest.records_imported if monitor_latest else 0,
                        "file_date": str(monitor_latest.file_date) if monitor_latest and monitor_latest.file_date else None,
                    },
                    "prelievo": {
                        "last_import": str(prelievo_latest.import_completed_at) if prelievo_latest else None,
                        "records_imported": prelievo_latest.records_imported if prelievo_latest else 0,
                        "file_date": str(prelievo_latest.file_date) if prelievo_latest and prelievo_latest.file_date else None,
                    },
                },
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")
