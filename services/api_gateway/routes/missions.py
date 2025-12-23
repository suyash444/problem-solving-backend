"""
Mission API Routes - VERSION v2 with BATCH SUPPORT
Endpoints for creating and managing missions

NEW ENDPOINTS:
- POST /missions/check-cesta - Check single cesta for missing items (preview)
- POST /missions/create-batch - Create ONE mission from MULTIPLE cestas
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List

from config.settings import settings
from services.mission_service.mission_creator import MissionCreator
from services.mission_service.position_generator import PositionGenerator

router = APIRouter(prefix="/missions", tags=["Missions"])

# Initialize services
mission_creator = MissionCreator()
position_generator = PositionGenerator()


# ============================================
# Pydantic models for request validation
# ============================================
class UpdateStatusRequest(BaseModel):
    """Request model for updating mission status"""
    new_status: str = Field(..., description="New status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)", example="IN_PROGRESS")


class BatchMissionRequest(BaseModel):
    """Request model for creating batch mission from multiple cestas"""
    cestas: List[str] = Field(
        ...,
        description="List of cesta codes to include in mission",
        example=["X0399", "X0239", "X0108"],
        min_items=1
    )


# ============================================
# SINGLE CESTA ENDPOINTS
# ============================================
@router.post("/from-cesta")
async def create_mission_from_cesta(
    cesta: str = Query(..., description="Scan or type basket code like example: X0005"),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Create a new mission from a SINGLE basket code (cesta)
    """
    result = mission_creator.create_mission_from_cesta(
        company=company,
        cesta=cesta.strip().upper(),
        created_by="System"
    )

    if not result.get("success", False):
        raise HTTPException(status_code=400, detail=result.get("message", "Unknown error"))

    return result


# ============================================
# BATCH MISSION ENDPOINTS
# ============================================
@router.post("/check-cesta")
async def check_cesta_for_missing(
    cesta: str = Query(..., description="Cesta code to check for missing items"),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Check a cesta for missing items WITHOUT creating a mission.
    """
    result = mission_creator.check_cesta_missing_items(
        company=company,
        cesta=cesta.strip().upper()
    )

    return result


@router.post("/create-batch")
async def create_batch_mission(
    request: BatchMissionRequest,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Create ONE mission from MULTIPLE cestas.
    """
    if not request.cestas or len(request.cestas) == 0:
        raise HTTPException(status_code=400, detail="No cestas provided")

    result = mission_creator.create_batch_mission(
        company=company,
        cestas=request.cestas,
        created_by="System"
    )

    if not result.get("success", False):
        raise HTTPException(status_code=400, detail=result.get("message", "Unknown error"))

    return result


# ============================================
# LIST MISSIONS
# ============================================
@router.get("/list")
async def list_missions(
    status: Optional[str] = Query(None, description="Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED, HAS_NOT_FOUND)"),
    limit: int = Query(50, description="Maximum number of results", ge=1, le=500),
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    List all missions with optional status filter
    """
    try:
        missions = position_generator.list_all_missions(company=company, status=status, limit=limit)

        return {
            "success": True,
            "company": company,
            "total": len(missions),
            "data": missions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# MISSION DETAILS ENDPOINTS
# ============================================
@router.get("/{mission_id}")
async def get_mission_details(
    mission_id: int,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Get complete details of a mission including items and checks
    """
    try:
        details = mission_creator.get_mission_details(company=company, mission_id=mission_id)

        if not details:
            raise HTTPException(status_code=404, detail="Mission not found")

        return {"success": True, "data": details}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/route")
async def get_mission_route(
    mission_id: int,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Get the checking route for a mission
    """
    try:
        route = position_generator.get_mission_route(company=company, mission_id=mission_id)

        if not route:
            raise HTTPException(status_code=404, detail="Mission not found")

        return {"success": True, "data": route}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/next-position")
async def get_next_position(
    mission_id: int,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Get the next position to check for a mission
    """
    try:
        next_pos = position_generator.get_next_position(company=company, mission_id=mission_id)

        if not next_pos:
            return {"success": True, "message": "All positions have been checked", "data": None}

        return {"success": True, "data": next_pos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/summary")
async def get_mission_summary(
    mission_id: int,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Get summary statistics for a mission
    """
    try:
        summary = position_generator.get_mission_summary(company=company, mission_id=mission_id)

        if not summary:
            raise HTTPException(status_code=404, detail="Mission not found")

        return {"success": True, "data": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{mission_id}/status")
async def update_mission_status(
    mission_id: int,
    request: UpdateStatusRequest,
    company: Optional[str] = Query(None,description="Company key (e.g., benetton101, sisley88, fashionteam108)"),
):
    """
    Update mission status manually
    """
    try:
        from services.position_service.check_handler import CheckHandler

        check_handler = CheckHandler()

        # FIX: CheckHandler.update_mission_status does not accept `company`
        result = check_handler.update_mission_status(company=company, mission_id=mission_id, new_status=request.new_status)


        if not result.get("success", False):
            raise HTTPException(status_code=400, detail=result.get("message", "Unknown error"))

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
