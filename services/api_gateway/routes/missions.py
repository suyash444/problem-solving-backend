"""
Mission API Routes
Endpoints for creating and managing missions
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional

from services.mission_service.mission_creator import MissionCreator
from services.mission_service.position_generator import PositionGenerator

router = APIRouter(prefix="/missions", tags=["Missions"])

# Initialize services
mission_creator = MissionCreator()
position_generator = PositionGenerator()


# Pydantic models for request validation
class CreateMissionRequest(BaseModel):
    """Request model for creating a mission from cesta"""
    cesta: str = Field(..., description="Basket code (cesta)", example="X0005", min_length=1)


class UpdateStatusRequest(BaseModel):
    """Request model for updating mission status"""
    new_status: str = Field(..., description="New status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)", example="IN_PROGRESS")


@router.post("/from-cesta")
async def create_mission_from_cesta(request: CreateMissionRequest):
    """
    Create a new mission from a basket code (cesta)
    
    Process:
    1. Calls GetSpedito2 API to get what was shipped
    2. Compares with DumpTrack data to find missing items
    3. Creates mission with missing items
    4. Generates position checks
    
    Example request:
```json
    {
        "cesta": "X0005"
    }
```
    """
    result = mission_creator.create_mission_from_cesta(
        cesta=request.cesta,
        created_by="System"
    )
    
    if not result.get('success', False):
        raise HTTPException(status_code=400, detail=result.get('message', 'Unknown error'))
    
    return result


# MOVE /list BEFORE /{mission_id} routes!
@router.get("/list")
async def list_missions(
    status: Optional[str] = Query(None, description="Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)"),
    limit: int = Query(50, description="Maximum number of results", ge=1, le=500)
):
    """
    List all missions with optional status filter
    
    Query parameters:
    - status: Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)
    - limit: Maximum number of results (default 50, max 500)
    """
    try:
        missions = position_generator.list_all_missions(status=status, limit=limit)
        
        return {
            "success": True,
            "total": len(missions),
            "data": missions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}")
async def get_mission_details(mission_id: int):
    """
    Get complete details of a mission including items and checks
    """
    try:
        details = mission_creator.get_mission_details(mission_id)
        
        if not details:
            raise HTTPException(status_code=404, detail="Mission not found")
        
        return {
            "success": True,
            "data": details
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/route")
async def get_mission_route(mission_id: int):
    """
    Get the checking route for a mission
    Returns positions in alphabetical order with item context
    
    This is what operators use to navigate through position checks
    """
    try:
        route = position_generator.get_mission_route(mission_id)
        
        if not route:
            raise HTTPException(status_code=404, detail="Mission not found")
        
        return {
            "success": True,
            "data": route
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/next-position")
async def get_next_position(mission_id: int):
    """
    Get the next position to check for a mission
    Returns the first unchecked position in alphabetical order
    """
    try:
        next_pos = position_generator.get_next_position(mission_id)
        
        if not next_pos:
            return {
                "success": True,
                "message": "All positions have been checked",
                "data": None
            }
        
        return {
            "success": True,
            "data": next_pos
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mission_id}/summary")
async def get_mission_summary(mission_id: int):
    """
    Get summary statistics for a mission
    Includes progress, completion percentage, etc.
    """
    try:
        summary = position_generator.get_mission_summary(mission_id)
        
        if not summary:
            raise HTTPException(status_code=404, detail="Mission not found")
        
        return {
            "success": True,
            "data": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{mission_id}/status")
async def update_mission_status(
    mission_id: int,
    request: UpdateStatusRequest
):
    """
    Update mission status manually
    
    Valid statuses: OPEN, IN_PROGRESS, COMPLETED, CANCELLED
    
    Example request:
```json
    {
        "new_status": "IN_PROGRESS"
    }
```
    """
    try:
        from services.position_service.check_handler import CheckHandler
        
        check_handler = CheckHandler()
        result = check_handler.update_mission_status(mission_id, request.new_status)
        
        if not result.get('success', False):
            raise HTTPException(status_code=400, detail=result.get('message', 'Unknown error'))
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))