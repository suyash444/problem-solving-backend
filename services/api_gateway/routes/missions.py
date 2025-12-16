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
# SINGLE CESTA ENDPOINTS (Existing)
# ============================================
@router.post("/from-cesta")
async def create_mission_from_cesta(
    cesta: str = Query(..., description="Scan or type basket code like example: X0005")
):
    """
    Create a new mission from a SINGLE basket code (cesta)
    
    **How to use:**
    - **Scan** the barcode directly into the cesta field, OR
    - **Type** the cesta code manually
    
    Process:
    1. Calls GetSpedito2 API to get what was shipped
    2. Compares with DumpTrack data to find missing items
    3. Creates mission with missing items
    4. Generates position checks
    
    Example: X0005, X0052, X0103
    """
    result = mission_creator.create_mission_from_cesta(
        cesta=cesta.strip().upper(),
        created_by="System"
    )
    
    if not result.get('success', False):
        raise HTTPException(status_code=400, detail=result.get('message', 'Unknown error'))
    
    return result


# ============================================
# BATCH MISSION ENDPOINTS (NEW!)
# ============================================
@router.post("/check-cesta")
async def check_cesta_for_missing(
    cesta: str = Query(..., description="Cesta code to check for missing items")
):
    """
    Check a cesta for missing items WITHOUT creating a mission.
    
    **Use for batch mode:**
    1. Scan cestas one by one using this endpoint
    2. See how many items are missing in each
    3. Build a list of cestas to include
    4. Call /create-batch to create ONE mission with all cestas
    
    Returns:
    - success: bool
    - cesta: str
    - missing_count: int
    - missing_items: list with details
    
    Example: X0399
    """
    result = mission_creator.check_cesta_missing_items(
        cesta=cesta.strip().upper()
    )
    
    return result


@router.post("/create-batch")
async def create_batch_mission(request: BatchMissionRequest):
    """
    Create ONE mission from MULTIPLE cestas.
    
    **How to use:**
    1. First, check each cesta using POST /check-cesta
    2. Build a list of cestas that have missing items
    3. Send all cestas to this endpoint
    4. Get ONE mission with all items combined!
    
    **Features:**
    - Combines items with same SKU + Listone
    - Sorts positions alphabetically for optimal walking route
    - Tracks which cesta each item came from
    - Skips cestas with no missing items
    
    **Request body:**
    ```json
    {
        "cestas": ["X0399", "X0239", "X0108", "X0146"]
    }
    ```
    
    **Response includes:**
    - mission_id, mission_code
    - cestas_processed: total cestas checked
    - cestas_with_missing: cestas that had missing items
    - cestas_skipped: cestas with no missing items
    - cestas_errors: cestas that had errors
    - total_missing_items: combined count
    - position_checks_created: number of positions to check
    """
    if not request.cestas or len(request.cestas) == 0:
        raise HTTPException(status_code=400, detail="No cestas provided")
    
    result = mission_creator.create_batch_mission(
        cestas=request.cestas,
        created_by="System"
    )
    
    if not result.get('success', False):
        raise HTTPException(status_code=400, detail=result.get('message', 'Unknown error'))
    
    return result


# ============================================
# LIST MISSIONS (Must be before /{mission_id})
# ============================================
@router.get("/list")
async def list_missions(
    status: Optional[str] = Query(None, description="Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED, HAS_NOT_FOUND)"),
    limit: int = Query(50, description="Maximum number of results", ge=1, le=500)
):
    """
    List all missions with optional status filter
    
    Query parameters:
    - status: Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED, HAS_NOT_FOUND)
    - limit: Maximum number of results (default 50, max 500)
    
    HAS_NOT_FOUND: Shows missions that have items marked as NOT_FOUND
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


# ============================================
# MISSION DETAILS ENDPOINTS
# ============================================
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
