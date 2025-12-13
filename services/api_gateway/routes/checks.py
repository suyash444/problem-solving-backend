"""
Position Check API Routes
Endpoints for handling position checks (found/not-found)
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from services.position_service.check_handler import CheckHandler

router = APIRouter(prefix="/checks", tags=["Position Checks"])

# Initialize check handler
check_handler = CheckHandler()


# Pydantic models for request validation
class MarkFoundRequest(BaseModel):
    """Request model for marking position as found"""
    checked_by: str = Field(..., description="Operator who checked", example="Mario")
    qty_found: Optional[float] = Field(None, description="Quantity found (defaults to 1)", example=1)
    notes: Optional[str] = Field(None, description="Additional notes", example="Found in correct position")


class MarkNotFoundRequest(BaseModel):
    """Request model for marking position as not found"""
    checked_by: str = Field(..., description="Operator who checked", example="Mario")
    notes: Optional[str] = Field(None, description="Additional notes", example="Position checked, item not found")


class UpdateCheckRequest(BaseModel):
    """Request model for generic position check update"""
    found_in_position: bool = Field(..., description="Was item found?", example=True)
    checked_by: str = Field(..., description="Operator who checked", example="Mario")
    qty_found: Optional[float] = Field(None, description="Quantity found if applicable", example=1)
    notes: Optional[str] = Field(None, description="Additional notes", example="Found in UDC")


@router.post("/{check_id}/found")
async def mark_position_found(
    check_id: int,
    request: MarkFoundRequest
):
    """
    Mark a position check as FOUND
    
    When an item is found:
    1. Mark this check as FOUND
    2. Update mission item qty_found
    3. If all missing qty found, mark item as resolved
    4. Auto-skip remaining positions for this item if resolved
    5. Check if entire mission is complete
    
    Example request:
```json
    {
        "checked_by": "Mario",
        "qty_found": 1,
        "notes": "Found in correct position"
    }
```
    """
    result = check_handler.mark_found(
        check_id=check_id,
        checked_by=request.checked_by,
        qty_found=request.qty_found,
        notes=request.notes
    )
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['message'])
    
    return {
        "success": True,
        "message": result['message'],
        "data": {
            "item_resolved": result.get('item_resolved', False),
            "mission_complete": result.get('mission_complete', False),
            "qty_found": result.get('qty_found', 0),
            "total_found_for_item": result.get('total_found_for_item', 0),
            "qty_still_missing": result.get('qty_still_missing', 0)
        }
    }


@router.post("/{check_id}/not-found")
async def mark_position_not_found(
    check_id: int,
    request: MarkNotFoundRequest
):
    """
    Mark a position check as NOT_FOUND
    
    When an item is not found:
    1. Mark this check as NOT_FOUND
    2. Operator continues to next position
    
    Example request:
```json
    {
        "checked_by": "Mario",
        "notes": "Position checked, item not found"
    }
```
    """
    result = check_handler.mark_not_found(
        check_id=check_id,
        checked_by=request.checked_by,
        notes=request.notes
    )
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['message'])
    
    return {
        "success": True,
        "message": result['message']
    }


@router.get("/{check_id}")
async def get_check_details(check_id: int):
    """
    Get details of a specific position check
    Includes position, UDC, item details, and check status
    """
    details = check_handler.get_check_details(check_id)
    
    if not details:
        raise HTTPException(status_code=404, detail="Position check not found")
    
    return {
        "success": True,
        "data": details
    }


@router.put("/{check_id}/update")
async def update_check(
    check_id: int,
    request: UpdateCheckRequest
):
    """
    Generic update endpoint for position checks
    Can mark as either found or not-found in one call
    
    Example request:
```json
    {
        "found_in_position": true,
        "checked_by": "Mario",
        "qty_found": 1,
        "notes": "Found in UDC"
    }
```
    """
    if request.found_in_position:
        result = check_handler.mark_found(
            check_id=check_id,
            checked_by=request.checked_by,
            qty_found=request.qty_found,
            notes=request.notes
        )
    else:
        result = check_handler.mark_not_found(
            check_id=check_id,
            checked_by=request.checked_by,
            notes=request.notes
        )
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result['message'])
    
    return result