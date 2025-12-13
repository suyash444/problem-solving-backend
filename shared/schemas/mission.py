"""
Pydantic schemas for Mission-related API endpoints
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from decimal import Decimal


# ============================================================================
# MISSION SCHEMAS
# ============================================================================

class MissionCreate(BaseModel):
    """Schema for creating a new mission"""
    cesta: str = Field(..., description="Basket code")
    created_by: Optional[str] = Field(None, description="Operator who created the mission")


class MissionItemResponse(BaseModel):
    """Schema for mission item in responses"""
    id: int
    n_ordine: str
    n_lista: int
    sku: str
    qty_ordered: Decimal
    qty_shipped: Decimal
    qty_missing: Decimal
    qty_found: Decimal
    is_resolved: bool
    created_at: datetime
    resolved_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class PositionCheckResponse(BaseModel):
    """Schema for position check in responses"""
    id: int
    mission_id: int
    mission_item_id: int
    position_code: str
    udc: Optional[str]
    listone: Optional[int]
    status: str
    found_in_position: Optional[bool]
    qty_found: Optional[Decimal]
    checked_at: Optional[datetime]
    checked_by: Optional[str]
    notes: Optional[str]
    
    class Config:
        from_attributes = True


class MissionResponse(BaseModel):
    """Schema for mission in responses"""
    id: int
    mission_code: str
    cesta: str
    status: str
    created_by: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    items: Optional[List[MissionItemResponse]] = []
    checks: Optional[List[PositionCheckResponse]] = []
    
    class Config:
        from_attributes = True


class MissionSummary(BaseModel):
    """Schema for mission summary (without details)"""
    id: int
    mission_code: str
    cesta: str
    status: str
    total_missing_items: int
    resolved_items: int
    total_positions: int
    positions_pending: int
    positions_found: int
    positions_not_found: int
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# POSITION CHECK SCHEMAS
# ============================================================================

class PositionCheckUpdate(BaseModel):
    """Schema for updating position check status"""
    found_in_position: bool = Field(..., description="Was item found in this position?")
    qty_found: Optional[Decimal] = Field(None, description="Quantity found (if applicable)")
    checked_by: str = Field(..., description="Operator who checked")
    notes: Optional[str] = Field(None, description="Additional notes")


class PositionCheckDetail(BaseModel):
    """Detailed position check with item context"""
    check_id: int
    position_code: str
    udc: Optional[str]
    listone: Optional[int]
    status: str
    sku: str
    qty_missing: Decimal
    qty_found_so_far: Decimal
    found_in_position: Optional[bool]
    checked_at: Optional[datetime]
    checked_by: Optional[str]
    
    class Config:
        from_attributes = True


# ============================================================================
# MISSION ROUTE SCHEMAS
# ============================================================================

class MissionRoute(BaseModel):
    """Schema for operator's checking route"""
    mission_code: str
    cesta: str
    positions: List[PositionCheckDetail]
    current_position_index: int = 0
    total_positions: int
    
    class Config:
        from_attributes = True


# ============================================================================
# API RESPONSE WRAPPERS
# ============================================================================

class ApiResponse(BaseModel):
    """Standard API response wrapper"""
    success: bool
    message: str
    data: Optional[dict] = None


class MissionCreateResponse(ApiResponse):
    """Response after creating a mission"""
    mission_code: Optional[str] = None
    mission_id: Optional[int] = None
    total_missing_items: Optional[int] = None
    total_positions_to_check: Optional[int] = None