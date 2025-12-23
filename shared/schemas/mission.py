"""
Pydantic schemas for Mission-related API endpoints (Pydantic v2)
Multi-company safe.
"""
from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal


# =============================================================================
# MISSION REQUEST SCHEMAS
# =============================================================================

class MissionCreate(BaseModel):
    """Schema for creating a new mission"""
    cesta: str = Field(..., description="Basket code")
    created_by: Optional[str] = Field(None, description="Operator who created the mission")

    # Optional but recommended: allow body-based company in addition to query param
    company: Optional[str] = Field(
        None,
        description="Company key (e.g., benetton101, sisley88, fashionteam108). If omitted, API may use default/query."
    )


# =============================================================================
# MISSION RESPONSE SCHEMAS
# =============================================================================

class MissionItemResponse(BaseModel):
    """Schema for mission item in responses"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    company: str

    n_ordine: str
    n_lista: int
    sku: str

    qty_ordered: Decimal
    qty_shipped: Decimal
    qty_missing: Decimal
    qty_found: Decimal

    is_resolved: bool
    created_at: datetime
    resolved_at: Optional[datetime] = None


class PositionCheckResponse(BaseModel):
    """Schema for position check in responses"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    company: str

    mission_id: int
    mission_item_id: int

    position_code: str
    udc: Optional[str] = None
    listone: Optional[int] = None

    status: str
    found_in_position: Optional[bool] = None
    qty_found: Optional[Decimal] = None

    checked_at: Optional[datetime] = None
    checked_by: Optional[str] = None
    notes: Optional[str] = None


class MissionResponse(BaseModel):
    """Schema for mission in responses"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    company: str

    mission_code: str
    cesta: str
    status: str

    created_by: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    items: List[MissionItemResponse] = Field(default_factory=list)
    checks: List[PositionCheckResponse] = Field(default_factory=list)


class MissionSummary(BaseModel):
    """
    Schema for mission summary.

    NOTE: aligned with PositionGenerator.get_mission_summary() output.
    Keep created_at as Optional[str] if your generator returns strings.
    """
    model_config = ConfigDict(from_attributes=True)

    mission_id: int
    company: str

    mission_code: str
    cesta: str
    status: str

    created_by: Optional[str] = None
    created_at: Optional[str] = None  # keep as str to match your service output

    total_missing_items: int
    resolved_items: int
    unresolved_items: int

    total_positions: int
    positions_pending: int
    positions_found: int
    positions_not_found: int
    positions_skipped: int

    completion_percentage: float


# =============================================================================
# POSITION CHECK REQUEST/DETAIL SCHEMAS
# =============================================================================

class PositionCheckUpdate(BaseModel):
    """Schema for updating position check status"""
    found_in_position: bool = Field(..., description="Was item found in this position?")
    qty_found: Optional[Decimal] = Field(None, description="Quantity found (if applicable)")
    checked_by: str = Field(..., description="Operator who checked")
    notes: Optional[str] = Field(None, description="Additional notes")


class PositionCheckDetail(BaseModel):
    """Detailed position check with item context"""
    model_config = ConfigDict(from_attributes=True)

    check_id: int
    position_code: str
    udc: Optional[str] = None
    listone: Optional[int] = None

    status: str
    sku: str
    qty_missing: Decimal
    qty_found_so_far: Decimal

    found_in_position: Optional[bool] = None
    checked_at: Optional[datetime] = None
    checked_by: Optional[str] = None
    cesta: Optional[str] = None


# =============================================================================
# MISSION ROUTE SCHEMA
# =============================================================================

class MissionRoute(BaseModel):
    """Schema for operator's checking route"""
    model_config = ConfigDict(from_attributes=True)

    mission_code: str
    cesta: str
    positions: List[PositionCheckDetail] = Field(default_factory=list)

    current_position_index: int = 0
    total_positions: int = 0


# =============================================================================
# API RESPONSE WRAPPERS
# =============================================================================

class ApiResponse(BaseModel):
    """Standard API response wrapper"""
    model_config = ConfigDict(from_attributes=True)

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class MissionCreateResponse(ApiResponse):
    """Response after creating a mission"""
    mission_code: Optional[str] = None
    mission_id: Optional[int] = None
    total_missing_items: Optional[int] = None
    total_positions_to_check: Optional[int] = None
