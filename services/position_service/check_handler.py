"""
Check Handler
Handles position check updates and auto-resolution logic
"""
from datetime import datetime
from typing import Dict, Optional
from decimal import Decimal
from loguru import logger

from shared.database import get_db_context
from shared.database.models import PositionCheck, MissionItem, Mission


class CheckHandler:
    """Handles position check operations"""
    
    def mark_found(
        self, 
        check_id: int,
        checked_by: str,
        qty_found: Optional[float] = None,
        notes: Optional[str] = None
    ) -> Dict:
        """
        Mark a position check as FOUND
        
        Logic:
        1. Mark this check as FOUND
        2. Update mission item qty_found
        3. If all missing qty found, mark item as resolved
        4. Auto-skip remaining positions for this item if resolved
        5. Check if entire mission is complete
        
        Args:
            check_id: Position check ID
            checked_by: Operator who checked
            qty_found: Quantity found (optional, defaults to 1)
            notes: Additional notes
            
        Returns:
            Dict with result and updated statistics
        """
        try:
            with get_db_context() as db:
                # Get the check
                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id
                ).first()
                
                if not check:
                    return {
                        "success": False,
                        "message": "Position check not found"
                    }
                
                if check.status != 'PENDING':  # ← CHANGED FROM TO_CHECK
                    return {
                        "success": False,
                        "message": f"Position already checked (status: {check.status})"
                    }
                
                # Get mission item
                mission_item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id
                ).first()
                
                if not mission_item:
                    return {
                        "success": False,
                        "message": "Mission item not found"
                    }
                
                # Default qty_found to 1 if not specified
                if qty_found is None:
                    qty_found = 1.0
                
                # Update check
                check.status = 'FOUND'
                check.found_in_position = True
                check.qty_found = Decimal(str(qty_found))
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes
                
                # Update mission item
                mission_item.qty_found = (mission_item.qty_found or Decimal('0')) + Decimal(str(qty_found))
                
                # Check if item is now resolved
                item_resolved = False
                if mission_item.qty_found >= mission_item.qty_missing:
                    mission_item.is_resolved = True
                    mission_item.resolved_at = datetime.utcnow()
                    item_resolved = True
                    
                    # Auto-skip all other PENDING positions for this item
                    skipped = self._auto_skip_remaining_positions(
                        db, 
                        check.mission_id,
                        check.mission_item_id
                    )
                    logger.info(f"✓ Item fully found! Auto-skipped {skipped} remaining positions")
                
                db.commit()
                
                # Check if entire mission is complete
                mission_complete = self._check_mission_completion(db, check.mission_id)
                
                return {
                    "success": True,
                    "message": "Position marked as FOUND",
                    "item_resolved": item_resolved,
                    "mission_complete": mission_complete,
                    "qty_found": float(qty_found),
                    "total_found_for_item": float(mission_item.qty_found),
                    "qty_still_missing": float(max(mission_item.qty_missing - mission_item.qty_found, 0))
                }
                
        except Exception as e:
            logger.error(f"Error marking position as found: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def mark_not_found(
        self,
        check_id: int,
        checked_by: str,
        notes: Optional[str] = None
    ) -> Dict:
        """
        Mark a position check as NOT_FOUND
        
        Args:
            check_id: Position check ID
            checked_by: Operator who checked
            notes: Additional notes
            
        Returns:
            Dict with result
        """
        try:
            with get_db_context() as db:
                # Get the check
                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id
                ).first()
                
                if not check:
                    return {
                        "success": False,
                        "message": "Position check not found"
                    }
                
                if check.status != 'PENDING':  # ← CHANGED FROM TO_CHECK
                    return {
                        "success": False,
                        "message": f"Position already checked (status: {check.status})"
                    }
                
                # Update check
                check.status = 'NOT_FOUND'
                check.found_in_position = False
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes
                
                db.commit()
                
                return {
                    "success": True,
                    "message": "Position marked as NOT_FOUND"
                }
                
        except Exception as e:
            logger.error(f"Error marking position as not found: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def _auto_skip_remaining_positions(
        self, 
        db, 
        mission_id: int,
        mission_item_id: int
    ) -> int:
        """
        Auto-skip all remaining PENDING positions for a resolved item
        
        Returns:
            Number of positions skipped
        """
        remaining_checks = db.query(PositionCheck).filter(
            PositionCheck.mission_id == mission_id,
            PositionCheck.mission_item_id == mission_item_id,
            PositionCheck.status == 'PENDING'  # ← CHANGED FROM TO_CHECK
        ).all()
        
        for check in remaining_checks:
            check.status = 'SKIPPED_AUTO'
            check.notes = 'Auto-skipped: All missing items found'
            check.checked_at = datetime.utcnow()
        
        return len(remaining_checks)
    
    def _check_mission_completion(self, db, mission_id: int) -> bool:
        """
        Check if all items in a mission are resolved
        If yes, mark mission as COMPLETED
        
        Returns:
            True if mission is now complete
        """
        mission = db.query(Mission).filter(
            Mission.id == mission_id
        ).first()
        
        if not mission:
            return False
        
        # Get all mission items and check if all resolved
        all_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission_id
        ).all()
        
        if not all_items:
            return False
        
        all_resolved = all(item.is_resolved for item in all_items)
        
        if all_resolved and mission.status != 'COMPLETED':
            mission.status = 'COMPLETED'
            mission.completed_at = datetime.utcnow()
            logger.info(f"✓✓✓ Mission {mission.mission_code} marked as COMPLETED!")
            return True
        elif mission.status == 'OPEN':
            mission.status = 'IN_PROGRESS'
            mission.started_at = datetime.utcnow()
        
        return mission.status == 'COMPLETED'
    
    def update_mission_status(
        self,
        mission_id: int,
        new_status: str
    ) -> Dict:
        """
        Update mission status manually
        
        Args:
            mission_id: Mission ID
            new_status: New status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)
            
        Returns:
            Dict with result
        """
        try:
            valid_statuses = ['OPEN', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED']
            
            if new_status not in valid_statuses:
                return {
                    "success": False,
                    "message": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
                }
            
            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.id == mission_id
                ).first()
                
                if not mission:
                    return {
                        "success": False,
                        "message": "Mission not found"
                    }
                
                old_status = mission.status
                mission.status = new_status
                
                if new_status == 'IN_PROGRESS' and not mission.started_at:
                    mission.started_at = datetime.utcnow()
                elif new_status == 'COMPLETED' and not mission.completed_at:
                    mission.completed_at = datetime.utcnow()
                
                db.commit()
                
                return {
                    "success": True,
                    "message": f"Mission status updated from {old_status} to {new_status}",
                    "mission_code": mission.mission_code,
                    "new_status": new_status
                }
                
        except Exception as e:
            logger.error(f"Error updating mission status: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def get_check_details(self, check_id: int) -> Optional[Dict]:
        """Get details of a specific position check"""
        try:
            with get_db_context() as db:
                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id
                ).first()
                
                if not check:
                    return None
                
                mission_item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id
                ).first()
                
                return {
                    'check_id': check.id,
                    'position_code': check.position_code,
                    'udc': check.udc,
                    'listone': check.listone,
                    'status': check.status,
                    'found_in_position': check.found_in_position,
                    'qty_found': float(check.qty_found) if check.qty_found else None,
                    'checked_at': str(check.checked_at) if check.checked_at else None,
                    'checked_by': check.checked_by,
                    'notes': check.notes,
                    'sku': mission_item.sku if mission_item else None,
                    'n_ordine': mission_item.n_ordine if mission_item else None,
                    'n_lista': mission_item.n_lista if mission_item else None
                }
                
        except Exception as e:
            logger.error(f"Error getting check details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None