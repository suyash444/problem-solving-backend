"""
Check Handler - FIXED VERSION 2
Handles position check updates and auto-resolution logic

BUG FIX: The _auto_skip_remaining_positions was including the CURRENT check
         because at the time of query, the check hadn't been flushed yet.
         Solution: Exclude the current check_id from the auto-skip query.
"""
from datetime import datetime
from typing import Dict, Optional
from decimal import Decimal
from loguru import logger

from shared.database import get_db_context
from shared.database.models import PositionCheck, MissionItem, Mission
from sqlalchemy.orm import Session


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
        4. Auto-skip remaining positions for this item if resolved (EXCLUDING current check!)
        5. Check if entire mission is complete
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
                
                # Accept both 'TO_CHECK' and 'PENDING' for backwards compatibility
                if check.status not in ['TO_CHECK', 'PENDING']:
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
                
                logger.info(f"🔍 Marking check {check_id} as FOUND with qty {qty_found}")
                
                # ==========================================
                # STEP 1: Update the check to FOUND
                # ==========================================
                check.status = 'FOUND'
                check.found_in_position = True
                check.qty_found = Decimal(str(qty_found))
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes
                
                # IMPORTANT: Flush to save the FOUND status before auto-skip logic
                db.flush()
                
                logger.info(f"✅ Check {check_id} updated: status=FOUND (flushed to DB)")
                
                # ==========================================
                # STEP 2: Update mission item qty_found
                # ==========================================
                mission_item.qty_found = (mission_item.qty_found or Decimal('0')) + Decimal(str(qty_found))
                
                # ==========================================
                # STEP 3: Check if item is now resolved
                # ==========================================
                item_resolved = False
                skipped_count = 0
                
                if mission_item.qty_found >= mission_item.qty_missing:
                    mission_item.is_resolved = True
                    mission_item.resolved_at = datetime.utcnow()
                    item_resolved = True
                    
                    # ==========================================
                    # STEP 4: Auto-skip OTHER positions (NOT this one!)
                    # ==========================================
                    skipped_count = self._auto_skip_remaining_positions(
                        db, 
                        check.mission_id,
                        check.mission_item_id,
                        exclude_check_id=check_id  # ← KEY FIX: Exclude current check!
                    )
                    logger.info(f"✓ Item fully found! Auto-skipped {skipped_count} OTHER positions")
                
                # ==========================================
                # STEP 5: Check if entire mission is complete
                # ==========================================
                mission_status = self._check_mission_completion(db, check.mission_id)
                mission_complete = (mission_status == 'COMPLETED')
                
                # ==========================================
                # STEP 6: Commit everything
                # ==========================================
                db.commit()
                logger.info(f"💾 Database committed successfully")
                
                # Verify the status after commit (for debugging)
                logger.info(f"🔍 Final check status: {check.status}")
                
                return {
                    "success": True,
                    "message": "Position marked as FOUND",
                    "data": {
                        "item_resolved": item_resolved,
                        "mission_complete": mission_complete,
                        "qty_found": float(qty_found),
                        "total_found_for_item": float(mission_item.qty_found),
                        "qty_still_missing": float(max(mission_item.qty_missing - mission_item.qty_found, 0)),
                        "positions_auto_skipped": skipped_count
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ Error marking position as found: {e}")
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
                
                # Accept both 'TO_CHECK' and 'PENDING' for backwards compatibility
                if check.status not in ['TO_CHECK', 'PENDING']:
                    return {
                        "success": False,
                        "message": f"Position already checked (status: {check.status})"
                    }
                
                logger.info(f"🔍 Marking check {check_id} as NOT_FOUND")
                
                # Update check
                check.status = 'NOT_FOUND'
                check.found_in_position = False
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes
                
                # Check mission status BEFORE committing
                self._check_mission_completion(db, check.mission_id)
                
                # Commit all changes
                db.commit()
                logger.info(f"💾 Database committed successfully")
                
                return {
                    "success": True,
                    "message": "Position marked as NOT_FOUND"
                }
                
        except Exception as e:
            logger.error(f"❌ Error marking position as not found: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def _auto_skip_remaining_positions(
        self, 
        db: Session, 
        mission_id: int,
        mission_item_id: int,
        exclude_check_id: int = None  # ← NEW PARAMETER!
    ) -> int:
        """
        Auto-skip all remaining TO_CHECK/PENDING positions for a resolved item
        EXCLUDING the current check that was just marked as FOUND!
        
        Args:
            db: Database session
            mission_id: Mission ID
            mission_item_id: Mission item ID
            exclude_check_id: Check ID to EXCLUDE from auto-skip (the one just marked FOUND)
        
        Returns:
            Number of positions skipped
        """
        # Build query for remaining checks
        query = db.query(PositionCheck).filter(
            PositionCheck.mission_id == mission_id,
            PositionCheck.mission_item_id == mission_item_id,
            PositionCheck.status.in_(['TO_CHECK', 'PENDING'])
        )
        
        # KEY FIX: Exclude the current check that was just marked FOUND!
        if exclude_check_id:
            query = query.filter(PositionCheck.id != exclude_check_id)
        
        remaining_checks = query.all()
        
        logger.info(f"🔄 Auto-skipping {len(remaining_checks)} remaining positions for mission_item {mission_item_id}")
        
        for check in remaining_checks:
            check.status = 'SKIPPED_AUTO'
            check.notes = 'Auto-skipped: All missing items found'
            check.checked_at = datetime.utcnow()
            logger.debug(f"   → Skipped check {check.id}")
        
        return len(remaining_checks)
    
    def _check_mission_completion(self, db: Session, mission_id: int) -> str:
        """
        Check if mission is complete and update status
        """
        mission = db.query(Mission).filter(Mission.id == mission_id).first()
        
        if not mission:
            return "UNKNOWN"
        
        # Count items
        total_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission_id
        ).count()
        
        resolved_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission_id,
            MissionItem.is_resolved == True
        ).count()
        
        # Count ALL checks
        all_checks = db.query(PositionCheck).filter(
            PositionCheck.mission_id == mission_id
        ).all()
        
        # Count both 'TO_CHECK' and 'PENDING' as pending
        pending_checks = sum(1 for c in all_checks if c.status in ['TO_CHECK', 'PENDING'])
        completed_checks = sum(1 for c in all_checks if c.status in ['FOUND', 'NOT_FOUND', 'SKIPPED_AUTO'])
        
        logger.info(f"Mission {mission_id}: {completed_checks} completed checks, {pending_checks} pending")
        
        # Update mission status
        if resolved_items == total_items:
            mission.status = 'COMPLETED'
            mission.completed_at = datetime.utcnow()
            logger.info(f"✓✓✓ Mission {mission.mission_code} COMPLETED!")
        elif pending_checks == 0 and resolved_items < total_items:
            mission.status = 'COMPLETED'
            mission.completed_at = datetime.utcnow()
            logger.info(f"Mission {mission.mission_code} completed (all checks done, {total_items - resolved_items} items still missing)")
        elif completed_checks > 0 and mission.status == 'OPEN':
            mission.status = 'IN_PROGRESS'
            mission.started_at = datetime.utcnow()
            logger.info(f"Mission {mission.mission_code} status: OPEN → IN_PROGRESS")
        
        return mission.status
    
    def update_mission_status(
        self,
        mission_id: int,
        new_status: str
    ) -> Dict:
        """
        Update mission status manually
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
