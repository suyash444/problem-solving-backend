"""
Position Generator - FIXED VERSION
Generates optimized checking routes for operators

FIX: Changed 'PENDING' to 'TO_CHECK' to match SQL schema
"""
from typing import List, Dict, Optional
from loguru import logger

from shared.database import get_db_context
from shared.database.models import Mission, MissionItem, PositionCheck
from shared.schemas import mission


class PositionGenerator:
    """Generates and manages position checking routes"""
    
    def get_mission_route(self, mission_id: int) -> Optional[Dict]:
        """
        Get the checking route for a mission
        Returns positions in alphabetical order with item context
        
        Args:
            mission_id: Mission ID
            
        Returns:
            Dict with route information
        """
        try:
            with get_db_context() as db:
                db.expire_all()
                mission = db.query(Mission).filter(
                    Mission.id == mission_id
                ).first()
                
                if not mission:
                    logger.warning(f"Mission not found: {mission_id}")
                    return None
                
                # Get all position checks for this mission
                checks = db.query(PositionCheck).filter(
                    PositionCheck.mission_id == mission_id
                ).order_by(PositionCheck.position_code).all()
                
                if not checks:
                    return {
                        'mission_code': mission.mission_code,
                        'cesta': mission.cesta,
                        'status': mission.status,
                        'positions': [],
                        'total_positions': 0,
                        'message': 'No position checks for this mission'
                    }
                
                # Build position details with item info from mission_item
                positions = []
                for check in checks:
                    # Get the mission item for this check to access SKU
                    mission_item = db.query(MissionItem).filter(
                        MissionItem.id == check.mission_item_id
                    ).first()
                    
                    if mission_item:
                        positions.append({
                            'check_id': check.id,
                            'position_code': check.position_code,
                            'udc': check.udc,
                            'listone': check.listone,
                            'status': check.status,
                            'found_in_position': check.found_in_position,
                            'qty_found': float(check.qty_found) if check.qty_found else None,
                            # Get SKU and order info from mission_item
                            'sku': mission_item.sku,
                            'qty_missing': float(mission_item.qty_missing) if mission_item.qty_missing else 0,
                            'n_ordine': mission_item.n_ordine,
                            'n_lista': mission_item.n_lista,
                            "cesta": mission_item.cesta or mission.cesta,
                        })
                
                return {
                    'mission_code': mission.mission_code,
                    'cesta': mission.cesta,
                    'status': mission.status,
                    'positions': positions,
                    'total_positions': len(positions)
                }
                
        except Exception as e:
            logger.error(f"Error getting mission route: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def get_next_position(self, mission_id: int) -> Optional[Dict]:
        """
        Get the next position to check for a mission
        
        Args:
            mission_id: Mission ID
            
        Returns:
            Dict with next position details or None if all checked
        """
        try:
            with get_db_context() as db:
                # FIX: Look for both 'TO_CHECK' and 'PENDING' for backwards compatibility
                mission_obj = db.query(Mission).filter(Mission.id == mission_id).first()

                check = db.query(PositionCheck).filter(
                    PositionCheck.mission_id == mission_id,
                    PositionCheck.status.in_(['TO_CHECK', 'PENDING'])
                ).order_by(PositionCheck.position_code).first()
                
                if not check:
                    return None
                
                # Get mission item for SKU info
                mission_item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id
                ).first()
                
                if not mission_item:
                    return None
                
                return {
                    'check_id': check.id,
                    'position_code': check.position_code,
                    'udc': check.udc,
                    'listone': check.listone,
                    'status': check.status,
                    'sku': mission_item.sku,
                    'qty_missing': float(mission_item.qty_missing) if mission_item.qty_missing else 0,
                    'n_ordine': mission_item.n_ordine,
                    'n_lista': mission_item.n_lista,
                    'cesta': mission_item.cesta or (mission_obj.cesta if mission_obj else None),

                }
                
        except Exception as e:
            logger.error(f"Error getting next position: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def get_mission_summary(self, mission_id: int) -> Optional[Dict]:
        """
        Get summary statistics for a mission
        
        Args:
            mission_id: Mission ID
            
        Returns:
            Dict with mission summary
        """
        try:
            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.id == mission_id
                ).first()
                
                if not mission:
                    return None
                
                # Count items
                items = db.query(MissionItem).filter(
                    MissionItem.mission_id == mission_id
                ).all()
                
                total_items = len(items)
                resolved_items = sum(1 for item in items if item.is_resolved)
                
                # Count checks by status - FIXED TO COUNT CORRECTLY!
                checks = db.query(PositionCheck).filter(
                    PositionCheck.mission_id == mission_id
                ).all()
                
                total_checks = len(checks)
                
                # FIX: Count both 'TO_CHECK' and 'PENDING' as pending
                pending_checks = 0
                found_checks = 0
                not_found_checks = 0
                skipped_checks = 0
                
                for check in checks:
                    if check.status in ['TO_CHECK', 'PENDING']:
                        pending_checks += 1
                    elif check.status == 'FOUND':
                        found_checks += 1
                    elif check.status == 'NOT_FOUND':
                        not_found_checks += 1
                    elif check.status == 'SKIPPED_AUTO':
                        skipped_checks += 1
                
                # Log for debugging
                logger.info(f"📊 Mission {mission_id} summary:")
                logger.info(f"  - PENDING/TO_CHECK: {pending_checks}")
                logger.info(f"  - FOUND: {found_checks}")
                logger.info(f"  - NOT_FOUND: {not_found_checks}")
                logger.info(f"  - SKIPPED: {skipped_checks}")
                
                # Completed = FOUND + NOT_FOUND + SKIPPED_AUTO
                completed_checks = found_checks + not_found_checks + skipped_checks
                
                # Calculate percentage based on completed checks
                completion_percentage = round(
                    (completed_checks / total_checks * 100) if total_checks > 0 else 0, 
                    2
                )
                
                return {
                    'mission_id': mission.id,
                    'mission_code': mission.mission_code,
                    'cesta': mission.cesta,
                    'status': mission.status,
                    'created_by': mission.created_by,
                    'created_at': str(mission.created_at) if mission.created_at else None,
                    'total_missing_items': total_items,
                    'resolved_items': resolved_items,
                    'unresolved_items': total_items - resolved_items,
                    'total_positions': total_checks,
                    'positions_pending': pending_checks,
                    'positions_found': found_checks,
                    'positions_not_found': not_found_checks,
                    'positions_skipped': skipped_checks,
                    'completion_percentage': completion_percentage
                }
                
        except Exception as e:
            logger.error(f"Error getting mission summary: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def list_all_missions(
        self, 
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        List all missions with optional status filter
        
        Args:
            status: Filter by status (OPEN, IN_PROGRESS, COMPLETED, CANCELLED)
            limit: Maximum number of results
            
        Returns:
            List of mission summaries
        """
        try:
            with get_db_context() as db:
                query = db.query(Mission)
                
                # Apply status filter if provided
                if status and status.strip():
                    query = query.filter(Mission.status == status.upper())
                
                # Get missions ordered by most recent first
                missions = query.order_by(
                    Mission.created_at.desc()
                ).limit(int(limit)).all()
                
                # Build summary for each mission
                results = []
                for mission in missions:
                    summary = self.get_mission_summary(mission.id)
                    if summary:
                        results.append(summary)
                
                return results
                
        except Exception as e:
            logger.error(f"Error listing missions: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
