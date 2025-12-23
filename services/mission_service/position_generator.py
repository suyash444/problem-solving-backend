"""
Position Generator - MULTI-COMPANY SAFE VERSION
Generates optimized checking routes for operators
"""
from typing import List, Dict, Optional
from loguru import logger

from sqlalchemy import exists, and_

from shared.database import get_db_context
from shared.database.models import Mission, MissionItem, PositionCheck


class PositionGenerator:
    """Generates and manages position checking routes (company-safe)"""

    # ==========================================================
    # ROUTE
    # ==========================================================
    def get_mission_route(self, company: str, mission_id: int) -> Optional[Dict]:
        """Get ordered checking route for a mission"""
        try:
            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.id == mission_id,
                    Mission.company == company
                ).first()

                if not mission:
                    return None

                checks = db.query(PositionCheck).filter(
                    PositionCheck.mission_id == mission_id,
                    PositionCheck.company == company
                ).order_by(PositionCheck.position_code).all()

                positions = []
                for check in checks:
                    item = db.query(MissionItem).filter(
                        MissionItem.id == check.mission_item_id,
                        MissionItem.company == company
                    ).first()

                    if not item:
                        continue

                    positions.append({
                        "check_id": check.id,
                        "position_code": check.position_code,
                        "udc": check.udc,
                        "listone": check.listone,
                        "status": check.status,
                        "found_in_position": check.found_in_position,
                        "qty_found": float(check.qty_found) if check.qty_found else None,
                        "sku": item.sku,
                        "qty_missing": float(item.qty_missing),
                        "n_ordine": item.n_ordine,
                        "n_lista": item.n_lista,
                        "cesta": item.cesta or mission.cesta,
                    })

                return {
                    "mission_code": mission.mission_code,
                    "company": company,
                    "cesta": mission.cesta,
                    "status": mission.status,
                    "total_positions": len(positions),
                    "positions": positions,
                }

        except Exception as e:
            logger.error(f"Error getting mission route: {e}")
            return None

    # ==========================================================
    # NEXT POSITION
    # ==========================================================
    def get_next_position(self, company: str, mission_id: int) -> Optional[Dict]:
        """Get next position to check"""
        try:
            with get_db_context() as db:
                check = db.query(PositionCheck).filter(
                    PositionCheck.company == company,
                    PositionCheck.mission_id == mission_id,
                    PositionCheck.status.in_(["TO_CHECK", "PENDING"])
                ).order_by(PositionCheck.position_code).first()

                if not check:
                    return None

                item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id,
                    MissionItem.company == company
                ).first()

                if not item:
                    return None

                return {
                    "check_id": check.id,
                    "company": company,
                    "position_code": check.position_code,
                    "udc": check.udc,
                    "listone": check.listone,
                    "status": check.status,
                    "sku": item.sku,
                    "qty_missing": float(item.qty_missing),
                    "n_ordine": item.n_ordine,
                    "n_lista": item.n_lista,
                    "cesta": item.cesta,
                }

        except Exception as e:
            logger.error(f"Error getting next position: {e}")
            return None

    # ==========================================================
    # SUMMARY
    # ==========================================================
    def get_mission_summary(self, company: str, mission_id: int) -> Optional[Dict]:
        """Get mission summary statistics"""
        try:
            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.id == mission_id,
                    Mission.company == company
                ).first()

                if not mission:
                    return None

                items = db.query(MissionItem).filter(
                    MissionItem.mission_id == mission_id,
                    MissionItem.company == company
                ).all()

                checks = db.query(PositionCheck).filter(
                    PositionCheck.mission_id == mission_id,
                    PositionCheck.company == company
                ).all()

                resolved_items = sum(1 for i in items if i.is_resolved)

                pending = sum(1 for c in checks if c.status in ["TO_CHECK", "PENDING"])
                found = sum(1 for c in checks if c.status == "FOUND")
                not_found = sum(1 for c in checks if c.status == "NOT_FOUND")
                skipped = sum(1 for c in checks if c.status == "SKIPPED_AUTO")

                completed = found + not_found + skipped
                percentage = round((completed / len(checks) * 100), 2) if checks else 0

                return {
                    "mission_id": mission.id,
                    "company": company,
                    "mission_code": mission.mission_code,
                    "cesta": mission.cesta,
                    "status": mission.status,
                    "created_at": str(mission.created_at),
                    "total_missing_items": len(items),
                    "resolved_items": resolved_items,
                    "unresolved_items": len(items) - resolved_items,
                    "total_positions": len(checks),
                    "positions_pending": pending,
                    "positions_found": found,
                    "positions_not_found": not_found,
                    "positions_skipped": skipped,
                    "completion_percentage": percentage,
                }

        except Exception as e:
            logger.error(f"Error getting mission summary: {e}")
            return None

    # ==========================================================
    # LIST MISSIONS
    # ==========================================================
    def list_all_missions(
        self,
        company: str,
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """List missions with optional status filter"""
        try:
            with get_db_context() as db:
                query = db.query(Mission).filter(Mission.company == company)

                if status:
                    s = status.upper()
                    if s == "PENDING":
                        query = query.filter(Mission.status.in_(["OPEN", "IN_PROGRESS"]))
                    elif s == "HAS_NOT_FOUND":
                        nf = exists().where(
                            and_(
                                PositionCheck.mission_id == Mission.id,
                                PositionCheck.company == company,
                                PositionCheck.status == "NOT_FOUND"
                            )
                        )
                        query = query.filter(nf)
                    elif s != "ALL":
                        query = query.filter(Mission.status == s)

                missions = query.order_by(Mission.created_at.desc()).limit(limit).all()

                return [
                    self.get_mission_summary(company=company, mission_id=m.id)
                    for m in missions
                    if self.get_mission_summary(company=company, mission_id=m.id)
                ]

        except Exception as e:
            logger.error(f"Error listing missions: {e}")
            return []
