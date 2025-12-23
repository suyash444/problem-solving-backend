"""
Check Handler - MULTI-COMPANY SAFE VERSION
Handles position check updates and auto-resolution logic

BUG FIX: The _auto_skip_remaining_positions was including the CURRENT check
         because at the time of query, the check hadn't been flushed yet.
         Solution: Exclude the current check_id from the auto-skip query.

MULTI-COMPANY FIX:
- Filters every query by `company`
- Uses check.company as the source of truth
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
                # Load check (company comes from DB row)
                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id
                ).first()

                if not check:
                    return {"success": False, "message": "Position check not found"}

                company_key = (check.company or "").strip().lower()

                # Re-load check with company filter (extra safety)
                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id,
                    PositionCheck.company == company_key
                ).first()

                if not check:
                    return {"success": False, "message": "Position check not found (company mismatch)"}

                # Accept both 'TO_CHECK' and 'PENDING' for backwards compatibility
                if check.status not in ["TO_CHECK", "PENDING"]:
                    return {
                        "success": False,
                        "message": f"Position already checked (status: {check.status})"
                    }

                # Load mission item (company-safe)
                mission_item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id,
                    MissionItem.company == company_key
                ).first()

                if not mission_item:
                    return {"success": False, "message": "Mission item not found"}

                # Default qty_found to 1 if not specified
                if qty_found is None:
                    qty_found = 1.0

                qty_found_dec = Decimal(str(qty_found))

                logger.info(f"🔍 [{company_key}] Marking check {check_id} as FOUND with qty {qty_found}")

                # STEP 1: Update check
                check.status = "FOUND"
                check.found_in_position = True
                check.qty_found = qty_found_dec
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes

                # Flush so this check is no longer returned by auto-skip query
                db.flush()

                # STEP 2: Update mission item qty_found
                mission_item.qty_found = (mission_item.qty_found or Decimal("0")) + qty_found_dec

                # STEP 3: Resolve item + auto-skip other positions
                item_resolved = False
                skipped_count = 0

                if mission_item.qty_found >= (mission_item.qty_missing or Decimal("0")):
                    mission_item.is_resolved = True
                    mission_item.resolved_at = datetime.utcnow()
                    item_resolved = True

                    skipped_count = self._auto_skip_remaining_positions(
                        db,
                        company_key=company_key,
                        mission_id=check.mission_id,
                        mission_item_id=check.mission_item_id,
                        exclude_check_id=check_id
                    )
                    logger.info(f"✓ [{company_key}] Item fully found! Auto-skipped {skipped_count} OTHER positions")

                # STEP 4: Mission completion check
                mission_status = self._check_mission_completion(
                    db,
                    company_key=company_key,
                    mission_id=check.mission_id
                )
                mission_complete = (mission_status == "COMPLETED")

                db.commit()
                logger.info("💾 Database committed successfully")

                qty_missing = mission_item.qty_missing or Decimal("0")
                qty_found_total = mission_item.qty_found or Decimal("0")

                return {
                    "success": True,
                    "message": "Position marked as FOUND",
                    "data": {
                        "company": company_key,
                        "item_resolved": item_resolved,
                        "mission_complete": mission_complete,
                        "qty_found": float(qty_found_dec),
                        "total_found_for_item": float(qty_found_total),
                        "qty_still_missing": float(max(qty_missing - qty_found_total, Decimal("0"))),
                        "positions_auto_skipped": skipped_count
                    }
                }

        except Exception as e:
            logger.error(f"❌ Error marking position as found: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Error: {str(e)}"}

    def mark_not_found(
        self,
        check_id: int,
        checked_by: str,
        notes: Optional[str] = None
    ) -> Dict:
        """Mark a position check as NOT_FOUND"""
        try:
            with get_db_context() as db:
                check = db.query(PositionCheck).filter(PositionCheck.id == check_id).first()

                if not check:
                    return {"success": False, "message": "Position check not found"}

                company_key = (check.company or "").strip().lower()

                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id,
                    PositionCheck.company == company_key
                ).first()

                if not check:
                    return {"success": False, "message": "Position check not found (company mismatch)"}

                if check.status not in ["TO_CHECK", "PENDING"]:
                    return {
                        "success": False,
                        "message": f"Position already checked (status: {check.status})"
                    }

                logger.info(f"🔍 [{company_key}] Marking check {check_id} as NOT_FOUND")

                check.status = "NOT_FOUND"
                check.found_in_position = False
                check.checked_at = datetime.utcnow()
                check.checked_by = checked_by
                check.notes = notes

                self._check_mission_completion(db, company_key=company_key, mission_id=check.mission_id)

                db.commit()
                logger.info("💾 Database committed successfully")

                return {"success": True, "message": "Position marked as NOT_FOUND"}

        except Exception as e:
            logger.error(f"❌ Error marking position as not found: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Error: {str(e)}"}

    def _auto_skip_remaining_positions(
        self,
        db: Session,
        company_key: str,
        mission_id: int,
        mission_item_id: int,
        exclude_check_id: Optional[int] = None
    ) -> int:
        """
        Auto-skip all remaining TO_CHECK/PENDING positions for a resolved item
        EXCLUDING the current check_id.
        """
        query = db.query(PositionCheck).filter(
            PositionCheck.company == company_key,
            PositionCheck.mission_id == mission_id,
            PositionCheck.mission_item_id == mission_item_id,
            PositionCheck.status.in_(["TO_CHECK", "PENDING"])
        )

        if exclude_check_id is not None:
            query = query.filter(PositionCheck.id != exclude_check_id)

        remaining_checks = query.all()

        logger.info(
            f"🔄 [{company_key}] Auto-skipping {len(remaining_checks)} remaining positions "
            f"for mission_item {mission_item_id}"
        )

        for chk in remaining_checks:
            chk.status = "SKIPPED_AUTO"
            chk.notes = "Auto-skipped: All missing items found"
            chk.checked_at = datetime.utcnow()
            chk.checked_by = "AUTO"

        return len(remaining_checks)

    def _check_mission_completion(self, db: Session, company_key: str, mission_id: int) -> str:
        """Check if mission is complete and update status (company-safe)"""
        mission = db.query(Mission).filter(
            Mission.id == mission_id,
            Mission.company == company_key
        ).first()

        if not mission:
            return "UNKNOWN"

        total_items = db.query(MissionItem).filter(
            MissionItem.company == company_key,
            MissionItem.mission_id == mission_id
        ).count()

        resolved_items = db.query(MissionItem).filter(
            MissionItem.company == company_key,
            MissionItem.mission_id == mission_id,
            MissionItem.is_resolved == True  # noqa: E712
        ).count()

        all_checks = db.query(PositionCheck).filter(
            PositionCheck.company == company_key,
            PositionCheck.mission_id == mission_id
        ).all()

        pending_checks = sum(1 for c in all_checks if c.status in ["TO_CHECK", "PENDING"])
        completed_checks = sum(1 for c in all_checks if c.status in ["FOUND", "NOT_FOUND", "SKIPPED_AUTO"])

        logger.info(f"[{company_key}] Mission {mission_id}: {completed_checks} completed checks, {pending_checks} pending")

        if total_items > 0 and resolved_items == total_items:
            if not mission.started_at:
                mission.started_at = datetime.utcnow()
            mission.status = "COMPLETED"
            mission.completed_at = datetime.utcnow()
            logger.info(f"✓✓✓ [{company_key}] Mission {mission.mission_code} COMPLETED!")

        elif pending_checks == 0 and resolved_items < total_items:
            if mission.status == "OPEN":
                mission.status = "IN_PROGRESS"
                if not mission.started_at:
                    mission.started_at = datetime.utcnow()
            mission.completed_at = None

            logger.info(
                f"[{company_key}] Mission {mission.mission_code} still pending "
                f"(all checks done, {total_items - resolved_items} items still missing)"
            )

        elif completed_checks > 0 and mission.status == "OPEN":
            mission.status = "IN_PROGRESS"
            if not mission.started_at:
                mission.started_at = datetime.utcnow()
            logger.info(f"[{company_key}] Mission {mission.mission_code} status: OPEN → IN_PROGRESS")

        return mission.status

    # ✅ FIXED: company-safe + matches route signature (company, mission_id, new_status)
    def update_mission_status(self, company: str, mission_id: int, new_status: str) -> Dict:
        """Update mission status manually (company-safe)"""
        try:
            company_key = (company or "").strip().lower()
            new_status_u = (new_status or "").strip().upper()

            valid_statuses = ["OPEN", "IN_PROGRESS", "COMPLETED", "CANCELLED"]
            if new_status_u not in valid_statuses:
                return {"success": False, "message": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"}

            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.company == company_key,
                    Mission.id == mission_id
                ).first()

                if not mission:
                    return {"success": False, "message": "Mission not found"}

                old_status = mission.status
                mission.status = new_status_u

                if new_status_u == "IN_PROGRESS" and not mission.started_at:
                    mission.started_at = datetime.utcnow()
                elif new_status_u == "COMPLETED" and not mission.completed_at:
                    mission.completed_at = datetime.utcnow()

                db.commit()

                return {
                    "success": True,
                    "message": f"Mission status updated from {old_status} to {new_status_u}",
                    "mission_code": mission.mission_code,
                    "new_status": new_status_u
                }

        except Exception as e:
            logger.error(f"Error updating mission status: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Error: {str(e)}"}

    def get_check_details(self, check_id: int) -> Optional[Dict]:
        """Get details of a specific position check (company-safe)"""
        try:
            with get_db_context() as db:
                check = db.query(PositionCheck).filter(PositionCheck.id == check_id).first()

                if not check:
                    return None

                company_key = (check.company or "").strip().lower()

                check = db.query(PositionCheck).filter(
                    PositionCheck.id == check_id,
                    PositionCheck.company == company_key
                ).first()

                if not check:
                    return None

                mission_item = db.query(MissionItem).filter(
                    MissionItem.id == check.mission_item_id,
                    MissionItem.company == company_key
                ).first()

                return {
                    "check_id": check.id,
                    "company": company_key,
                    "position_code": check.position_code,
                    "udc": check.udc,
                    "listone": check.listone,
                    "status": check.status,
                    "found_in_position": check.found_in_position,
                    "qty_found": float(check.qty_found) if check.qty_found is not None else None,
                    "checked_at": str(check.checked_at) if check.checked_at else None,
                    "checked_by": check.checked_by,
                    "notes": check.notes,
                    "sku": mission_item.sku if mission_item else None,
                    "n_ordine": mission_item.n_ordine if mission_item else None,
                    "n_lista": mission_item.n_lista if mission_item else None
                }

        except Exception as e:
            logger.error(f"Error getting check details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
