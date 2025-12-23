"""
Mission Creator Service - VERSION v5 with BATCH SUPPORT
Creates missions from basket (cesta) codes by comparing shipped vs ordered items

MULTI-COMPANY SAFE:
- All reads are filtered by company
- All inserts set company (NOT NULL in DB)
- UDCLocation lookup uses composite PK (company, udc)

FIX:
- _generate_mission_code is now COMPANY-SAFE (sequence per company per day)
  to avoid collisions across companies.
"""
from datetime import datetime
from typing import Dict, List, Optional, Set
from decimal import Decimal
from loguru import logger
from sqlalchemy.orm import Session

from shared.database import get_db_context
from shared.database.models import (
    Mission, MissionItem, PositionCheck, OrderItem,
    UDCInventory, UDCLocation, Order
)
from services.ingestion_service.api_client import PowerStoreAPIClient
from config.settings import settings


class MissionCreator:
    """Creates and manages missions for finding missing items with BATCH SUPPORT"""

    def __init__(self):
        self.api_client = PowerStoreAPIClient()

    # ============================================
    # SINGLE CESTA MISSION
    # ============================================
    def create_mission_from_cesta(
        self,
        cesta: str,
        created_by: str = "System",
        company: Optional[str] = None
    ) -> Dict:
        """
        Create a mission from a SINGLE cesta (basket) code
        """
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        try:
            logger.info(f"Creating mission [{company_key}] for cesta: {cesta}")

            # Step 1: Get shipped items from API (company-specific token)
            api_result = self.api_client.call_get_spedito2(cesta, company=company_key)

            if not api_result.get("success", False):
                return {"success": False, "message": f"Failed to get shipped items: {api_result['message']}"}

            shipped_items = api_result.get("data") or []
            if not shipped_items:
                return {"success": False, "message": "No shipped items found for this cesta"}

            shipped_n_listas = sorted({
                int(x.get("nLista"))
                for x in shipped_items
                if x.get("nLista") is not None
            })

            if not shipped_n_listas:
                return {"success": False, "message": "No nLista found in shipped items (cannot create mission)"}

            reference_n_lista = shipped_n_listas[0]
            logger.info(f"Found {len(shipped_items)} shipped items from API")

            with get_db_context() as db:
                # Prevent duplicate missions for same company + cesta + reference_n_lista
                existing_mission = db.query(Mission).filter(
                    Mission.company == company_key,
                    Mission.cesta == cesta,
                    Mission.reference_n_lista == reference_n_lista,
                    Mission.status.in_(['OPEN', 'IN_PROGRESS'])
                ).order_by(Mission.created_at.desc()).first()

                if existing_mission:
                    return {
                        "success": True,
                        "message": f"Mission already exists for cesta {cesta}",
                        "mission_id": existing_mission.id,
                        "mission_code": existing_mission.mission_code,
                        "cesta": existing_mission.cesta,
                        "status": existing_mission.status,
                        "already_exists": True
                    }

                # Find missing items (company-safe)
                missing_items = self._find_missing_items_fixed(db, cesta, shipped_items, company_key)

                if len(missing_items) == 0:
                    return {
                        "success": True,
                        "message": "No missing items found - everything was shipped!",
                        "mission_created": False,
                        "total_missing_items": 0
                    }

                logger.info(f"Found {len(missing_items)} missing items")

                mission_code = self._generate_mission_code(db, company_key=company_key)

                mission = Mission(
                    company=company_key,
                    mission_code=mission_code,
                    cesta=cesta,
                    reference_n_lista=reference_n_lista,
                    created_by=created_by,
                    status='OPEN'
                )

                db.add(mission)
                db.flush()

                for item_data in missing_items:
                    mission_item = MissionItem(
                        company=company_key,
                        mission_id=mission.id,
                        cesta=cesta,
                        n_ordine=item_data['n_ordine'],
                        n_lista=item_data['n_lista'],
                        sku=item_data['sku'],
                        listone=item_data['listone'],
                        qty_ordered=item_data['qty_ordered'],
                        qty_shipped=item_data['qty_shipped'],
                        qty_missing=item_data['qty_missing'],
                        qty_found=Decimal('0'),
                        is_resolved=False
                    )
                    db.add(mission_item)

                db.flush()

                position_checks_created = self._generate_position_checks(
                    db, mission, missing_items, company_key
                )

                db.commit()
                db.refresh(mission)

                logger.info(f"✓✓✓ Mission {mission.mission_code} created successfully!")

                return {
                    "success": True,
                    "message": f"Mission created successfully with {len(missing_items)} missing items",
                    "mission_id": mission.id,
                    "mission_code": mission.mission_code,
                    "cesta": mission.cesta,
                    "total_missing_items": len(missing_items),
                    "position_checks_created": position_checks_created,
                    "status": mission.status,
                    "created_at": str(mission.created_at) if mission.created_at else None
                }

        except Exception as e:
            logger.error(f"Error creating mission: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Error creating mission: {str(e)}"}

    # ============================================
    # CHECK SINGLE CESTA (batch preview)
    # ============================================
    def check_cesta_missing_items(self, cesta: str, company: Optional[str] = None) -> Dict:
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        try:
            logger.info(f"Checking cesta [{company_key}] for missing items: {cesta}")

            api_result = self.api_client.call_get_spedito2(cesta, company=company_key)

            if not api_result.get("success", False):
                return {
                    "success": False,
                    "cesta": cesta,
                    "missing_count": 0,
                    "missing_items": [],
                    "message": f"API error: {api_result['message']}"
                }

            shipped_items = api_result.get("data") or []
            if not shipped_items:
                return {
                    "success": False,
                    "cesta": cesta,
                    "missing_count": 0,
                    "missing_items": [],
                    "message": "No shipped items found for this cesta"
                }

            shipped_n_listas = sorted({
                int(x.get("nLista"))
                for x in shipped_items
                if x.get("nLista") is not None
            })

            with get_db_context() as db:
                missing_items = self._find_missing_items_fixed(db, cesta, shipped_items, company_key)

                if len(missing_items) == 0:
                    return {
                        "success": True,
                        "cesta": cesta,
                        "missing_count": 0,
                        "missing_items": [],
                        "shipped_n_listas": shipped_n_listas,
                        "message": "No missing items - everything was shipped!"
                    }

                for item in missing_items:
                    item['cesta'] = cesta

                return {
                    "success": True,
                    "cesta": cesta,
                    "missing_count": len(missing_items),
                    "missing_items": missing_items,
                    "shipped_n_listas": shipped_n_listas,
                    "message": f"Found {len(missing_items)} missing items"
                }

        except Exception as e:
            logger.error(f"Error checking cesta: {e}")
            return {
                "success": False,
                "cesta": cesta,
                "missing_count": 0,
                "missing_items": [],
                "message": f"Error: {str(e)}"
            }

    # ============================================
    # BATCH MISSION CREATION
    # ============================================
    def create_batch_mission(
        self,
        cestas: List[str],
        created_by: str = "System",
        company: Optional[str] = None
    ) -> Dict:
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        try:
            logger.info(f"Creating BATCH mission [{company_key}] for {len(cestas)} cestas: {cestas}")

            if not cestas:
                return {"success": False, "message": "No cestas provided"}

            cestas = [c.strip().upper() for c in cestas]

            seen = set()
            unique_cestas = []
            for c in cestas:
                if c not in seen:
                    seen.add(c)
                    unique_cestas.append(c)
            cestas = unique_cestas

            all_missing_items = []
            cestas_processed = []
            cestas_with_missing = []
            cestas_skipped = []
            cestas_errors = []
            all_shipped_n_listas: Set[int] = set()

            for cesta in cestas:
                logger.info(f"Checking cesta: {cesta}")
                result = self.check_cesta_missing_items(cesta, company=company_key)

                cestas_processed.append(cesta)

                if not result['success']:
                    cestas_errors.append({"cesta": cesta, "error": result['message']})
                    continue

                for nl in (result.get("shipped_n_listas") or []):
                    all_shipped_n_listas.add(int(nl))

                if result['missing_count'] == 0:
                    cestas_skipped.append({"cesta": cesta, "reason": "No missing items"})
                    continue

                cestas_with_missing.append({"cesta": cesta, "missing_count": result['missing_count']})
                all_missing_items.extend(result['missing_items'])

            logger.info(f"Total missing items from all cestas: {len(all_missing_items)}")

            if len(all_missing_items) == 0:
                return {
                    "success": True,
                    "message": "No missing items found in any cesta",
                    "mission_created": False,
                    "cestas_processed": len(cestas_processed),
                    "cestas_with_missing": 0,
                    "cestas_skipped": cestas_skipped,
                    "cestas_errors": cestas_errors,
                    "total_missing_items": 0
                }

            grouped_items = self._group_items_by_sku_listone(all_missing_items)
            logger.info(f"Grouped into {len(grouped_items)} unique combinations")

            reference_n_lista = min(all_shipped_n_listas) if all_shipped_n_listas else None

            with get_db_context() as db:
                cestas_list = [c['cesta'] for c in cestas_with_missing]
                cestas_str = self._normalize_cestas_str(cestas_list)

                existing_mission = db.query(Mission).filter(
                    Mission.company == company_key,
                    Mission.cesta == cestas_str,
                    Mission.reference_n_lista == reference_n_lista,
                    Mission.status.in_(['OPEN', 'IN_PROGRESS'])
                ).order_by(Mission.created_at.desc()).first()

                if existing_mission:
                    return {
                        "success": True,
                        "message": "Batch mission already exists",
                        "mission_id": existing_mission.id,
                        "mission_code": existing_mission.mission_code,
                        "cestas": existing_mission.cesta,
                        "status": existing_mission.status,
                        "already_exists": True
                    }

                mission_code = self._generate_mission_code(db, company_key=company_key)

                mission = Mission(
                    company=company_key,
                    mission_code=mission_code,
                    cesta=cestas_str,
                    reference_n_lista=reference_n_lista,
                    created_by=created_by,
                    status='OPEN'
                )

                db.add(mission)
                db.flush()

                for item_data in grouped_items:
                    item_cesta = ",".join(item_data["cestas"]) if item_data.get("cestas") else None

                    mission_item = MissionItem(
                        company=company_key,
                        mission_id=mission.id,
                        cesta=item_cesta,
                        n_ordine=item_data['n_ordine'],
                        n_lista=item_data['n_lista'],
                        sku=item_data['sku'],
                        listone=item_data['listone'],
                        qty_ordered=item_data['qty_ordered'],
                        qty_shipped=item_data['qty_shipped'],
                        qty_missing=item_data['qty_missing'],
                        qty_found=Decimal('0'),
                        is_resolved=False
                    )
                    db.add(mission_item)

                db.flush()

                position_checks_created = self._generate_position_checks_batch(
                    db, mission, grouped_items, company_key
                )

                db.commit()
                db.refresh(mission)

                return {
                    "success": True,
                    "message": f"Batch mission created with {len(grouped_items)} items from {len(cestas_with_missing)} cestas",
                    "mission_id": mission.id,
                    "mission_code": mission.mission_code,
                    "cestas": cestas_str,
                    "cestas_processed": len(cestas_processed),
                    "cestas_with_missing": len(cestas_with_missing),
                    "cestas_with_missing_details": cestas_with_missing,
                    "cestas_skipped": cestas_skipped,
                    "cestas_errors": cestas_errors,
                    "total_missing_items": len(grouped_items),
                    "total_positions": position_checks_created,
                    "position_checks_created": position_checks_created,
                    "status": mission.status,
                    "created_at": str(mission.created_at) if mission.created_at else None
                }

        except Exception as e:
            logger.error(f"Error creating batch mission: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Error creating batch mission: {str(e)}"}

    def _normalize_cestas_str(self, cestas_list: List[str]) -> str:
        return ",".join(sorted([c.strip().upper() for c in cestas_list if c and c.strip()]))

    def _group_items_by_sku_listone(self, items: List[Dict]) -> List[Dict]:
        grouped = {}
        for item in items:
            key = (item['sku'], item['listone'], item['n_ordine'], item['n_lista'])
            if key not in grouped:
                grouped[key] = {
                    'sku': item['sku'],
                    'listone': item['listone'],
                    'n_ordine': item['n_ordine'],
                    'n_lista': item['n_lista'],
                    'qty_ordered': Decimal('0'),
                    'qty_shipped': Decimal('0'),
                    'qty_missing': Decimal('0'),
                    'cestas': []
                }

            grouped[key]['qty_ordered'] += item['qty_ordered']
            grouped[key]['qty_shipped'] += item['qty_shipped']
            grouped[key]['qty_missing'] += item['qty_missing']

            if item.get('cesta') and item['cesta'] not in grouped[key]['cestas']:
                grouped[key]['cestas'].append(item['cesta'])

        return list(grouped.values())

    def _generate_position_checks_batch(
        self,
        db: Session,
        mission: Mission,
        grouped_items: List[Dict],
        company_key: str
    ) -> int:
        checks_to_create = []
        seen_checks = set()

        mission_items = db.query(MissionItem).filter(
            MissionItem.company == company_key,
            MissionItem.mission_id == mission.id
        ).all()

        mission_items_map = {}
        for mi in mission_items:
            if mi.listone:
                key = (mi.sku, mi.listone, mi.n_ordine, mi.n_lista)
                mission_items_map[key] = mi.id

        for item in grouped_items:
            if not item.get('listone'):
                continue

            udcs = db.query(UDCInventory).filter(
                UDCInventory.company == company_key,
                UDCInventory.listone == item['listone'],
                UDCInventory.sku == item['sku'],
                UDCInventory.qty > 0
            ).all()

            if not udcs:
                continue

            key = (item['sku'], item['listone'], item['n_ordine'], item['n_lista'])
            mission_item_id = mission_items_map.get(key)
            if not mission_item_id:
                continue

            for udc_inv in udcs:
                location = db.query(UDCLocation).filter(
                    UDCLocation.company == company_key,
                    UDCLocation.udc == udc_inv.udc
                ).first()

                position_code_raw = location.position_code if location else 'UNKNOWN'
                position_code_ascii = self._convert_position_to_ascii(position_code_raw)

                key_dup = (mission.id, mission_item_id, udc_inv.udc, position_code_ascii)
                if key_dup in seen_checks:
                    continue
                seen_checks.add(key_dup)

                checks_to_create.append({
                    'mission_id': mission.id,
                    'mission_item_id': mission_item_id,
                    'udc': udc_inv.udc,
                    'listone': item['listone'],
                    'position_code': position_code_ascii,
                })

        checks_to_create.sort(key=lambda x: x['position_code'])

        for check_data in checks_to_create:
            db.add(PositionCheck(
                company=company_key,
                mission_id=check_data['mission_id'],
                mission_item_id=check_data['mission_item_id'],
                udc=check_data['udc'],
                listone=check_data['listone'],
                position_code=check_data['position_code'],
                status='TO_CHECK',
                found_in_position=None,
                qty_found=None
            ))

        db.flush()
        return len(checks_to_create)

    def _find_missing_items_fixed(
        self,
        db: Session,
        cesta: str,
        shipped_items: List[Dict],
        company_key: str
    ) -> List[Dict]:
        missing = []

        shipped_n_listas: Set[int] = set()
        shipped_map = {}

        for shipped in shipped_items:
            n_lista = shipped.get('nLista')
            n_ordine = shipped.get('nOrdine')
            sku = shipped.get('CodiceArticolo')
            qty = shipped.get('Quantita', 0)

            if n_lista:
                shipped_n_listas.add(int(n_lista))

            if n_ordine and n_lista and sku:
                key = (str(n_ordine), int(n_lista), str(sku))
                qty_decimal = Decimal(str(qty)) if qty else Decimal('0')
                shipped_map[key] = shipped_map.get(key, Decimal('0')) + qty_decimal

        if not shipped_n_listas:
            return []

        order_items = db.query(OrderItem).filter(
            OrderItem.company == company_key,
            OrderItem.n_lista.in_(shipped_n_listas)
        ).all()

        for order_item in order_items:
            order = db.query(Order).filter(
                Order.company == company_key,
                Order.id == order_item.order_id
            ).first()
            if not order:
                continue

            key = (str(order.order_number), int(order_item.n_lista), str(order_item.sku))

            qty_ordered = order_item.qty_ordered or Decimal('0')
            qty_shipped = shipped_map.get(key, Decimal('0'))
            qty_missing = qty_ordered - qty_shipped

            if qty_missing > 0:
                missing.append({
                    'n_ordine': order.order_number,
                    'n_lista': order_item.n_lista,
                    'listone': order_item.listone,
                    'sku': order_item.sku,
                    'qty_ordered': qty_ordered,
                    'qty_shipped': qty_shipped,
                    'qty_missing': qty_missing
                })

        return missing

    def _generate_position_checks(
        self,
        db: Session,
        mission: Mission,
        missing_items: List[Dict],
        company_key: str
    ) -> int:
        checks_created = 0
        seen_checks = set()

        mission_items = db.query(MissionItem).filter(
            MissionItem.company == company_key,
            MissionItem.mission_id == mission.id
        ).all()

        mission_items_map = {}
        for mi in mission_items:
            if mi.listone:
                key = (mi.sku, mi.listone, mi.n_ordine, mi.n_lista)
                mission_items_map[key] = mi.id

        for item in missing_items:
            if not item.get('listone'):
                continue

            key = (item['sku'], item['listone'], item['n_ordine'], item['n_lista'])
            mission_item_id = mission_items_map.get(key)
            if not mission_item_id:
                continue

            udcs = db.query(UDCInventory).filter(
                UDCInventory.company == company_key,
                UDCInventory.listone == item['listone'],
                UDCInventory.sku == item['sku'],
                UDCInventory.qty > 0
            ).all()

            for udc_inv in udcs:
                location = db.query(UDCLocation).filter(
                    UDCLocation.company == company_key,
                    UDCLocation.udc == udc_inv.udc
                ).first()

                position_code_raw = location.position_code if location else 'UNKNOWN'
                position_code_ascii = self._convert_position_to_ascii(position_code_raw)

                key_dup = (mission.id, mission_item_id, udc_inv.udc, position_code_ascii)
                if key_dup in seen_checks:
                    continue
                seen_checks.add(key_dup)

                db.add(PositionCheck(
                    company=company_key,
                    mission_id=mission.id,
                    mission_item_id=mission_item_id,
                    udc=udc_inv.udc,
                    listone=item['listone'],
                    position_code=position_code_ascii,
                    status='TO_CHECK',
                    found_in_position=None,
                    qty_found=None
                ))
                checks_created += 1

        db.flush()
        return checks_created

    def _convert_position_to_ascii(self, position_code: str) -> str:
        if not position_code or position_code == 'UNKNOWN':
            return position_code
        try:
            parts = position_code.split('-')
            if len(parts) >= 1 and parts[0]:
                mag = parts[0]
                if len(mag) >= 5:
                    first_two = mag[0:2]
                    middle = mag[2:3]
                    next_two = mag[3:5]
                    remaining = mag[5:]
                    try:
                        ascii_first = int(first_two)
                        ascii_next = int(next_two)
                        if 32 <= ascii_first <= 126 and 32 <= ascii_next <= 126:
                            parts[0] = f"{chr(ascii_first)}{middle}{chr(ascii_next)}{remaining}"
                    except ValueError:
                        pass
                elif len(mag) >= 2:
                    first_two = mag[0:2]
                    remaining = mag[2:]
                    try:
                        ascii_val = int(first_two)
                        if 32 <= ascii_val <= 126:
                            parts[0] = f"{chr(ascii_val)}{remaining}"
                    except ValueError:
                        pass
            return '-'.join(parts)
        except Exception as e:
            logger.warning(f"Could not convert position {position_code}: {e}")
            return position_code

    # ✅ ONLY CHANGE: company-safe mission code generation
    def _generate_mission_code(self, db: Session, company_key: str) -> str:
        today = datetime.now().strftime('%Y%m%d')
        prefix = f"PSM-{today}-"

        existing = db.query(Mission).filter(
            Mission.company == company_key,
            Mission.mission_code.like(f"{prefix}%")
        ).order_by(Mission.mission_code.desc()).first()

        if existing:
            try:
                last_num = int(existing.mission_code.split('-')[-1])
                next_num = last_num + 1
            except Exception:
                next_num = 1
        else:
            next_num = 1

        return f"{prefix}{next_num:03d}"

    def get_mission_details(self, mission_id: int, company: Optional[str] = None) -> Optional[Dict]:
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()
        try:
            with get_db_context() as db:
                mission = db.query(Mission).filter(
                    Mission.company == company_key,
                    Mission.id == mission_id
                ).first()

                if not mission:
                    return None

                items = db.query(MissionItem).filter(
                    MissionItem.company == company_key,
                    MissionItem.mission_id == mission_id
                ).all()

                checks = db.query(PositionCheck).filter(
                    PositionCheck.company == company_key,
                    PositionCheck.mission_id == mission_id
                ).all()

                return {
                    "mission_id": mission.id,
                    "mission_code": mission.mission_code,
                    "cesta": mission.cesta,
                    "status": mission.status,
                    "created_by": mission.created_by,
                    "created_at": str(mission.created_at) if mission.created_at else None,
                    "items": [
                        {
                            "item_id": item.id,
                            "n_ordine": item.n_ordine,
                            "n_lista": item.n_lista,
                            "sku": item.sku,
                            "listone": item.listone,
                            "qty_ordered": float(item.qty_ordered) if item.qty_ordered else 0,
                            "qty_shipped": float(item.qty_shipped) if item.qty_shipped else 0,
                            "qty_missing": float(item.qty_missing) if item.qty_missing else 0,
                            "qty_found": float(item.qty_found) if item.qty_found else 0,
                            "is_resolved": item.is_resolved
                        } for item in items
                    ],
                    "position_checks": [
                        {
                            "check_id": check.id,
                            "mission_item_id": check.mission_item_id,
                            "udc": check.udc,
                            "listone": check.listone,
                            "position_code": check.position_code,
                            "status": check.status,
                            "found_in_position": check.found_in_position,
                            "qty_found": float(check.qty_found) if check.qty_found else None
                        } for check in checks
                    ]
                }
        except Exception as e:
            logger.error(f"Error getting mission details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None


def convert_position_to_ascii(position_code: str) -> str:
    if not position_code or position_code == 'UNKNOWN':
        return position_code
    try:
        parts = position_code.split('-')
        if len(parts) >= 1 and parts[0] and len(parts[0]) >= 5:
            mag = parts[0]
            first_two = int(mag[0:2])
            middle = mag[2:3]
            next_two = int(mag[3:5])
            remaining = mag[5:]
            if 32 <= first_two <= 126 and 32 <= next_two <= 126:
                parts[0] = f"{chr(first_two)}{middle}{chr(next_two)}{remaining}"
        return '-'.join(parts)
    except Exception:
        return position_code
