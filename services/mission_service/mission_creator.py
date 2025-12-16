"""
Mission Creator Service - VERSION v5 with BATCH SUPPORT
Creates missions from basket (cesta) codes by comparing shipped vs ordered items

FEATURES:
1. Single cesta mission creation (existing)
2. BATCH mission creation - multiple cestas in ONE mission (NEW!)
3. Filter order_items by n_lista ONLY (not cesta!)
4. Convert position codes to ASCII format (86265 → V2A)
5. Sort positions alphabetically for optimal route
6. Combine items with same SKU + Listone
"""
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from decimal import Decimal
from loguru import logger
from sqlalchemy.orm import Session

from shared.database import get_db_context
from shared.database.models import (
    Mission, MissionItem, PositionCheck, OrderItem, ShippedItem,
    UDCInventory, UDCLocation, Order
)
from services.ingestion_service.api_client import PowerStoreAPIClient


class MissionCreator:
    """Creates and manages missions for finding missing items with BATCH SUPPORT"""
    
    def __init__(self):
        self.api_client = PowerStoreAPIClient()
    
    # ============================================
    # SINGLE CESTA MISSION (Existing functionality)
    # ============================================
    def create_mission_from_cesta(self, cesta: str, created_by: str = "System") -> Dict:
        """
        Create a mission from a SINGLE cesta (basket) code
        """
        try:
            logger.info(f"Creating mission for cesta: {cesta}")
            
            # Step 1: Get shipped items from API
            api_result = self.api_client.call_get_spedito2(cesta)
            
            if not api_result.get("success",False):
                return {
                    "success": False,
                    "message": f"Failed to get shipped items: {api_result['message']}"
                }
            
            shipped_items = api_result.get("data") or []

            if not shipped_items :
                return {
                    "success": False,
                    "message": "No shipped items found for this cesta"
                }
            

            # Build reference_n_lista deterministically from shipped items
            shipped_n_listas = sorted({
                int(x.get("nLista"))
                for x in shipped_items
                if x.get("nLista") is not None
            })

            if not shipped_n_listas:
                return {
                   "success": False,
                   "message": "No nLista found in shipped items (cannot create mission)"
                }

            # Use lowest nLista as reference (stable + deterministic)
            reference_n_lista = shipped_n_listas[0]

            logger.info(f"Found {len(shipped_items)} shipped items from API")
            
            
            
            with get_db_context() as db:
                # Prevent duplicate missions for same cesta
                existing_mission = db.query(Mission).filter(
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
                
                # Find missing items
                missing_items = self._find_missing_items_fixed(db, cesta, shipped_items)
                
                if len(missing_items) == 0:
                    return {
                        "success": True,
                        "message": "No missing items found - everything was shipped!",
                        "mission_created": False,
                        "total_missing_items": 0
                    }
                
                logger.info(f"Found {len(missing_items)} missing items")
                
                # Create mission
                mission_code = self._generate_mission_code(db)
                
                mission = Mission(
                    mission_code=mission_code,
                    cesta=cesta,
                    reference_n_lista=reference_n_lista,
                    created_by=created_by,
                    status='OPEN'
                )
                
                db.add(mission)
                db.flush()
                
                # Add mission items
                for item_data in missing_items:
                    mission_item = MissionItem(
                        mission_id=mission.id,
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
                
                # Generate position checks
                position_checks_created = self._generate_position_checks(db, mission, missing_items)
                
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
            return {
                "success": False,
                "message": f"Error creating mission: {str(e)}"
            }
    
    # ============================================
    # CHECK SINGLE CESTA (for batch preview)
    # ============================================
    def check_cesta_missing_items(self, cesta: str) -> Dict:
        """
        Check a cesta for missing items WITHOUT creating a mission.
        Used for batch mode preview - operator can see what's missing before creating mission.
        
        Returns:
        - success: bool
        - cesta: str
        - missing_count: int
        - missing_items: list of items with details
        - message: str (error message if failed)
        """
        try:
            logger.info(f"Checking cesta for missing items: {cesta}")
            
            # Get shipped items from API
            api_result = self.api_client.call_get_spedito2(cesta)
            
            if not api_result.get("success",False):
                return {
                    "success": False,
                    "cesta": cesta,
                    "missing_count": 0,
                    "missing_items": [],
                    "message": f"API error: {api_result['message']}"
                }
            
            shipped_items = api_result.get("data") or []


            
            if not shipped_items  :
                return {
                    "success": False,
                    "cesta": cesta,
                    "missing_count": 0,
                    "missing_items": [],
                    "message": "No shipped items found for this cesta"
                }
            # NEW (BATCH SUPPORT): collect nLista values from shipped items
            shipped_n_listas = sorted({
                int(x.get("nLista"))
                for x in shipped_items
                if x.get("nLista") is not None
            })
            
            with get_db_context() as db:
                # Find missing items
                missing_items = self._find_missing_items_fixed(db, cesta, shipped_items)
                
                if len(missing_items) == 0:
                    return {
                        "success": True,
                        "cesta": cesta,
                        "missing_count": 0,
                        "missing_items": [],
                        "n_listas": shipped_n_listas,
                        "message": "No missing items - everything was shipped!"
                    }
                
                # Add cesta to each item for tracking
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
    # BATCH MISSION CREATION (NEW!)
    # ============================================
    def create_batch_mission(self, cestas: List[str], created_by: str = "System") -> Dict:
        """
        Create ONE mission from MULTIPLE cestas.
        
        Process:
        1. Check each cesta for missing items
        2. Combine all missing items into one list
        3. Group items by SKU + Listone (combine quantities)
        4. Create ONE mission with all items
        5. Generate position checks sorted alphabetically
        
        Args:
            cestas: List of cesta codes ["X0399", "X0239", "X0108"]
            created_by: Username creating the mission
            
        Returns:
            Dict with mission details and summary
        """
        try:
            logger.info(f"Creating BATCH mission for {len(cestas)} cestas: {cestas}")
            
            if not cestas or len(cestas) == 0:
                return {
                    "success": False,
                    "message": "No cestas provided"
                }
            
            # Clean and uppercase all cestas
            cestas = [c.strip().upper() for c in cestas]
            
            # Remove duplicates while preserving order
            seen = set()
            unique_cestas = []
            for c in cestas:
                if c not in seen:
                    seen.add(c)
                    unique_cestas.append(c)
            cestas = unique_cestas
            
            # ============================================
            # STEP 1: Collect missing items from ALL cestas
            # ============================================
            all_missing_items = []
            cestas_processed = []
            cestas_with_missing = []
            cestas_skipped = []
            cestas_errors = []
            all_shipped_n_listas: Set[int] = set()
            
            for cesta in cestas:
                logger.info(f"Checking cesta: {cesta}")
                result = self.check_cesta_missing_items(cesta)
                
                cestas_processed.append(cesta)
                
                if not result['success']:
                    cestas_errors.append({
                        "cesta": cesta,
                        "error": result['message']
                    })
                    continue

                #  collect n_listas even if missing_count == 0
                for nl in (result.get("shipped_n_listas") or []):
                    all_shipped_n_listas.add(int(nl))


                if result['missing_count'] == 0:
                    cestas_skipped.append({
                        "cesta": cesta,
                        "reason": "No missing items"
                    })
                    continue
                
                cestas_with_missing.append({
                    "cesta": cesta,
                    "missing_count": result['missing_count']
                })
                
                all_missing_items.extend(result['missing_items'])
            
            logger.info(f"Total missing items from all cestas: {len(all_missing_items)}")
            
            # Check if we have any missing items
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
            
            # ============================================
            # STEP 2: Group items by SKU + Listone
            # ============================================
            grouped_items = self._group_items_by_sku_listone(all_missing_items)
            logger.info(f"Grouped into {len(grouped_items)} unique SKU+Listone combinations")
            
            # ============================================
            # STEP 3: Create the mission
            # ============================================

            reference_n_lista = min(all_shipped_n_listas) if all_shipped_n_listas else None
            with get_db_context() as db:
                mission_code = self._generate_mission_code(db)
                
                # Store all cestas as comma-separated in cesta field
                cestas_str = ",".join([c['cesta'] for c in cestas_with_missing])
                
                mission = Mission(
                    mission_code=mission_code,
                    cesta=cestas_str,  # Multiple cestas stored here
                    reference_n_lista=reference_n_lista,
                    created_by=created_by,
                    status='OPEN'
                )
                
                db.add(mission)
                db.flush()
                
                logger.info(f"Mission created with ID: {mission.id}, code: {mission.mission_code}")
                
                # ============================================
                # STEP 4: Add mission items (grouped)
                # ============================================
                for item_data in grouped_items:
                    mission_item = MissionItem(
                        mission_id=mission.id,
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
                logger.info(f"Added {len(grouped_items)} mission items")
                
                # ============================================
                # STEP 5: Generate position checks (sorted!)
                # ============================================
                position_checks_created = self._generate_position_checks_batch(
                    db, mission, grouped_items
                )
                logger.info(f"Created {position_checks_created} position checks")
                
                db.commit()
                db.refresh(mission)
                
                logger.info(f"✓✓✓ BATCH Mission {mission.mission_code} created successfully!")
                
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
            return {
                "success": False,
                "message": f"Error creating batch mission: {str(e)}"
            }
    
    def _group_items_by_sku_listone(self, items: List[Dict]) -> List[Dict]:
        """
        SAFE grouping for batch:
        group by SKU + Listone + n_ordine + n_lista
        so we NEVER mix different orders/lists together.
        
        
        """
        grouped = {}  # key: (sku, listone, n_ordine, n_lista)

        
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
            
            # Aggregate quantities
            grouped[key]['qty_ordered'] += item['qty_ordered']
            grouped[key]['qty_shipped'] += item['qty_shipped']
            grouped[key]['qty_missing'] += item['qty_missing']
            
            # Track which cesta this item came from
            if item.get('cesta') and item['cesta'] not in grouped[key]['cestas']:
                grouped[key]['cestas'].append(item['cesta'])
        
        return list(grouped.values())
    
    def _generate_position_checks_batch(self, db: Session, mission: Mission, grouped_items: List[Dict]) -> int:
        """
        Generate position checks for batch mission.
        Positions are sorted alphabetically for optimal route!
        """
        checks_to_create = []
        
        # Build map of (sku, listone) -> mission_item_id
        mission_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission.id
        ).all()
        
        mission_items_map = {}
        for mi in mission_items:
            if mi.listone:
                key = (mi.sku, mi.listone)
                mission_items_map[key] = mi.id
        
        for item in grouped_items:
            if not item.get('listone'):
                logger.debug(f"No listone for SKU {item['sku']}, skipping position check")
                continue
            
            # Find UDCs that have this SKU for this listone
            udcs = db.query(UDCInventory).filter(
                UDCInventory.listone == item['listone'],
                UDCInventory.sku == item['sku'],
                UDCInventory.qty > 0
            ).all()
            
            if not udcs:
                logger.debug(f"No UDC inventory found for listone {item['listone']}, SKU {item['sku']}")
                continue
            
            # Get mission_item_id
            key = (item['sku'], item['listone'])
            mission_item_id = mission_items_map.get(key)
            
            if not mission_item_id:
                logger.warning(f"Could not find mission_item_id for SKU {item['sku']}, listone {item['listone']}")
                continue
            
            # Create position check for each UDC
            for udc_inv in udcs:
                # Get UDC location
                location = db.query(UDCLocation).filter(
                    UDCLocation.udc == udc_inv.udc
                ).first()
                
                # Get position code and convert to ASCII format
                position_code_raw = location.position_code if location else 'UNKNOWN'
                position_code_ascii = self._convert_position_to_ascii(position_code_raw)
                
                checks_to_create.append({
                    'mission_id': mission.id,
                    'mission_item_id': mission_item_id,
                    'udc': udc_inv.udc,
                    'listone': item['listone'],
                    'position_code': position_code_ascii,
                    'sku': item['sku'],
                    'cestas': item.get('cestas', [])
                })
        
        # ============================================
        # SORT positions alphabetically for optimal route!
        # ============================================
        checks_to_create.sort(key=lambda x: x['position_code'])
        
        # Create position checks in sorted order
        for check_data in checks_to_create:
            position_check = PositionCheck(
                mission_id=check_data['mission_id'],
                mission_item_id=check_data['mission_item_id'],
                udc=check_data['udc'],
                listone=check_data['listone'],
                position_code=check_data['position_code'],
                status='TO_CHECK',
                found_in_position=None,
                qty_found=None
            )
            db.add(position_check)
        
        db.flush()
        return len(checks_to_create)
    
    # ============================================
    # EXISTING HELPER METHODS (unchanged)
    # ============================================
    def _find_missing_items_fixed(self, db: Session, cesta: str, shipped_items: List[Dict]) -> List[Dict]:
        """
        Compare shipped items with ordered items to find what's missing
        FIXED v4: Filter by n_lista ONLY, not cesta!
        """
        missing = []
        
        # Extract n_lista values from shipped items
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
        
        logger.info(f"Shipped items from n_lista: {shipped_n_listas}")
        
        if not shipped_n_listas:
            logger.warning("No n_lista values found in shipped items!")
            return []
        
        # Get order_items by n_lista ONLY
        order_items = db.query(OrderItem).filter(
            OrderItem.n_lista.in_(shipped_n_listas)
        ).all()
        
        logger.info(f"Found {len(order_items)} order items matching n_lista filter")
        
        # Compare ordered vs shipped
        for order_item in order_items:
            order = db.query(Order).filter(Order.id == order_item.order_id).first()
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
        
        logger.info(f"Total missing items: {len(missing)}")
        return missing
    
    def _generate_position_checks(self, db: Session, mission: Mission, missing_items: List[Dict]) -> int:
        """Generate position checks for single cesta mission"""
        checks_created = 0
        
        mission_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission.id
        ).all()
        
        mission_items_map = {}
        for mi in mission_items:
            if mi.listone:
                key = (mi.sku, mi.listone)
                mission_items_map[key] = mi.id
        
        for item in missing_items:
            if not item.get('listone'):
                continue
            
            udcs = db.query(UDCInventory).filter(
                UDCInventory.listone == item['listone'],
                UDCInventory.sku == item['sku'],
                UDCInventory.qty > 0
            ).all()
            
            if not udcs:
                continue
            
            key = (item['sku'], item['listone'])
            mission_item_id = mission_items_map.get(key)
            
            if not mission_item_id:
                continue
            
            for udc_inv in udcs:
                location = db.query(UDCLocation).filter(
                    UDCLocation.udc == udc_inv.udc
                ).first()
                
                position_code_raw = location.position_code if location else 'UNKNOWN'
                position_code_ascii = self._convert_position_to_ascii(position_code_raw)
                
                position_check = PositionCheck(
                    mission_id=mission.id,
                    mission_item_id=mission_item_id,
                    udc=udc_inv.udc,
                    listone=item['listone'],
                    position_code=position_code_ascii,
                    status='TO_CHECK',
                    found_in_position=None,
                    qty_found=None
                )
                
                db.add(position_check)
                checks_created += 1
        
        db.flush()
        return checks_created
    
    def _convert_position_to_ascii(self, position_code: str) -> str:
        """Convert position code to ASCII format: 86265 → V2A"""
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
                            char_first = chr(ascii_first)
                            char_next = chr(ascii_next)
                            parts[0] = f"{char_first}{middle}{char_next}{remaining}"
                    except ValueError:
                        pass
                
                elif len(mag) >= 2:
                    first_two = mag[0:2]
                    remaining = mag[2:]
                    
                    try:
                        ascii_val = int(first_two)
                        if 32 <= ascii_val <= 126:
                            char = chr(ascii_val)
                            parts[0] = f"{char}{remaining}"
                    except ValueError:
                        pass
            
            return '-'.join(parts)
            
        except Exception as e:
            logger.warning(f"Could not convert position {position_code}: {e}")
            return position_code
    
    def _generate_mission_code(self, db: Session) -> str:
        """Generate unique mission code in format: PSM-YYYYMMDD-NNN"""
        today = datetime.now().strftime('%Y%m%d')
        prefix = f"PSM-{today}-"
        
        existing = db.query(Mission).filter(
            Mission.mission_code.like(f"{prefix}%")
        ).order_by(Mission.mission_code.desc()).first()
        
        if existing:
            try:
                last_num = int(existing.mission_code.split('-')[-1])
                next_num = last_num + 1
            except:
                next_num = 1
        else:
            next_num = 1
        
        return f"{prefix}{next_num:03d}"
    
    def get_mission_details(self, mission_id: int) -> Optional[Dict]:
        """Get complete mission details"""
        try:
            with get_db_context() as db:
                mission = db.query(Mission).filter(Mission.id == mission_id).first()
                
                if not mission:
                    return None
                
                items = db.query(MissionItem).filter(MissionItem.mission_id == mission_id).all()
                checks = db.query(PositionCheck).filter(PositionCheck.mission_id == mission_id).all()
                
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
                        }
                        for item in items
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
                        }
                        for check in checks
                    ]
                }
        except Exception as e:
            logger.error(f"Error getting mission details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None


# ============================================
# UTILITY FUNCTION
# ============================================
def convert_position_to_ascii(position_code: str) -> str:
    """Standalone utility function to convert position code to ASCII format."""
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
        
    except:
        return position_code
