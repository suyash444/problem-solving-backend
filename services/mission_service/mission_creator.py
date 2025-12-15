"""
Mission Creator Service - FIXED VERSION v4
Creates missions from basket (cesta) codes by comparing shipped vs ordered items

FIXES:
1. Filter order_items by n_lista ONLY (not cesta!) 
2. Compare only items from the SAME n_lista (last order of the box)
3. Convert position codes to ASCII format (86265 → V2A)
"""
from datetime import datetime
from typing import Dict, List, Optional, Set
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
    """Creates and manages missions for finding missing items - FIXED VERSION v4"""
    
    def __init__(self):
        self.api_client = PowerStoreAPIClient()
    
    def create_mission_from_cesta(self, cesta: str, created_by: str = "System") -> Dict:
        """
        Create a mission from a cesta (basket) code
        
        FIXED LOGIC v4:
        1. Call GetSpedito2 API to get what was shipped
        2. Extract n_lista values from shipped items
        3. Filter order_items by n_lista ONLY (not cesta!) - some items have NULL cesta
        4. Compare: filtered_ordered - shipped = missing
        5. Create mission with missing items
        6. Generate position checks with ASCII-formatted positions
        """
        try:
            logger.info(f"Creating mission for cesta: {cesta}")
            
            # Step 1: Get shipped items from API
            api_result = self.api_client.call_get_spedito2(cesta)
            
            if not api_result['success']:
                return {
                    "success": False,
                    "message": f"Failed to get shipped items: {api_result['message']}"
                }
            
            shipped_items = api_result['data']
            logger.info(f"Found {len(shipped_items)} shipped items from API")
            
            if len(shipped_items) == 0:
                return {
                    "success": False,
                    "message": "No shipped items found for this cesta"
                }
            
            with get_db_context() as db:
                #  prevent duplicate missions for same cesta
                existing_mission = db.query(Mission).filter(
                    Mission.cesta == cesta,
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
                # Step 2: Find missing items (FIXED v4 - filter by n_lista ONLY!)
                missing_items = self._find_missing_items_fixed(db, cesta, shipped_items)
                
                if len(missing_items) == 0:
                    return {
                        "success": True,
                        "message": "No missing items found - everything was shipped!",
                        "mission_created": False,
                        "total_missing_items": 0
                    }
                
                logger.info(f"Found {len(missing_items)} missing items")
                
                # Step 3: Create mission
                mission_code = self._generate_mission_code(db)
                
                mission = Mission(
                    mission_code=mission_code,
                    cesta=cesta,
                    created_by=created_by,
                    status='OPEN'
                )
                
                db.add(mission)
                db.flush()
                
                logger.info(f"Mission created with ID: {mission.id}, code: {mission.mission_code}")
                
                # Step 4: Add mission items
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
                logger.info(f"Added {len(missing_items)} mission items")
                
                # Step 5: Generate position checks (with ASCII conversion!)
                position_checks_created = self._generate_position_checks(db, mission, missing_items)
                logger.info(f"Created {position_checks_created} position checks")
                
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
    
    def _find_missing_items_fixed(self, db: Session, cesta: str, shipped_items: List[Dict]) -> List[Dict]:
        """
        Compare shipped items with ordered items to find what's missing
        
        FIXED v4: Filter by n_lista ONLY, not cesta!
        This ensures we find ALL items for that n_lista, even if some have NULL cesta.
        
        Example:
        - n_lista 5318801 has 7 items in order_items
        - 6 items have cesta = 'X0170'
        - 1 item has cesta = NULL  <-- This was being missed before!
        - Now we get all 7 items by filtering only on n_lista
        """
        missing = []
        
        # ============================================
        # STEP 1: Extract n_lista values from shipped items
        # ============================================
        shipped_n_listas: Set[int] = set()
        shipped_map = {}  # (n_ordine, n_lista, sku) -> qty_shipped
        
        for shipped in shipped_items:
            n_lista = shipped.get('nLista')
            n_ordine = shipped.get('nOrdine')
            sku = shipped.get('CodiceArticolo')
            qty = shipped.get('Quantita', 0)
            
            if n_lista:
                shipped_n_listas.add(int(n_lista))
            
            if n_ordine and n_lista and sku:
                key = (
                    str(n_ordine),
                    int(n_lista),
                    str(sku)
                )
                qty_decimal = Decimal(str(qty)) if qty else Decimal('0')
                shipped_map[key] = shipped_map.get(key, Decimal('0')) + qty_decimal
        
        logger.info(f"Shipped items from n_lista: {shipped_n_listas}")
        logger.info(f"Shipped map has {len(shipped_map)} unique items")
        
        if not shipped_n_listas:
            logger.warning("No n_lista values found in shipped items!")
            return []
        
        # ============================================
        # STEP 2: Get order_items by n_lista ONLY (not cesta!)
        # THIS IS THE KEY FIX v4!
        # Some items have cesta = NULL, so we can't filter by cesta
        # ============================================
        order_items = db.query(OrderItem).filter(
            OrderItem.n_lista.in_(shipped_n_listas)  # ← ONLY filter by n_lista!
            # REMOVED: OrderItem.cesta == cesta  <-- This was excluding NULL cesta items!
        ).all()
        
        logger.info(f"Found {len(order_items)} order items matching n_lista filter (including NULL cesta)")
        
        # ============================================
        # STEP 3: Compare ordered vs shipped
        # ============================================
        for order_item in order_items:
            # Get order number
            order = db.query(Order).filter(Order.id == order_item.order_id).first()
            if not order:
                continue
            
            key = (str(order.order_number), int(order_item.n_lista), str(order_item.sku))
            
            qty_ordered = order_item.qty_ordered or Decimal('0')
            qty_shipped = shipped_map.get(key, Decimal('0'))
            qty_missing = qty_ordered - qty_shipped
            
            logger.debug(f"Item {order_item.sku} (cesta={order_item.cesta}): ordered={qty_ordered}, shipped={qty_shipped}, missing={qty_missing}")
            
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
        """
        Generate position checks for UDCs that might contain missing items
        Position codes are converted to ASCII format!
        """
        checks_created = 0
        
        # Build map of (sku, listone) -> mission_item_id
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
                
                position_check = PositionCheck(
                    mission_id=mission.id,
                    mission_item_id=mission_item_id,
                    udc=udc_inv.udc,
                    listone=item['listone'],
                    position_code=position_code_ascii,  # ← ASCII format!
                    status='TO_CHECK',
                    found_in_position=None,
                    qty_found=None
                )
                
                db.add(position_check)
                checks_created += 1
        
        db.flush()
        return checks_created
    
    def _convert_position_to_ascii(self, position_code: str) -> str:
        """
        Convert position code to ASCII format
        
        Input: 86265-21-1-3
        
        Conversion for first part (MAG):
        - 86 → chr(86) = 'V'
        - 2 → stays as '2'
        - 65 → chr(65) = 'A'
        
        Output: V2A-21-1-3
        
        Uses Python built-in chr() function for ASCII conversion.
        """
        if not position_code or position_code == 'UNKNOWN':
            return position_code
        
        try:
            parts = position_code.split('-')
            
            if len(parts) >= 1 and parts[0]:
                mag = parts[0]  # e.g., "86265"
                
                if len(mag) >= 5:
                    # Extract components
                    first_two = mag[0:2]    # "86"
                    middle = mag[2:3]        # "2"
                    next_two = mag[3:5]      # "65"
                    remaining = mag[5:]      # anything after (usually empty)
                    
                    # Convert to ASCII
                    try:
                        ascii_first = int(first_two)
                        ascii_next = int(next_two)
                        
                        # Check if valid printable ASCII (32-126)
                        if 32 <= ascii_first <= 126 and 32 <= ascii_next <= 126:
                            char_first = chr(ascii_first)  # 86 → 'V'
                            char_next = chr(ascii_next)    # 65 → 'A'
                            
                            # Reconstruct: V + 2 + A + remaining
                            parts[0] = f"{char_first}{middle}{char_next}{remaining}"
                        else:
                            logger.debug(f"ASCII values out of range: {ascii_first}, {ascii_next}")
                    except ValueError:
                        logger.debug(f"Could not convert to int: {first_two}, {next_two}")
                
                elif len(mag) >= 2:
                    # Shorter format - just convert first 2 digits
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
# UTILITY FUNCTION: Convert any position to ASCII
# ============================================
def convert_position_to_ascii(position_code: str) -> str:
    """
    Standalone utility function to convert position code to ASCII format.
    Can be used anywhere in the application.
    
    Input: 86265-21-1-3
    Output: V2A-21-1-3
    
    Logic:
    - First 2 digits (86) → chr(86) = 'V'
    - Third digit (2) → stays as '2'
    - Next 2 digits (65) → chr(65) = 'A'
    """
    if not position_code or position_code == 'UNKNOWN':
        return position_code
    
    try:
        parts = position_code.split('-')
        
        if len(parts) >= 1 and parts[0] and len(parts[0]) >= 5:
            mag = parts[0]
            
            first_two = int(mag[0:2])    # 86
            middle = mag[2:3]             # 2
            next_two = int(mag[3:5])      # 65
            remaining = mag[5:]           # anything after
            
            if 32 <= first_two <= 126 and 32 <= next_two <= 126:
                parts[0] = f"{chr(first_two)}{middle}{chr(next_two)}{remaining}"
        
        return '-'.join(parts)
        
    except:
        return position_code


# ============================================
# TEST ASCII CONVERSION
# ============================================
if __name__ == "__main__":
    # Test the ASCII conversion
    test_positions = [
        "86265-21-1-3",
        "86265-23-65-2",
        "65066-10-5-1",
        "UNKNOWN",
        None
    ]
    
    print("Testing ASCII conversion:")
    print("-" * 40)
    for pos in test_positions:
        result = convert_position_to_ascii(pos)
        print(f"{pos} → {result}")
