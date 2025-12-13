"""
Mission Creator Service
Creates missions from basket (cesta) codes by comparing shipped vs ordered items
"""
from datetime import datetime
from typing import Dict, List, Optional
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
    """Creates and manages missions for finding missing items"""
    
    def __init__(self):
        self.api_client = PowerStoreAPIClient()
    
    def create_mission_from_cesta(self, cesta: str, created_by: str = "System") -> Dict:
        """
        Create a mission from a cesta (basket) code
        
        Process:
        1. Call GetSpedito2 API to get what was shipped
        2. Compare with DumpTrack data to find missing items
        3. Create mission with missing items
        4. Generate position checks for UDCs containing missing items
        
        Args:
            cesta: Basket code (e.g., 'X0103')
            created_by: Username or system creating the mission (default: "System")
            
        Returns:
            Dict with mission details
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
                # Step 2: Compare with DumpTrack to find missing items
                missing_items = self._find_missing_items(db, cesta, shipped_items)
                
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
                db.flush()  # Get mission ID
                
                logger.info(f"Mission created with ID: {mission.id}, code: {mission.mission_code}")
                
                # Step 4: Add mission items (NOW WITH LISTONE!)
                for item_data in missing_items:
                    mission_item = MissionItem(
                        mission_id=mission.id,
                        n_ordine=item_data['n_ordine'],
                        n_lista=item_data['n_lista'],
                        sku=item_data['sku'],
                        listone=item_data['listone'],  # ← ADDED LISTONE!
                        qty_ordered=item_data['qty_ordered'],
                        qty_shipped=item_data['qty_shipped'],
                        qty_missing=item_data['qty_missing'],
                        qty_found=Decimal('0'),
                        is_resolved=False
                    )
                    db.add(mission_item)
                
                db.flush()
                logger.info(f"Added {len(missing_items)} mission items")
                
                # Step 5: Generate position checks
                position_checks_created = self._generate_position_checks(db, mission, missing_items)
                logger.info(f"Created {position_checks_created} position checks")
                
                # Commit everything
                db.commit()
                
                # Refresh to get latest data
                db.refresh(mission)
                
                logger.info(f"✓✓✓ Mission {mission.mission_code} created successfully!")
                
                # Return mission data as dict
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
    
    def _find_missing_items(self, db: Session, cesta: str, shipped_items: List[Dict]) -> List[Dict]:
        """
        Compare shipped items with ordered items to find what's missing
        """
        missing = []
        
        # Get all order items for this cesta
        order_items = db.query(OrderItem).filter(OrderItem.cesta == cesta).all()
        
        logger.info(f"Found {len(order_items)} order items for cesta {cesta}")
        
        # Create a map of shipped quantities by (n_ordine, n_lista, sku)
        shipped_map = {}
        for shipped in shipped_items:
            key = (
                str(shipped.get('nOrdine', '')),
                int(shipped.get('nLista', 0)),
                str(shipped.get('CodiceArticolo', ''))
            )
            qty = Decimal(str(shipped.get('Quantita', 0)))
            shipped_map[key] = shipped_map.get(key, Decimal('0')) + qty
        
        # Compare ordered vs shipped
        for order_item in order_items:
            # Get order number
            order = db.query(Order).filter(Order.id == order_item.order_id).first()
            if not order:
                continue
            
            key = (order.order_number, order_item.n_lista, order_item.sku)
            
            qty_ordered = order_item.qty_ordered or Decimal('0')
            qty_shipped = shipped_map.get(key, Decimal('0'))
            qty_missing = qty_ordered - qty_shipped
            
            if qty_missing > 0:
                missing.append({
                    'n_ordine': order.order_number,
                    'n_lista': order_item.n_lista,
                    'listone': order_item.listone,  # ← INCLUDE LISTONE
                    'sku': order_item.sku,
                    'qty_ordered': qty_ordered,
                    'qty_shipped': qty_shipped,
                    'qty_missing': qty_missing
                })
        
        return missing
    
    def _generate_position_checks(self, db: Session, mission: Mission, missing_items: List[Dict]) -> int:
        """
        Generate position checks for UDCs that might contain missing items
        """
        checks_created = 0
        
        # First, get all mission items to map SKU+Listone to mission_item_id
        mission_items = db.query(MissionItem).filter(
            MissionItem.mission_id == mission.id
        ).all()
        
        mission_items_map = {}  # Map (sku, listone) -> mission_item_id
        for mi in mission_items:
            if mi.listone:  # Only map if listone exists
                key = (mi.sku, mi.listone)
                mission_items_map[key] = mi.id
        
        for item in missing_items:
            # Find UDCs that have this SKU for this listone
            if not item.get('listone'):
                logger.debug(f"No listone for SKU {item['sku']}, skipping position check")
                continue
            
            udcs = db.query(UDCInventory).filter(
                UDCInventory.listone == item['listone'],
                UDCInventory.sku == item['sku'],
                UDCInventory.qty > 0
            ).all()
            
            if not udcs:
                logger.debug(f"No UDC inventory found for listone {item['listone']}, SKU {item['sku']}")
                continue
            
            # Get mission_item_id for this item
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
                
                position_code = location.position_code if location else 'UNKNOWN'
                
                position_check = PositionCheck(
                    mission_id=mission.id,
                    mission_item_id=mission_item_id,
                    udc=udc_inv.udc,
                    listone=item['listone'],
                    position_code=position_code,
                    status='PENDING',
                    found_in_position=None,
                    qty_found=None
                )
                
                db.add(position_check)
                checks_created += 1
        
        db.flush()
        return checks_created
    
    def _generate_mission_code(self, db: Session) -> str:
        """
        Generate unique mission code in format: PSM-YYYYMMDD-NNN
        PSM = Problem Solving Mission
        """
        today = datetime.now().strftime('%Y%m%d')
        prefix = f"PSM-{today}-"
        
        # Find highest number for today
        existing = db.query(Mission).filter(
            Mission.mission_code.like(f"{prefix}%")
        ).order_by(Mission.mission_code.desc()).first()
        
        if existing:
            # Extract number and increment
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
                
                # Get mission items
                items = db.query(MissionItem).filter(MissionItem.mission_id == mission_id).all()
                
                # Get position checks
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