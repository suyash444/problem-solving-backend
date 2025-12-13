"""
Rebuild UDC Inventory from Picking Events
Run this once to populate the udc_inventory table
"""
from loguru import logger
from sqlalchemy import func
from decimal import Decimal

from shared.database import get_db_context
from shared.database.models import PickingEvent, UDCInventory, OrderItem


def rebuild_udc_inventory():
    """
    Rebuild UDC inventory from picking events
    
    This aggregates all picking events to determine current UDC contents
    """
    try:
        logger.info("=== Starting UDC Inventory Rebuild ===")
        
        with get_db_context() as db:
            # Clear existing inventory
            logger.info("Clearing existing UDC inventory...")
            db.query(UDCInventory).delete()
            db.commit()
            
            # Get all picking events with their order items (to get SKU and listone)
            logger.info("Aggregating picking events with order items...")
            
            results = db.query(
                PickingEvent.udc,
                OrderItem.sku,
                OrderItem.listone,
                func.sum(PickingEvent.qty_picked).label('total_qty')
            ).join(
                OrderItem, PickingEvent.order_item_id == OrderItem.id
            ).filter(
                PickingEvent.udc.isnot(None),
                OrderItem.sku.isnot(None),
                OrderItem.listone.isnot(None),
                PickingEvent.qty_picked > 0
            ).group_by(
                PickingEvent.udc,
                OrderItem.sku,
                OrderItem.listone
            ).all()
            
            logger.info(f"Found {len(results)} unique UDC+SKU+Listone combinations")
            
            # Create inventory records
            inventory_records = []
            for row in results:
                if row.total_qty and row.total_qty > 0:
                    inventory_records.append(UDCInventory(
                        udc=row.udc,
                        sku=row.sku,
                        listone=row.listone,
                        qty=Decimal(str(row.total_qty))
                    ))
            
            # Bulk insert
            logger.info(f"Inserting {len(inventory_records)} inventory records...")
            db.bulk_save_objects(inventory_records)
            db.commit()
            
            logger.info(f"✓✓✓ SUCCESS! Created {len(inventory_records)} UDC inventory records")
            
            return {
                "success": True,
                "records_created": len(inventory_records)
            }
            
    except Exception as e:
        logger.error(f"Error rebuilding UDC inventory: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    result = rebuild_udc_inventory()
    print(result)