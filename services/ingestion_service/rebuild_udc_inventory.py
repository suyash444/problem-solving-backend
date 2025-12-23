"""
Rebuild UDC Inventory from Picking Events
Run this once to populate the udc_inventory table

MULTI-COMPANY UPDATE:
- Rebuilds inventory PER company
- Does NOT mix companies
- Writes company into udc_inventory (NOT NULL)
"""
from loguru import logger
from sqlalchemy import func
from decimal import Decimal
from typing import Optional

from shared.database import get_db_context
from shared.database.models import PickingEvent, UDCInventory, OrderItem
from config.settings import settings


def rebuild_udc_inventory(company: Optional[str] = None):
    """
    Rebuild UDC inventory from picking events (PER COMPANY)

    This aggregates all picking events to determine current UDC contents
    """
    try:
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        logger.info(f"=== Starting UDC Inventory Rebuild [{company_key}] ===")

        with get_db_context() as db:
            # Clear existing inventory ONLY for this company
            logger.info("Clearing existing UDC inventory for company...")
            db.query(UDCInventory).filter(UDCInventory.company == company_key).delete()
            db.commit()

            # Aggregate picking events with order items (PER COMPANY)
            logger.info("Aggregating picking events with order items...")

            results = (
                db.query(
                    PickingEvent.udc,
                    OrderItem.sku,
                    OrderItem.listone,
                    func.sum(PickingEvent.qty_picked).label("total_qty"),
                )
                .join(OrderItem, PickingEvent.order_item_id == OrderItem.id)
                .filter(
                    PickingEvent.company == company_key,
                    OrderItem.company == company_key,
                    PickingEvent.udc.isnot(None),
                    OrderItem.sku.isnot(None),
                    OrderItem.listone.isnot(None),
                    PickingEvent.qty_picked > 0,
                )
                .group_by(
                    PickingEvent.udc,
                    OrderItem.sku,
                    OrderItem.listone,
                )
                .all()
            )

            logger.info(f"Found {len(results)} unique UDC+SKU+Listone combinations [{company_key}]")

            inventory_records = []
            for row in results:
                if row.total_qty and row.total_qty > 0:
                    inventory_records.append(
                        UDCInventory(
                            company=company_key,
                            udc=row.udc,
                            sku=row.sku,
                            listone=row.listone,
                            qty=Decimal(str(row.total_qty)),
                        )
                    )

            logger.info(f"Inserting {len(inventory_records)} inventory records...")
            if inventory_records:
                db.bulk_save_objects(inventory_records)
            db.commit()

            logger.info(f"✓✓✓ SUCCESS! Created {len(inventory_records)} UDC inventory records [{company_key}]")

            return {"success": True, "records_created": len(inventory_records), "company": company_key}

    except Exception as e:
        logger.error(f"Error rebuilding UDC inventory: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    result = rebuild_udc_inventory()
    print(result)
