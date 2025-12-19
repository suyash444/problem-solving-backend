"""
API Client for external PowerStore APIs - FIXED VERSION WITH DUPLICATE HANDLING
Handles PrelievoPowerSort and GetSpedito2 API calls

FIX: Skips duplicate records when importing overlapping date ranges
FIX2: GetSpedito2 now also skips duplicates in shipped_items table
"""
import requests
from datetime import date, datetime
from typing import Optional, List, Dict, Set
from decimal import Decimal
from loguru import logger
from sqlalchemy import text

from shared.database import get_db_context
from shared.database.models import (
    ImportPrelievo, ImportSpedito, ImportLog,
    PickingEvent, OrderItem, ShippedItem
)
from config.settings import settings


class PowerStoreAPIClient:
    """Client for PowerStore API endpoints - FIXED VERSION WITH DUPLICATE HANDLING"""
    
    def __init__(self):
        self.base_url = settings.ORDERS_API_BASE_URL
        self.bearer_token = settings.BEARER_TOKEN
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
    
    def call_prelievo_powersort(
        self, 
        start_date: date, 
        end_date: date
    ) -> Dict:
        """
        Call PrelievoPowerSort API - FIXED VERSION WITH DUPLICATE HANDLING
        Now skips duplicate records instead of failing
        """
        try:
            # NOTE: We removed the "already imported" check because
            # we want to allow re-importing overlapping date ranges
            # and just skip the duplicate records
            
            # Call API
            endpoint = f"{self.base_url}/Utility/PrelievoPowerSort"
            params = {
                "Inizio": start_date.strftime('%Y-%m-%d'),
                "Fine": end_date.strftime('%Y-%m-%d')
            }
            
            logger.info(f"Calling PrelievoPowerSort API: {start_date} to {end_date}")
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or len(data) == 0:
                return {
                    "success": True,
                    "message": "No data returned from API",
                    "records_imported": 0
                }
            
            logger.info(f"Received {len(data)} records from API")
            
            # Start import
            import_log = ImportLog(
                source_type='PRELIEVO',
                file_path=f"{endpoint}?Inizio={start_date}&Fine={end_date}",
                file_hash=f"PRELIEVO_{start_date}_{end_date}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                file_date=start_date,
                records_imported=0,
                import_started_at=datetime.utcnow()
            )
            
            with get_db_context() as db:
                db.add(import_log)
                db.flush()
                
                # Import raw data (SKIP DUPLICATES)
                logger.info("Step 1/2: Importing raw data (with duplicate check)...")
                raw_result = self._import_prelievo_raw_skip_duplicates(data, db)
                logger.info(f"✓ Imported {raw_result['inserted']} new records, skipped {raw_result['skipped']} duplicates")
                
                # Create picking events (SKIP DUPLICATES)
                logger.info("Step 2/2: Creating picking events (with duplicate check)...")
                events_result = self._create_picking_events_skip_duplicates(data, db)
                logger.info(f"✓ Created {events_result['inserted']} new events, skipped {events_result['skipped']} duplicates")
                
                # Update import log
                import_log.records_imported = raw_result['inserted']
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            logger.info(f"✓✓✓ SUCCESS! Imported {raw_result['inserted']} new records from PrelievoPowerSort")
            
            from services.ingestion_service.rebuild_udc_inventory import rebuild_udc_inventory
            rebuild_result = rebuild_udc_inventory()
            
            if rebuild_result.get('success'):
                logger.info(f"✓✓✓ UDC inventory rebuilt: {rebuild_result.get('records_created', 0)} records")
            else:
                logger.error(f"✗ Failed to rebuild UDC inventory: {rebuild_result.get('error')}")
            
            return {
                "success": True,
                "message": f"Successfully imported {raw_result['inserted']} new records ({raw_result['skipped']} duplicates skipped)",
                "records_imported": raw_result['inserted'],
                "records_skipped": raw_result['skipped'],
                "picking_events_created": events_result['inserted'],
                "picking_events_skipped": events_result['skipped'],
                "udc_inventory_rebuilt": rebuild_result.get("success", False),
                "udc_inventory_records": rebuild_result.get("records_created", 0)
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {
                "success": False,
                "message": f"API request failed: {str(e)}",
                "records_imported": 0
            }
        except Exception as e:
            logger.error(f"Error calling PrelievoPowerSort: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Import failed: {str(e)}",
                "records_imported": 0
            }
    
    def _import_prelievo_raw_skip_duplicates(self, data: List[Dict], db) -> Dict:
        """
        Import raw PrelievoPowerSort data - SKIP DUPLICATES
        Unique key: Listone + UDC + CodiceArticolo + DataPrelievo
        """
        
        def safe_datetime(dt_str):
            if not dt_str:
                return None
            try:
                formats = ['%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M']
                for fmt in formats:
                    try:
                        return datetime.strptime(dt_str, fmt)
                    except:
                        continue
                return None
            except:
                return None
        
        # Load existing records for duplicate check
        logger.info("  Loading existing Prelievo records...")
        existing_keys: Set[tuple] = set()
        
        try:
            existing_records = db.execute(text("""
                SELECT DISTINCT 
                    Listone,
                    UDC, 
                    CodiceArticolo,
                    CONVERT(VARCHAR(20), DataPrelievo, 120) as DataPrelievoStr
                FROM import_prelievo_powersort
                WHERE Listone IS NOT NULL AND UDC IS NOT NULL
            """)).fetchall()
            
            for row in existing_records:
                key = (
                    str(row[0]) if row[0] else '',
                    str(row[1]) if row[1] else '',
                    str(row[2]) if row[2] else '',
                    str(row[3]) if row[3] else ''
                )
                existing_keys.add(key)
        except Exception as e:
            logger.warning(f"  Could not load existing keys: {e}")
        
        logger.info(f"  Found {len(existing_keys)} existing unique records")
        
        records_to_insert = []
        skipped_count = 0
        
        for item in data:
            # Create unique key
            listone = str(item.get('Listone', '')) if item.get('Listone') else ''
            udc = str(item.get('UDC', '')) if item.get('UDC') else ''
            codice = str(item.get('CodiceArticolo', '')) if item.get('CodiceArticolo') else ''
            data_prelievo = ''
            dt = safe_datetime(item.get('DataPrelievo'))
            if dt:
                data_prelievo = dt.strftime('%Y-%m-%d %H:%M:%S')
            
            key = (listone, udc, codice, data_prelievo)
            
            # Skip if exists
            if key in existing_keys:
                skipped_count += 1
                continue
            
            existing_keys.add(key)
            
            records_to_insert.append(ImportPrelievo(
                Listone=item.get('Listone'),
                Carrello=item.get('Carrello'),
                UDC=item.get('UDC'),
                CodiceArticolo=item.get('CodiceArticolo'),
                Descrizione=item.get('Descrizione'),
                Quantita=item.get('Quantita'),
                Utente=item.get('Utente'),
                DataPrelievo=dt,
                CodiceProprieta=item.get('CodiceProprieta'),
                Azienda=item.get('Azienda')
            ))
        
        # Bulk insert
        if records_to_insert:
            batch_size = 1000
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i+batch_size]
                db.bulk_save_objects(batch)
            db.flush()
        
        return {"inserted": len(records_to_insert), "skipped": skipped_count}
    
    def _create_picking_events_skip_duplicates(self, data: List[Dict], db) -> Dict:
        """
        Create picking events - SKIP DUPLICATES
        Unique key: order_item_id + udc + picked_at
        """
        
        def safe_datetime(dt_str):
            if not dt_str:
                return None
            try:
                formats = ['%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M']
                for fmt in formats:
                    try:
                        return datetime.strptime(dt_str, fmt)
                    except:
                        continue
                return None
            except:
                return None
        
        # Build a map of (listone, sku) -> order_item_ids
        logger.info("  Building order item index...")
        order_items = db.query(OrderItem).filter(
            OrderItem.listone.isnot(None),
            OrderItem.sku.isnot(None)
        ).all()
        
        item_map: Dict[tuple, List[int]] = {}
        for item in order_items:
            key = (item.listone, item.sku)
            if key not in item_map:
                item_map[key] = []
            item_map[key].append(item.id)
        
        logger.info(f"  Found {len(item_map)} unique listone/sku combinations")
        
        # Load existing picking events for duplicate check
        logger.info("  Loading existing picking events...")
        existing_events: Set[tuple] = set()
        
        try:
            existing_records = db.execute(text("""
                SELECT DISTINCT 
                    order_item_id,
                    udc,
                    CONVERT(VARCHAR(20), picked_at, 120) as PickedAtStr
                FROM picking_events
                WHERE order_item_id IS NOT NULL AND udc IS NOT NULL
            """)).fetchall()
            
            for row in existing_records:
                key = (
                    int(row[0]) if row[0] else 0,
                    str(row[1]) if row[1] else '',
                    str(row[2]) if row[2] else ''
                )
                existing_events.add(key)
        except Exception as e:
            logger.warning(f"  Could not load existing events: {e}")
        
        logger.info(f"  Found {len(existing_events)} existing picking events")
        
        # Create picking events
        events_to_insert = []
        skipped_count = 0
        
        for item in data:
            listone = item.get('Listone')
            sku = item.get('CodiceArticolo')
            udc = item.get('UDC')
            
            if not listone or not sku or not udc:
                continue
            
            key = (listone, sku)
            if key not in item_map:
                continue
            
            picked_at = safe_datetime(item.get('DataPrelievo'))
            picked_at_str = picked_at.strftime('%Y-%m-%d %H:%M:%S') if picked_at else ''
            
            # Create event for each matching order item
            for order_item_id in item_map[key]:
                event_key = (order_item_id, str(udc), picked_at_str)
                
                # Skip if exists
                if event_key in existing_events:
                    skipped_count += 1
                    continue
                
                existing_events.add(event_key)
                
                events_to_insert.append(PickingEvent(
                    order_item_id=order_item_id,
                    udc=udc,
                    carrello=item.get('Carrello'),
                    qty_picked=Decimal(str(item.get('Quantita', 0))) if item.get('Quantita') else Decimal('0'),
                    operator=item.get('Utente'),
                    picked_at=picked_at
                ))
        
        logger.info(f"  Inserting {len(events_to_insert)} picking events...")
        
        # Bulk insert in batches
        if events_to_insert:
            batch_size = 1000
            for i in range(0, len(events_to_insert), batch_size):
                batch = events_to_insert[i:i+batch_size]
                db.bulk_save_objects(batch)
                db.flush()
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"    Inserted {i + len(batch)} events...")
        
        return {"inserted": len(events_to_insert), "skipped": skipped_count}
    
    def call_get_spedito2(self, cesta: str) -> Dict:
        """
        Call GetSpedito2 API to get shipped items for a basket
        
        FIXED: Now skips duplicate records in shipped_items and import_spedito tables
        Unique key: cesta + n_ordine + n_lista + sku
        """
        try:
            endpoint = f"{self.base_url}/Orders/GetSpedito2"
            params = {
                "Barcode": "",
                "Cesta": cesta
            }
            
            logger.info(f"Calling GetSpedito2 API for cesta: {cesta}")
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or 'Spedito' not in data:
                return {
                    "success": False,
                    "message": "No data returned from API",
                    "data": []
                }
            
            spedito_items = data.get('Spedito', [])
            
            def safe_datetime(dt_str):
                if not dt_str:
                    return None
                try:
                    formats = ['%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M']
                    for fmt in formats:
                        try:
                            return datetime.strptime(dt_str, fmt)
                        except:
                            continue
                    return None
                except:
                    return None
            
            # Import into database with DUPLICATE CHECK
            with get_db_context() as db:
                # Load existing shipped items for this cesta
                logger.info(f"  Loading existing shipped items for cesta {cesta}...")
                existing_shipped: Set[tuple] = set()
                
                try:
                    existing_records = db.execute(text("""
                        SELECT DISTINCT cesta, n_ordine, n_lista, sku
                        FROM shipped_items
                        WHERE cesta = :cesta
                    """), {"cesta": cesta}).fetchall()
                    
                    for row in existing_records:
                        key = (
                            str(row[0]) if row[0] else '',
                            str(row[1]) if row[1] else '',
                            str(row[2]) if row[2] else '',
                            str(row[3]) if row[3] else ''
                        )
                        existing_shipped.add(key)
                except Exception as e:
                    logger.warning(f"  Could not load existing shipped items: {e}")
                
                logger.info(f"  Found {len(existing_shipped)} existing shipped items")
                
                # Load existing import_spedito records
                existing_raw: Set[tuple] = set()
                
                try:
                    existing_raw_records = db.execute(text("""
                        SELECT DISTINCT Cesta, nOrdine, nLista, CodiceArticolo
                        FROM import_spedito
                        WHERE Cesta = :cesta
                    """), {"cesta": cesta}).fetchall()
                    
                    for row in existing_raw_records:
                        key = (
                            str(row[0]) if row[0] else '',
                            str(row[1]) if row[1] else '',
                            str(row[2]) if row[2] else '',
                            str(row[3]) if row[3] else ''
                        )
                        existing_raw.add(key)
                except Exception as e:
                    logger.warning(f"  Could not load existing raw spedito: {e}")
                
                inserted_shipped = 0
                skipped_shipped = 0
                inserted_raw = 0
                skipped_raw = 0
                
                for item in spedito_items:
                    n_ordine = str(item.get('nOrdine', '')) if item.get('nOrdine') else ''
                    n_lista = str(item.get('nLista', '')) if item.get('nLista') else ''
                    sku = str(item.get('CodiceArticolo', '')) if item.get('CodiceArticolo') else ''
                    
                    shipped_key = (cesta, n_ordine, n_lista, sku)
                    
                    # Insert shipped_item only if not exists
                    if shipped_key not in existing_shipped:
                        shipped_item = ShippedItem(
                            cesta=cesta,
                            n_ordine=item.get('nOrdine'),
                            n_lista=item.get('nLista'),
                            sku=item.get('CodiceArticolo'),
                            qty_shipped=Decimal(str(item.get('Quantita', 0))) if item.get('Quantita') else Decimal('0'),
                            descrizione=item.get('Descrizione'),
                            sovracollo=item.get('Sovracollo'),
                            vettore=item.get('Vettore'),
                            shipped_at=safe_datetime(item.get('DataOra'))
                        )
                        db.add(shipped_item)
                        existing_shipped.add(shipped_key)
                        inserted_shipped += 1
                    else:
                        skipped_shipped += 1
                    
                    # Insert import_spedito only if not exists
                    if shipped_key not in existing_raw:
                        raw_record = ImportSpedito(
                            CodiceProprieta=item.get('CodiceProprieta'),
                            Azienda=item.get('Azienda'),
                            Vettore=item.get('Vettore'),
                            Sovracollo=item.get('Sovracollo'),
                            nOrdine=item.get('nOrdine'),
                            nLista=item.get('nLista'),
                            CodiceArticolo=item.get('CodiceArticolo'),
                            Descrizione=item.get('Descrizione'),
                            Quantita=item.get('Quantita'),
                            Cesta=cesta,
                            CodiceLetto=item.get('CodiceLetto'),
                            DataOra=safe_datetime(item.get('DataOra'))
                        )
                        db.add(raw_record)
                        existing_raw.add(shipped_key)
                        inserted_raw += 1
                    else:
                        skipped_raw += 1
                
                db.commit()
            
            logger.info(f"✓ GetSpedito2 for cesta {cesta}:")
            logger.info(f"  shipped_items: {inserted_shipped} new, {skipped_shipped} skipped")
            logger.info(f"  import_spedito: {inserted_raw} new, {skipped_raw} skipped")
            
            return {
                "success": True,
                "message": f"Found {len(spedito_items)} shipped items ({inserted_shipped} new, {skipped_shipped} duplicates)",
                "data": spedito_items,
                "inserted": inserted_shipped,
                "skipped": skipped_shipped
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {
                "success": False,
                "message": f"API request failed: {str(e)}",
                "data": []
            }
        except Exception as e:
            logger.error(f"Error calling GetSpedito2: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "data": []
            }
