"""
API Client for external PowerStore APIs - FAST VERSION
Handles PrelievoPowerSort and GetSpedito2 API calls
"""
import requests
from datetime import date, datetime
from typing import Optional, List, Dict
from decimal import Decimal
from loguru import logger

from shared.database import get_db_context
from shared.database.models import (
    ImportPrelievo, ImportSpedito, ImportLog,
    PickingEvent, OrderItem, ShippedItem
)
from config.settings import settings


class PowerStoreAPIClient:
    """Client for PowerStore API endpoints - FAST VERSION"""
    
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
        Call PrelievoPowerSort API - FAST VERSION
        """
        try:
            # Check if already imported
            with get_db_context() as db:
                existing = db.query(ImportLog).filter(
                    ImportLog.source_type == 'PRELIEVO',
                    ImportLog.file_date >= start_date,
                    ImportLog.file_date <= end_date,
                    ImportLog.status == 'SUCCESS'
                ).first()
                
                if existing:
                    logger.info(f"Date range {start_date} to {end_date} already imported")
                    return {
                        "success": True,
                        "message": "Date range already imported (duplicate)",
                        "records_imported": 0
                    }
            
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
                file_path=f"{endpoint}?{params}",
                file_hash=f"PRELIEVO_{start_date}_{end_date}",
                file_date=start_date,
                records_imported=0,
                import_started_at=datetime.utcnow()
            )
            
            with get_db_context() as db:
                db.add(import_log)
                db.flush()
                
                # Import raw data (FAST)
                logger.info("Step 1/2: Importing raw data...")
                raw_records = self._import_prelievo_raw_fast(data, db)
                logger.info(f"✓ Imported {raw_records} raw records")
                
                # Enrich order items (FAST)
                logger.info("Step 2/2: Creating picking events...")
                enriched = self._create_picking_events_fast(data, db)
                logger.info(f"✓ Created {enriched} picking events")
                
                # Update import log
                import_log.records_imported = raw_records
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            logger.info(f"✓✓✓ SUCCESS! Imported {raw_records} records from PrelievoPowerSort")
            
            return {
                "success": True,
                "message": f"Successfully imported {raw_records} records",
                "records_imported": raw_records,
                "picking_events_created": enriched
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
            return {
                "success": False,
                "message": f"Import failed: {str(e)}",
                "records_imported": 0
            }
    
    def _import_prelievo_raw_fast(self, data: List[Dict], db) -> int:
        """Import raw PrelievoPowerSort data - FAST bulk insert"""
        
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
        
        records = []
        for item in data:
            records.append(ImportPrelievo(
                Listone=item.get('Listone'),
                Carrello=item.get('Carrello'),
                UDC=item.get('UDC'),
                CodiceArticolo=item.get('CodiceArticolo'),
                Descrizione=item.get('Descrizione'),
                Quantita=item.get('Quantita'),
                Utente=item.get('Utente'),
                DataPrelievo=safe_datetime(item.get('DataPrelievo')),
                CodiceProprieta=item.get('CodiceProprieta'),
                Azienda=item.get('Azienda')
            ))
        
        # Bulk insert
        db.bulk_save_objects(records)
        db.flush()
        return len(records)
    
    def _create_picking_events_fast(self, data: List[Dict], db) -> int:
        """Create picking events - FAST version without duplicate checks"""
        
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
        
        # Build a map of (listone, sku) -> order_item_id
        logger.info("  Building order item index...")
        order_items = db.query(OrderItem).all()
        item_map = {}
        for item in order_items:
            if item.listone and item.sku:
                key = (item.listone, item.sku)
                if key not in item_map:
                    item_map[key] = []
                item_map[key].append(item.id)
        
        logger.info(f"  Found {len(item_map)} unique listone/sku combinations")
        
        # Create picking events in bulk
        events = []
        for item in data:
            listone = item.get('Listone')
            sku = item.get('CodiceArticolo')
            udc = item.get('UDC')
            
            if not listone or not sku or not udc:
                continue
            
            key = (listone, sku)
            if key in item_map:
                # Create picking event for each matching order item
                for order_item_id in item_map[key]:
                    events.append(PickingEvent(
                        order_item_id=order_item_id,
                        udc=udc,
                        carrello=item.get('Carrello'),
                        qty_picked=Decimal(str(item.get('Quantita', 0))) if item.get('Quantita') else Decimal('0'),
                        operator=item.get('Utente'),
                        picked_at=safe_datetime(item.get('DataPrelievo'))
                    ))
        
        logger.info(f"  Inserting {len(events)} picking events...")
        
        # Bulk insert in batches
        batch_size = 1000
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            db.bulk_save_objects(batch)
            db.flush()
            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"    Inserted {i + len(batch)} events...")
        
        return len(events)
    
    def call_get_spedito2(self, cesta: str) -> Dict:
        """
        Call GetSpedito2 API to get shipped items for a basket
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
            
            # Import into database
            with get_db_context() as db:
                for item in spedito_items:
                    # Import to shipped_items table
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
                    
                    # Also import to raw table
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
                
                db.commit()
            
            logger.info(f"Successfully imported {len(spedito_items)} shipped items for cesta {cesta}")
            
            return {
                "success": True,
                "message": f"Found {len(spedito_items)} shipped items",
                "data": spedito_items
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
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "data": []
            }