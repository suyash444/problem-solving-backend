"""
DumpTrack CSV Importer - ENHANCED VERSION
Imports order data from DumpTrack CSV files with date range support
"""
import os
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict
from decimal import Decimal
from loguru import logger
from sqlalchemy import text

from shared.database import get_pyodbc_connection, get_db_context
from shared.database.models import (
    ImportDumptrack, Order, OrderItem, PickingEvent,
    UDCInventory, ImportLog
)
from config.settings import settings


class DumptrackImporter:
    """Handles DumpTrack CSV file imports with date range support"""
    
    def __init__(self):
        self.source_path = settings.DUMPTRACK_PATH
        
    def get_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file to detect duplicates"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def is_already_imported(self, file_hash: str) -> bool:
        """Check if file was already imported"""
        with get_db_context() as db:
            exists = db.query(ImportLog).filter(
                ImportLog.source_type == 'DUMPTRACK',
                ImportLog.file_hash == file_hash
            ).first()
            return exists is not None
    
    def find_files_in_date_range(self, start_date: date, end_date: date) -> List[str]:
        """Find DumpTrack files within a date range"""
        try:
            logger.info(f"Scanning DumpTrack folder: {self.source_path}")
            
            # List ALL files in directory
            all_files = os.listdir(self.source_path)
            logger.info(f"Found {len(all_files)} total files in folder")
            
            files_to_import = []
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                
                # Try WITHOUT extension first
                target_filename = f"DumpTrackBenetton_{date_str}"
                
                # Check if this exact filename exists
                if target_filename in all_files:
                    filepath = os.path.join(self.source_path, target_filename)
                    file_hash = self.get_file_hash(filepath)
                    
                    if not self.is_already_imported(file_hash):
                        files_to_import.append(filepath)
                        logger.info(f"✓ Found file to import (no ext): {target_filename}")
                    else:
                        logger.info(f"Already imported (no ext): {target_filename}")
                else:
                    # Try WITH .csv extension
                    target_filename_csv = f"{target_filename}.csv"
                    if target_filename_csv in all_files:
                        filepath = os.path.join(self.source_path, target_filename_csv)
                        file_hash = self.get_file_hash(filepath)
                        
                        if not self.is_already_imported(file_hash):
                            files_to_import.append(filepath)
                            logger.info(f"✓ Found file to import (.csv): {target_filename_csv}")
                        else:
                            logger.info(f"Already imported (.csv): {target_filename_csv}")
                    else:
                        logger.debug(f"File not found (tried both): {target_filename}")
                
                current_date += timedelta(days=1)
            
            logger.info(f"=== TOTAL FILES TO IMPORT: {len(files_to_import)} ===")
            return files_to_import
            
        except Exception as e:
            logger.error(f"Error finding files in date range: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def find_latest_file(self) -> Optional[str]:
        """Find the latest DumpTrack file in the directory"""
        try:
            all_files = os.listdir(self.source_path)
            
            # Filter DumpTrack files (with or without .csv)
            dumptrack_files = [f for f in all_files if f.startswith('DumpTrackBenetton_')]
            
            if not dumptrack_files:
                logger.warning("No DumpTrack files found")
                return None
            
            dumptrack_files.sort(reverse=True)
            latest_file = os.path.join(self.source_path, dumptrack_files[0])
            logger.info(f"Latest DumpTrack file: {dumptrack_files[0]}")
            return latest_file
            
        except Exception as e:
            logger.error(f"Error finding latest file: {e}")
            return None
    
    def import_date_range(self, start_date: date, end_date: date) -> Dict:
        """
        Import DumpTrack files for a date range
        Automatically rebuilds UDC inventory after completion
        """
        try:
            logger.info(f"=== Starting DumpTrack import for {start_date} to {end_date} ===")
            
            # Find files in date range
            filepaths = self.find_files_in_date_range(start_date, end_date)
            
            if not filepaths:
                return {
                    "success": True,
                    "message": "No new files to import in date range",
                    "files_imported": 0,
                    "total_records": 0
                }
            
            total_records = 0
            total_orders = 0
            total_items = 0
            files_imported = 0
            
            for idx, filepath in enumerate(filepaths, 1):
                logger.info(f"[{idx}/{len(filepaths)}] Importing: {os.path.basename(filepath)}")
                result = self.import_file(filepath)
                
                if result['success']:
                    files_imported += 1
                    total_records += result['records_imported']
                    total_orders += result.get('orders_processed', 0)
                    total_items += result.get('items_processed', 0)
                    logger.info(f"✓ Imported {result['records_imported']} records")
                else:
                    logger.error(f"✗ Failed: {result['message']}")
            
            logger.info(f"=== ✓✓✓ DumpTrack import complete! {files_imported} files ===")
            logger.info(f"Total: {total_records} records, {total_orders} orders, {total_items} items")
            
            # AUTO-REBUILD UDC INVENTORY
            logger.info("=== Auto-rebuilding UDC inventory ===")
            from services.ingestion_service.rebuild_udc_inventory import rebuild_udc_inventory
            
            rebuild_result = rebuild_udc_inventory()
            
            if rebuild_result['success']:
                logger.info(f"✓✓✓ UDC inventory rebuilt: {rebuild_result['records_created']} records")
            else:
                logger.error(f"✗ Failed to rebuild UDC inventory: {rebuild_result.get('error')}")
            
            return {
                "success": True,
                "message": f"Imported {files_imported} files with {total_records} records",
                "files_imported": files_imported,
                "total_records": total_records,
                "orders_processed": total_orders,
                "items_processed": total_items,
                "udc_inventory_rebuilt": rebuild_result['success'],
                "udc_inventory_records": rebuild_result.get('records_created', 0)
            }
            
        except Exception as e:
            logger.error(f"Error importing date range: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Import failed: {str(e)}",
                "files_imported": 0,
                "total_records": 0
            }
    
    def import_file(self, filepath: str, from_date: Optional[date] = None) -> Dict:
        """
        Import single DumpTrack CSV file
        """
        try:
            # Check if already imported
            file_hash = self.get_file_hash(filepath)
            if self.is_already_imported(file_hash):
                logger.info(f"File already imported: {filepath}")
                return {
                    "success": True,
                    "message": "File already imported (duplicate)",
                    "records_imported": 0
                }
            
            # Read CSV
            logger.info(f"Reading DumpTrack file: {filepath}")
            df = pd.read_csv(filepath, delimiter='$', encoding='utf-8')
            logger.info(f"Total rows in file: {len(df)}")
            
            # Filter by date if specified
            if from_date:
                df['DataRegistrazione'] = pd.to_datetime(df['DataRegistrazione'], errors='coerce')
                df = df[df['DataRegistrazione'] >= pd.Timestamp(from_date)]
                logger.info(f"Filtered to {len(df)} records from {from_date}")
            
            if len(df) == 0:
                return {"success": False, "message": "No records to import", "records_imported": 0}
            
            # Start import
            import_log = ImportLog(
                source_type='DUMPTRACK',
                file_path=filepath,
                file_hash=file_hash,
                file_date=self._extract_date_from_filename(os.path.basename(filepath)),
                records_imported=0,
                import_started_at=datetime.utcnow()
            )
            
            with get_db_context() as db:
                db.add(import_log)
                db.flush()
                
                logger.info("Step 1/3: Importing raw data...")
                raw_records = self._import_raw_data_fast(df, filepath, db)
                logger.info(f"✓ Raw data imported: {raw_records} records")
                
                logger.info("Step 2/3: Processing orders...")
                processed = self._process_orders_fast(df, db)
                logger.info(f"✓ Orders: {processed['orders']}, Items: {processed['items']}")
                
                logger.info("Step 3/3: Finalizing...")
                import_log.records_imported = raw_records
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            logger.info(f"✓✓✓ File imported successfully: {raw_records} records")
            
            return {
                "success": True,
                "message": f"Successfully imported {raw_records} records",
                "records_imported": raw_records,
                "orders_processed": processed['orders'],
                "items_processed": processed['items']
            }
            
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Import failed: {str(e)}", "records_imported": 0}
    
    def _import_raw_data_fast(self, df: pd.DataFrame, filepath: str, db) -> int:
        """Import raw data - FAST bulk insert without checks"""
        
        def safe_val(val, type_='str'):
            if pd.isna(val) or val == '':
                return None
            if type_ == 'int':
                try: return int(float(val))
                except: return None
            elif type_ == 'float':
                try: return float(val)
                except: return None
            elif type_ == 'datetime':
                try:
                    dt = pd.to_datetime(val, errors='coerce')
                    return dt.to_pydatetime() if pd.notna(dt) else None
                except: return None
            else:
                return str(val)
        
        records = []
        for idx, row in df.iterrows():
            if idx % 10000 == 0 and idx > 0:
                logger.info(f"  Processing row {idx}/{len(df)}...")
            
            records.append(ImportDumptrack(
                Batch=safe_val(row.get('Batch'), 'int'),
                OrdinePrivalia=safe_val(row.get('OrdinePrivalia')),
                DataRegistrazione=safe_val(row.get('DataRegistrazione'), 'datetime'),
                nLista=safe_val(row.get('nLista'), 'int'),
                CodiceArticolo=safe_val(row.get('CodiceArticolo')),
                QtaRichiestaTotale=safe_val(row.get('QtaRichiestaTotale'), 'float'),
                QtaPrelevata=safe_val(row.get('QtaPrelevata'), 'float'),
                nListaComposta=safe_val(row.get('nListaComposta'), 'int'),
                Commessa=safe_val(row.get('Commessa')),
                Utente=safe_val(row.get('Utente')),
                DataPrelievo=safe_val(row.get('DataPrelievo'), 'datetime'),
                UDC=safe_val(row.get('UDC')),
                NCollo=safe_val(row.get('NCollo'), 'int'),
                CodiceImballo=safe_val(row.get('CodiceImballo')),
                DataOraArrivoPrivalia=safe_val(row.get('DataOraArrivoPrivalia'), 'datetime'),
                LetteraVettura=safe_val(row.get('LetteraVettura')),
                Vettore=safe_val(row.get('Vettore')),
                DataStampa=safe_val(row.get('DataStampa'), 'datetime'),
                CodiceProprieta=safe_val(row.get('CodiceProprieta')),
                StatoArticolo=safe_val(row.get('StatoArticolo')),
                Uds=safe_val(row.get('Uds')),
                source_file=filepath
            ))
        
        # Bulk insert
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            db.bulk_save_objects(batch)
            if i % 10000 == 0 and i > 0:
                logger.info(f"  Inserted {i} records...")
        
        db.flush()
        return len(records)
    
    def _process_orders_fast(self, df: pd.DataFrame, db) -> Dict:
        """Process orders - FAST version with minimal queries"""
        
        # Clean dataframe
        df = df[df['OrdinePrivalia'].notna()]
        df = df[df['nLista'].notna()]
        df = df[df['CodiceArticolo'].notna()]
        
        # Aggregate data BEFORE inserting
        logger.info("  Aggregating orders...")
        orders_data = df.groupby('OrdinePrivalia').first().reset_index()
        
        logger.info(f"  Inserting {len(orders_data)} orders...")
        order_map = {}
        for _, row in orders_data.iterrows():
            order_num = str(row['OrdinePrivalia'])
            try:
                order = Order(
                    order_number=order_num,
                    data_registrazione=pd.to_datetime(row['DataRegistrazione'], errors='coerce').to_pydatetime() if pd.notna(row.get('DataRegistrazione')) else None,
                    commessa=str(row['Commessa']) if pd.notna(row.get('Commessa')) else None,
                    codice_proprieta=str(row['CodiceProprieta']) if pd.notna(row.get('CodiceProprieta')) else None
                )
                db.add(order)
                db.flush()
                order_map[order_num] = order.id
            except:
                pass
        
        db.commit()
        logger.info(f"  ✓ {len(order_map)} orders inserted")
        
        # Aggregate order items
        logger.info("  Aggregating order items...")
        items_data = df.groupby(['OrdinePrivalia', 'nLista', 'CodiceArticolo']).agg({
            'QtaRichiestaTotale': 'first',
            'nListaComposta': 'first',
            'CodiceImballo': 'first'
        }).reset_index()
        
        logger.info(f"  Inserting {len(items_data)} order items...")
        item_count = 0
        for _, row in items_data.iterrows():
            order_num = str(row['OrdinePrivalia'])
            if order_num in order_map:
                try:
                    item = OrderItem(
                        order_id=order_map[order_num],
                        n_lista=int(row['nLista']),
                        listone=int(row['nListaComposta']) if pd.notna(row['nListaComposta']) else None,
                        sku=str(row['CodiceArticolo']),
                        qty_ordered=Decimal(str(row['QtaRichiestaTotale'])) if pd.notna(row['QtaRichiestaTotale']) else Decimal('0'),
                        cesta=str(row['CodiceImballo']) if pd.notna(row['CodiceImballo']) else None
                    )
                    db.add(item)
                    item_count += 1
                    
                    if item_count % 1000 == 0:
                        db.flush()
                        logger.info(f"    Inserted {item_count} items...")
                except:
                    pass
        
        db.commit()
        logger.info(f"  ✓ {item_count} order items inserted")
        
        return {"orders": len(order_map), "items": item_count}
    
    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from filename"""
        try:
            # Remove .csv if present
            filename = filename.replace('.csv', '')
            date_str = filename.replace('DumpTrackBenetton_', '')
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def import_latest(self, from_date: Optional[date] = None) -> Dict:
        """Find and import latest file, then rebuild UDC inventory"""
        filepath = self.find_latest_file()
        if not filepath:
            return {"success": False, "message": "No file found", "records_imported": 0}
        
        result = self.import_file(filepath, from_date)
        
        if result['success'] and result['records_imported'] > 0:
            # Auto-rebuild UDC inventory
            logger.info("=== Auto-rebuilding UDC inventory ===")
            from services.ingestion_service.rebuild_udc_inventory import rebuild_udc_inventory
            
            rebuild_result = rebuild_udc_inventory()
            
            if rebuild_result['success']:
                logger.info(f"✓✓✓ UDC inventory rebuilt: {rebuild_result['records_created']} records")
                result['udc_inventory_rebuilt'] = True
                result['udc_inventory_records'] = rebuild_result['records_created']
            else:
                logger.error(f"✗ Failed to rebuild UDC inventory: {rebuild_result.get('error')}")
                result['udc_inventory_rebuilt'] = False
        
        return result