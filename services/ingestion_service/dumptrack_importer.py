"""
DumpTrack CSV Importer - FIXED VERSION WITH DUPLICATE HANDLING
Imports order data from DumpTrack CSV files with date range support

FIX: Skips duplicate records when importing overlapping data from daily files
"""
import os
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Set
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
    """Handles DumpTrack CSV file imports with date range support and duplicate handling"""
    
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
            
            all_files = os.listdir(self.source_path)
            logger.info(f"Found {len(all_files)} total files in folder")
            
            files_to_import = []
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                target_filename = f"DumpTrackBenetton_{date_str}"
                
                if target_filename in all_files:
                    filepath = os.path.join(self.source_path, target_filename)
                    file_hash = self.get_file_hash(filepath)
                    
                    if not self.is_already_imported(file_hash):
                        files_to_import.append(filepath)
                        logger.info(f"✓ Found file to import (no ext): {target_filename}")
                    else:
                        logger.info(f"Already imported (no ext): {target_filename}")
                else:
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
        """Import DumpTrack files for a date range with duplicate handling"""
        try:
            logger.info(f"=== Starting DumpTrack import for {start_date} to {end_date} ===")
            
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
            total_skipped = 0
            files_imported = 0
            
            for idx, filepath in enumerate(filepaths, 1):
                logger.info(f"[{idx}/{len(filepaths)}] Importing: {os.path.basename(filepath)}")
                result = self.import_file(filepath)
                
                if result['success']:
                    files_imported += 1
                    total_records += result['records_imported']
                    total_orders += result.get('orders_processed', 0)
                    total_items += result.get('items_processed', 0)
                    total_skipped += result.get('records_skipped', 0)
                    logger.info(f"✓ Imported {result['records_imported']} records, skipped {result.get('records_skipped', 0)} duplicates")
                else:
                    logger.error(f"✗ Failed: {result['message']}")
            
            logger.info(f"=== ✓✓✓ DumpTrack import complete! {files_imported} files ===")
            logger.info(f"Total: {total_records} new records, {total_skipped} duplicates skipped")
            
            return {
                "success": True,
                "message": f"Imported {files_imported} files with {total_records} new records ({total_skipped} duplicates skipped)",
                "files_imported": files_imported,
                "total_records": total_records,
                "records_skipped": total_skipped,
                "orders_processed": total_orders,
                "items_processed": total_items
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
        """Import single DumpTrack CSV file with duplicate handling"""
        try:
            file_hash = self.get_file_hash(filepath)
            if self.is_already_imported(file_hash):
                logger.info(f"File already imported: {filepath}")
                return {
                    "success": True,
                    "message": "File already imported (duplicate)",
                    "records_imported": 0,
                    "records_skipped": 0
                }
            
            logger.info(f"Reading DumpTrack file: {filepath}")
            df = pd.read_csv(filepath, delimiter='$', encoding='utf-8')
            logger.info(f"Total rows in file: {len(df)}")
            
            if from_date:
                df['DataRegistrazione'] = pd.to_datetime(df['DataRegistrazione'], errors='coerce')
                df = df[df['DataRegistrazione'] >= pd.Timestamp(from_date)]
                logger.info(f"Filtered to {len(df)} records from {from_date}")
            
            if len(df) == 0:
                return {"success": False, "message": "No records to import", "records_imported": 0, "records_skipped": 0}
            
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
                
                logger.info("Step 1/3: Importing raw data (with duplicate check)...")
                raw_result = self._import_raw_data_skip_duplicates(df, filepath, db)
                logger.info(f"✓ Raw data: {raw_result['inserted']} new, {raw_result['skipped']} skipped")
                
                logger.info("Step 2/3: Processing orders (with duplicate check)...")
                processed = self._process_orders_skip_duplicates(df, db)
                logger.info(f"✓ Orders: {processed['orders_new']} new ({processed['orders_skipped']} existing), Items: {processed['items_new']} new ({processed['items_skipped']} existing)")
                
                logger.info("Step 3/3: Finalizing...")
                import_log.records_imported = raw_result['inserted']
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            logger.info(f"✓✓✓ File imported successfully: {raw_result['inserted']} new records")
            
            return {
                "success": True,
                "message": f"Successfully imported {raw_result['inserted']} new records ({raw_result['skipped']} duplicates skipped)",
                "records_imported": raw_result['inserted'],
                "records_skipped": raw_result['skipped'],
                "orders_processed": processed['orders_new'],
                "items_processed": processed['items_new']
            }
            
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Import failed: {str(e)}", "records_imported": 0, "records_skipped": 0}
    
    def _import_raw_data_skip_duplicates(self, df: pd.DataFrame, filepath: str, db) -> Dict:
        """
        Import raw data - SKIP DUPLICATES based on unique key
        Unique key: OrdinePrivalia + nLista + CodiceArticolo + DataRegistrazione
        """
        
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
        
        # Get existing records to check for duplicates
        logger.info("  Loading existing records for duplicate check...")
        existing_keys = set()
        
        # Query existing unique keys from import_dumptrack
        existing_records = db.execute(text("""
            SELECT DISTINCT 
                OrdinePrivalia, 
                nLista, 
                CodiceArticolo, 
                CONVERT(VARCHAR(10), DataRegistrazione, 120) as DataReg
            FROM import_dumptrack
            WHERE OrdinePrivalia IS NOT NULL
        """)).fetchall()
        
        for row in existing_records:
            key = (str(row[0]) if row[0] else '', 
                   str(row[1]) if row[1] else '', 
                   str(row[2]) if row[2] else '',
                   str(row[3]) if row[3] else '')
            existing_keys.add(key)
        
        logger.info(f"  Found {len(existing_keys)} existing unique records")
        
        records_to_insert = []
        skipped_count = 0
        
        for idx, row in df.iterrows():
            if idx % 10000 == 0 and idx > 0:
                logger.info(f"  Processing row {idx}/{len(df)}...")
            
            # Create unique key for this record
            ordine = str(row.get('OrdinePrivalia', '')) if pd.notna(row.get('OrdinePrivalia')) else ''
            n_lista = str(int(row.get('nLista'))) if pd.notna(row.get('nLista')) else ''
            codice = str(row.get('CodiceArticolo', '')) if pd.notna(row.get('CodiceArticolo')) else ''
            data_reg = ''
            if pd.notna(row.get('DataRegistrazione')):
                try:
                    dt = pd.to_datetime(row.get('DataRegistrazione'), errors='coerce')
                    if pd.notna(dt):
                        data_reg = dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            key = (ordine, n_lista, codice, data_reg)
            
            # Skip if already exists
            if key in existing_keys:
                skipped_count += 1
                continue
            
            # Add to existing keys to prevent duplicates within same file
            existing_keys.add(key)
            
            records_to_insert.append(ImportDumptrack(
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
        
        # Bulk insert new records
        if records_to_insert:
            batch_size = 1000
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i+batch_size]
                db.bulk_save_objects(batch)
                if i % 10000 == 0 and i > 0:
                    logger.info(f"  Inserted {i} records...")
            
            db.flush()
        
        logger.info(f"  Inserted {len(records_to_insert)} new records, skipped {skipped_count} duplicates")
        return {"inserted": len(records_to_insert), "skipped": skipped_count}
    
    def _process_orders_skip_duplicates(self, df: pd.DataFrame, db) -> Dict:
        """Process orders - SKIP DUPLICATES version"""
        
        df = df[df['OrdinePrivalia'].notna()]
        df = df[df['nLista'].notna()]
        df = df[df['CodiceArticolo'].notna()]
        
        # Get existing order numbers
        logger.info("  Loading existing orders...")
        existing_orders = set()
        existing_order_rows = db.execute(text("SELECT order_number FROM orders")).fetchall()
        for row in existing_order_rows:
            existing_orders.add(str(row[0]))
        logger.info(f"  Found {len(existing_orders)} existing orders")
        
        # Get existing order items (order_id, n_lista, sku)
        logger.info("  Loading existing order items...")
        existing_items = set()
        existing_item_rows = db.execute(text("""
            SELECT o.order_number, oi.n_lista, oi.sku 
            FROM order_items oi 
            JOIN orders o ON oi.order_id = o.id
        """)).fetchall()
        for row in existing_item_rows:
            key = (str(row[0]), str(row[1]), str(row[2]))
            existing_items.add(key)
        logger.info(f"  Found {len(existing_items)} existing order items")
        
        # Aggregate orders data
        logger.info("  Aggregating orders...")
        orders_data = df.groupby('OrdinePrivalia').first().reset_index()
        
        order_map = {}
        orders_new = 0
        orders_skipped = 0
        
        for _, row in orders_data.iterrows():
            order_num = str(row['OrdinePrivalia'])
            
            if order_num in existing_orders:
                # Get existing order ID
                existing_order = db.execute(
                    text("SELECT id FROM orders WHERE order_number = :num"),
                    {"num": order_num}
                ).fetchone()
                if existing_order:
                    order_map[order_num] = existing_order[0]
                orders_skipped += 1
                continue
            
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
                existing_orders.add(order_num)
                orders_new += 1
            except Exception as e:
                logger.debug(f"  Error inserting order {order_num}: {e}")
                pass
        
        db.commit()
        logger.info(f"  ✓ Orders: {orders_new} new, {orders_skipped} existing")
        
        # Aggregate order items
        logger.info("  Aggregating order items...")
        items_data = df.groupby(['OrdinePrivalia', 'nLista', 'CodiceArticolo']).agg({
            'QtaRichiestaTotale': 'first',
            'nListaComposta': 'first',
            'CodiceImballo': 'first'
        }).reset_index()
        
        items_new = 0
        items_skipped = 0
        
        for _, row in items_data.iterrows():
            order_num = str(row['OrdinePrivalia'])
            n_lista = str(int(row['nLista']))
            sku = str(row['CodiceArticolo'])
            
            # Check if item already exists
            item_key = (order_num, n_lista, sku)
            if item_key in existing_items:
                items_skipped += 1
                continue
            
            if order_num not in order_map:
                continue
            
            try:
                item = OrderItem(
                    order_id=order_map[order_num],
                    n_lista=int(row['nLista']),
                    listone=int(row['nListaComposta']) if pd.notna(row['nListaComposta']) else None,
                    sku=sku,
                    qty_ordered=Decimal(str(row['QtaRichiestaTotale'])) if pd.notna(row['QtaRichiestaTotale']) else Decimal('0'),
                    cesta=str(row['CodiceImballo']) if pd.notna(row['CodiceImballo']) else None
                )
                db.add(item)
                existing_items.add(item_key)
                items_new += 1
                
                if items_new % 1000 == 0:
                    db.flush()
                    logger.info(f"    Inserted {items_new} items...")
            except Exception as e:
                logger.debug(f"  Error inserting item: {e}")
                pass
        
        db.commit()
        logger.info(f"  ✓ Items: {items_new} new, {items_skipped} existing")
        
        return {
            "orders_new": orders_new, 
            "orders_skipped": orders_skipped,
            "items_new": items_new, 
            "items_skipped": items_skipped
        }
    
    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from filename"""
        try:
            filename = filename.replace('.csv', '')
            date_str = filename.replace('DumpTrackBenetton_', '')
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def import_latest(self, from_date: Optional[date] = None) -> Dict:
        """Find and import latest file"""
        filepath = self.find_latest_file()
        if not filepath:
            return {"success": False, "message": "No file found", "records_imported": 0}
        
        result = self.import_file(filepath, from_date)
        
        return result
