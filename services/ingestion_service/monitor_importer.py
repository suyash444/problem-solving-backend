"""
Monitor CSV Importer - FIXED VERSION WITH DUPLICATE HANDLING
Imports UDC position data from Monitor files

FIX: 
- Raw data: Skip duplicates (same UDC + Article + DateTime)
- UDC Locations: Update existing records (positions can change)
"""
import os
import hashlib
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Set
from loguru import logger
from sqlalchemy import text

from shared.database import get_db_context
from shared.database.models import ImportMonitor, UDCLocation, ImportLog
from config.settings import settings


class MonitorImporter:
    """Handles Monitor file imports with duplicate handling"""
    
    def __init__(self):
        self.source_path = settings.MONITOR_PATH
        
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
                ImportLog.source_type == 'MONITOR',
                ImportLog.file_hash == file_hash
            ).first()
            return exists is not None
    
    def find_files_in_date_range(self, start_date: date, end_date: date) -> List[str]:
        """Find Monitor files within a date range"""
        try:
            logger.info(f"Scanning Monitor folder: {self.source_path}")
            
            all_files = os.listdir(self.source_path)
            logger.info(f"Found {len(all_files)} total files in folder")
            
            files_to_import = []
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                target_filename = f"MonitorBenettonS{date_str}F{date_str}"
                
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
    
    def import_date_range(self, start_date: date, end_date: date) -> Dict:
        """Import Monitor files for a date range with duplicate handling"""
        try:
            logger.info(f"=== Starting Monitor import for {start_date} to {end_date} ===")
            
            filepaths = self.find_files_in_date_range(start_date, end_date)
            
            if not filepaths:
                return {
                    "success": True,
                    "message": "No new files to import in date range",
                    "files_imported": 0,
                    "total_records": 0
                }
            
            total_records = 0
            total_skipped = 0
            total_positions_new = 0
            total_positions_updated = 0
            files_imported = 0
            
            for idx, filepath in enumerate(filepaths, 1):
                logger.info(f"[{idx}/{len(filepaths)}] Importing: {os.path.basename(filepath)}")
                result = self._import_file_skip_duplicates(filepath)
                
                if result['success']:
                    files_imported += 1
                    total_records += result['records_imported']
                    total_skipped += result.get('records_skipped', 0)
                    total_positions_new += result.get('positions_new', 0)
                    total_positions_updated += result.get('positions_updated', 0)
                    logger.info(f"✓ Imported {result['records_imported']} records, skipped {result.get('records_skipped', 0)} duplicates")
                else:
                    logger.error(f"✗ Failed: {result['message']}")
            
            logger.info(f"=== ✓✓✓ SUCCESS! Imported {files_imported} files ===")
            logger.info(f"Total: {total_records} new records, {total_skipped} duplicates skipped")
            logger.info(f"Positions: {total_positions_new} new, {total_positions_updated} updated")
            
            return {
                "success": True,
                "message": f"Imported {files_imported} files with {total_records} new records ({total_skipped} duplicates skipped)",
                "files_imported": files_imported,
                "total_records": total_records,
                "records_skipped": total_skipped,
                "positions_new": total_positions_new,
                "positions_updated": total_positions_updated
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
    
    def _import_file_skip_duplicates(self, filepath: str) -> Dict:
        """Import single Monitor file with duplicate handling"""
        try:
            file_hash = self.get_file_hash(filepath)
            
            df = pd.read_csv(filepath, delimiter='$', encoding='utf-8')
            
            if len(df) == 0:
                return {"success": False, "message": "No records in file", "records_imported": 0}
            
            logger.info(f"Read {len(df)} rows from file")
            
            import_log = ImportLog(
                source_type='MONITOR',
                file_path=filepath,
                file_hash=file_hash,
                file_date=self._extract_date_from_filename(os.path.basename(filepath)),
                records_imported=0,
                import_started_at=datetime.utcnow()
            )
            
            with get_db_context() as db:
                db.add(import_log)
                db.flush()
                
                # Import raw data (skip duplicates)
                raw_result = self._import_raw_data_skip_duplicates(df, filepath, db)
                
                # Update UDC positions (update existing)
                position_result = self._update_positions_upsert(df, db)
                
                import_log.records_imported = raw_result['inserted']
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            return {
                "success": True,
                "message": f"Imported {raw_result['inserted']} new records",
                "records_imported": raw_result['inserted'],
                "records_skipped": raw_result['skipped'],
                "positions_new": position_result['new'],
                "positions_updated": position_result['updated']
            }
            
        except Exception as e:
            logger.error(f"Error importing file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "records_imported": 0
            }
    
    def _import_raw_data_skip_duplicates(self, df: pd.DataFrame, filepath: str, db) -> Dict:
        """
        Import raw data - SKIP DUPLICATES
        Unique key: Pallet + Articolo + DataOra
        """
        
        def safe_val(val, type_='str'):
            if pd.isna(val) or val == '':
                return None
            if type_ == 'int':
                try: return int(float(val))
                except: return None
            elif type_ == 'float':
                try:
                    if isinstance(val, str):
                        val = val.replace(',', '.')
                    return float(val)
                except: return None
            elif type_ == 'datetime':
                try:
                    dt = pd.to_datetime(val, errors='coerce', dayfirst=True)
                    return dt.to_pydatetime() if pd.notna(dt) else None
                except: return None
            else:
                return str(val)
        
        # Get existing records to check for duplicates
        logger.info("  Loading existing records for duplicate check...")
        existing_keys: Set[tuple] = set()
        
        try:
            existing_records = db.execute(text("""
                SELECT DISTINCT 
                    Pallet, 
                    Articolo, 
                    CONVERT(VARCHAR(20), DataOra, 120) as DataOraStr
                FROM import_monitor
                WHERE Pallet IS NOT NULL
            """)).fetchall()
            
            for row in existing_records:
                key = (str(row[0]) if row[0] else '', 
                       str(row[1]) if row[1] else '', 
                       str(row[2]) if row[2] else '')
                existing_keys.add(key)
        except Exception as e:
            logger.warning(f"  Could not load existing keys: {e}")
        
        logger.info(f"  Found {len(existing_keys)} existing unique records")
        
        records_to_insert = []
        skipped_count = 0
        
        for idx, row in df.iterrows():
            if idx % 10000 == 0 and idx > 0:
                logger.info(f"  Processing row {idx}/{len(df)}...")
            
            # Create unique key
            pallet = str(row.get('Pallet', '')) if pd.notna(row.get('Pallet')) else ''
            articolo = str(row.get('Articolo', '')) if pd.notna(row.get('Articolo')) else ''
            data_ora = ''
            if pd.notna(row.get('DataOra')):
                try:
                    dt = pd.to_datetime(row.get('DataOra'), errors='coerce', dayfirst=True)
                    if pd.notna(dt):
                        data_ora = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            key = (pallet, articolo, data_ora)
            
            # Skip if already exists
            if key in existing_keys:
                skipped_count += 1
                continue
            
            # Add to existing keys
            existing_keys.add(key)
            
            records_to_insert.append(ImportMonitor(
                DataOra=safe_val(row.get('DataOra'), 'datetime'),
                Movimento=safe_val(row.get('Movimento')),
                Pallet=safe_val(row.get('Pallet')),
                Articolo=safe_val(row.get('Articolo')),
                Descrizione=safe_val(row.get('Descrizione')),
                Quantita=safe_val(row.get('Quantita'), 'float'),
                LottoEntrata=safe_val(row.get('LottoEntrata')),
                LottoConfezionamento=safe_val(row.get('LottoConfezionamento')),
                Matricola=safe_val(row.get('Matricola')),
                LottoFornitore=safe_val(row.get('LottoFornitore')),
                Made=safe_val(row.get('Made')),
                Mag=safe_val(row.get('Mag')),
                Scaf=safe_val(row.get('Scaf')),
                Col=safe_val(row.get('Col')),
                Pia=safe_val(row.get('Pia')),
                Sc=safe_val(row.get('Sc')),
                Comp=safe_val(row.get('Comp')),
                ListaRif=safe_val(row.get('ListaRif')),
                DescrizioneBrand=safe_val(row.get('BrandDescrizioneBrand')),
                PackingList=safe_val(row.get('PackingList')),
                DataBolla=safe_val(row.get('DataBolla'), 'datetime'),
                Tag=safe_val(row.get('Tag')),
                CodiceProprieta=safe_val(row.get('StatoCodiceProprieta')),
                Causaleprelievo=safe_val(row.get('Causaleprelievo')),
                CodicePallet=safe_val(row.get('CodicePallet')),
                Categoria=safe_val(row.get('Categoria')),
                CodiceCategoria=safe_val(row.get('CodiceCategoria')),
                EuroUDC=safe_val(row.get('EuroUDC')),
                Riga=safe_val(row.get('Riga')),
                QtaCorrente=safe_val(row.get('QtaCorrente'), 'float'),
                DeltaQTA=safe_val(row.get('DeltaQTA'), 'float'),
                source_file=filepath
            ))
        
        # Bulk insert new records
        if records_to_insert:
            batch_size = 1000
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i+batch_size]
                db.bulk_save_objects(batch)
            db.flush()
        
        logger.info(f"  Inserted {len(records_to_insert)} new records, skipped {skipped_count} duplicates")
        return {"inserted": len(records_to_insert), "skipped": skipped_count}
    
    def _update_positions_upsert(self, df: pd.DataFrame, db) -> Dict:
        """
        Update UDC positions - UPSERT (Insert or Update)
        Updates existing positions with latest data
        """
        
        df_sorted = df.copy()
        if 'DataOra' in df_sorted.columns:
            df_sorted['DataOra'] = pd.to_datetime(df_sorted['DataOra'], errors='coerce', dayfirst=True)
        df_sorted = df_sorted.sort_values('DataOra', ascending=False, na_position='last')
        latest_positions = df_sorted.drop_duplicates(subset=['Pallet'], keep='first')
        
        positions_new = 0
        positions_updated = 0
        
        def coerce_dt(value):
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            try:
                dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
                return dt.to_pydatetime() if pd.notna(dt) else None
            except:
                return None
        
        for _, row in latest_positions.iterrows():
            udc = row.get('Pallet')
            if pd.isna(udc):
                continue
            
            udc = str(udc)
            
            position_parts = []
            for col in ['Mag', 'Scaf', 'Col', 'Pia']:
                val = row.get(col)
                if pd.notna(val) and str(val).strip():
                    position_parts.append(str(val))
            
            position_code = '-'.join(position_parts) if position_parts else 'UNKNOWN'
            
            data_ora = row.get('DataOra')
            last_movement = coerce_dt(data_ora)
            
            # Try to find existing location
            location = db.query(UDCLocation).filter(UDCLocation.udc == udc).first()
            
            if location:
                # UPDATE existing - only if new data is more recent
                should_update = True
                loc_last = coerce_dt(location.last_movement)
                if loc_last and last_movement:
                    should_update = last_movement >= loc_last
                
                if should_update:
                    location.mag = str(row['Mag']) if pd.notna(row.get('Mag')) else None
                    location.scaf = str(row['Scaf']) if pd.notna(row.get('Scaf')) else None
                    location.col = str(row['Col']) if pd.notna(row.get('Col')) else None
                    location.pia = str(row['Pia']) if pd.notna(row.get('Pia')) else None
                    location.sc = str(row['Sc']) if pd.notna(row.get('Sc')) else None
                    location.comp = str(row['Comp']) if pd.notna(row.get('Comp')) else None
                    location.position_code = position_code
                    location.last_movement = last_movement
                    location.last_updated = datetime.utcnow()
                    positions_updated += 1
            else:
                # INSERT new
                location = UDCLocation(
                    udc=udc,
                    mag=str(row['Mag']) if pd.notna(row.get('Mag')) else None,
                    scaf=str(row['Scaf']) if pd.notna(row.get('Scaf')) else None,
                    col=str(row['Col']) if pd.notna(row.get('Col')) else None,
                    pia=str(row['Pia']) if pd.notna(row.get('Pia')) else None,
                    sc=str(row['Sc']) if pd.notna(row.get('Sc')) else None,
                    comp=str(row['Comp']) if pd.notna(row.get('Comp')) else None,
                    position_code=position_code,
                    last_movement=last_movement
                )
                db.add(location)
                positions_new += 1
        
        db.flush()
        logger.info(f"  UDC positions: {positions_new} new, {positions_updated} updated")
        return {"new": positions_new, "updated": positions_updated}
    
    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from filename"""
        try:
            filename = filename.replace('.csv', '')
            date_str = filename.split('S')[1].split('F')[0]
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def import_yesterday(self) -> Dict:
        """Find and import yesterday's Monitor file"""
        try:
            yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            target_filename = f"MonitorBenettonS{yesterday}F{yesterday}"
            filepath = os.path.join(self.source_path, target_filename)
            
            if not os.path.exists(filepath):
                filepath = f"{filepath}.csv"
            
            if os.path.exists(filepath):
                logger.info(f"Found yesterday's Monitor file: {os.path.basename(filepath)}")
                return self._import_file_skip_duplicates(filepath)
            else:
                logger.warning(f"Yesterday's Monitor file not found")
                return {
                    "success": False,
                    "message": "Yesterday's Monitor file not found (may be weekend/holiday)",
                    "records_imported": 0
                }
        except Exception as e:
            logger.error(f"Error importing yesterday's file: {e}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "records_imported": 0
            }
