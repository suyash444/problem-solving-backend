"""
Monitor CSV Importer - DEBUG VERSION
Imports UDC position data from Monitor files
"""
import os
import hashlib
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict
from loguru import logger

from shared.database import get_db_context
from shared.database.models import ImportMonitor, UDCLocation, ImportLog
from config.settings import settings


class MonitorImporter:
    """Handles Monitor file imports - DEBUG VERSION"""
    
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
        """Find Monitor files within a date range - WITH DEBUG"""
        try:
            logger.info(f"Scanning Monitor folder: {self.source_path}")
            
            # List ALL files in directory
            all_files = os.listdir(self.source_path)
            logger.info(f"Found {len(all_files)} total files in folder")
            
            # SHOW FIRST 10 FILES FOR DEBUGGING
            logger.info(f"=== FIRST 10 FILES IN DIRECTORY ===")
            for f in all_files[:10]:
                logger.info(f"  File: '{f}'")
            
            # SHOW FILES THAT CONTAIN "2025-11"
            nov_files = [f for f in all_files if '2025-11' in f and 'Monitor' in f]
            logger.info(f"=== FILES CONTAINING '2025-11' (showing first 10) ===")
            logger.info(f"Total matching: {len(nov_files)}")
            for f in nov_files[:10]:
                logger.info(f"  File: '{f}'")
            
            # SHOW FILES THAT CONTAIN "2025-12"
            dec_files = [f for f in all_files if '2025-12' in f and 'Monitor' in f]
            logger.info(f"=== FILES CONTAINING '2025-12' (showing first 10) ===")
            logger.info(f"Total matching: {len(dec_files)}")
            for f in dec_files[:10]:
                logger.info(f"  File: '{f}'")
            
            files_to_import = []
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                
                # Try WITHOUT extension
                target_filename = f"MonitorBenettonS{date_str}F{date_str}"
                
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
    
    def import_date_range(self, start_date: date, end_date: date) -> Dict:
        """
        Import Monitor files for a date range
        """
        try:
            logger.info(f"=== Starting Monitor import for {start_date} to {end_date} ===")
            
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
            total_positions = 0
            files_imported = 0
            
            for idx, filepath in enumerate(filepaths, 1):
                logger.info(f"[{idx}/{len(filepaths)}] Importing: {os.path.basename(filepath)}")
                result = self._import_file_fast(filepath)
                
                if result['success']:
                    files_imported += 1
                    total_records += result['records_imported']
                    total_positions += result.get('positions_updated', 0)
                    logger.info(f"✓ Imported {result['records_imported']} records, {result.get('positions_updated', 0)} positions")
                else:
                    logger.error(f"✗ Failed: {result['message']}")
            
            logger.info(f"=== ✓✓✓ SUCCESS! Imported {files_imported} files ===")
            logger.info(f"Total records: {total_records}, Total positions: {total_positions}")
            
            return {
                "success": True,
                "message": f"Imported {files_imported} files with {total_records} records",
                "files_imported": files_imported,
                "total_records": total_records,
                "total_positions": total_positions
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
    
    def _import_file_fast(self, filepath: str) -> Dict:
        """Import single Monitor file"""
        try:
            # Get file hash
            file_hash = self.get_file_hash(filepath)
            
            # Read file with $ delimiter
            df = pd.read_csv(filepath, delimiter='$', encoding='utf-8')
            
            if len(df) == 0:
                return {"success": False, "message": "No records in file", "records_imported": 0}
            
            logger.info(f"Read {len(df)} rows from file")
            
            # Start import log
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
                
                # Import raw data
                raw_records = self._import_raw_data_fast(df, filepath, db)
                
                # Update UDC positions
                positions_updated = self._update_positions_fast(df, db)
                
                # Update import log
                import_log.records_imported = raw_records
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = 'SUCCESS'
                
                db.commit()
            
            return {
                "success": True,
                "message": f"Imported {raw_records} records",
                "records_imported": raw_records,
                "positions_updated": positions_updated
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
    
    def _import_raw_data_fast(self, df: pd.DataFrame, filepath: str, db) -> int:
        """Import raw data - FAST bulk insert"""
        
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
        
        records = []
        for _, row in df.iterrows():
            records.append(ImportMonitor(
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
        
        db.bulk_save_objects(records)
        db.flush()
        logger.info(f"Bulk inserted {len(records)} raw records")
        return len(records)
    
    def _update_positions_fast(self, df: pd.DataFrame, db) -> int:
        """Update UDC positions - FAST version"""
        
        df_sorted = df.sort_values('DataOra', ascending=False)
        latest_positions = df_sorted.drop_duplicates(subset=['Pallet'], keep='first')
        
        positions_updated = 0
        
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
            if pd.notna(data_ora):
                try:
                    last_movement = pd.to_datetime(data_ora, errors='coerce', dayfirst=True).to_pydatetime()
                except:
                    last_movement = None
            else:
                last_movement = None
            
            location = db.query(UDCLocation).filter(UDCLocation.udc == udc).first()
            
            if location:
                location.mag = str(row['Mag']) if pd.notna(row.get('Mag')) else None
                location.scaf = str(row['Scaf']) if pd.notna(row.get('Scaf')) else None
                location.col = str(row['Col']) if pd.notna(row.get('Col')) else None
                location.pia = str(row['Pia']) if pd.notna(row.get('Pia')) else None
                location.sc = str(row['Sc']) if pd.notna(row.get('Sc')) else None
                location.comp = str(row['Comp']) if pd.notna(row.get('Comp')) else None
                location.position_code = position_code
                location.last_movement = last_movement
                location.last_updated = datetime.utcnow()
            else:
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
            
            positions_updated += 1
        
        db.flush()
        logger.info(f"Updated {positions_updated} UDC positions")
        return positions_updated
    
    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from filename"""
        try:
            # Remove .csv if present
            filename = filename.replace('.csv', '')
            date_str = filename.split('S')[1].split('F')[0]
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def import_yesterday(self) -> Dict:
        """Find and import yesterday's Monitor file"""
        try:
            yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Try without extension first
            target_filename = f"MonitorBenettonS{yesterday}F{yesterday}"
            filepath = os.path.join(self.source_path, target_filename)
            
            # If not found, try with .csv
            if not os.path.exists(filepath):
                filepath = f"{filepath}.csv"
            
            if os.path.exists(filepath):
                logger.info(f"Found yesterday's Monitor file: {os.path.basename(filepath)}")
                return self._import_file_fast(filepath)
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