"""
DumpTrack CSV Importer - FIXED VERSION 
Imports order data from DumpTrack CSV files with date range support

FIX:
- SQL Server ODBC 
  NVARCHAR(MAX)/Text columns with None. import_log.error_message is NVARCHAR(MAX).
  => Always set error_message to "" (never None) before flush/commit.

MULTI-COMPANY:
- Writes `company` into ImportDumptrack / Order / OrderItem / ImportLog
- Duplicate checks filtered by company
"""
import os
import hashlib
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Set
from decimal import Decimal
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db_context
from shared.database.models import ImportDumptrack, Order, OrderItem, ImportLog
from config.settings import settings


class DumptrackImporter:
    """Handles DumpTrack CSV file imports with date range support and duplicate handling"""

    def __init__(self):
        self.source_path = settings.DUMPTRACK_PATH

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------
    def get_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file to detect duplicates"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _extract_date_from_filename(self, filename: str, company_key: str) -> Optional[date]:
        """Extract date from filename based on company prefix"""
        try:
            cfg = settings.get_company_config(company_key)
            prefix = cfg["dumptrack_prefix"]

            name = filename.replace(".csv", "")
            date_str = name.replace(prefix, "")
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return None

    def _is_already_imported(self, db: Session, file_hash: str, company_key: str) -> bool:
        """Check if file was already imported (PER COMPANY)"""
        exists_row = db.query(ImportLog).filter(
            ImportLog.company == company_key,
            ImportLog.source_type == "DUMPTRACK",
            ImportLog.file_hash == file_hash
        ).first()
        return exists_row is not None

    # ---------------------------------------------------------
    # File discovery
    # ---------------------------------------------------------
    def find_files_in_date_range(self, start_date: date, end_date: date, company: Optional[str] = None) -> List[str]:
        """Find DumpTrack files within a date range"""
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()
        cfg = settings.get_company_config(company_key)
        prefix = cfg["dumptrack_prefix"]

        try:
            logger.info(f"Scanning DumpTrack folder: {self.source_path}")
            all_files = os.listdir(self.source_path)
            logger.info(f"Found {len(all_files)} total files in folder")

            files_to_import: List[str] = []
            current_date = start_date

            with get_db_context() as db:
                while current_date <= end_date:
                    date_str = current_date.strftime("%Y-%m-%d")
                    base = f"{prefix}{date_str}"

                    # Try without extension and with .csv
                    candidates = [base, f"{base}.csv"]

                    found_path = None
                    found_name = None
                    for c in candidates:
                        if c in all_files:
                            found_name = c
                            found_path = os.path.join(self.source_path, c)
                            break

                    if found_path:
                        file_hash = self.get_file_hash(found_path)
                        if not self._is_already_imported(db, file_hash, company_key):
                            files_to_import.append(found_path)
                            logger.info(f"✓ Found file to import: {found_name}")
                        else:
                            logger.info(f"Already imported: {found_name}")
                    else:
                        logger.debug(f"File not found (tried): {candidates}")

                    current_date += timedelta(days=1)

            logger.info(f"=== TOTAL FILES TO IMPORT: {len(files_to_import)} ===")
            return files_to_import

        except Exception as e:
            logger.error(f"Error finding files in date range: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def find_latest_file(self, company: Optional[str] = None) -> Optional[str]:
        """Find the latest DumpTrack file in the directory"""
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()
        cfg = settings.get_company_config(company_key)
        prefix = cfg["dumptrack_prefix"]

        try:
            all_files = os.listdir(self.source_path)
            dumptrack_files = [f for f in all_files if f.startswith(prefix)]
            if not dumptrack_files:
                logger.warning("No DumpTrack files found")
                return None

            dumptrack_files.sort(reverse=True)
            latest = dumptrack_files[0]
            logger.info(f"Latest DumpTrack file: {latest}")
            return os.path.join(self.source_path, latest)
        except Exception as e:
            logger.error(f"Error finding latest file: {e}")
            return None

    # ---------------------------------------------------------
    # Import APIs
    # ---------------------------------------------------------
    def import_date_range(self, start_date: date, end_date: date, company: Optional[str] = None) -> Dict:
        """Import DumpTrack files for a date range with duplicate handling"""
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        try:
            logger.info(f"=== Starting DumpTrack import [{company_key}] for {start_date} to {end_date} ===")

            filepaths = self.find_files_in_date_range(start_date, end_date, company_key)
            if not filepaths:
                return {
                    "success": True,
                    "message": "No new files to import in date range",
                    "files_imported": 0,
                    "total_records": 0
                }

            totals = {
                "records": 0,
                "orders": 0,
                "items": 0,
                "skipped": 0,
                "files": 0
            }

            for idx, fp in enumerate(filepaths, 1):
                logger.info(f"[{idx}/{len(filepaths)}] Importing: {os.path.basename(fp)}")
                res = self.import_file(fp, company=company_key)

                if res.get("success"):
                    totals["files"] += 1
                    totals["records"] += res.get("records_imported", 0)
                    totals["orders"] += res.get("orders_processed", 0)
                    totals["items"] += res.get("items_processed", 0)
                    totals["skipped"] += res.get("records_skipped", 0)
                else:
                    logger.error(f"✗ Failed: {res.get('message')}")

            return {
                "success": True,
                "message": f"Imported {totals['files']} files with {totals['records']} new records ({totals['skipped']} duplicates skipped)",
                "files_imported": totals["files"],
                "total_records": totals["records"],
                "records_skipped": totals["skipped"],
                "orders_processed": totals["orders"],
                "items_processed": totals["items"]
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

    def import_latest(self, from_date: Optional[date] = None, company: Optional[str] = None) -> Dict:
        """Find and import latest file"""
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()
        fp = self.find_latest_file(company_key)
        if not fp:
            return {"success": False, "message": "No file found", "records_imported": 0}
        return self.import_file(fp, from_date=from_date, company=company_key)

    def import_file(self, filepath: str, from_date: Optional[date] = None, company: Optional[str] = None) -> Dict:
        """Import single DumpTrack CSV file with duplicate handling"""
        company_key = (company or settings.DEFAULT_COMPANY).strip().lower()

        try:
            file_hash = self.get_file_hash(filepath)

            with get_db_context() as db:
                if self._is_already_imported(db, file_hash, company_key):
                    logger.info(f"File already imported [{company_key}]: {filepath}")
                    return {
                        "success": True,
                        "message": "File already imported (duplicate)",
                        "records_imported": 0,
                        "records_skipped": 0
                    }

                logger.info(f"Reading DumpTrack file [{company_key}]: {filepath}")
                df = pd.read_csv(filepath, delimiter="$", encoding="utf-8")
                logger.info(f"Total rows in file: {len(df)}")

                if from_date:
                    df["DataRegistrazione"] = pd.to_datetime(df["DataRegistrazione"], errors="coerce")
                    df = df[df["DataRegistrazione"] >= pd.Timestamp(from_date)]
                    logger.info(f"Filtered to {len(df)} records from {from_date}")

                if len(df) == 0:
                    return {"success": False, "message": "No records to import", "records_imported": 0, "records_skipped": 0}

                import_log = ImportLog(
                    company=company_key,
                    source_type="DUMPTRACK",
                    file_path=filepath,
                    file_hash=file_hash,
                    file_date=self._extract_date_from_filename(os.path.basename(filepath), company_key),
                    records_imported=0,
                    import_started_at=datetime.utcnow(),
                    import_completed_at=None,
                    status="RUNNING",
                    
                )

                db.add(import_log)
                db.flush()

                raw_result = self._import_raw_data_skip_duplicates(df, filepath, db, company_key)
                processed = self._process_orders_skip_duplicates(df, db, company_key)

                import_log.records_imported = int(raw_result["inserted"])
                import_log.import_completed_at = datetime.utcnow()
                import_log.status = "SUCCESS"

                db.commit()

                return {
                    "success": True,
                    "message": f"Successfully imported {raw_result['inserted']} new records ({raw_result['skipped']} duplicates skipped)",
                    "records_imported": raw_result["inserted"],
                    "records_skipped": raw_result["skipped"],
                    "orders_processed": processed["orders_new"],
                    "items_processed": processed["items_new"]
                }

        except Exception as e:
            logger.error(f"✗ Import failed [{company_key}]: {e}")
            import traceback
            logger.error(traceback.format_exc())

            try:
                with get_db_context() as db2:
                    row = db2.query(ImportLog).filter(
                        ImportLog.company == company_key,
                        ImportLog.source_type == "DUMPTRACK",
                        ImportLog.file_hash == file_hash
                    ).order_by(ImportLog.id.desc()).first()
                    if row:
                        row.status = "FAILED"
                        row.import_completed_at = datetime.utcnow()
                        row.error_message = (str(e) or "")[:4000]
                        db2.commit()
            except Exception:
                pass

            return {"success": False, "message": f"Import failed: {str(e)}", "records_imported": 0, "records_skipped": 0}

    # ---------------------------------------------------------
    # Raw import + duplicate skipping
    # ---------------------------------------------------------
    def _import_raw_data_skip_duplicates(self, df: pd.DataFrame, filepath: str, db: Session, company_key: str) -> Dict:
        """
        Import raw data - SKIP DUPLICATES (PER COMPANY)
        Unique key: company + OrdinePrivalia + nLista + CodiceArticolo + DataRegistrazione (date)
        """

        def safe_val(val, type_="str"):
            if pd.isna(val) or val == "":
                return None
            if type_ == "int":
                try:
                    return int(float(val))
                except Exception:
                    return None
            if type_ == "float":
                try:
                    return float(val)
                except Exception:
                    return None
            if type_ == "datetime":
                try:
                    dt = pd.to_datetime(val, errors="coerce")
                    return dt.to_pydatetime() if pd.notna(dt) else None
                except Exception:
                    return None
            return str(val)

        existing_keys: Set[tuple] = set()

        existing_records = db.execute(text("""
            SELECT DISTINCT
                OrdinePrivalia,
                nLista,
                CodiceArticolo,
                CONVERT(VARCHAR(10), DataRegistrazione, 120) as DataReg
            FROM import_dumptrack
            WHERE company = :company AND OrdinePrivalia IS NOT NULL
        """), {"company": company_key}).fetchall()

        for row in existing_records:
            existing_keys.add((
                str(row[0] or ""),
                str(row[1] or ""),
                str(row[2] or ""),
                str(row[3] or "")
            ))

        records_to_insert = []
        skipped = 0

        for idx, row in df.iterrows():
            ordine = str(row.get("OrdinePrivalia", "")) if pd.notna(row.get("OrdinePrivalia")) else ""
            n_lista = str(int(row.get("nLista"))) if pd.notna(row.get("nLista")) else ""
            codice = str(row.get("CodiceArticolo", "")) if pd.notna(row.get("CodiceArticolo")) else ""

            data_reg = ""
            if pd.notna(row.get("DataRegistrazione")):
                dt = pd.to_datetime(row.get("DataRegistrazione"), errors="coerce")
                if pd.notna(dt):
                    data_reg = dt.strftime("%Y-%m-%d")

            key = (ordine, n_lista, codice, data_reg)
            if key in existing_keys:
                skipped += 1
                continue
            existing_keys.add(key)

            records_to_insert.append(ImportDumptrack(
                company=company_key,
                Batch=safe_val(row.get("Batch"), "int"),
                OrdinePrivalia=safe_val(row.get("OrdinePrivalia")),
                DataRegistrazione=safe_val(row.get("DataRegistrazione"), "datetime"),
                nLista=safe_val(row.get("nLista"), "int"),
                CodiceArticolo=safe_val(row.get("CodiceArticolo")),
                QtaRichiestaTotale=safe_val(row.get("QtaRichiestaTotale"), "float"),
                QtaPrelevata=safe_val(row.get("QtaPrelevata"), "float"),
                nListaComposta=safe_val(row.get("nListaComposta"), "int"),
                Commessa=safe_val(row.get("Commessa")),
                Utente=safe_val(row.get("Utente")),
                DataPrelievo=safe_val(row.get("DataPrelievo"), "datetime"),
                UDC=safe_val(row.get("UDC")),
                NCollo=safe_val(row.get("NCollo"), "int"),
                CodiceImballo=safe_val(row.get("CodiceImballo")),
                DataOraArrivoPrivalia=safe_val(row.get("DataOraArrivoPrivalia"), "datetime"),
                LetteraVettura=safe_val(row.get("LetteraVettura")),
                Vettore=safe_val(row.get("Vettore")),
                DataStampa=safe_val(row.get("DataStampa"), "datetime"),
                CodiceProprieta=safe_val(row.get("CodiceProprieta")),
                StatoArticolo=safe_val(row.get("StatoArticolo")),
                Uds=safe_val(row.get("Uds")),
                source_file=filepath
            ))

        if records_to_insert:
            batch_size = 1000
            for i in range(0, len(records_to_insert), batch_size):
                db.bulk_save_objects(records_to_insert[i:i + batch_size])
            db.flush()

        return {"inserted": len(records_to_insert), "skipped": skipped}

    # ---------------------------------------------------------
    # Orders/items processing + duplicate skipping
    # ---------------------------------------------------------
    def _process_orders_skip_duplicates(self, df: pd.DataFrame, db: Session, company_key: str) -> Dict:
        """Process orders - SKIP DUPLICATES (PER COMPANY)"""

        df = df[df["OrdinePrivalia"].notna()]
        df = df[df["nLista"].notna()]
        df = df[df["CodiceArticolo"].notna()]

        existing_orders: Set[str] = set(
            r[0] for r in db.execute(
                text("SELECT order_number FROM orders WHERE company = :company"),
                {"company": company_key}
            ).fetchall()
        )

        existing_items: Set[tuple] = set()
        rows = db.execute(text("""
            SELECT o.order_number, oi.n_lista, oi.sku
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            WHERE o.company = :company AND oi.company = :company
        """), {"company": company_key}).fetchall()
        for r in rows:
            existing_items.add((str(r[0]), str(r[1]), str(r[2])))

        orders_data = df.groupby("OrdinePrivalia").first().reset_index()

        order_map: Dict[str, int] = {}
        orders_new = 0
        orders_skipped = 0

        for _, row in orders_data.iterrows():
            order_num = str(row["OrdinePrivalia"])

            if order_num in existing_orders:
                existing = db.execute(
                    text("SELECT id FROM orders WHERE company = :company AND order_number = :num"),
                    {"company": company_key, "num": order_num}
                ).fetchone()
                if existing:
                    order_map[order_num] = int(existing[0])
                orders_skipped += 1
                continue

            order = Order(
                company=company_key,
                order_number=order_num,
                data_registrazione=pd.to_datetime(row.get("DataRegistrazione"), errors="coerce").to_pydatetime()
                if pd.notna(row.get("DataRegistrazione")) else None,
                commessa=str(row.get("Commessa")) if pd.notna(row.get("Commessa")) else None,
                codice_proprieta=str(row.get("CodiceProprieta")) if pd.notna(row.get("CodiceProprieta")) else None,
            )
            db.add(order)
            db.flush()
            order_map[order_num] = order.id
            existing_orders.add(order_num)
            orders_new += 1

        items_data = df.groupby(["OrdinePrivalia", "nLista", "CodiceArticolo"]).agg({
            "QtaRichiestaTotale": "first",
            "nListaComposta": "first",
            "CodiceImballo": "first"
        }).reset_index()

        items_new = 0
        items_skipped = 0

        for _, row in items_data.iterrows():
            order_num = str(row["OrdinePrivalia"])
            n_lista = str(int(row["nLista"]))
            sku = str(row["CodiceArticolo"])

            key = (order_num, n_lista, sku)
            if key in existing_items:
                items_skipped += 1
                continue

            if order_num not in order_map:
                continue

            item = OrderItem(
                company=company_key,
                order_id=order_map[order_num],
                n_lista=int(row["nLista"]),
                listone=int(row["nListaComposta"]) if pd.notna(row.get("nListaComposta")) else None,
                sku=sku,
                qty_ordered=Decimal(str(row["QtaRichiestaTotale"])) if pd.notna(row.get("QtaRichiestaTotale")) else Decimal("0"),
                cesta=str(row["CodiceImballo"]) if pd.notna(row.get("CodiceImballo")) else None
            )
            db.add(item)
            existing_items.add(key)
            items_new += 1

            if items_new % 1000 == 0:
                db.flush()

        db.flush()
        return {
            "orders_new": orders_new,
            "orders_skipped": orders_skipped,
            "items_new": items_new,
            "items_skipped": items_skipped
        }
