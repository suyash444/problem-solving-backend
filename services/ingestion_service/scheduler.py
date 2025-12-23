"""
Scheduler for automatic daily imports - IMPROVED VERSION
Runs at configured time daily to import data

IMPROVEMENTS:
1. Imports DumpTrack latest file (contains historical data)
2. Imports Prelievo for a wider date range to catch all picking events
3. Imports Monitor for yesterday
4. All importers now handle duplicates, so safe to re-import overlapping data
"""
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from loguru import logger
from pytz import timezone

from config.settings import settings
from .dumptrack_importer import DumptrackImporter
from .monitor_importer import MonitorImporter
from .api_client import PowerStoreAPIClient
from .rebuild_udc_inventory import rebuild_udc_inventory


class ImportScheduler:
    """Handles scheduled automatic imports"""

    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=timezone("Europe/Rome"))
        self.dumptrack_importer = DumptrackImporter()
        self.monitor_importer = MonitorImporter()
        self.api_client = PowerStoreAPIClient()

    def daily_import_job(self):
        """
        Job that runs daily

        For EACH company:
        1. DumpTrack (orders) - latest file
        2. Prelievo (picking events) - last 7 days
        3. Rebuild UDC inventory (PER COMPANY)
        4. Monitor (UDC positions) - yesterday only
        """
        logger.info("=" * 60)
        logger.info("STARTING SCHEDULED DAILY IMPORT JOB")
        logger.info("=" * 60)

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        companies = list(settings.COMPANIES.keys())

        results = {
            "companies": {},
            "success": True
        }

        try:
            for company_key in companies:
                logger.info("")
                logger.info("=" * 60)
                logger.info(f"COMPANY: {company_key}")
                logger.info("=" * 60)

                company_results = {
                    "dumptrack": None,
                    "prelievo": None,
                    "monitor": None,
                    "udc_inventory_rebuilt": False,
                    "udc_inventory_records": 0,
                    "success": True
                }

                # ============================================
                # STEP 1: DumpTrack (latest)
                # ============================================
                logger.info("")
                logger.info("=" * 40)
                logger.info("STEP 1/3: Importing DumpTrack")
                logger.info("=" * 40)

                dumptrack_result = self.dumptrack_importer.import_latest(company=company_key)
                company_results["dumptrack"] = dumptrack_result

                if dumptrack_result.get("success"):
                    logger.info(f"✓ DumpTrack: {dumptrack_result.get('records_imported', 0)} new records")
                    logger.info(f"  Skipped: {dumptrack_result.get('records_skipped', 0)} duplicates")
                else:
                    logger.error(f"✗ DumpTrack failed: {dumptrack_result.get('message')}")
                    company_results["success"] = False

                # ============================================
                # STEP 2: Prelievo (last 7 days) + rebuild UDC inventory
                # ============================================
                logger.info("")
                logger.info("=" * 40)
                logger.info("STEP 2/3: Importing PrelievoPowerSort (last 7 days)")
                logger.info("=" * 40)

                prelievo_start = today - timedelta(days=7)
                prelievo_end = yesterday

                logger.info(f"  Date range: {prelievo_start} to {prelievo_end}")

                prelievo_result = self.api_client.call_prelievo_powersort(
                    start_date=prelievo_start,
                    end_date=prelievo_end,
                    company=company_key
                )
                company_results["prelievo"] = prelievo_result

                if prelievo_result.get("success"):
                    logger.info(f"✓ Prelievo: {prelievo_result.get('records_imported', 0)} new records")
                    logger.info(f"  Skipped: {prelievo_result.get('records_skipped', 0)} duplicates")
                    logger.info(f"  Picking events: {prelievo_result.get('picking_events_created', 0)} new")

                    # IMPORTANT: rebuild inventory PER COMPANY
                    rebuild_result = rebuild_udc_inventory(company=company_key)
                    company_results["udc_inventory_rebuilt"] = rebuild_result.get("success", False)
                    company_results["udc_inventory_records"] = rebuild_result.get("records_created", 0)

                    if rebuild_result.get("success"):
                        logger.info(f"✓ UDC inventory rebuilt: {rebuild_result.get('records_created', 0)} records")
                    else:
                        logger.error(f"✗ UDC inventory rebuild failed: {rebuild_result.get('error')}")
                        company_results["success"] = False
                else:
                    logger.error(f"✗ Prelievo failed: {prelievo_result.get('message')}")
                    company_results["success"] = False

                # ============================================
                # STEP 3: Monitor (yesterday)
                # ============================================
                logger.info("")
                logger.info("=" * 40)
                logger.info("STEP 3/3: Importing Monitor (yesterday)")
                logger.info("=" * 40)

                monitor_result = self.monitor_importer.import_yesterday(company=company_key)
                company_results["monitor"] = monitor_result

                if monitor_result.get("success"):
                    logger.info(f"✓ Monitor: {monitor_result.get('records_imported', 0)} new records")
                    logger.info(f"  Skipped: {monitor_result.get('records_skipped', 0)} duplicates")
                    logger.info(f"  Positions: {monitor_result.get('positions_new', 0)} new, {monitor_result.get('positions_updated', 0)} updated")
                else:
                    logger.warning(f"⚠ Monitor: {monitor_result.get('message')}")
                    # Monitor failure is not critical (weekend/holiday)

                results["companies"][company_key] = company_results

                if not company_results["success"]:
                    results["success"] = False

            logger.info("")
            logger.info("=" * 60)
            if results["success"]:
                logger.info("✓✓✓ DAILY IMPORT JOB COMPLETED SUCCESSFULLY")
            else:
                logger.warning("⚠⚠⚠ DAILY IMPORT JOB COMPLETED WITH ERRORS")
            logger.info("=" * 60)

            return results

        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in daily import job: {e}")
            import traceback
            logger.error(traceback.format_exc())
            results["success"] = False
            results["error"] = str(e)
            return results

    def start(self):
        """Start the scheduler"""
        self.scheduler.add_job(
            self.daily_import_job,
            'cron',
            hour=settings.IMPORT_SCHEDULE_HOUR,
            minute=settings.IMPORT_SCHEDULE_MINUTE,
            id='daily_import',
            replace_existing=True
        )

        self.scheduler.start()
        logger.info(f"Scheduler started - Daily imports at {settings.IMPORT_SCHEDULE_HOUR}:{settings.IMPORT_SCHEDULE_MINUTE:02d}")

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")

    def run_now(self):
        """Manually trigger the daily import job"""
        logger.info("Manually triggering daily import job via API")
        return self.daily_import_job()


def run_daily_import():
    """Standalone function to run daily import (Task Scheduler compatible)"""
    logger.info("Running daily import via standalone function")

    scheduler = ImportScheduler()
    result = scheduler.daily_import_job()

    if result.get("success"):
        logger.info("Daily import completed successfully")
        return 0
    else:
        logger.error("Daily import failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(run_daily_import())
