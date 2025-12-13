"""
Scheduler for automatic daily imports
Runs at 3:30 AM daily to import yesterday's data
"""
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from loguru import logger

from config.settings import settings
from .dumptrack_importer import DumptrackImporter
from .monitor_importer import MonitorImporter
from .api_client import PowerStoreAPIClient


class ImportScheduler:
    """Handles scheduled automatic imports"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.dumptrack_importer = DumptrackImporter()
        self.monitor_importer = MonitorImporter()
        self.api_client = PowerStoreAPIClient()
        
    def daily_import_job(self):
        """Job that runs daily at 3:30 AM"""
        logger.info("Starting scheduled daily import job")
        
        try:
            # 1. Import latest DumpTrack file
            logger.info("Step 1: Importing DumpTrack")
            dumptrack_result = self.dumptrack_importer.import_latest()
            logger.info(f"DumpTrack import result: {dumptrack_result}")
            
            # 2. Import PrelievoPowerSort for yesterday
            logger.info("Step 2: Importing PrelievoPowerSort")
            yesterday = (datetime.now() - timedelta(days=1)).date()
            prelievo_result = self.api_client.call_prelievo_powersort(
                start_date=yesterday,
                end_date=yesterday
            )
            logger.info(f"PrelievoPowerSort import result: {prelievo_result}")
            
            # 3. Import yesterday's Monitor file
            logger.info("Step 3: Importing Monitor")
            monitor_result = self.monitor_importer.import_yesterday()
            logger.info(f"Monitor import result: {monitor_result}")
            
            logger.info("Daily import job completed successfully")
            
        except Exception as e:
            logger.error(f"Error in daily import job: {e}")
    
    def start(self):
        """Start the scheduler"""
        # Schedule daily import at configured time (default 3:30 AM)
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
        """Manually trigger the daily import job (for testing)"""
        logger.info("Manually triggering daily import job")
        self.daily_import_job()