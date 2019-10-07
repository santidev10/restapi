import logging

from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from saas import celery_app

__all__ = [
    "update_audiences"
]
logger = logging.getLogger(__name__)


@celery_app.task()
def update_audiences():
    logger.error("Updating audiences")
    GoogleAdsUpdater.update_audiences()
    logger.error("Audience update complete")
