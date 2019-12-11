import logging

from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from saas import celery_app

__all__ = [
    "update_geo_targets"
]
logger = logging.getLogger(__name__)


@celery_app.task()
def update_geo_targets():
    logger.debug("Updating geo targets")
    GoogleAdsUpdater.update_geo_targets()
    logger.debug("Geo targets update complete")
