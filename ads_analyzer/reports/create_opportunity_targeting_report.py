import logging

from saas import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def create_opportunity_targeting_report(opportunity_id: str, date_from_str: str, date_to_str: str):
    pass
